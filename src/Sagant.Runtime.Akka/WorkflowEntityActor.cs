using System.Collections.Immutable;
using Akka.Persistence.Journal;
using Sagant.Protocol;
using Sagant.Descriptors;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Idempotency;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Execution;
using Sagant.Runtime.Akka.Execution;
using System.Diagnostics;
using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.Sharding;
using Akka.Delivery;
using Akka.Event;
using Akka.Persistence;

namespace Sagant.Runtime.Akka;

/// <summary>
/// The persistent actor driving one workflow instance's step machine: dispatches external
/// commands, executes steps via fire-and-<c>PipeTo</c> (never inline <c>await</c> — see the design
/// doc), retries/fails over per <see cref="RecoverStrategy"/>, persists each transition as the
/// <see cref="WorkflowEvent"/>s it changed — one atomic batch per transition, folded into the live
/// envelope by <see cref="WorkflowEventFold"/> and folded again from scratch on recovery. A snapshot
/// shortens that replay, taken once a transition makes the workflow terminal or every
/// <c>snapshotEveryNEvents</c> persisted events otherwise (see
/// <see cref="Execution.SnapshotPolicy"/>). Enforces step/workflow/pause timeouts
/// by persisting an absolute deadline and re-arming a live timer identically on a fresh start and on
/// recovery (no durable-timer infrastructure needed for v1 — see
/// <see cref="IWorkflowTimeoutScheduler"/>). Command dispatch, step lifecycle, and persistence live on
/// this class directly; distributed tracing (<see cref="Execution.StepTracingContext"/>), timeout
/// handles (<see cref="Execution.TimeoutHandles"/>), and child-workflow orchestration mechanics
/// (<see cref="Execution.ChildOrchestrator{TState}"/>) are each a separate collaborator.
/// </summary>
public sealed class WorkflowEntityActor<TWorkflow, TState> : ReceivePersistentActor
    where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>
{
    // Concurrency model this actor maintains, and which every persist site below must preserve:
    //
    //   At most one step in flight. N queries in flight. Commands never in flight — they complete
    //   atomically on the actor thread.
    //
    // That is what makes UserState safe to replace wholesale on every transition: two overlapping
    // writers would race over the entirety of TState, so there are none. Commands are synchronous
    // and are additionally deferred while a step runs; queries return a QueryEffect, which carries a
    // reply and no persistence, so they are free to run alongside a step; control commands
    // (Suspend/Terminate/Delete) leave UserState untouched and bump _stepEpoch; a child lifecycle
    // notification passes UserState through unchanged.
    // ReceivePersistentActor already implements IWithUnboundedStash (via Eventsourced), and Akka
    // wires its Stash property automatically — that's what HandleDelivery and the decision interpreter
    // use below to defer business commands while a step is in flight. The property stays inherited
    // here, left undeclared: the base's own setter is what pairs the injected stash with
    // Eventsourced's private _internalStash, and redeclaring the property would shadow that setter,
    // leaving the pairing unwired.

    private readonly string _persistenceId;
    private readonly string _entityId;
    private readonly TWorkflow _workflow;

    /// <summary>Tags carried by every event this instance writes, so a reader can select a stream of
    /// them without scanning every persistence id the journal holds. Fixed per instance.</summary>
    private readonly IImmutableSet<string> _eventTags;

    /// <summary><see cref="_eventTags"/> plus this instance's deadline-shard tag, carried by the
    /// events that move a deadline. Both sets are built once, so choosing between them per event
    /// costs a type test. See <see cref="WorkflowEventTags.MovesADeadline"/>.</summary>
    private readonly IImmutableSet<string> _deadlineEventTags;

    /// <summary>Set while a restart's snapshot is in flight, so the snapshot that lands releases the
    /// history before it (see <see cref="WorkflowDecision.ReclaimHistory"/>).</summary>
    private bool _reclaimingHistory;
    private readonly IActorRef _consumerController;
    private readonly IWorkflowTimeoutScheduler _timeoutScheduler;
    private readonly TimeProvider _timeProvider;
    private readonly WorkflowHandleRegistry _workflowHandleRegistry = WorkflowHandleRegistryProvider.Instance.Apply(Context.System);
    private readonly SnapshotPolicy _snapshotPolicy;
    private readonly ResolvedWorkflowSettings _settings;
    private readonly TimeoutHandles _timeouts = new();
    private readonly StepTracingContext _tracing = new();
    private readonly ChildOrchestrator<TState> _children;
    private WorkflowRuntimeState<TState> _envelope;
    private int _stepEpoch;
    private PendingPurge? _pendingPurge;
    private readonly List<IActorRef> _pendingCompletionWatchers = new();
    private bool _stepInFlight;
    private CancellationTokenSource? _currentStepCts;
    private readonly Dictionary<long, InFlightQuery> _inFlightQueries = new();
    private long _querySequence;
    private readonly TimeSpan _gracefulShutdownGrace;
    private bool _shuttingDown;

    /// <summary>How often this entity announces itself to its own shard while it holds work, or
    /// <c>null</c> where the deployment leaves idle passivation off and nothing has to be announced.
    /// See <see cref="EntityKeepAlive"/>.</summary>
    private readonly TimeSpan? _keepAliveInterval;

    /// <summary>
    /// The address this instance is running at, as every span tags it — or <c>null</c> where the
    /// deployment runs no cluster. Read once: an <c>ActorSystem</c>'s own address holds still for as
    /// long as it runs, and formatting it per span builds the same string each time.
    /// </summary>
    private readonly string? _nodeAddress;

    /// <summary>Set once <see cref="WarnIfDeadlineOutlastsResidency"/> has considered this instance,
    /// so a long-lived one holding several deadlines says it at most once.</summary>
    private bool _deadlineResidencyWarned;
    private ICancelable? _keepAliveTick;

    public WorkflowEntityActor(
        string persistenceId,
        Func<TWorkflow> workflowFactory,
        IActorRef consumerController,
        IWorkflowTimeoutScheduler? timeoutScheduler = null,
        TimeSpan? gracefulShutdownGrace = null,
        TimeProvider? timeProvider = null,
        int snapshotEveryNEvents = 10,
        // The plain, unprefixed entity id ClusterSharding itself routes by —
        // WorkflowClusterShardingExtensions.WithWorkflow passes this separately from persistenceId
        // (which it constructs as "{typeName}-{entityId}") specifically so ChildOrchestrator can
        // address a ChildLifecycleNotification back to this actor by the id WorkflowMessageExtractor
        // actually understands. Defaults to persistenceId unchanged for a caller that constructs
        // this actor directly, outside real ClusterSharding (bare-actor tests, mainly) — there, the
        // two are already the same value in practice.
        string? entityId = null,
        // How often to announce this entity to its own shard region while it holds work, keeping the
        // shard's idle clock fresh — see EntityKeepAlive. WithWorkflow derives it from the
        // PassivateIdleEntityAfter the deployment configured, and leaves it null where passivation is
        // off, which is the default.
        TimeSpan? keepAliveInterval = null)
    {
        _persistenceId = persistenceId;
        _entityId = entityId ?? persistenceId;
        _nodeAddress = Context.System.HasExtension<Cluster>()
            ? Cluster.Get(Context.System).SelfAddress.ToString()
            : null;
        _workflow = workflowFactory();
        // An entity of a registration reads what WithWorkflow already derived for it; an actor built
        // directly derives its own, which is what lets a fixture construct several actors of one class
        // with different settings and have each keep its own. Settings are immutable per registration,
        // and Settings() is a virtual method a workflow may rebuild on every call, which a transition
        // reads several times — see WorkflowTypeProfile.
        var resolved = WorkflowTypeProfileRegistryProvider.Instance.Apply(Context.System)
            .ResolveOrDerive<TWorkflow, TState>(_workflow, Context.System.Settings.Config);
        _eventTags = resolved.EventTags;
        _deadlineEventTags = resolved.DeadlineEventTags;
        _consumerController = consumerController;
        _timeoutScheduler = timeoutScheduler ?? new NativeWorkflowTimeoutScheduler(Context.System.Scheduler);
        _snapshotPolicy = new SnapshotPolicy(snapshotEveryNEvents);
        _settings = resolved.Settings;
        _children = new ChildOrchestrator<TState>(_workflowHandleRegistry);

        // Defaults to a thin adapter over the ActorSystem's own scheduler (see
        // AkkaSchedulerTimeProvider), which makes deadline math line up with
        // Akka.TestKit.TestScheduler for free: swapping the scheduler implementation via
        // akka.scheduler.implementation (as the timeout test suites already do) advances this
        // actor's notion of "now" in lockstep with Scheduler.Advance(), with no separate test-only
        // time seam to wire up.
        _timeProvider = timeProvider ?? new AkkaSchedulerTimeProvider(Context.System.Scheduler);
        // Ledgers are sized on the profile, once, so folding a SeqNrRecorded/IdempotencyRecorded event
        // needs no access to settings — which is what keeps WorkflowEventFold a pure function of state
        // and event, identical live and on recovery. Both are immutable and record by returning a new
        // one, so every instance of a registration starts from the same empty value.
        // Seeded as NotStarted, so an entity addressed before anything was written to it says so.
        // Sharding activates an entity for any message, a status query included, and folding the first
        // event this instance persists moves it on from here.
        _envelope = new WorkflowRuntimeState<TState>(
            _workflow.EmptyState(), null, null, 0, WorkflowStatus.NotStarted,
            HighestAppliedSeqNr: resolved.EmptySeqNrLedger,
            IdempotencyLedger: resolved.EmptyIdempotencyLedger);

        // A caller-supplied grace can only shorten the window, never push it past the ceiling the
        // profile derived from the configured hand-off timeout (see WorkflowTypeProfile.GraceCeiling
        // for why that ceiling is what it is).
        _gracefulShutdownGrace = gracefulShutdownGrace is { } requested && requested < resolved.GraceCeiling
            ? requested
            : resolved.GraceCeiling;

        _keepAliveInterval = keepAliveInterval is { } interval && interval > TimeSpan.Zero ? interval : null;

        Command<KeepAliveTick>(_ => AnnounceIfHoldingWork());
        // The round trip's whole purpose is the shard timestamp it touched on the way here.
        Command<EntityKeepAlive>(_ => { });
        Command<GracefulShutdown>(_ => HandleGracefulShutdown());
        Command<GracefulShutdownGraceExpired>(_ =>
        {
            // Cancel first so a cooperative step gets a last chance to unwind cleanly before the
            // hard stop — same as StepTimedOut. Doesn't buy time (we stop regardless), but a step
            // built on HttpClient/EF/etc. will still observe it immediately on its own thread.
            _currentStepCts?.Cancel();
            Context.Stop(Self);
        });
        Command<StepCompleted>(HandleStepCompleted);
        Command<StepFailed>(HandleStepFailed);
        Command<StepTimedOut>(HandleStepTimedOut);
        Command<RetryDue>(HandleRetryDue);
        Command<WorkflowTimedOut>(_ => HandleWorkflowTimedOut());
        Command<PauseTimedOut>(_ => HandlePauseTimedOut());
        Command<HoldTimedOut>(_ => HandleHoldTimedOut());
        Command<ChildGroupTimedOut>(msg => HandleChildGroupTimedOut(msg.GroupId));
        Command<SaveSnapshotSuccess>(HandleSnapshotSuccess);
        Command<SaveSnapshotFailure>(_ => { });
        // Shared with the confirmed-purge path (see PurgeThenStop): with a purge pending, any
        // completion here belongs to it, taking priority over the routine prune-after-snapshot
        // counter bump. See PendingPurge's own doc comment for why attributing any completion seen
        // while a purge is pending to that purge — a coarser approximation than a precise per-call
        // correlation id, which Akka.Persistence's plugin API doesn't expose — is accepted.
        Command<DeleteMessagesSuccess>(_ =>
        {
            if (_pendingPurge is { } pending)
            {
                pending.MessagesDone = true;
                CompletePurgeIfReady(pending);
            }
        });
        Command<DeleteMessagesFailure>(msg =>
        {
            if (_pendingPurge is { } pending)
            {
                Context.GetLogger().Warning(msg.Cause, "{0}: purge failed to delete journal messages; stopping anyway.", _persistenceId);
                pending.MessagesDone = true;
                CompletePurgeIfReady(pending);
            }
        });
        Command<DeleteSnapshotsSuccess>(_ =>
        {
            if (_pendingPurge is { } pending)
            {
                pending.SnapshotsDone = true;
                CompletePurgeIfReady(pending);
            }
        });
        Command<DeleteSnapshotsFailure>(msg =>
        {
            if (_pendingPurge is { } pending)
            {
                Context.GetLogger().Warning(msg.Cause, "{0}: purge failed to delete snapshots; stopping anyway.", _persistenceId);
                pending.SnapshotsDone = true;
                CompletePurgeIfReady(pending);
            }
        });
        Command<GetDiagnostics<TState>>(_ => Sender.Tell(new Diagnostics<TState>(_envelope)));
        Command<GetStatus>(_ => Sender.Tell(new WorkflowStatusReply(_envelope.Status)));
        // Reaching this handler means recovery already ran and OnRecoveryCompleted already re-armed
        // every deadline this instance holds, firing any that had elapsed. So the reply is all that
        // is left to do, and it carries the information the sender wants: this instance is up.
        Command<Wake>(_ => Sender.Tell(Done.Instance));
        // The one query the framework ships (see GetState's own doc comment). Handled directly here,
        // outside the author's own query table, since it is generic across every workflow and needs
        // no per-workflow handler. UserState is boxed the same way any reply already is.
        Command<GetState>(_ => Sender.Tell(_envelope.UserState!));
        Command<QueryCompleted>(HandleQueryCompleted);
        Command<QueryFailed>(HandleQueryFailed);
        Command<QueryTimedOut>(HandleQueryTimedOut);
        Command<WatchForCompletion<TState>>(_ => HandleWatchForCompletion());
        // Ahead of CommandAny, so a watcher's death reaches its own handler.
        Command<Terminated>(HandleWatcherTerminated);
        Command<Suspend>(HandleSuspend);
        Command<Resume>(_ => HandleResume());
        Command<Terminate>(HandleTerminate);
        Command<Cancel>(HandleCancel);
        Command<Delete>(HandleDelete);
        // Echoed back by a real Shard once it has recorded this entity as deliberately passivating
        // (see PurgeStopMessage's own doc comment) — only then is it safe to actually stop.
        Command<PurgeStopMessage>(_ => Context.Stop(Self));
        Command<ChildLifecycleNotification>(HandleChildLifecycleNotification);
        Command<ConsumerController.Delivery<WorkflowEnvelope>>(HandleDelivery);
        CommandAny(HandleExternalCommand);

        Recover<SnapshotOffer>(offer =>
        {
            _envelope = (WorkflowRuntimeState<TState>)offer.Snapshot;
            _snapshotPolicy.RecordSnapshot(offer.Metadata.SequenceNr);
        });
        // Recovery folds through the very function the live path uses, so a recovered instance and a
        // running one cannot disagree about what a fact meant.
        Recover<WorkflowEvent>(e => _envelope = WorkflowEventFold.Apply(_envelope, e));
        // A journal that indexes tags lifts them off the event and replays the payload alone; one
        // with no tag support stores the wrapper as written and replays that. Accepting both is what
        // lets an application choose either journal and recover identically.
        Recover<Tagged>(t => _envelope = WorkflowEventFold.Apply(_envelope, (WorkflowEvent)t.Payload));
        Recover<RecoveryCompleted>(_ => OnRecoveryCompleted());
    }

    public override string PersistenceId => _persistenceId;

    /// <summary>
    /// Registers with the Akka.Delivery consumer side (see <see cref="_consumerController"/>'s doc
    /// comment on the constructor param) so it starts forwarding <see cref="ConsumerController.Delivery{T}"/>
    /// messages to this actor. A no-op in every test/production path that hasn't wired a real
    /// <c>ShardingConsumerController</c> ref yet — defaults to <see cref="ActorRefs.Nobody"/> in
    /// tests that don't care; wired for real by <c>WorkflowClusterShardingExtensions</c>.
    /// </summary>
    protected override void PreStart()
    {
        base.PreStart();
        _consumerController.Tell(new ConsumerController.Start<WorkflowEnvelope>(Self));
        ScheduleKeepAliveTick();
    }

    /// <summary>
    /// Arms the next keep-alive tick. The tick runs for as long as the entity does, and decides
    /// message by message whether there is anything to announce — see
    /// <see cref="AnnounceIfHoldingWork"/>. Keeping the tick independent of when work starts and stops
    /// leaves the eight places a step settles free of timer bookkeeping.
    /// </summary>
    private void ScheduleKeepAliveTick()
    {
        if (_keepAliveInterval is not { } interval)
        {
            return;
        }

        _keepAliveTick = _timeoutScheduler.ScheduleTimeout(interval, Self, new KeepAliveTick());
    }

    /// <summary>
    /// Announces this entity to its own shard region while it holds work the shard cannot see: a step
    /// running off-actor-thread, or a retry backoff waiting out its delay. Both live entirely on this
    /// actor, so the shard sees an entity that has received nothing for as long as the work takes.
    ///
    /// An idle entity announces nothing and passivates, which is what the deployment turned
    /// passivation on for.
    /// </summary>
    private void AnnounceIfHoldingWork()
    {
        ScheduleKeepAliveTick();

        var holdingWork = _stepInFlight || _envelope.RetryDelayUntil is not null;
        if (_shuttingDown || !holdingWork)
        {
            return;
        }

        // Addressed by entity id and sent to the region, so the shard routes it back down here and
        // touches this entity's timestamp on the way through. That routing is the whole trip.
        if (_workflowHandleRegistry.TryResolveByTypeName(_workflow.WorkflowTypeName, out var targets))
        {
            targets.ShardRegion.Tell(new WorkflowEnvelope(_entityId, EntityKeepAlive.Instance));
        }
    }

    /// <summary>Local, and never leaves this actor: it only prompts the announcement itself, which is
    /// <see cref="EntityKeepAlive"/>.</summary>
    private sealed record KeepAliveTick;

    private void HandleExternalCommand(object message)
    {
        // Queries arrive on this plain path, bypassing Akka.Delivery entirely (see
        // WorkflowRef.Query): at-least-once delivery of a read buys nothing, and a query never
        // persists, so there is no seqNr to record and no confirm to send. Matched ahead of the
        // command table — a query is dispatched immediately even while a step is running.
        if (_workflow.TryGetQuery(message.GetType(), out var queryDescriptor))
        {
            DispatchQuery(message, queryDescriptor);
            return;
        }

        if (!_workflow.TryGetHandler(message.GetType(), out var descriptor))
        {
            Unhandled(message);
            return;
        }

        // Same deferral HandleDelivery applies: a step is running off-actor-thread and its effect
        // hasn't persisted yet, so this command's state is about to be superseded. Whole-state
        // persistence means two overlapping writers race over all of TState, so command dispatch
        // waits for the step chain to settle — a ReleaseDeferredCommands decision unstashes.
        if (_stepInFlight)
        {
            Stash.Stash();
            return;
        }

        // Span creation/lifecycle lives in CommandDescriptor.Invoke now (Core) — this just supplies
        // what only the runtime knows: where the span fits (StepTracingContext.ResolveParentContext)
        // and the persistence-id tag.
        Activity? activity = null;
        var effect = descriptor.Invoke(
            _workflow, _envelope.UserState, message, _entityId,
            _tracing.ResolveParentContext(),
            configureActivity: a =>
            {
                activity = a;
                a?.SetTag("workflow.persistence_id", _persistenceId);
                a?.SetTag("workflow.node", _nodeAddress);
            });
        // A pure query (no persistence, no transition — e.g. a workflow-author-defined read-only
        // command like a sample's GetOrderState) doesn't become the next span's parent: it's not a
        // causal link in the workflow's actual execution, and letting it chain would make the
        // *next real* command/step's span parent off an unrelated read, obscuring whatever business
        // action actually preceded it. Its own span still gets created (still parented off whatever
        // _tracing.LastActivityTraceParent already was, since that's fixed before the effect is known
        // — see StepTracingContext.ResolveParentContext above), just doesn't advance the chain for
        // what comes after it.
        if (!IsNoOpEffect(effect))
        {
            _tracing.LastActivityTraceParent = activity?.Id ?? _tracing.LastActivityTraceParent;
        }

        // Fired before ApplyCommandEffect's own side effects (a StepTransition's StepStarted, a
        // PauseTransition's WorkflowPaused, ...) — same "cause, then its effects" ordering as
        // everywhere else notifications get published, and the only signal a subscriber gets that a
        // plain step-to-step transition (the common case for e.g. an approval) was driven by a
        // command at all — the engine's own decision to move on by itself has no such signal.
        ApplyCommandEffect(effect, new TransitionCause.Command(message.GetType().Name));
    }

    private static bool IsNoOpEffect(CommandEffect<TState> effect) =>
        effect.Persistence is PersistenceEffect<TState>.NoPersistence && effect.Transition is Transition.NoTransition;

    private void ApplyCommandEffect(CommandEffect<TState> effect, TransitionCause cause)
    {
        if (effect.Persistence is PersistenceEffect<TState>.NoPersistence && effect.Transition is Transition.NoTransition)
        {
            SendReply(effect.Reply);
            return;
        }

        PersistEnvelopeThen(effect.Persistence, effect.Transition, cause, () => SendReply(effect.Reply));
    }

    private void SendReply(Reply reply)
    {
        switch (reply)
        {
            case Reply.ReplyValue rv:
                Sender.Tell(rv.Value!);
                break;
            case Reply.ErrorValue ev:
                Sender.Tell(new Status.Failure(new WorkflowCommandException(ev.Message)));
                break;
        }
    }

    /// <summary>
    /// The <c>Akka.Delivery</c> counterpart of <see cref="HandleExternalCommand"/> — same dispatch
    /// through <see cref="IWorkflowCommandDispatcher{TState}"/>, but layered with the two closes the
    /// design doc calls for: a transport-level dedup check against <see cref="WorkflowRuntimeState{TState}.HighestAppliedSeqNr"/>
    /// (a redelivery of something already durably applied — nothing new to do, just re-confirm) and
    /// a caller-level idempotency-key replay against <see cref="WorkflowRuntimeState{TState}.IdempotencyLedger"/>
    /// (a genuinely new seqNr, but the caller's own retry after an ambiguous outcome — replays the
    /// cached reply without re-invoking the handler). Neither applies to the still-plain
    /// <see cref="HandleExternalCommand"/> path: <see cref="Sender"/> there is the real caller (Akka's
    /// own implicit chaining), so there's no transport redelivery to dedup and no envelope carrying
    /// an idempotency key at all.
    /// </summary>
    private void HandleDelivery(ConsumerController.Delivery<WorkflowEnvelope> delivery)
    {
        var envelope = delivery.Message;

        // Transport-level redelivery (ConsumerController retried because an earlier Confirmed was
        // lost, e.g. this entity crashed before persisting) — genuinely nothing new to apply.
        if (_envelope.HighestAppliedSeqNr is { } seqNrs
            && seqNrs.TryGetHighest(delivery.ProducerId, out var highest)
            && delivery.SeqNr <= highest)
        {
            delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
            return;
        }

        // Caller-code retry after an ambiguous Ask timeout, same idempotency key — replays the
        // cached reply without re-invoking the handler.
        if (envelope.IdempotencyKey is { } key
            && _envelope.IdempotencyLedger is { } ledger
            && ledger.TryGetCachedReply(key, out var cachedReply))
        {
            PersistEventsThen(
                new WorkflowEvent[] { new WorkflowEvent.SeqNrRecorded(delivery.ProducerId, delivery.SeqNr) },
                Array.Empty<WorkflowDecision>(),
                afterPersist: () =>
                {
                    SendReplyTo(envelope.ReplyTo, cachedReply);
                    delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
                });
            return;
        }

        // A child's report of its own terminal outcome rides the same Akka.Delivery pipeline as
        // every other command (see ChildLifecycleNotification's own doc comment) — matched by type
        // here, ahead of the author's own [WorkflowCommandHandler] table, dispatching straight to
        // ApplyChildLifecycleNotification as an internal-framework message. The generic
        // redelivery/idempotency-key checks above already cover it exactly like any other delivered
        // message; this branch only determines what happens once those checks pass.
        if (envelope.Message is ChildLifecycleNotification notification)
        {
            ApplyChildLifecycleNotification(
                notification,
                seqNrUpdate: (delivery.ProducerId, delivery.SeqNr),
                confirmDelivery: () => delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance));
            return;
        }

        // SendTerminate's own cascade send (ParentClosePolicy/RemainingChildrenPolicy): the engine
        // sends this itself with no live caller present to retry it, so it rides this reliable
        // pipeline for at-least-once delivery (see HandleTerminate's two-overload doc comments).
        // Matched by type here, ahead of the author's own [WorkflowCommandHandler] table — the
        // framework's own Terminate type is handled internally; no real workflow registers its own
        // handler for it.
        if (envelope.Message is Terminate terminate)
        {
            HandleTerminate(terminate, delivery);
            return;
        }

        // A parent's own cancel cascade (see ChildOrchestrator.SendCancel): sent by the engine with
        // no live caller to retry it, so it rides this reliable pipeline like the Terminate above.
        if (envelope.Message is Cancel cancel)
        {
            HandleCancel(cancel);
            delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
            return;
        }

        // SendDelete's own cascade send (a parent purging its owned subtree): same reasoning as the
        // Terminate branch above — no live caller present to retry it, so it rides this reliable
        // pipeline. Matched by type here, ahead of the author's own [WorkflowCommandHandler] table —
        // the framework's own Delete type is handled internally; no real workflow registers its own
        // handler for it.
        if (envelope.Message is Delete delete)
        {
            HandleDelete(delete, delivery);
            return;
        }

        if (!_workflow.TryGetHandler(envelope.Message.GetType(), out var descriptor))
        {
            delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
            Unhandled(envelope.Message);
            return;
        }

        // A step is running off-actor-thread (fire-and-PipeTo — see StartStep); its own eventual
        // effect hasn't persisted yet, so this command's state is about to be superseded. It defers,
        // stashed until the step chain settles and a ReleaseDeferredCommands decision unstashes it.
        // Left
        // unconfirmed: Akka.Delivery's producer buffers it until Confirmed, so a crash while this
        // sits in Stash just means the producer redelivers it once this entity (or its next
        // incarnation) recovers — the redelivery/dedup check above already covers that replay.
        // A read that must not wait for a running step is a [WorkflowQuery] (see
        // WorkflowQueryAttribute), which never reaches this path at all.
        if (_stepInFlight)
        {
            Stash.Stash();
            return;
        }

        // Span creation/lifecycle lives in CommandDescriptor.Invoke now (Core) — same as
        // HandleExternalCommand; this just supplies what only the runtime knows.
        Activity? activity = null;
        var effect = descriptor.Invoke(
            _workflow, _envelope.UserState, envelope.Message, _entityId,
            _tracing.ResolveParentContext(),
            _tracing.ConsumeParentLink(_envelope.LastTraceParent, envelope.ParentRelationship?.TraceParent ?? envelope.TraceParent),
            a =>
            {
                activity = a;
                a?.SetTag("workflow.persistence_id", _persistenceId);
                a?.SetTag("workflow.node", _nodeAddress);
            });
        // See HandleExternalCommand's matching check — a pure query doesn't advance the trace chain.
        if (!IsNoOpEffect(effect))
        {
            _tracing.LastActivityTraceParent = activity?.Id ?? _tracing.LastActivityTraceParent;
        }


        ApplyDeliveryCommandEffect(effect, envelope, delivery);
    }

    private void ApplyDeliveryCommandEffect(
        CommandEffect<TState> effect, WorkflowEnvelope envelope, ConsumerController.Delivery<WorkflowEnvelope> delivery)
    {
        // A NoPersistence effect stays zero-write UNLESS the caller supplied an idempotency key —
        // then it's forced to persist anyway (just the ledger entry), otherwise a duplicate would
        // silently re-run a handler the caller explicitly flagged as needing dedup (see the design
        // doc's Error handling section — e.g. a read-only handler that still fires a notification).
        var mustPersistForLedger = envelope.IdempotencyKey is not null;

        if (IsNoOpEffect(effect) && !mustPersistForLedger)
        {
            SendReplyTo(envelope.ReplyTo, effect.Reply);
            delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
            return;
        }

        // The delivery's own bookkeeping, written in the same batch as whatever the handler decided.
        var bookkeeping = new List<WorkflowEvent>
        {
            new WorkflowEvent.SeqNrRecorded(delivery.ProducerId, delivery.SeqNr),
        };
        if (envelope.IdempotencyKey is { } key)
        {
            bookkeeping.Add(new WorkflowEvent.IdempotencyRecorded(key, effect.Reply));
        }

        // A redelivered child-start envelope (Akka.Delivery's own retry after a lost Confirmed)
        // carries the same ParentRelationship on every attempt — only the very first delivery this
        // instance ever persists should write it. Guarding on _envelope.ParentRelationship already
        // being set keeps a later redelivery's copy from overwriting whatever this instance's own
        // lifecycle has since moved the relationship to. Applies on both branches below: a
        // child-start command's own handler can just as easily produce a NoTransition effect
        // (e.g. a plain acknowledging reply) as a real transition.
        if (_envelope.ParentRelationship is null && envelope.ParentRelationship is { } parentRelationship)
        {
            bookkeeping.Add(new WorkflowEvent.ParentRelationshipSet(parentRelationship));
        }

        if (effect.Transition is Transition.NoTransition)
        {
            if (effect.Persistence is PersistenceEffect<TState>.UpdateState updatedState)
            {
                bookkeeping.Add(new WorkflowEvent.UserStateChanged<TState>(updatedState.NewState));
            }

            PersistEventsThen(bookkeeping, Array.Empty<WorkflowDecision>(), afterPersist: () =>
            {
                SendReplyTo(envelope.ReplyTo, effect.Reply);
                delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
            });
            return;
        }

        // Confirm happens INSIDE afterPersist, only once the write actually completes: Persist(...)
        // is asynchronous (it queues the journal write and returns immediately — the callback only
        // runs once the write actually completes), so code sitting immediately after this call
        // still executes before persistence. Confirming there would tell ConsumerController the
        // transition landed before it durably had — a crash in that window would be silent data
        // loss, exactly what Akka.Delivery's ack is supposed to rule out. Same reasoning as the
        // NoTransition branch above, which already gets this right.
        PersistEnvelopeThen(effect.Persistence, effect.Transition,
            new TransitionCause.Command(envelope.Message.GetType().Name) { Metadata = envelope.Metadata }, () =>
        {
            SendReplyTo(envelope.ReplyTo, effect.Reply);
            delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
        }, extraEvents: bookkeeping);
    }



    /// <summary>
    /// <see cref="SendReply"/>'s <c>Akka.Delivery</c> counterpart: the <see cref="Sender"/> on a
    /// <see cref="ConsumerController.Delivery{T}"/> is always the internal <c>ConsumerController</c>
    /// itself (see <see cref="WorkflowEnvelope.ReplyTo"/>'s doc comment) — so a
    /// <c>null</c> <paramref name="replyTo"/> (fire-and-forget <c>Send</c>) falls back to the
    /// still-plain <see cref="SendReply"/>/<see cref="Sender"/> path, so the reply is still
    /// delivered there.
    /// </summary>
    private void SendReplyTo(IActorRef? replyTo, Reply reply)
    {
        if (replyTo is null)
        {
            SendReply(reply);
            return;
        }

        switch (reply)
        {
            case Reply.ReplyValue rv:
                replyTo.Tell(rv.Value!);
                break;
            case Reply.ErrorValue ev:
                replyTo.Tell(new Status.Failure(new WorkflowCommandException(ev.Message)));
                break;
        }
    }

    /// <summary>
    /// Starts a query handler off-actor-thread and pipes its outcome back, exactly like a step —
    /// the mailbox keeps draining, so a slow query can never block <c>Suspend</c>/<c>Terminate</c>/
    /// <c>GetStatus</c> the way suspending the mailbox would.
    ///
    /// Unlike a step there is no epoch and no in-flight flag: a query persists nothing, so several
    /// run concurrently and a stale result harms nothing. What each one does need is its own caller
    /// and its own bound, so <see cref="Sender"/> is captured into the tracking entry here (by the
    /// time the result pipes back, <see cref="Sender"/> is whoever sent the pipe message) and a
    /// correlation id decides whether a result still has anyone waiting for it.
    /// </summary>
    private void DispatchQuery(object query, QueryDescriptor<TState> descriptor)
    {
        var replyTo = Sender;
        var id = ++_querySequence;
        var cts = new CancellationTokenSource();
        var timeout = _settings.QueryTimeoutFor(descriptor.QueryTypeName);
        var deadline = _timeoutScheduler.ScheduleTimeout(timeout, Self, new QueryTimedOut(id));
        _inFlightQueries[id] = new InFlightQuery(replyTo, cts, deadline);

        // A query observes the workflow; it doesn't advance it. So its span is parented off wherever
        // the trace currently stands, but _tracing.LastActivityTraceParent is deliberately left
        // alone — letting a read become the next step's parent would misattribute that step to
        // whoever happened to poll for status just before it started.
        descriptor.Invoke(
            _workflow, _envelope.UserState, query, cts.Token, _entityId,
            _tracing.ResolveParentContext(),
            links: null,
            configureActivity: a =>
            {
                a?.SetTag("workflow.persistence_id", _persistenceId);
                a?.SetTag("workflow.node", _nodeAddress);
            })
            .PipeTo(Self,
                success: effect => new QueryCompleted(id, effect),
                failure: ex => new QueryFailed(id, ex));
    }

    private void HandleQueryCompleted(QueryCompleted msg)
    {
        if (!TryReleaseQuery(msg.Id, out var pending))
        {
            return;
        }

        SendReplyTo(pending.ReplyTo, msg.Effect.Reply);
    }

    private void HandleQueryFailed(QueryFailed msg)
    {
        if (!TryReleaseQuery(msg.Id, out var pending))
        {
            return;
        }

        pending.ReplyTo.Tell(new Status.Failure(msg.Cause));
    }

    /// <summary>
    /// The query ran past its bound. Replies now and frees the slot; cancelling the token gives a
    /// cooperative handler the chance to unwind, and a handler that ignores it simply finds no entry
    /// left when it eventually pipes its result back.
    /// </summary>
    private void HandleQueryTimedOut(QueryTimedOut msg)
    {
        if (!_inFlightQueries.Remove(msg.Id, out var pending))
        {
            return;
        }

        pending.Cts.Cancel();
        pending.Cts.Dispose();
        pending.ReplyTo.Tell(new Status.Failure(new WorkflowQueryTimeoutException(
            $"Query on workflow '{_persistenceId}' exceeded its timeout.")));
    }

    private bool TryReleaseQuery(long id, out InFlightQuery pending)
    {
        if (!_inFlightQueries.Remove(id, out pending!))
        {
            return false;
        }

        pending.Deadline.Cancel();
        pending.Cts.Dispose();
        return true;
    }


    /// <summary>
    /// Cancels every in-flight query as this instance goes away — passivation, a rebalance, or a
    /// terminate. The caller's own request timeout would otherwise be its only signal, and that can
    /// be far longer than the wait was ever going to be useful for.
    /// </summary>
    protected override void PostStop()
    {
        foreach (var pending in _inFlightQueries.Values)
        {
            pending.Deadline.Cancel();
            pending.Cts.Cancel();
            pending.Cts.Dispose();
        }

        _inFlightQueries.Clear();
        _keepAliveTick?.Cancel();

        // The step this instance was last running holds a token source of its own. Starting a step
        // disposes the one before it, so this is the last one, and cancelling it hands a step still
        // running off-actor-thread the same signal every other stop gives it.
        _currentStepCts?.Cancel();
        _currentStepCts?.Dispose();
        _currentStepCts = null;

        // Passivation stops an entity that may still hold armed deadlines. Each is a persisted
        // absolute instant the next activation arms again, so dropping them here costs nothing and
        // keeps them from firing at an actor that has gone.
        _timeouts.CancelAll();

        base.PostStop();
    }

    private void HandleStepCompleted(StepCompleted msg)
    {
        if (msg.Epoch != _stepEpoch)
        {
            return;
        }

        _timeouts.CancelStep();
        // Ok/Error status and disposal already happened inside StepDescriptor.Invoke by the time
        // this message exists at all — it's only sent once that Task completed. Just the
        // bookkeeping this handler itself owns (in-flight flag, force-close eligibility) is left.
        _stepInFlight = false;
        // Whatever comes next (the transition below) is caused by THIS attempt succeeding, so it
        // becomes the new parent. This step is genuinely concluding, so it earns that; a retry (see
        // HandleStepFailed's willRetry branch) only ever stays a sibling of the attempt it followed.
        _tracing.LastActivityTraceParent = _tracing.CurrentStepActivity?.Id ?? _tracing.LastActivityTraceParent;
        _tracing.CurrentStepActivity = null;
        var attempt = _envelope.RetryCount + 1;
        var duration = _timeProvider.GetUtcNow() - _tracing.CurrentStepStartedAt;

        var effect = msg.Effect;
        PersistEnvelopeThen(
            effect.Persistence, effect.Transition,
            cause: new TransitionCause.StepSucceeded(msg.StepName, attempt, duration));
    }

    private void HandleStepTimedOut(StepTimedOut msg)
    {
        if (msg.Epoch != _stepEpoch)
        {
            return;
        }

        // A normal failure means the Task already settled; here it is very possibly still running
        // (that's the whole reason this timer fired) — cancel it so a cooperative step actually
        // stops here, before it runs on orphaned past its own deadline.
        _currentStepCts?.Cancel();
        HandleStepFailed(new StepFailed(msg.StepName, msg.Epoch, new TimeoutException($"Step '{msg.StepName}' timed out")));
    }

    private void HandleRetryDue(RetryDue msg)
    {
        if (msg.Epoch != _stepEpoch)
        {
            return;
        }

        StartStep();
    }

    private void HandleStepFailed(StepFailed msg)
    {
        if (msg.Epoch != _stepEpoch)
        {
            return;
        }

        _timeouts.CancelStep();
        // See the matching comment in HandleStepCompleted — Invoke already closed the span itself.
        _stepInFlight = false;
        var failedActivityId = _tracing.CurrentStepActivity?.Id;
        _tracing.CurrentStepActivity = null;

        var now = _timeProvider.GetUtcNow();
        var attempt = _envelope.RetryCount + 1;
        var duration = now - _tracing.CurrentStepStartedAt;
        var plan = WorkflowTransitionPlanner.PlanStepFailure(
            _envelope, msg.StepName, msg.Exception.Message, now, _settings, duration: duration);

        if (plan is StepFailurePlan<TState>.Retry retry)
        {
            WorkflowDiagnostics.RecordStepRetryScheduled(_workflow.WorkflowTypeName, msg.StepName);

            // Deliberately does not update _tracing.LastActivityTraceParent here: every retry attempt
            // of the SAME step is a sibling of the first, parented off the context that triggered the
            // step originally. The previous attempt has already ended by the time this one starts, so
            // parenting off it would misrepresent independent retries as one nested inside the next.
            PersistEventsThen(retry.Events, Array.Empty<WorkflowDecision>(), afterPersist: () =>
            {
                if (StopIfShuttingDown())
                {
                    return;
                }

                if (retry.RetryDelayUntil is not { } retryDelayUntil)
                {
                    StartStep();
                    return;
                }

                // The envelope carries RetryDelayUntil as an absolute deadline, so a crash or
                // rebalance mid-wait resumes on reactivation (OnRecoveryCompleted) at exactly the
                // delay's remaining length.
                _timeouts.CancelRetryDelay();
                _timeouts.RetryDelay = _timeoutScheduler.ScheduleTimeout(
                    retryDelayUntil - now, Self, new RetryDue(msg.StepName, _stepEpoch));
            });
            return;
        }

        // The budget is exhausted, so this step is genuinely concluding here — whatever comes next is
        // caused by this specific failed attempt and parents off it, the same reasoning as the
        // success path in HandleStepCompleted.
        _tracing.LastActivityTraceParent = failedActivityId ?? _tracing.LastActivityTraceParent;
        PersistEnvelopeThen(
            PersistenceEffect<TState>.NoPersistence.Instance,
            ((StepFailurePlan<TState>.Conclude)plan).Transition,
            new TransitionCause.StepFailed(msg.StepName, attempt, msg.Exception.Message, duration, WillRetry: false));
    }

    private void HandleWorkflowTimedOut()
    {
        if (WorkflowTransitionPlanner.PlanWorkflowTimeout(_envelope, _settings) is { } transition)
        {
            PersistEnvelopeThen(
                PersistenceEffect<TState>.NoPersistence.Instance, transition, new TransitionCause.Control("WorkflowTimedOut"));
        }
    }

    private void HandlePauseTimedOut()
    {
        if (WorkflowTransitionPlanner.PlanPauseTimeout(_envelope) is { } transition)
        {
            PersistEnvelopeThen(
                PersistenceEffect<TState>.NoPersistence.Instance, transition, new TransitionCause.Control("PauseTimedOut"));
        }
    }

    /// <summary>
    /// Nobody came back for a held instance, so it runs the step its hold named and that step decides
    /// what becomes of it. A hold already released by any other route leaves nothing to plan, so a
    /// timer that fires just after a resume does nothing.
    /// </summary>
    private void HandleHoldTimedOut()
    {
        if (WorkflowTransitionPlanner.PlanHoldTimeout(_envelope) is { } transition)
        {
            PersistEnvelopeThen(
                PersistenceEffect<TState>.NoPersistence.Instance, transition, new TransitionCause.Control("HoldTimedOut"));
        }
    }

    /// <summary>
    /// A group's children never finished in the time it was given, so the parent runs the step that
    /// group named and decides there. A group that has since resolved leaves nothing to plan.
    /// </summary>
    private void HandleChildGroupTimedOut(string groupId)
    {
        if (WorkflowTransitionPlanner.PlanChildGroupTimeout(_envelope, groupId) is { } transition)
        {
            PersistEnvelopeThen(
                PersistenceEffect<TState>.NoPersistence.Instance, transition,
                new TransitionCause.Control("ChildGroupTimedOut"));
        }
    }

    private void HandleSuspend(Suspend msg)
    {
        // Invalidate any in-flight step attempt — its eventual result (if any) is discarded via
        // the epoch check, same mechanism as everything else. CurrentStepName/Input are preserved
        // by the plan so Resume knows what to re-execute. Cancelling here gives a cooperative step a
        // chance to actually stop, beyond simply discarding its eventual result.
        ApplyControlPlan(
            WorkflowTransitionPlanner.PlanSuspend(
                _envelope, new TransitionCause.Control("Suspend"), _timeProvider.GetUtcNow(), _settings),
            beforePersist: () =>
        {
            _stepEpoch++;
            _currentStepCts?.Cancel();
            _timeouts.CancelForSuspend();
            _stepInFlight = false;
            _tracing.ForceCloseCurrentStepActivity("suspended");
        });
    }

    /// <summary>
    /// Persists a control command's envelope and carries out its decisions, on the same terms as a
    /// transition: nothing observable happens before the write. <paramref name="beforePersist"/> is
    /// the actor-local teardown only this driver can do — invalidating an in-flight step attempt,
    /// cancelling its token — which has no persisted form and so has no place in the plan.
    /// </summary>
    private void ApplyControlPlan(ControlPlan<TState> plan, Action? beforePersist = null)
    {
        if (plan is ControlPlan<TState>.Reject reject)
        {
            Sender.Tell(new Status.Failure(new WorkflowCommandException(reject.Reason)));
            return;
        }

        var apply = (ControlPlan<TState>.Apply)plan;
        beforePersist?.Invoke();

        var replyTo = Sender;
        PersistEventsThen(apply.Events, apply.AfterPersist, afterPersist: () => replyTo.Tell(Done.Instance));
    }

    private void HandleResume() =>
        ApplyControlPlan(
            WorkflowTransitionPlanner.PlanResume(
                _envelope, _timeProvider.GetUtcNow(), _settings, new TransitionCause.Control("Resume")),
            beforePersist: () => _timeouts.CancelRetryDelay());

    /// <summary>
    /// Bare-Tell entry point for a live <c>Terminate</c> Ask against this actor's shard region (see
    /// <see cref="Clustering.WorkflowRef{TWorkflow, TState}.Terminate"/>) — replies <c>Done</c> to
    /// <see cref="Sender"/>, the caller's own Ask.
    /// </summary>
    private void HandleTerminate(Terminate msg) => ApplyTerminate(msg, seqNrUpdate: null, onDone: () => Sender.Tell(Done.Instance));

    /// <summary>
    /// Graceful stop: what a cancellation means is the planner's decision (see
    /// <see cref="WorkflowTransitionPlanner.PlanCancel{TState}"/>) — unwind through the configured
    /// step, or finish straight away when there is nothing to unwind.
    ///
    /// Whatever step was running is invalidated first, exactly as <c>Suspend</c> and
    /// <c>Terminate</c> do: its eventual result is discarded by the epoch check and its token is
    /// cancelled so a cooperative step stops before the compensation starts. The reply goes out as
    /// soon as the decision is durable — the compensation itself is still running at that point, so a
    /// caller wanting to observe the unwind finishing watches for completion separately.
    /// </summary>
    private void HandleCancel(Cancel msg)
    {
        if (WorkflowTransitionPlanner.PlanCancel(_envelope, msg.Reason, _settings) is not { } transition)
        {
            Sender.Tell(Done.Instance);
            return;
        }

        _stepEpoch++;
        _currentStepCts?.Cancel();
        _timeouts.CancelForSuspend();
        _stepInFlight = false;
        _tracing.ForceCloseCurrentStepActivity("cancelled");

        var replyTo = Sender;
        PersistEnvelopeThen(
            PersistenceEffect<TState>.NoPersistence.Instance, transition, new TransitionCause.Control("Cancel"),
            afterPersist: () => replyTo.Tell(Done.Instance));
    }

    /// <summary>
    /// <see cref="Akka.Delivery"/> entry point: <see cref="SendTerminate"/> (the engine's own
    /// <c>ParentClosePolicy</c>/<c>RemainingChildrenPolicy</c> cascade, with no live caller to retry
    /// on a lost message) rides the same reliable producer/consumer pipeline as a child-start
    /// command — same at-least-once delivery, same automatic resend across a shard relocation, no
    /// separate "hope it landed" fire-and-forget send for something that must actually happen. This
    /// mirrors <see cref="HandleDelivery"/>'s <c>ChildLifecycleNotification</c> branch: recorded
    /// against <see cref="WorkflowRuntimeState{TState}.HighestAppliedSeqNr"/> like any other
    /// delivered message, confirmed only once the terminal state is actually persisted (never
    /// before — confirming early and then crashing before persisting would let a genuinely-applied
    /// <c>Terminate</c> go unrecorded on recovery).
    /// </summary>
    private void HandleTerminate(Terminate msg, ConsumerController.Delivery<WorkflowEnvelope> delivery) =>
        ApplyTerminate(msg, seqNrUpdate: (delivery.ProducerId, delivery.SeqNr), onDone: () => delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance));

    private void ApplyTerminate(Terminate msg, (string ProducerId, long SeqNr)? seqNrUpdate, Action onDone)
    {
        if (WorkflowTransitionPlanner.PlanTerminate(
                _envelope, msg.Reason, _timeProvider.GetUtcNow(), new TransitionCause.Control("Terminate")) is not ControlPlan<TState>.Apply apply)
        {
            // Already finished: nothing to stop, and the caller still gets its acknowledgement.
            onDone();
            return;
        }

        _stepEpoch++;
        _currentStepCts?.Cancel();
        _timeouts.CancelForTerminate();
        _stepInFlight = false;
        _tracing.ForceCloseCurrentStepActivity("terminated");

        // The delivery bookkeeping rides the same batch as the termination, so a crash can never
        // apply one without the other (guarantee D5).
        var events = seqNrUpdate is { } s
            ? apply.Events.Prepend<WorkflowEvent>(new WorkflowEvent.SeqNrRecorded(s.ProducerId, s.SeqNr)).ToList()
            : apply.Events;

        PersistEventsThen(events, apply.AfterPersist, afterPersist: onDone);
    }

    /// <summary>
    /// Bare-Tell entry point for a live <c>Delete</c> Ask against this actor's shard region — mirrors
    /// <see cref="HandleTerminate(Terminate)"/>. Replies <c>Done</c> only once the physical purge
    /// completes (see <see cref="PurgeThenStop"/>), which spans a second, independent incoming
    /// message (<c>DeleteMessagesSuccess</c>/<c>DeleteSnapshotsSuccess</c>, sent by the journal/
    /// snapshot-store plugin) carrying its own <see cref="Sender"/> — so the caller to reply to is
    /// captured into a local here, while <see cref="Sender"/> still reflects this message's actual
    /// caller, for the later callback to close over.
    /// </summary>
    private void HandleDelete(Delete msg)
    {
        var replyTo = Sender;
        ApplyDelete(msg, onDone: () => replyTo.Tell(Done.Instance));
    }

    /// <summary>
    /// <see cref="Akka.Delivery"/> entry point: <see cref="ChildOrchestrator{TState}.SendDelete"/>'s
    /// own cascade send rides this reliable pipeline the same way <see cref="HandleTerminate(Terminate, ConsumerController.Delivery{WorkflowEnvelope})"/>'s
    /// counterpart does.
    /// </summary>
    private void HandleDelete(Delete msg, ConsumerController.Delivery<WorkflowEnvelope> delivery) =>
        ApplyDelete(msg, onDone: () => delivery.ConfirmTo.Tell(ConsumerController.Confirmed.Instance));

    /// <summary>
    /// Runs at any status, including an already-terminal one — purging an already-<c>Ended</c>/
    /// <c>Terminated</c>/<c>Deleted</c> workflow's leftover data is the primary use case this command
    /// exists for. Stays true zero-residue: <c>_envelope</c> is only updated in-memory, just enough
    /// for a concurrent <c>GetStatus</c>/<c>WatchForCompletion</c> in the brief window before this
    /// actor actually stops to see it as <c>Deleted</c> — no new envelope is ever persisted for it.
    /// </summary>
    private void ApplyDelete(Delete msg, Action onDone)
    {
        _stepEpoch++;
        _currentStepCts?.Cancel();
        _timeouts.CancelForTerminate();
        _stepInFlight = false;
        _tracing.ForceCloseCurrentStepActivity("deleted");

        var (_, childrenToDelete) = ChildGroupPolicy.ApplyParentClosePolicyToChildren(_envelope);
        foreach (var child in childrenToDelete)
        {
            _children.SendDelete(child);
        }

        if (_envelope.ParentRelationship is { } relationship)
        {
            _children.SendChildLifecycleNotification(
                relationship, _envelope.Outcome, _envelope.UserState, _tracing.LastActivityTraceParent ?? _envelope.LastTraceParent);
        }

        // A route out of Paused/Suspended alongside BuildDecisions/PlanTerminate/PlanResume above:
        // this command bypasses the planner entirely, so it reports its own duration the same way
        // those do.
        if (_envelope.Status == WorkflowStatus.Paused && _envelope.PausedAt is { } pausedAt)
        {
            WorkflowDiagnostics.RecordPauseDuration(_workflow.WorkflowTypeName, _timeProvider.GetUtcNow() - pausedAt);
        }
        else if (_envelope.Status == WorkflowStatus.Suspended && _envelope.HeldAt is { } heldAt)
        {
            WorkflowDiagnostics.RecordSuspendedDuration(_workflow.WorkflowTypeName, _timeProvider.GetUtcNow() - heldAt);
        }

        _envelope = _envelope with
        {
            Status = WorkflowStatus.Deleted, CurrentStepName = null, CurrentStepInput = null, PausedAt = null, HeldAt = null,
        };
        WorkflowDiagnostics.RecordStatusChange(_workflow.WorkflowTypeName, WorkflowStatus.Deleted);
        NotifyPendingCompletionWatchersIfTerminal();

        PurgeThenStop(onDone);
    }

    /// <summary>
    /// Physically deletes everything persisted for this instance — every journal event and every
    /// snapshot — then, once the persistence backend confirms both deletes, acks
    /// <paramref name="onDone"/> and detaches from <c>ClusterSharding</c> via <c>Passivate</c> (see
    /// <see cref="PurgeStopMessage"/>'s own doc comment for the self-passivation protocol this class's
    /// other stop sites don't need). Shared by both <see cref="ApplyDelete"/> (the external command)
    /// and the business-level <c>Transition.DeleteTransition</c> case in
    /// <see cref="PersistEnvelopeThen"/> — the same zero-residue outcome either way, whichever entry
    /// point triggered it.
    /// </summary>
    private void PurgeThenStop(Action onDone)
    {
        var pending = new PendingPurge(onDone);
        _pendingPurge = pending;
        DeleteMessages(LastSequenceNr);
        // long.MaxValue matches every snapshot ever taken for this persistence id, unconditionally —
        // a full purge deliberately wants "match everything." HandleSnapshotSuccess's routine
        // prune-after-snapshot guards seqNr > 1 specifically to dodge the in-mem store's
        // maxSequenceNr:0 wildcard quirk on an incremental prune; here "match everything" already
        // includes whatever that guard exists to avoid over-deleting, so it doesn't apply.
        DeleteSnapshots(new SnapshotSelectionCriteria(maxSequenceNr: long.MaxValue));
    }

    /// <summary>
    /// Fires once both halves of a purge have settled (success or failure — see <see cref="PendingPurge"/>).
    /// Non-fatal on failure: by the time a purge is running the entity is already unreachable to
    /// further business commands, so a plugin-level failure to physically reclaim a stray event/
    /// snapshot isn't worth leaving the caller blocked or this actor alive over.
    /// </summary>
    private void CompletePurgeIfReady(PendingPurge pending)
    {
        if (!pending.MessagesDone || !pending.SnapshotsDone)
        {
            return;
        }

        _pendingPurge = null;
        pending.OnDone();
        Context.Parent.Tell(new Passivate(new PurgeStopMessage()));
    }

    /// <summary>
    /// Tracks one in-flight <see cref="PurgeThenStop"/> call. <see cref="DeleteMessagesSuccess"/>/
    /// <see cref="DeleteSnapshotsSuccess"/> (and their Failure counterparts) carry no correlation id
    /// of their own, so any completion observed while a purge is pending is attributed to it. The
    /// actor processes one message at a time, and this purge's own two delete calls are issued
    /// synchronously, inside the same handler that sets this field — a routine prune-after-snapshot
    /// call issued earlier has essentially always already round-tripped by that point, so attributing
    /// a stray earlier completion to this purge stays a theoretical possibility, costing nothing worse
    /// than a marginally early ack in that rare case (the purge's own delete calls are issued
    /// regardless, before this tracking even starts waiting on them).
    /// </summary>
    private sealed class PendingPurge(Action onDone)
    {
        public bool MessagesDone;
        public bool SnapshotsDone;
        public Action OnDone => onDone;
    }

    /// <summary>See <see cref="GracefulShutdown"/>. Idempotent: ClusterSharding may resend the
    /// hand-off-stop message.</summary>
    private void HandleGracefulShutdown()
    {
        if (_shuttingDown)
        {
            return;
        }

        _shuttingDown = true;

        if (!_stepInFlight)
        {
            // Nothing in flight — an ordinary, immediate stop.
            Context.Stop(Self);
            return;
        }

        // A step is running off-actor-thread; let it finish and persist normally (its own
        // StepCompleted/StepFailed handler checks _shuttingDown before deciding whether to start
        // the next step), bounded by a grace window in case it never returns.
        _timeouts.GracefulShutdownDeadline = _timeoutScheduler.ScheduleTimeout(_gracefulShutdownGrace, Self, new GracefulShutdownGraceExpired());
    }

    /// <summary>
    /// Registers <see cref="Sender"/> to be told how this run ends, or answers straight away where it
    /// has already ended.
    ///
    /// A registered watcher is watched, so one that goes away — a caller whose <c>Ask</c> timed out and
    /// took its temporary ref with it — is dropped when its <see cref="Terminated"/> arrives. An
    /// instance that runs for days while callers come and go holds only the ones still waiting.
    /// </summary>
    private void HandleWatchForCompletion()
    {
        if (CompletionResult() is { } result)
        {
            Sender.Tell(result);
            return;
        }

        _pendingCompletionWatchers.Add(Sender);
        Context.Watch(Sender);
    }

    /// <summary>A watcher that stopped before this run ended. See <see cref="HandleWatchForCompletion"/>.</summary>
    private void HandleWatcherTerminated(Terminated terminated)
    {
        _pendingCompletionWatchers.RemoveAll(watcher => watcher.Equals(terminated.ActorRef));
        Context.Unwatch(terminated.ActorRef);
    }

    /// <summary>
    /// The result to hand a completion watcher, or <c>null</c> while the run is still going. An
    /// instance deleted before it ever finished has no outcome of its own, so it reports as
    /// terminated — the run stopped, and there is nothing else true to say about it.
    /// </summary>
    private WorkflowResult<TState>? CompletionResult() =>
        _envelope.Status switch
        {
            WorkflowStatus.Finished => new WorkflowResult<TState>.Finished(_envelope.Outcome!, _envelope.UserState),
            WorkflowStatus.Deleted => new WorkflowResult<TState>.Finished(
                _envelope.Outcome ?? new WorkflowOutcome.Terminated("deleted"), _envelope.UserState),
            // Held on a failure, so a caller waiting on this run has nothing left to wait for until
            // someone acts on it. An operator hold carries no failure and reports nothing, since a
            // human already knows it is held and decides when it resumes.
            WorkflowStatus.Suspended when _envelope.ParkedFailure is { } parked =>
                new WorkflowResult<TState>.Parked(parked, _envelope.UserState),
            _ => null,
        };

    private void NotifyPendingCompletionWatchersIfTerminal()
    {
        if (CompletionResult() is not { } result)
        {
            return;
        }

        foreach (var watcher in _pendingCompletionWatchers)
        {
            watcher.Tell(result);
            Context.Unwatch(watcher);
        }

        _pendingCompletionWatchers.Clear();
    }


    /// <summary>
    /// <paramref name="seqNrUpdate"/>/<paramref name="ledgerUpdate"/> let the <c>Akka.Delivery</c>
    /// dedup/idempotency bookkeeping (see <see cref="HandleDelivery"/>) ride in the SAME <see cref="Persist"/>
    /// call as the transition itself — one atomic write covering both, so a crash between them can
    /// never leave a transition applied without its seqNr/ledger record (or vice versa). <c>null</c> for
    /// both on the still-plain <see cref="HandleExternalCommand"/> path, which has neither.
    /// <paramref name="parentRelationshipUpdate"/> is the same idea for a child-start envelope's
    /// <see cref="WorkflowEnvelope.ParentRelationship"/> — set once, atomically with the command's own
    /// effect, by <see cref="HandleDelivery"/>'s command-dispatch path.
    /// </summary>
    /// <summary>
    /// Plans <paramref name="transition"/>, writes the facts it produces, and carries out what
    /// follows. <paramref name="extraEvents"/> are additional facts this same action established —
    /// a delivery's sequence number, an idempotency key, a child's report — written in the same
    /// atomic batch so a crash can never record one without the others (guarantee D5).
    /// </summary>
    private void PersistEnvelopeThen(
        PersistenceEffect<TState> persistence, Transition transition, TransitionCause cause,
        Action? afterPersist = null,
        IReadOnlyList<WorkflowEvent>? extraEvents = null)
    {
        var plan = WorkflowTransitionPlanner.Plan(
            _envelope, transition, persistence, _timeProvider.GetUtcNow(), _settings,
            new WorkflowInstanceIdentity(_persistenceId, _entityId, _workflow.WorkflowTypeName),
            cause,
            _tracing.LastActivityTraceParent);

        var events = extraEvents is { Count: > 0 }
            ? extraEvents.Concat(plan.Events).ToList()
            : plan.Events;

        PersistEventsThen(events, plan.AfterPersist, afterPersist);
    }

    /// <summary>
    /// Snapshots when <see cref="_snapshotPolicy"/> says to (terminal, or every N persisted events —
    /// see <see cref="Execution.SnapshotPolicy"/>). Called once a whole batch is durable, so
    /// <see cref="Akka.Persistence.Eventsourced.LastSequenceNr"/> already covers every event in it.
    /// </summary>
    private void MaybeSnapshot(WorkflowRuntimeState<TState> persisted)
    {
        if (_snapshotPolicy.ShouldSnapshot(persisted.Status, LastSequenceNr))
        {
            SaveSnapshot(persisted);
        }
    }

    /// <summary>
    /// Writes <paramref name="events"/> as one atomic batch, folds each into the live envelope as it
    /// lands, then carries out <paramref name="decisions"/>.
    ///
    /// The fold here is the same function recovery uses, so a live instance and a recovered one
    /// cannot disagree about what these events meant. Decisions run only after the last event is
    /// durable, which is guarantee D1.
    /// </summary>
    private void PersistEventsThen(
        IReadOnlyList<WorkflowEvent> events,
        IReadOnlyList<WorkflowDecision> decisions,
        Action? afterPersist = null)
    {
        if (events.Count == 0)
        {
            ApplyDecisions(decisions);
            afterPersist?.Invoke();
            return;
        }

        var remaining = events.Count;

        // Tagged is the journal's own wrapper: it strips the tags on the way in and hands recovery
        // back the plain event, so this actor's Recover<WorkflowEvent> and the fold are untouched.
        // Tagging happens here, at the write site, so it stays independent of which journal plugin an
        // application configures. An IEventAdapter is registered per journal, where several workflow
        // types share one and none of them is identifiable from an event alone — the per-type tag
        // needs this actor's own knowledge of which workflow it is.
        // An event that moves a deadline carries one extra tag, which is what lets a reader follow
        // deadline changes across every instance while reading a small fraction of the journal.
        var tagged = new Tagged[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            tagged[i] = new Tagged(e, WorkflowEventTags.MovesADeadline(e) ? _deadlineEventTags : _eventTags);
        }

        PersistAll(
            tagged,
            persisted =>
        {
            var @event = (WorkflowEvent)persisted.Payload;
            _envelope = WorkflowEventFold.Apply(_envelope, @event);
            PublishPersisted(@event);
            if (--remaining > 0)
            {
                return;
            }

            MaybeSnapshot(_envelope);
            ApplyDecisions(decisions);
            afterPersist?.Invoke();
        });
    }

    /// <summary>
    /// Carries out a plan's decisions in order, now that its envelope is durably written. Every
    /// decision is post-persist by construction — a plan hands over its decisions alongside the
    /// envelope the caller has yet to write, so there is no arrangement in which one runs early
    /// (guarantee D1).
    /// </summary>
    private void ApplyDecisions(IReadOnlyList<WorkflowDecision> decisions)
    {
        foreach (var decision in decisions)
        {
            switch (decision)
            {
                case WorkflowDecision.RecordStatusChange rsc:
                    WorkflowDiagnostics.RecordStatusChange(_workflow.WorkflowTypeName, rsc.Status);
                    break;
                case WorkflowDecision.RecordPauseDuration rpd:
                    WorkflowDiagnostics.RecordPauseDuration(_workflow.WorkflowTypeName, rpd.Duration);
                    break;
                case WorkflowDecision.RecordSuspendedDuration rsd:
                    WorkflowDiagnostics.RecordSuspendedDuration(_workflow.WorkflowTypeName, rsd.Duration);
                    break;
                case WorkflowDecision.RecordOutcome ro:
                    WorkflowDiagnostics.RecordOutcome(_workflow.WorkflowTypeName, ro.Outcome);
                    break;
                case WorkflowDecision.ArmTimer { Kind: WorkflowTimerKind.Workflow } arm:
                    WarnIfDeadlineOutlastsResidency(arm.Deadline);
                    ArmWorkflowTimeout(arm.Deadline);
                    break;
                case WorkflowDecision.ArmTimer { Kind: WorkflowTimerKind.Pause } arm:
                    WarnIfDeadlineOutlastsResidency(arm.Deadline);
                    ArmPauseTimeout(arm.Deadline);
                    break;
                case WorkflowDecision.ArmTimer { Kind: WorkflowTimerKind.Hold } arm:
                    WarnIfDeadlineOutlastsResidency(arm.Deadline);
                    ArmHoldTimeout(arm.Deadline);
                    break;
                case WorkflowDecision.ArmTimer { Kind: WorkflowTimerKind.ChildGroup, Discriminator: { } groupId } arm:
                    WarnIfDeadlineOutlastsResidency(arm.Deadline);
                    ArmChildGroupTimeout(groupId, arm.Deadline);
                    break;
                case WorkflowDecision.CancelTimer { Kind: WorkflowTimerKind.Workflow }:
                    _timeouts.CancelWorkflow();
                    break;
                case WorkflowDecision.CancelTimer { Kind: WorkflowTimerKind.Pause }:
                    _timeouts.CancelPause();
                    break;
                case WorkflowDecision.CancelTimer { Kind: WorkflowTimerKind.Hold }:
                    _timeouts.CancelHold();
                    break;
                case WorkflowDecision.CancelTimer { Kind: WorkflowTimerKind.ChildGroup, Discriminator: { } groupId }:
                    _timeouts.CancelChildGroup(groupId);
                    break;
                case WorkflowDecision.StartChild startChild:
                    // A false return means this parent must end (unregistered child type) — stop
                    // acting on further decisions for an instance that has already decided to end,
                    // and avoid stacking another nested Persist per remaining relationship.
                    if (!_children.TrySendChildStart(startChild.Relationship, out var unregisteredTypeError))
                    {
                        PersistEnvelopeThen(
                            PersistenceEffect<TState>.NoPersistence.Instance,
                            new Transition.TerminalTransition(new WorkflowOutcome.Failed(new WorkflowFailure(unregisteredTypeError!))),
                            new TransitionCause.Control("ChildStartFailed"));
                        return;
                    }

                    break;
                case WorkflowDecision.TerminateChild terminateChild:
                    _children.SendTerminate(terminateChild.Relationship);
                    break;
                case WorkflowDecision.CancelChild cancelChild:
                    _children.SendCancel(cancelChild.Relationship, cancelChild.Reason);
                    break;
                case WorkflowDecision.DeleteChild deleteChild:
                    _children.SendDelete(deleteChild.Relationship);
                    break;
                case WorkflowDecision.NotifyParent notifyParent:
                    _children.SendChildLifecycleNotification(
                        notifyParent.Relationship, notifyParent.Outcome, _envelope.UserState,
                        _tracing.LastActivityTraceParent ?? _envelope.LastTraceParent);
                    break;
                case WorkflowDecision.ReclaimHistory:
                    // Snapshot the fresh envelope, then release everything below it once that
                    // snapshot is durable (see HandleSnapshotSuccess). Taken unconditionally, on the
                    // spot, is what makes the release safe: the events being dropped are the only
                    // other record of this state, so nothing may skip taking it here.
                    _reclaimingHistory = true;
                    SaveSnapshot(_envelope);
                    break;
                case WorkflowDecision.PurgeAndStop:
                    // Converges on the same physical purge the external Delete command triggers. No
                    // caller waits on completion here: the business command that produced this
                    // transition gets its own reply independently.
                    PurgeThenStop(onDone: () => { });
                    break;
                case WorkflowDecision.StartStep:
                    if (!StopIfShuttingDown())
                    {
                        StartStep();
                    }

                    break;
                case WorkflowDecision.ReleaseDeferredCommands:
                    if (!StopIfShuttingDown())
                    {
                        Stash.UnstashAll();
                    }

                    break;
                case WorkflowDecision.NotifyCompletionWatchers:
                    NotifyPendingCompletionWatchersIfTerminal();
                    break;
            }
        }
    }

    /// <summary>
    /// Graceful handoff in progress (see <see cref="GracefulShutdown"/>): the persisted envelope
    /// already reflects wherever the workflow now stands, and a respawn on the new owning node picks
    /// up from exactly there. Stop without starting more work — the remaining
    /// decisions still run, since stopping is asynchronous and a completion watcher or a parent
    /// waiting on this instance's terminal status must still be told.
    /// </summary>
    private bool StopIfShuttingDown()
    {
        if (!_shuttingDown)
        {
            return false;
        }

        _timeouts.CancelGracefulShutdownDeadline();
        Context.Stop(Self);
        return true;
    }

    /// <summary>
    /// Registered directly against the actor's own mailbox for a bare, non-Delivery
    /// <see cref="ChildLifecycleNotification"/> Tell. <see cref="HandleDelivery"/>'s own branch for
    /// this message type calls <see cref="ApplyChildLifecycleNotification"/> directly, threading
    /// through a delivery to confirm and a seqNr to record.
    /// </summary>
    private void HandleChildLifecycleNotification(ChildLifecycleNotification notification) =>
        ApplyChildLifecycleNotification(notification, seqNrUpdate: null, confirmDelivery: null);

    /// <summary>
    /// Records a child's terminal report, evaluates the owning group's policy, and — once that
    /// policy resolves the group — resumes through the ordinary <see cref="Transition.StepTransition"/>
    /// path. The relationship/group mutation and that transition are one persisted envelope, so a
    /// recovered actor cannot resume twice or forget that it requested straggler termination.
    /// </summary>
    private void ApplyChildLifecycleNotification(
        ChildLifecycleNotification notification, (string ProducerId, long SeqNr)? seqNrUpdate, Action? confirmDelivery)
    {
        // A parent may reach an independent terminal transition while an AwaitChildren group is
        // still open. Its late child notifications must not re-enter the resume step and turn a
        // terminal workflow back into Running; ParentClosePolicy has already decided what to do
        // with those relationships at the terminal persist boundary.
        if (_envelope.Status is WorkflowStatus.Finished or WorkflowStatus.Deleted)
        {
            confirmDelivery?.Invoke();
            return;
        }

        // Locate the reported member before copying anything: a notification for a relationship this
        // instance doesn't know, or one whose group has moved on, costs a single key lookup and no
        // allocation at all.
        var existing = _envelope.Children;
        if (existing is null || !existing.TryGetValue(notification.RelationshipId, out var member))
        {
            confirmDelivery?.Invoke();
            return;
        }
        var group = _envelope.ChildGroups?.GetValueOrDefault(member.GroupId);
        if (group is null || group.Finalized || notification.Generation != group.Generation)
        {
            confirmDelivery?.Invoke();
            return;
        }

        // A member reports exactly once (H4); the seqNr-deduped delivery path already stops a
        // redelivered notification from reaching here twice, but this member's own status is the
        // direct check — cheap, since it is already in hand, and what lets the O(1) tally below trust
        // that every report it counts is genuinely this member's first.
        if (member.Status is not (ChildStatus.Pending or ChildStatus.TerminationRequested))
        {
            confirmDelivery?.Invoke();
            return;
        }

        var memberUpdated = new WorkflowEvent.ChildMemberUpdated(
            notification.RelationshipId, notification.Status, notification.Result,
            notification.Failure, notification.ResultTraceParent);

        // Read directly off the group's own running counters — O(1), no scan of this instance's
        // children. The members themselves are collected below, by the one report that resolves the
        // group and needs them.
        var tally = ChildGroupPolicy.TallyGroup(group, notification.Status);

        // The parent's own handler sees this child before its group's policy is consulted, so it can
        // fold the report into state and — where the report settles the matter on business grounds
        // the policy cannot express — declare the group over.
        //
        // Writing state here is safe because a parent awaiting children runs no step of its own
        // (ChildrenAwaited clears CurrentStepName), so nothing else is touching state at this
        // instant; guarantee C2 has nothing to violate.
        var childResult = InvokeChildResultHandler(member, notification, tally);
        var outcome = childResult?.StopWaiting ?? ChildGroupPolicy.EvaluateGroupOutcome(group, tally);
        if (outcome is null)
        {
            // The group is still open, so this report is all that happened: one small fact naming
            // the single member it concerns, which is what keeps a fan-out linear (guarantee H5).
            PersistEventsThen(
                WithBookkeeping(seqNrUpdate, WithChildResultState(childResult, memberUpdated)),
                Array.Empty<WorkflowDecision>(),
                afterPersist: confirmDelivery);
            return;
        }

        // The group is over, so this is the report that needs its members: the resume step is handed
        // them, and the trace links and any stragglers are read off them.
        var groupMembers = new List<ChildWorkflowRelationship>(tally.Total);
        foreach (var child in existing.Values)
        {
            if (child.GroupId != member.GroupId)
            {
                continue;
            }

            groupMembers.Add(child.RelationshipId == notification.RelationshipId
                ? child with
                {
                    Status = notification.Status,
                    Result = notification.Result,
                    Failure = notification.Failure,
                    ResultTraceParent = notification.ResultTraceParent,
                }
                : child);
        }

        var stragglers = group.RemainingChildrenPolicy == RemainingChildrenPolicy.Terminate
            ? groupMembers.Where(c => c.Status is ChildStatus.Pending or ChildStatus.TerminationRequested).ToList()
            : new List<ChildWorkflowRelationship>();

        var finalized = new WorkflowEvent.ChildGroupFinalized(
            member.GroupId,
            stragglers.Select(c => c.RelationshipId).ToList(),
            _settings.PruneFinalizedChildren);

        // The group has resolved, so its own wait is over whatever it was counting down to.
        _timeouts.CancelChildGroup(member.GroupId);

        var result = new ChildGroupResult(outcome.Value, groupMembers);
        _tracing.SetPendingResumeLinks(StepTracingContext.BuildResultLinks(groupMembers));

        PersistEnvelopeThen(
            childResult?.Persistence ?? PersistenceEffect<TState>.NoPersistence.Instance,
            new Transition.StepTransition(group.ResumeStepName, result),
            new TransitionCause.Control("ChildGroupResolved"),
            afterPersist: () =>
            {
                foreach (var straggler in stragglers)
                {
                    _children.SendTerminate(straggler);
                }

                confirmDelivery?.Invoke();
            },
            extraEvents: WithBookkeeping(seqNrUpdate, memberUpdated, finalized));
    }

    /// <summary>Prefixes a delivery's sequence number, when there is one, so it is written in the
    /// same batch as the facts it accompanies (guarantee D5).</summary>
    /// <summary>
    /// Runs this workflow's <c>[WorkflowChildResult]</c> handler for a settled child, or returns
    /// <c>null</c> where it declares none — which is the common case, and costs a parent nothing.
    /// </summary>
    private ChildResultEffect<TState>? InvokeChildResultHandler(
        ChildWorkflowRelationship member,
        ChildLifecycleNotification notification,
        ChildGroupPolicy.ChildGroupTally tally)
    {
        if (!((IWorkflowChildResultDispatcher<TState>)_workflow).TryGetChildResultHandler(out var descriptor))
        {
            return null;
        }

        return descriptor.Invoke(_workflow, new ChildResultContext<TState>(
            _envelope.UserState,
            member,
            notification.Status,
            notification.Result,
            notification.Failure,
            tally.Settled,
            tally.Total));
    }

    /// <summary>Prepends the handler's state change, when it made one, so it lands in the same
    /// atomic batch as the report that produced it.</summary>
    private static WorkflowEvent[] WithChildResultState(
        ChildResultEffect<TState>? childResult, params WorkflowEvent[] events) =>
        childResult?.Persistence is PersistenceEffect<TState>.UpdateState updated
            ? events.Prepend<WorkflowEvent>(new WorkflowEvent.UserStateChanged<TState>(updated.NewState)).ToArray()
            : events;

    private static IReadOnlyList<WorkflowEvent> WithBookkeeping(
        (string ProducerId, long SeqNr)? seqNrUpdate, params WorkflowEvent[] events) =>
        seqNrUpdate is { } s
            ? events.Prepend<WorkflowEvent>(new WorkflowEvent.SeqNrRecorded(s.ProducerId, s.SeqNr)).ToList()
            : events;

    /// <summary>
    /// Announces a durably-written event to anything watching this <c>ActorSystem</c>, immediately
    /// and best-effort. A subscriber that misses one reads it back from the journal, which is the
    /// transport that promises delivery — see <see cref="WorkflowFeedItem"/>.
    ///
    /// <see cref="WorkflowFeedItem.Position"/> stays null here: resuming needs a position in the
    /// journal's own global order, which a live in-process publish has no knowledge of.
    /// </summary>
    private void PublishPersisted(WorkflowEvent persisted)
    {
        // Delivery bookkeeping records how a message arrived — transport detail, distinct from
        // anything that happened to the workflow. The journal keeps it so dedup survives a crash;
        // a subscriber watching the run has no use for it, so the feed leaves it out. Any reader of
        // the stored events applies this same skip, which is what keeps both transports carrying the
        // same sequence.
        if (persisted is WorkflowEvent.SeqNrRecorded or WorkflowEvent.IdempotencyRecorded)
        {
            return;
        }

        Context.System.EventStream.Publish(new WorkflowFeedItem(
            Position: null,
            EntityId: _entityId,
            WorkflowType: _workflow.WorkflowTypeName,
            SequenceNr: LastSequenceNr,
            Timestamp: _timeProvider.GetUtcNow(),
            Event: persisted));
    }

    private void StartStep()
    {
        var stepName = _envelope.CurrentStepName!;
        if (!_workflow.TryGetStep(stepName, out var descriptor))
        {
            // Guarantee E5: this instance is persisted on a step this deployment has no code for, so
            // it is held there with everything a resume needs — see
            // WorkflowTransitionPlanner.PlanUnknownStep.
            PersistEnvelopeThen(
                PersistenceEffect<TState>.NoPersistence.Instance,
                WorkflowTransitionPlanner.PlanUnknownStep(stepName),
                new TransitionCause.Control("UnknownStep"));
            return;
        }

        var epoch = ++_stepEpoch;
        var input = _envelope.CurrentStepInput;
        var attempt = _envelope.RetryCount + 1;

        // Cancelled on Suspend/Terminate/StepTimedOut/graceful-handoff grace expiry — cooperative
        // only (see StepDescriptor's doc comment); a step that doesn't declare a CancellationToken
        // parameter never observes it, and simply keeps running orphaned same as before.
        _currentStepCts?.Dispose();
        _currentStepCts = new CancellationTokenSource();

        // Span creation/lifecycle lives in StepDescriptor.Invoke now (Core) — tied to the returned
        // Task itself, avoiding this actor having to manually track it across the PipeTo hop (see
        // that method's doc comment). This just supplies what only the runtime knows: where the span
        // fits (StepTracingContext.ResolveParentContext/ConsumeRecoveredLink) and the
        // persistence-id/step/attempt tags. configureActivity runs synchronously before any await, so
        // _tracing.CurrentStepActivity is set before StartStep returns — needed for
        // ForceCloseCurrentStepActivity's early-close path, since Invoke can't reliably run its own
        // finally block for a step that never honors cancellation. Each retry attempt gets its own
        // distinct span, so a flaky step's history is visible as separate spans in a trace waterfall.
        var task = descriptor.Invoke(
            _workflow, _envelope.UserState, input, attempt, _currentStepCts.Token, _entityId,
            _tracing.ResolveParentContext(),
            StepTracingContext.CombineLinks(_tracing.ConsumeRecoveredLink(), _tracing.ConsumeResumeLinks()),
            a =>
            {
                _tracing.CurrentStepActivity = a;
                a?.SetTag("workflow.persistence_id", _persistenceId);
                a?.SetTag("workflow.step", stepName);
                a?.SetTag("workflow.step_attempt", attempt);
                a?.SetTag("workflow.node", _nodeAddress);
            });

        // Deliberately does not update _tracing.LastActivityTraceParent here — see
        // HandleStepCompleted/Failed, which update it once the step's actual OUTCOME is known.
        // Updating it here would make every retry parent off the previous attempt, forming a chain.
        // Each attempt shares the same parent this step itself was started under, as siblings.
        _tracing.CurrentStepStartedAt = _timeProvider.GetUtcNow();
        _stepInFlight = true;

        _timeouts.CancelStep();
        _timeouts.CancelRetryDelay();
        if (_envelope.StepDeadline is { } deadline)
        {
            _timeouts.Step = _timeoutScheduler.ScheduleTimeout(
                deadline - _timeProvider.GetUtcNow(), Self, new StepTimedOut(stepName, epoch));
        }

        task.PipeTo(Self,
            success: stepEffect => new StepCompleted(stepName, epoch, stepEffect),
            failure: ex => new StepFailed(stepName, epoch, ex));
    }

    /// <summary>
    /// Says so, once per instance, when this instance is about to wait longer than it will stay
    /// resident and nothing is watching its deadline for it.
    ///
    /// Both conditions have to hold for lateness to be possible, and both are settled by the moment
    /// this runs, which is why the check sits here and not at registration: an instance that
    /// passivates fires its deadline whenever something next activates it (guarantee <c>D8</c>), and
    /// a deadline scheduler is what bounds that
    /// (<c>D8b</c>). A deployment whose deadlines all land inside the passivation window needs
    /// neither, and hears nothing.
    ///
    /// Checking at the moment of arming also sidesteps the order the host's builder calls happen in,
    /// since by now every one of them has run.
    /// </summary>
    private void WarnIfDeadlineOutlastsResidency(DateTimeOffset deadline)
    {
        // A null interval means passivation is off, so this instance stays resident and holds its own
        // timer for as long as it takes.
        if (_deadlineResidencyWarned || _keepAliveInterval is not { } interval)
        {
            return;
        }

        // WithWorkflow derives the keep-alive interval as half the passivation window, so the window
        // is twice it.
        var residency = interval + interval;
        if (deadline - _timeProvider.GetUtcNow() <= residency)
        {
            return;
        }

        _deadlineResidencyWarned = true;
        if (Deadlines.WorkflowDeadlineSchedulerProvider.Instance.Apply(Context.System).IsConfigured)
        {
            return;
        }

        Context.GetLogger().Warning(
            "Workflow {0}/{1} is waiting until {2}, past the {3} it stays resident for, and this "
            + "ActorSystem runs no deadline scheduler. The deadline will fire whenever something next "
            + "activates the instance. Call WithWorkflowDeadlines(readJournalPluginId) to have it woken "
            + "on time, or set PassivateIdleEntityAfter to TimeSpan.Zero via configureShardOptions to "
            + "hold instances resident.",
            _workflow.WorkflowTypeName, _entityId, deadline, residency);
    }

    private void ArmWorkflowTimeout(DateTimeOffset deadline)
    {
        _timeouts.CancelWorkflow();
        _timeouts.Workflow = _timeoutScheduler.ScheduleTimeout(deadline - _timeProvider.GetUtcNow(), Self, new WorkflowTimedOut());
    }

    private void ArmPauseTimeout(DateTimeOffset deadline)
    {
        _timeouts.CancelPause();
        _timeouts.Pause = _timeoutScheduler.ScheduleTimeout(deadline - _timeProvider.GetUtcNow(), Self, new PauseTimedOut());
    }

    private void ArmHoldTimeout(DateTimeOffset deadline)
    {
        _timeouts.CancelHold();
        _timeouts.Hold = _timeoutScheduler.ScheduleTimeout(deadline - _timeProvider.GetUtcNow(), Self, new HoldTimedOut());
    }

    private void ArmChildGroupTimeout(string groupId, DateTimeOffset deadline) =>
        _timeouts.SetChildGroup(
            groupId,
            _timeoutScheduler.ScheduleTimeout(
                deadline - _timeProvider.GetUtcNow(), Self, new ChildGroupTimedOut(groupId)));

    private void HandleSnapshotSuccess(SaveSnapshotSuccess msg)
    {
        // The event stream is kept whole, so a projection can be built from an instance's first event
        // and rebuilt from zero whenever a read model needs reshaping. Snapshots bound how far back
        // recovery replays; reclaiming the events themselves belongs to whoever decides an instance's
        // history has served its purpose, through delete/purge or a restart.
        if (_reclaimingHistory)
        {
            _reclaimingHistory = false;
            // This snapshot holds the fresh cycle's envelope in full, so everything at or below it has
            // become redundant. The release waits for right here, after the snapshot lands, which is
            // what keeps the restart durable: a crash in between replays the old events plus
            // RunRestarted and folds to this same envelope.
            DeleteMessages(msg.Metadata.SequenceNr);
        }

        // Akka.Persistence's in-mem snapshot store treats SnapshotSelectionCriteria(maxSequenceNr: 0)
        // as an unbounded match-everything wildcard: it matches every snapshot regardless of
        // sequence number, a surprising interpretation of 0 as a bound. Calling
        // DeleteSnapshots after the very first-ever snapshot would wipe every snapshot for this
        // persistenceId, including ones written after this delete was issued but before it
        // completed: real, total data loss, reachable in ordinary operation. There's nothing older to
        // prune after the first snapshot anyway, so skipping is correct on its own terms, whatever the
        // exact store semantics turn out to be.
        if (msg.Metadata.SequenceNr > 1)
        {
            DeleteSnapshots(new SnapshotSelectionCriteria(maxSequenceNr: msg.Metadata.SequenceNr - 1));
        }
    }

    private void OnRecoveryCompleted()
    {
        // Cross-restart/cross-node trace continuity: the first span created in this new episode
        // (whichever comes first — a resumed step, or the next external command) links back to
        // wherever the trace left off, preserving that continuity as a link into a fresh, bounded
        // trace per restart — never merging into one trace that would sprawl across every
        // relocation of a long-lived workflow.
        _tracing.RecordRecoveredLink(_envelope.LastTraceParent);

        if (_envelope.WorkflowDeadline is { } workflowDeadline && _envelope.Status is WorkflowStatus.Running or WorkflowStatus.Paused)
        {
            ArmWorkflowTimeout(workflowDeadline);
        }

        if (_envelope.Status == WorkflowStatus.Running && _envelope.CurrentStepName is not null)
        {
            // A retry backoff wait in progress at the moment of the crash/rebalance resumes exactly
            // its *remaining* delay here — RetryDelayUntil carries the same durability as
            // StepDeadline/WorkflowDeadline/PauseDeadline. Already-elapsed (the
            // entity was down longer than the remaining delay) falls through to starting right away,
            // same "fires late, never lost" tradeoff as every other timer here.
            if (_envelope.RetryDelayUntil is { } retryDelayUntil && retryDelayUntil > _timeProvider.GetUtcNow())
            {
                _timeouts.RetryDelay = _timeoutScheduler.ScheduleTimeout(
                    retryDelayUntil - _timeProvider.GetUtcNow(), Self, new RetryDue(_envelope.CurrentStepName, _stepEpoch));
            }
            else
            {
                StartStep();
            }
        }
        else if (_envelope.Status == WorkflowStatus.Paused && _envelope.PauseDeadline is { } pauseDeadline)
        {
            ArmPauseTimeout(pauseDeadline);
        }
        else if (_envelope.Status == WorkflowStatus.Suspended && _envelope.HoldDeadline is { } holdDeadline)
        {
            ArmHoldTimeout(holdDeadline);
        }

        // Every group still waiting gets its own timer back, from the instant it recorded when it
        // opened — so a group's wait resumes at its remaining length like every other deadline.
        foreach (var group in _envelope.ChildGroups?.Values ?? Enumerable.Empty<ChildGroupState>())
        {
            if (group is { Finalized: false, Deadline: { } groupDeadline })
            {
                ArmChildGroupTimeout(group.GroupId, groupDeadline);
            }
        }

        // A relationship is persisted before its first child-start/terminate Tell. If this actor
        // died in that gap, no delivery layer could have buffered a message it never received;
        // relationship state is therefore the durable source of truth for recovery redelivery.
        foreach (var child in _envelope.Children?.Values ?? Enumerable.Empty<ChildWorkflowRelationship>())
        {
            switch (child.Status)
            {
                case ChildStatus.Pending:
                    if (!_children.TrySendChildStart(child, out var unregisteredTypeError))
                    {
                        PersistEnvelopeThen(
                            PersistenceEffect<TState>.NoPersistence.Instance,
                            new Transition.TerminalTransition(new WorkflowOutcome.Failed(new WorkflowFailure(unregisteredTypeError!))),
                            new TransitionCause.Control("ChildStartFailed"));
                        return;
                    }
                    break;
                case ChildStatus.TerminationRequested:
                    _children.SendTerminate(child);
                    break;
            }
        }
    }

    private sealed record StepCompleted(string StepName, int Epoch, StepEffect<TState> Effect);

    private sealed record StepFailed(string StepName, int Epoch, Exception Exception);

    private sealed record StepTimedOut(string StepName, int Epoch);

    private sealed record RetryDue(string StepName, int Epoch);

    private sealed record WorkflowTimedOut;

    private sealed record PauseTimedOut;

    private sealed record HoldTimedOut;

    private sealed record ChildGroupTimedOut(string GroupId);

    private sealed record GracefulShutdownGraceExpired;

    private sealed record QueryCompleted(long Id, QueryEffect Effect);

    private sealed record QueryFailed(long Id, Exception Cause);

    private sealed record QueryTimedOut(long Id);

    /// <summary>One query still running: who is waiting for it, the token cancelled when this actor
    /// gives up on it, and the scheduled deadline that does the giving up.</summary>
    private sealed record InFlightQuery(IActorRef ReplyTo, CancellationTokenSource Cts, ICancelable Deadline);
}
