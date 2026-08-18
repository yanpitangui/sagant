using Sagant.Protocol;
using Sagant.Descriptors;
using Sagant.Runtime.Akka;
using Sagant.Runtime.Akka.Execution;
using Sagant.Runtime.Akka.Serialization;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding.Delivery;
using Akka.Hosting;
using Akka.Util;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Registers a workflow type on <c>ClusterSharding</c> via <c>Akka.Hosting</c>'s fluent builder —
/// not raw HOCON. One active <see cref="WorkflowEntityActor{TWorkflow, TState}"/> per workflow id,
/// cluster-wide, addressed through <see cref="WorkflowRef{TWorkflow, TState}"/>. A single-node
/// cluster (self-join) is a normal, fully-supported configuration — dev/local mode isn't a
/// separate code path.
///
/// Every entity is wrapped in a <c>ShardingConsumerController</c> (the consumer side of
/// <c>Akka.Delivery</c>), and one <c>ShardingProducerController</c>/<see cref="WorkflowProducerAdapter"/>
/// pair per workflow type per <see cref="ActorSystem"/> is spun up at startup — this is what gives
/// <see cref="WorkflowRef{TWorkflow, TState}"/>'s business-command methods (<c>Send</c>/<c>Ask</c>/
/// <c>RunAndAwaitResult</c>) at-least-once delivery. Control commands
/// (<c>Suspend</c>/<c>Resume</c>/<c>Terminate</c>/<c>GetStatus</c>) bypass all of this and still go
/// straight to the shard region.
/// </summary>
public static class WorkflowClusterShardingExtensions
{
    /// <summary>
    /// How long an idle instance stays resident before cluster sharding passivates it. Override it
    /// per deployment through <c>configureShardOptions</c>; <see cref="TimeSpan.Zero"/> there holds
    /// every instance resident until it reaches a terminal status.
    ///
    /// Keep <see cref="Deadlines.WorkflowDeadlineSettings.ExternalArmThreshold"/> below whatever this
    /// is set to, so a deadline the deadline scheduler leaves alone is one the instance is still
    /// resident to fire.
    /// </summary>
    public static readonly TimeSpan DefaultPassivateIdleEntityAfter = TimeSpan.FromSeconds(120);

    /// <param name="configureShardOptions">
    /// Deployment-level shard tuning. Runs after this method sets its own defaults, so the callback
    /// sees — and may override — them: <c>HandOffStopMessage</c> is a <c>GracefulShutdown</c> so an
    /// in-flight step can finish across a rebalance, and <c>PassivateIdleEntityAfter</c> is
    /// <see cref="DefaultPassivateIdleEntityAfter"/>, the same 120-second stock default Akka Cluster
    /// Sharding ships with.
    ///
    /// <para>Passivation is safe for work in progress: an entity running a step or waiting out a
    /// retry backoff announces itself to its own shard at half the idle window, so it stays resident
    /// for as long as the work takes (see <see cref="EntityKeepAlive"/>). What it costs is deadline
    /// lateness — a paused or long-running instance that passivates fires its deadline whenever
    /// something next activates it (guarantee D8), unless <c>WithWorkflowDeadlines</c> is running to
    /// bound that lateness (guarantee D8b). Pass <see cref="TimeSpan.Zero"/> here to hold every
    /// instance resident instead, at the memory cost that implies.</para>
    /// </param>
    /// <param name="producerBufferCapacity">
    /// Depth of the bounded local queue <c>WorkflowProducerAdapter</c> holds pending sends in before
    /// handing them to the <c>ShardingProducerController</c>. A <c>Send</c>/<c>Ask</c>/
    /// <c>RunAndAwaitResult</c> call fails fast with <c>ProducerBufferFullException</c> once this fills
    /// up, never blocking the caller. Raise it if high-throughput callers see spurious
    /// <c>ProducerBufferFullException</c>s; lower it on memory-constrained deployments.
    /// </param>
    /// <param name="configureProducerControllerSettings">
    /// Optional pass-through tuning for <c>ShardingProducerController.Settings</c> — resend
    /// intervals, buffer size, ask timeout.
    /// </param>
    /// <param name="configureConsumerControllerSettings">
    /// Optional pass-through tuning for <c>ShardingConsumerController.Settings</c> — flow-control
    /// window, resend intervals. Runs after <c>AllowBypass</c> is forced to <c>true</c> internally
    /// (see the comment at the call site), so the settings this callback receives already have it
    /// set — but because the callback returns the settings it applies, it <i>can</i> flip
    /// <c>AllowBypass</c> back to <c>false</c>. Doing so isn't forced away; it would silently break
    /// the <c>Suspend</c>/<c>Resume</c>/<c>Terminate</c>/<c>GetStatus</c> path for this workflow type.
    /// </param>
    /// <param name="snapshotEveryNEvents">
    /// How often <see cref="WorkflowEntityActor{TWorkflow, TState}"/> takes a periodic snapshot,
    /// counted in persisted events since the last one — see <see cref="Execution.SnapshotPolicy"/>.
    /// A snapshot is always taken once a transition makes the workflow terminal regardless of this
    /// value; this only governs the cadence while it's still running/paused/suspended. Bounds
    /// recovery replay depth to roughly this many trailing events — one transition can write several
    /// at once, so a batch crossing the threshold carries its remainder past it. Deployment-level knob (I/O vs.
    /// recovery-latency tradeoff), so it lives here alongside <paramref name="gracefulShutdownGrace"/>/<paramref name="timeProvider"/>.
    /// </param>
    public static AkkaConfigurationBuilder WithWorkflow<TWorkflow, TState>(
        this AkkaConfigurationBuilder builder,
        Func<TWorkflow> workflowFactory,
        IWorkflowTimeoutScheduler? timeoutScheduler = null,
        Action<ShardOptions>? configureShardOptions = null,
        int numberOfShards = 100,
        TimeSpan? gracefulShutdownGrace = null,
        TimeProvider? timeProvider = null,
        int producerBufferCapacity = 1024,
        Func<ShardingProducerController.Settings, ShardingProducerController.Settings>? configureProducerControllerSettings = null,
        Func<ShardingConsumerController.Settings, ShardingConsumerController.Settings>? configureConsumerControllerSettings = null,
        int snapshotEveryNEvents = 10)
        where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>, IWorkflowTypeInfo
    {
        var typeName = typeof(TWorkflow).Name;

        // Binds WorkflowRuntimeStateSerializer to this call's own closed WorkflowRuntimeState<TState>
        // — scoped to this one type, so nothing else this ActorSystem serializes is affected. A
        // second WithWorkflow call for a different TState adds its own binding line alongside this
        // one; the "serializers" line naming the class is identical every time, so repeating it
        // across calls on the same ActorSystem is harmless. See WorkflowRuntimeStateSerializer's own
        // doc comment for why this exists.
        builder = builder.AddHocon(WorkflowRuntimeStateSerializerSetup.HoconFor<TState>(), HoconAddMode.Prepend);

        // SagantSerializer covers the engine's own closed types (WorkflowEvent and the rest — see its
        // own doc comment); the same HOCON regardless of TState, so repeating it across multiple
        // WithWorkflow calls on one ActorSystem is harmless.
        builder = builder.AddHocon(SagantSerializerSetup.Hocon, HoconAddMode.Prepend);
        var shardOptions = new ShardOptions
        {
            // Default hand-off-stop message: lets an in-flight step (fire-and-PipeTo, running
            // off-actor-thread) finish and persist normally across a hand-off, where the default
            // PoisonPill would kill it out from under itself — see GracefulShutdown's doc comment.
            // Still overridable via configureShardOptions below for callers who want the plain
            // PoisonPill back.
            HandOffStopMessage = new GracefulShutdown(),

            // Idle passivation on at ClusterSharding's own 120-second window, so an instance holds
            // memory while it is doing something and releases it while it waits. Two mechanisms make
            // that safe. Work in progress keeps its instance resident, because a step running
            // off-actor-thread announces itself to the shard (guarantee D8a, see keepAliveInterval
            // below). A deadline further out than the window is recorded by
            // WithWorkflowDeadlines(...), which wakes the instance when it comes due (guarantee D8b).
            //
            // A deployment that starts no deadline scheduler gets D8's lateness on the deadlines that
            // outlast this window: they fire whenever something next activates the instance. Setting
            // this to TimeSpan.Zero through configureShardOptions holds every instance resident
            // instead, at the memory cost that implies.
            PassivateIdleEntityAfter = DefaultPassivateIdleEntityAfter,
        };
        configureShardOptions?.Invoke(shardOptions);

        // Half the idle window, so an entity holding work is at most half a window away from its last
        // announcement whenever the shard's own tick — which runs at that same cadence — looks. A
        // deployment leaving passivation off has nothing to announce to, so the tick never runs.
        // See EntityKeepAlive.
        var keepAliveInterval = shardOptions.PassivateIdleEntityAfter is { Ticks: > 0 } idleWindow
            ? TimeSpan.FromTicks(idleWindow.Ticks / 2)
            : (TimeSpan?)null;

        // This WithShardRegion overload calls entityPropsFactory(system, registry, resolver)
        // synchronously, before starting the shard region (ClusterSharding.StartAsync) — ActorSystem
        // is available here before any entity could be asked to start.
        builder = builder.WithShardRegion<TWorkflow>(
            typeName,
            (system, _, _) =>
            {
                // AllowBypass = true is what keeps Suspend/Resume/Terminate/GetStatus working: those
                // control commands travel as plain Tell/Ask straight to the shard region, never
                // wrapped in a WorkflowEnvelope, so they never enter the Akka.Delivery protocol this
                // ShardingConsumerController implements. With AllowBypass true, ShardingConsumerController
                // forwards any message that isn't part of that protocol straight through to this child
                // entity actor with Sender preserved (so Ask's implicit reply-to still works) — the
                // mechanism this reference runtime relies on to reach the entity actor at all for
                // these commands.
                var consumerSettings = ShardingConsumerController.Settings.Create(system) with { AllowBypass = true };
                consumerSettings = configureConsumerControllerSettings?.Invoke(consumerSettings) ?? consumerSettings;

                // Settings, tag sets and empty ledgers are the same for every instance this
                // registration starts, and deriving them is the most expensive thing an activation
                // does. Resolved once here, from one workflow built for the purpose, and read by
                // every entity this registration starts — see WorkflowTypeProfile.
                // The registry is how an entity reaches it: Props.Create hands its constructor
                // arguments to Activator, which binds to public constructors, so the argument list
                // carries what the public surface declares and this travels beside it.
                WorkflowTypeProfileRegistryProvider.Instance.Apply(system)
                    .Register<TWorkflow, TState>(WorkflowTypeProfile<TState>.For(workflowFactory(), system.Settings.Config));

                return entityId => ShardingConsumerController.Create<WorkflowEnvelope>(
                    consumerController => Props.Create(() => new WorkflowEntityActor<TWorkflow, TState>(
                        typeName + "-" + entityId, workflowFactory, consumerController, timeoutScheduler, gracefulShutdownGrace, timeProvider,
                        snapshotEveryNEvents, entityId, keepAliveInterval)),
                    consumerSettings);
            },
            new WorkflowMessageExtractor(numberOfShards),
            shardOptions);

        // Akka.Hosting resolves shard regions asynchronously, at host start — this defers populating
        // the per-ActorSystem WorkflowHandleRegistry (see
        // WorkflowClientRegistrationExtensions) until the region is actually resolvable via
        // ActorRegistry, mirroring the ActorRegistry.For(system).GetAsync<TWorkflow>() pattern used
        // elsewhere in this codebase (e.g. the OrderFulfillment sample/test harness). The producer
        // side (ShardingProducerController + WorkflowProducerAdapter) is spun up in this same
        // callback, for the same reason — it needs the resolved shard region too.
        return builder.AddStartup(async (system, actorRegistry) =>
        {
            // Forwarding is workflow-type-agnostic, so this fixed name identifies one bridge per
            // ActorSystem shared across every workflow type registered on it. A WithWorkflow call
            // for a second workflow type on the same system hits the name an earlier call already
            // claimed and no-ops here.
            try
            {
                system.ActorOf(Props.Create<WorkflowEventPubSubBridge>(), WorkflowEventPubSubBridge.ActorName);
            }
            catch (InvalidActorNameException)
            {
            }

            var shardRegion = await actorRegistry.GetAsync<TWorkflow>();

            var producerSettings = ShardingProducerController.Settings.Create(system);
            producerSettings = configureProducerControllerSettings?.Invoke(producerSettings) ?? producerSettings;

            // A fresh Guid per producer-actor incarnation keeps each incarnation's producerId unique:
            // a stable producerId reused across restarts would let a restarted producer's own
            // in-memory seqNr counter reset to 1 while the consumer side's persisted
            // HighestAppliedSeqNr for that same id is already past 1 — making genuinely new sends
            // look like stale duplicates and get silently dropped by the dedup check in
            // WorkflowEntityActor.HandleDelivery.
            var producerId = $"{typeName}-{Guid.NewGuid():N}";
            var producerController = system.ActorOf(
                ShardingProducerController.Create<WorkflowEnvelope>(producerId, shardRegion, Option<Props>.None, producerSettings),
                typeName + "-producerController");

            var producerAdapter = system.ActorOf(WorkflowProducerAdapter.Props(producerBufferCapacity), typeName + "-producerAdapter");
            producerAdapter.Tell(new WorkflowProducerAdapter.RegisterProducerController(producerController));

            WorkflowHandleRegistryProvider.Instance.Apply(system).Register<TWorkflow, TState>(shardRegion, producerAdapter);
        });
    }
}
