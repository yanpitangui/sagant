using Sagant.Protocol;
using Sagant.Descriptors;
using Sagant.Runtime.Akka;
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
    /// <param name="configureShardOptions">
    /// Deployment-level shard tuning. Runs after this method sets its own defaults, so the callback
    /// sees — and may override — them: <c>HandOffStopMessage</c> is a <c>GracefulShutdown</c> so an
    /// in-flight step can finish across a rebalance, and <c>PassivateIdleEntityAfter</c> is disabled
    /// so an instance holding a deadline keeps the live timer that fires it.
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
        var shardOptions = new ShardOptions
        {
            // Default hand-off-stop message: lets an in-flight step (fire-and-PipeTo, running
            // off-actor-thread) finish and persist normally instead of being killed out from under
            // it by the default PoisonPill — see GracefulShutdown's doc comment. Still overridable
            // via configureShardOptions below for callers who want the plain PoisonPill back.
            HandOffStopMessage = new GracefulShutdown(),

            // Idle passivation off (TimeSpan.Zero), overriding ClusterSharding's own 120-second
            // default. A workflow instance legitimately sits idle while holding a deadline — a pause
            // awaiting approval, a long workflow timeout — and a live timer belongs to a live
            // instance. Under the stock default, an instance holding a deadline hours away
            // passivates two minutes in, its timer dies with it, and the deadline only fires
            // whenever something next activates the instance (see docs/guarantees.md D8).
            //
            // The cost is memory: instances stay resident until they reach a terminal status. A
            // deployment with many long-lived workflows can trade that back via
            // configureShardOptions, accepting the lateness. The fix that needs neither trade is a
            // deadline sweeper that wakes only instances with a near-due deadline — see
            // docs/deferred-work.md G4.
            PassivateIdleEntityAfter = TimeSpan.Zero,
        };
        configureShardOptions?.Invoke(shardOptions);

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

                return entityId => ShardingConsumerController.Create<WorkflowEnvelope>(
                    consumerController => Props.Create(() => new WorkflowEntityActor<TWorkflow, TState>(
                        typeName + "-" + entityId, workflowFactory, consumerController, timeoutScheduler, gracefulShutdownGrace, timeProvider,
                        snapshotEveryNEvents, entityId)),
                    consumerSettings);
            },
            new WorkflowMessageExtractor(numberOfShards),
            shardOptions);

        // Akka.Hosting resolves shard regions asynchronously at host start, not synchronously here
        // — defer populating the per-ActorSystem WorkflowHandleRegistry (see
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
