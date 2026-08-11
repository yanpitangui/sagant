namespace Sagant.Runtime.Akka;

/// <summary>
/// The <c>stopMessage</c> payload <see cref="WorkflowEntityActor{TWorkflow, TState}"/> hands to
/// <c>Akka.Cluster.Sharding.Passivate</c> once a <c>Delete</c> purge has been confirmed — see
/// <c>WorkflowEntityActor.PurgeThenStop</c>. Sent by the entity to its own <c>Shard</c> parent, then
/// echoed straight back by the <c>Shard</c> once it has recorded this entity as deliberately
/// passivating — the officially-sanctioned way for an entity to proactively retire itself. A
/// <c>Shard</c> with <c>RememberEntities</c> enabled reads a plain <c>Context.Stop(Self)</c> it never
/// saw a matching <c>Passivate</c> for as a crash, and restarts the entity — resurrecting a phantom,
/// empty instance against the journal <c>Delete</c> just wiped. This round trip is what tells the
/// <c>Shard</c> the stop is deliberate, correct under either <c>RememberEntities</c> setting;
/// receiving this message back is what tells the actor it's safe to actually stop.
///
/// Internal: nothing outside this assembly ever needs to construct or configure this type.
/// <c>GracefulShutdown</c> is this codebase's one real, consumer-configurable
/// <c>ShardOptions.HandOffStopMessage</c>; this type only ever flows from the entity to its own
/// <c>Shard</c> and back. A test simulating that echo (the same pattern
/// <c>WorkflowGracefulShutdownTests</c> already uses for <c>GracefulShutdown</c>) reaches it via
/// <c>InternalsVisibleTo</c>.
/// </summary>
internal sealed record PurgeStopMessage;
