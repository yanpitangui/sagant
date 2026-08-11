namespace Sagant.Runtime.Akka;

/// <summary>
/// The default <c>ShardOptions.HandOffStopMessage</c> for workflow entities (see
/// <see cref="Clustering.WorkflowClusterShardingExtensions.WithWorkflow{TWorkflow, TState}"/>) —
/// sent by <c>ClusterSharding</c> in place of the default <c>PoisonPill</c> when this entity's
/// shard is being rebalanced or its region is shutting down.
///
/// <c>PoisonPill</c> only drains the actor's own mailbox before stopping, with no awareness that a
/// step's <c>Task</c> may still be running off-actor-thread (fire-and-<c>PipeTo</c>, by design; see
/// <see cref="WorkflowEntityActor{TWorkflow, TState}"/>'s doc comment) — killing the actor out from
/// under an in-flight step with no chance to record whatever that step's real-world side effect
/// already did. This message lets an in-flight step run to its own completion (bounded — see the
/// <c>gracefulShutdownGrace</c> parameter on <c>WithWorkflow</c>) and persist normally, with no
/// further step started here — the persisted envelope already reflects wherever the workflow now
/// stands, and a respawn on the new owning node picks up exactly there, same as any other
/// interruption. If nothing is in flight, this is an immediate, ordinary stop.
/// </summary>
public sealed record GracefulShutdown;
