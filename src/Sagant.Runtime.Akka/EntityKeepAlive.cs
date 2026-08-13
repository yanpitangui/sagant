namespace Sagant.Runtime.Akka;

/// <summary>
/// Sent by a working entity to itself <em>through its own shard region</em>, and dropped on arrival.
///
/// Cluster sharding decides an entity is idle from the last message it routed to it: the timestamp
/// lives on the <c>Shard</c> and is touched in <c>DeliverMessage</c>, so only traffic that travels
/// through the region counts. An entity holding work of its own — a step running off-actor-thread, a
/// retry backoff waiting out its delay — receives nothing from outside while it does so, and looks
/// idle from where the shard is standing. Passivated there, the work in flight is abandoned and
/// nothing is scheduled to bring the instance back, so the run stalls until some other message
/// happens to arrive.
///
/// The round trip through the region is the whole point: this message exists for the timestamp it
/// touches on the way past, and <see cref="WorkflowEntityActor{TWorkflow, TState}"/> does nothing
/// with it once it lands.
/// </summary>
public sealed record EntityKeepAlive
{
    public static readonly EntityKeepAlive Instance = new();
}
