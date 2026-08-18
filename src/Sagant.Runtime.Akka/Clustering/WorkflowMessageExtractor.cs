using Akka.Cluster.Sharding;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Routes <see cref="WorkflowEnvelope"/>s (still-plain <c>Suspend</c>/<c>Resume</c>/
/// <c>Terminate</c>/<c>GetStatus</c> traffic, sent directly by <c>WorkflowRef</c>) for
/// <c>ClusterSharding</c>. Akka.Delivery business-command traffic arrives wrapped in
/// <see cref="ShardingEnvelope"/> on the wire, but <c>ClusterSharding</c>'s own internal
/// <c>ExtractorAdapter</c> (present since Akka.NET v1.5.15) intercepts and strips that outer
/// envelope BEFORE ever calling into this class. So <see cref="EntityId"/> only ever needs to
/// handle <see cref="WorkflowEnvelope"/>, and <see cref="EntityMessage"/>'s fallback branch
/// receives the ALREADY-unwrapped inner payload — for Delivery traffic, a bare
/// <c>ConsumerController.SequencedMessage&lt;WorkflowEnvelope&gt;</c>, with the outer
/// <see cref="ShardingEnvelope"/> already gone.
/// Extends <see cref="HashCodeMessageExtractor"/> (the official pattern — see
/// https://getakka.net/articles/clustering/cluster-sharding.html), whose own <c>ShardId</c> uses a
/// stable hash: the same input produces the same shard id across independent process runs, which
/// matters because .NET randomizes <see cref="string.GetHashCode()"/> per-process (documented: "can
/// differ each time you run your application"). Every node in the cluster must derive the SAME
/// shard id for the same entity id; a raw <c>GetHashCode()</c>-based extractor could let two nodes
/// disagree on which shard a given workflow id belongs to, risking two live copies of the same
/// entity. <see cref="HashCodeMessageExtractor"/>'s stable hash avoids that failure mode, with no
/// need to reimplement it here.
/// </summary>
public sealed class WorkflowMessageExtractor : HashCodeMessageExtractor
{
    public WorkflowMessageExtractor(int numberOfShards = 100) : base(numberOfShards)
    {
    }

    public override string EntityId(object message) => message switch
    {
        WorkflowEnvelope we => we.EntityId,
        _ => throw new InvalidOperationException(
            $"Unexpected message routed to workflow sharding: {message.GetType()}. " +
            "Only WorkflowEnvelope (plain control commands) should ever reach this extractor directly — " +
            "Akka.Cluster.Sharding.ShardingEnvelope (Akka.Delivery business-command traffic) is unwrapped " +
            "automatically by ClusterSharding's own ExtractorAdapter before this method is ever called."),
    };

    public override object EntityMessage(object message) => message switch
    {
        WorkflowEnvelope we => we.Message,
        // Reached for Akka.Delivery business-command traffic: ClusterSharding's ExtractorAdapter already
        // stripped the outer ShardingEnvelope before calling this method, so `message` here is the INNER
        // payload (ConsumerController.SequencedMessage<WorkflowEnvelope>) — pass it through unchanged for
        // ShardingConsumerController (the registered entity Props) to handle.
        _ => message,
    };
}
