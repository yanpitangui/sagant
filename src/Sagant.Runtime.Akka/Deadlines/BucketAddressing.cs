using Akka.Cluster.Sharding;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>Routing wrapper for a bucket entity, carrying the bucket it is for alongside the
/// message. <see cref="BucketMessageExtractor"/> strips it before delivery.</summary>
internal sealed record BucketEnvelope(string BucketId, object Message);

/// <summary>
/// Routes <see cref="BucketEnvelope"/>s for <c>ClusterSharding</c>. Extends
/// <see cref="HashCodeMessageExtractor"/> for the same reason
/// <c>Clustering.WorkflowMessageExtractor</c> does: its hash holds across independent process runs,
/// so every node derives the same shard for a bucket id and one bucket has one live entity.
/// </summary>
internal sealed class BucketMessageExtractor : HashCodeMessageExtractor
{
    public BucketMessageExtractor(int numberOfShards = 100) : base(numberOfShards)
    {
    }

    public override string EntityId(object message) => message switch
    {
        BucketEnvelope be => be.BucketId,
        _ => throw new InvalidOperationException(
            $"Unexpected message routed to deadline bucket sharding: {message.GetType()}. "
            + "Everything reaching this extractor arrives as a BucketEnvelope."),
    };

    public override object EntityMessage(object message) => message switch
    {
        BucketEnvelope be => be.Message,
        _ => message,
    };
}
