using Akka.Delivery;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowMessageExtractorTests
{
    private readonly WorkflowMessageExtractor _extractor = new(10);

    [Fact]
    public void EntityId_UnwrapsWorkflowEnvelope()
    {
        var envelope = new WorkflowEnvelope("order-1", "payload");
        Assert.Equal("order-1", _extractor.EntityId(envelope));
    }

    [Fact]
    public void EntityMessage_UnwrapsWorkflowEnvelope()
    {
        var envelope = new WorkflowEnvelope("order-1", "payload");
        Assert.Equal("payload", _extractor.EntityMessage(envelope));
    }

    [Fact]
    public void EntityId_ThrowsOnUnexpectedMessage()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => _extractor.EntityId("not-an-envelope"));
        Assert.Contains("WorkflowEnvelope", ex.Message);
    }

    [Fact]
    public void EntityMessage_PassesThroughAlreadyUnwrappedDeliveryPayload()
    {
        // ClusterSharding's own internal ExtractorAdapter strips the outer Akka.Cluster.Sharding.
        // ShardingEnvelope (Akka.Delivery business-command traffic) before ever calling into our
        // extractor, so EntityMessage only ever sees either a WorkflowEnvelope (plain control
        // commands) or the already-unwrapped inner payload — the real
        // ConsumerController.SequencedMessage<WorkflowEnvelope> Akka.Delivery hands to a
        // ShardingConsumerController. The extractor must pass it through unchanged.
        var innerEnvelope = new WorkflowEnvelope("order-2", "payload");
        var sequencedMessage = new ConsumerController.SequencedMessage<WorkflowEnvelope>(
            "producer-1", 1L, innerEnvelope, First: true, Ack: false);

        Assert.Same(sequencedMessage, _extractor.EntityMessage(sequencedMessage));
    }
}
