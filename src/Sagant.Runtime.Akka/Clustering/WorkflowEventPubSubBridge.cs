using Sagant.Protocol;
using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Subscribes to this <see cref="ActorSystem"/>'s local <c>EventStream</c> for
/// <see cref="WorkflowFeedItem"/> and republishes each one onto <see cref="PubSubTopic"/>, a
/// cluster-wide <c>DistributedPubSub</c> topic — so a subscriber on any node in the cluster receives
/// it, wherever the entity it concerns happens to be hosted. Started once per
/// <see cref="ActorSystem"/> by <see cref="WorkflowClusterShardingExtensions.WithWorkflow{TWorkflow, TState}"/>.
/// </summary>
public sealed class WorkflowEventPubSubBridge : ReceiveActor
{
    /// <summary>Cluster-wide <c>DistributedPubSub</c> topic every <see cref="WorkflowFeedItem"/> is
    /// republished to. One flat topic covering every workflow type and event — a subscriber
    /// pattern-matches the events it cares about after subscribing to this single topic.</summary>
    public const string PubSubTopic = "sagant.workflow-events";

    /// <summary>Actor name this bridge is started under — fixed, so a second
    /// <see cref="WorkflowClusterShardingExtensions.WithWorkflow{TWorkflow, TState}"/> call for a
    /// different workflow type on the same <see cref="ActorSystem"/> always resolves to this same
    /// actor, keeping exactly one bridge alive per <see cref="ActorSystem"/>.</summary>
    public const string ActorName = "sagant-workflow-notification-pubsub-bridge";

    public WorkflowEventPubSubBridge()
    {
        Context.System.EventStream.Subscribe(Self, typeof(WorkflowFeedItem));
        var mediator = DistributedPubSub.Get(Context.System).Mediator;
        Receive<WorkflowFeedItem>(item => mediator.Tell(new Publish(PubSubTopic, item)));
    }
}
