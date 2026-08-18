using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Verifies <see cref="WorkflowEventPubSubBridge"/>: a subscriber that never talks to the
/// shard region directly, only to <see cref="DistributedPubSub"/>'s mediator, still receives
/// <see cref="WorkflowFeedItem"/>s for a workflow run driven through the real
/// <see cref="IWorkflowClient"/>/<c>ClusterSharding</c> path.
/// </summary>
public class WorkflowEventPubSubBridgeTests
{
    private sealed class ProbeActor : ReceiveActor
    {
        public ProbeActor(TaskCompletionSource subscribed, TaskCompletionSource<WorkflowFeedItem> received)
        {
            Receive<SubscribeAck>(_ => subscribed.TrySetResult());
            // Delivery bookkeeping (SeqNrRecorded) rides the same topic, so wait for the event that
            // actually names the command.
            Receive<WorkflowFeedItem>(item =>
            {
                if (item.Event is WorkflowEvent.CausedEvent { Cause: TransitionCause.Command })
                {
                    received.TrySetResult(item);
                }
            });
        }
    }

    [Fact]
    public async Task NotificationForEntity_ReachesSubscriberThatOnlyKnowsThePubSubTopic()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("pubsub-bridge-test-system", builder =>
        {
            builder
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<EchoWorkflow, EchoState>(() => new EchoWorkflow());
        }).AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();
        try
        {
            var system = host.Services.GetRequiredService<ActorSystem>();
            var cluster = global::Akka.Cluster.Cluster.Get(system);
            cluster.Join(cluster.SelfAddress);

            using (var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
            {
                while (!cluster.State.Members.Any(m => m.UniqueAddress == cluster.SelfUniqueAddress && m.Status == global::Akka.Cluster.MemberStatus.Up))
                {
                    upCts.Token.ThrowIfCancellationRequested();
                    await Task.Delay(100, upCts.Token);
                }
            }

            var subscribed = new TaskCompletionSource();
            var received = new TaskCompletionSource<WorkflowFeedItem>();
            var probe = system.ActorOf(Props.Create(() => new ProbeActor(subscribed, received)));

            var mediator = DistributedPubSub.Get(system).Mediator;
            // SubscribeAck is sent back to the Tell's Sender — the Subscribe message's own Ref plays
            // no part — so this needs an explicit sender: this Tell comes from plain test code,
            // outside any actor, where Self would otherwise be the implicit sender.
            mediator.Tell(new Subscribe(WorkflowEventPubSubBridge.PubSubTopic, probe), probe);
            await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(10));

            var client = host.Services.GetRequiredService<IWorkflowClient>();
            var reply = await client.For<EchoWorkflow>("pubsub-bridge-1")
                .Request<EchoPing, string>(new EchoPing("hello"), TimeSpan.FromSeconds(15));
            Assert.Equal("accepted", reply);

            var notification = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var caused = Assert.IsAssignableFrom<WorkflowEvent.CausedEvent>(notification.Event);
            var command = Assert.IsType<TransitionCause.Command>(caused.Cause);
            Assert.Equal(nameof(EchoPing), command.CommandType);
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
