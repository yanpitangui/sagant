using Sagant.Runtime.Akka.Clustering;
using Sagant.Protocol;
using Akka.Actor;
using Akka.Cluster.Sharding.Delivery;
using Akka.TestKit.Xunit2;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowProducerAdapterTests : TestKit
{
    public WorkflowProducerAdapterTests() : base("akka.loglevel = OFF")
    {
    }

    [Fact]
    public async Task Send_WithDemandAlreadyAvailable_ForwardsImmediately()
    {
        var producerControllerProbe = CreateTestProbe();
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));
        adapter.Tell(new WorkflowProducerAdapter.RegisterProducerController(producerControllerProbe.Ref));

        var sendNextToProbe = CreateTestProbe();
        adapter.Tell(new ShardingProducerController.RequestNext<WorkflowEnvelope>(
            sendNextToProbe.Ref, sendNextToProbe.Ref,
            System.Collections.Immutable.ImmutableHashSet<string>.Empty,
            System.Collections.Immutable.ImmutableDictionary<string, int>.Empty));

        var envelope = new WorkflowEnvelope("wf-1", "cmd");
        var ackTask = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-1", envelope), TimeSpan.FromSeconds(5));

        var sent = sendNextToProbe.ExpectMsg<global::Akka.Cluster.Sharding.ShardingEnvelope>();
        Assert.Equal("wf-1", sent.EntityId);
        Assert.Same(envelope, sent.Message);

        Assert.Equal(Done.Instance, await ackTask);
    }

    [Fact]
    public async Task Send_WithoutDemandYet_QueuesUntilRequestNextArrives()
    {
        var producerControllerProbe = CreateTestProbe();
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));
        adapter.Tell(new WorkflowProducerAdapter.RegisterProducerController(producerControllerProbe.Ref));

        var envelope = new WorkflowEnvelope("wf-1", "cmd");
        var ackTask = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-1", envelope), TimeSpan.FromSeconds(5));

        var sendNextToProbe = CreateTestProbe();
        adapter.Tell(new ShardingProducerController.RequestNext<WorkflowEnvelope>(
            sendNextToProbe.Ref, sendNextToProbe.Ref,
            System.Collections.Immutable.ImmutableHashSet<string>.Empty,
            System.Collections.Immutable.ImmutableDictionary<string, int>.Empty));

        var sent = sendNextToProbe.ExpectMsg<global::Akka.Cluster.Sharding.ShardingEnvelope>();
        Assert.Equal("wf-1", sent.EntityId);
        Assert.Equal(Done.Instance, await ackTask);
    }

    [Fact]
    public async Task Send_MultipleQueuedItems_DrainOneAtATimeInFifoOrderAsRequestNextArrives()
    {
        var producerControllerProbe = CreateTestProbe();
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));
        adapter.Tell(new WorkflowProducerAdapter.RegisterProducerController(producerControllerProbe.Ref));

        // No RequestNext has arrived yet, so all three queue up in FIFO order.
        var envelope1 = new WorkflowEnvelope("wf-1", "cmd-1");
        var envelope2 = new WorkflowEnvelope("wf-2", "cmd-2");
        var envelope3 = new WorkflowEnvelope("wf-3", "cmd-3");
        var ackTask1 = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-1", envelope1), TimeSpan.FromSeconds(5));
        var ackTask2 = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-2", envelope2), TimeSpan.FromSeconds(5));
        var ackTask3 = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-3", envelope3), TimeSpan.FromSeconds(5));

        // A 4th item queued after a demand token was consumed must wait for its own RequestNext —
        // asserted below by checking the probe stays silent until each fresh RequestNext arrives.
        var envelope4 = new WorkflowEnvelope("wf-4", "cmd-4");

        var sendNextToProbe = CreateTestProbe();

        // First RequestNext drains exactly wf-1, nothing else.
        adapter.Tell(new ShardingProducerController.RequestNext<WorkflowEnvelope>(
            sendNextToProbe.Ref, sendNextToProbe.Ref,
            System.Collections.Immutable.ImmutableHashSet<string>.Empty,
            System.Collections.Immutable.ImmutableDictionary<string, int>.Empty));
        var sent1 = sendNextToProbe.ExpectMsg<global::Akka.Cluster.Sharding.ShardingEnvelope>();
        Assert.Equal("wf-1", sent1.EntityId);
        Assert.Same(envelope1, sent1.Message);
        Assert.Equal(Done.Instance, await ackTask1);
        sendNextToProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Enqueue a 4th item while demand is exhausted — it must queue behind wf-2/wf-3, not jump ahead.
        var ackTask4 = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-4", envelope4), TimeSpan.FromSeconds(5));

        // Second RequestNext drains exactly wf-2.
        adapter.Tell(new ShardingProducerController.RequestNext<WorkflowEnvelope>(
            sendNextToProbe.Ref, sendNextToProbe.Ref,
            System.Collections.Immutable.ImmutableHashSet<string>.Empty,
            System.Collections.Immutable.ImmutableDictionary<string, int>.Empty));
        var sent2 = sendNextToProbe.ExpectMsg<global::Akka.Cluster.Sharding.ShardingEnvelope>();
        Assert.Equal("wf-2", sent2.EntityId);
        Assert.Same(envelope2, sent2.Message);
        Assert.Equal(Done.Instance, await ackTask2);
        sendNextToProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Third RequestNext drains exactly wf-3.
        adapter.Tell(new ShardingProducerController.RequestNext<WorkflowEnvelope>(
            sendNextToProbe.Ref, sendNextToProbe.Ref,
            System.Collections.Immutable.ImmutableHashSet<string>.Empty,
            System.Collections.Immutable.ImmutableDictionary<string, int>.Empty));
        var sent3 = sendNextToProbe.ExpectMsg<global::Akka.Cluster.Sharding.ShardingEnvelope>();
        Assert.Equal("wf-3", sent3.EntityId);
        Assert.Same(envelope3, sent3.Message);
        Assert.Equal(Done.Instance, await ackTask3);
        sendNextToProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Fourth RequestNext finally drains wf-4 — confirms one send per RequestNext, never more.
        adapter.Tell(new ShardingProducerController.RequestNext<WorkflowEnvelope>(
            sendNextToProbe.Ref, sendNextToProbe.Ref,
            System.Collections.Immutable.ImmutableHashSet<string>.Empty,
            System.Collections.Immutable.ImmutableDictionary<string, int>.Empty));
        var sent4 = sendNextToProbe.ExpectMsg<global::Akka.Cluster.Sharding.ShardingEnvelope>();
        Assert.Equal("wf-4", sent4.EntityId);
        Assert.Same(envelope4, sent4.Message);
        Assert.Equal(Done.Instance, await ackTask4);
    }

    [Fact]
    public async Task Send_WhenQueueAtCapacity_FailsFast()
    {
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 1));
        adapter.Tell(new WorkflowProducerAdapter.RegisterProducerController(CreateTestProbe().Ref));

        // No RequestNext ever arrives, so nothing drains — first Enqueue fills the 1-deep queue.
        _ = adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-1", new WorkflowEnvelope("wf-1", "cmd-1")), TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<WorkflowProducerAdapter.ProducerBufferFullException>(() =>
            adapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue("wf-1", new WorkflowEnvelope("wf-1", "cmd-2")), TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task CreateReplyWaiter_ReturnsATellableActorRef()
    {
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));

        var waiterRef = await adapter.Ask<IActorRef>(
            new WorkflowProducerAdapter.CreateReplyWaiter(OnReply: _ => { }, OnFailure: _ => { }),
            TimeSpan.FromSeconds(5));

        Assert.NotNull(waiterRef);
        Assert.NotEqual(ActorRefs.Nobody, waiterRef);
    }

    [Fact]
    public async Task CreateReplyWaiter_OnPlainTell_InvokesOnReplyAndStops()
    {
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));

        object? received = null;
        var onReplyCalled = new TaskCompletionSource<object?>();
        var waiterRef = await adapter.Ask<IActorRef>(
            new WorkflowProducerAdapter.CreateReplyWaiter(
                OnReply: value =>
                {
                    received = value;
                    onReplyCalled.TrySetResult(value);
                },
                OnFailure: ex => onReplyCalled.TrySetException(ex)),
            TimeSpan.FromSeconds(5));

        Watch(waiterRef);
        waiterRef.Tell("reply-value");

        var observed = await onReplyCalled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("reply-value", observed);
        Assert.Equal("reply-value", received);
        ExpectTerminated(waiterRef);
    }

    [Fact]
    public async Task CreateReplyWaiter_OnStatusFailure_InvokesOnFailureAndStops()
    {
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));

        var onFailureCalled = new TaskCompletionSource<Exception>();
        var waiterRef = await adapter.Ask<IActorRef>(
            new WorkflowProducerAdapter.CreateReplyWaiter(
                OnReply: value => onFailureCalled.TrySetException(new InvalidOperationException($"expected OnFailure, got OnReply({value})")),
                OnFailure: ex => onFailureCalled.TrySetResult(ex)),
            TimeSpan.FromSeconds(5));

        Watch(waiterRef);
        var cause = new InvalidOperationException("nope");
        waiterRef.Tell(new Status.Failure(cause));

        var observed = await onFailureCalled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Same(cause, observed);
        ExpectTerminated(waiterRef);
    }

    [Fact]
    public async Task CreateReplyWaiter_StopsOnPoisonPillWithoutInvokingEitherCallback()
    {
        var adapter = Sys.ActorOf(WorkflowProducerAdapter.Props(bufferCapacity: 4));

        var callbackInvoked = false;
        var waiterRef = await adapter.Ask<IActorRef>(
            new WorkflowProducerAdapter.CreateReplyWaiter(
                OnReply: _ => callbackInvoked = true,
                OnFailure: _ => callbackInvoked = true),
            TimeSpan.FromSeconds(5));

        Watch(waiterRef);
        waiterRef.Tell(PoisonPill.Instance);

        ExpectTerminated(waiterRef);
        Assert.False(callbackInvoked);
    }
}
