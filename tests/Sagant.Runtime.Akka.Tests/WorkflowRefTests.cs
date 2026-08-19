using Sagant.Runtime.Akka.Clustering;
using Sagant.Protocol;
using Sagant.Descriptors;
using Akka.Actor;
using Akka.TestKit.Xunit2;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowRefTests : TestKit
{
    public WorkflowRefTests() : base("akka.loglevel = OFF")
    {
    }

    public sealed record SomeCommand(int Value);

    // WorkflowRef only needs TWorkflow/TState to satisfy the same constraint as the actor itself;
    // it never actually invokes anything on TWorkflow, so a minimal stub is enough here.
    private sealed class StubWorkflow : Workflow<string>, IWorkflowStepDispatcher<string>, IWorkflowCommandDispatcher<string>, IWorkflowQueryDispatcher<string>, IWorkflowChildResultDispatcher<string>
    {
        public override string EmptyState() => string.Empty;

        bool IWorkflowChildResultDispatcher<string>.TryGetChildResultHandler(out ChildResultDescriptor<string> descriptor)
        {
            descriptor = default;
            return false;
        }

        bool IWorkflowStepDispatcher<string>.TryGetStep(string stepName, out StepDescriptor<string> descriptor)
        {
            descriptor = default;
            return false;
        }

        System.Collections.Generic.IReadOnlyCollection<string> IWorkflowStepDispatcher<string>.StepNames => Array.Empty<string>();

        bool IWorkflowCommandDispatcher<string>.TryGetHandler(Type commandType, out CommandDescriptor<string> descriptor)
        {
            descriptor = default;
            return false;
        }

        bool IWorkflowQueryDispatcher<string>.TryGetQuery(Type queryType, out QueryDescriptor<string> descriptor)
        {
            descriptor = default;
            return false;
        }
    }

    // A minimal stand-in for WorkflowProducerAdapter's private ReplyWaiterActor: producerAdapterProbe
    // below is a plain TestProbe, with no real WorkflowProducerAdapter behind it able to spawn the
    // real thing when asked for a WorkflowProducerAdapter.CreateReplyWaiter — this reproduces just
    // enough of its behavior (relay the first Tell/Status.Failure to the callbacks the message
    // carried, then stop) so these tests exercise WorkflowRef's actual two-round-trip protocol
    // (create waiter, then enqueue) in full, with no shortcut around it.
    private sealed class TestReplyWaiterActor : ReceiveActor
    {
        public TestReplyWaiterActor(Action<object?> onReply, Action<Exception> onFailure)
        {
            ReceiveAny(msg =>
            {
                if (msg is Status.Failure failure)
                {
                    onFailure(failure.Cause);
                }
                else
                {
                    onReply(msg);
                }

                Context.Stop(Self);
            });
        }
    }

    // Answers the CreateReplyWaiter ask that WorkflowRef.Ask now issues before enqueueing, using a
    // real TestReplyWaiterActor: Tell'ing a bare TestProbe ref instead wouldn't invoke the
    // CreateReplyWaiter message's OnReply/OnFailure callbacks the way the real
    // WorkflowProducerAdapter-spawned waiter does, and it's exactly those callbacks that resolve
    // WorkflowRef.Ask's returned Task.
    private IActorRef AnswerCreateReplyWaiter(global::Akka.TestKit.TestProbe producerAdapterProbe)
    {
        var createWaiter = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.CreateReplyWaiter>();
        var waiterRef = Sys.ActorOf(Props.Create(() => new TestReplyWaiterActor(createWaiter.OnReply, createWaiter.OnFailure)));
        producerAdapterProbe.LastSender.Tell(waiterRef, producerAdapterProbe.Ref);
        return waiterRef;
    }

    [Fact]
    public async Task Send_EnqueuesEnvelopeOnProducerAdapter()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var sendTask = workflowRef.Send(new SomeCommand(1)).AsTask();

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        Assert.Equal("order-42", enqueue.EntityId);
        Assert.Equal(new SomeCommand(1), enqueue.Envelope.Message);
        Assert.Null(enqueue.Envelope.IdempotencyKey);
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        await sendTask;
    }

    [Fact]
    public async Task Send_WithIdempotencyKey_CarriesKeyOnEnvelope()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        _ = workflowRef.Send(new SomeCommand(1), idempotencyKey: "key-1").AsTask();

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        Assert.Equal("key-1", enqueue.Envelope.IdempotencyKey);
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);
    }

    [Fact]
    public async Task Ask_EnqueuesEnvelopeWithReplyTo_AndReturnsReply()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var askTask = workflowRef.Ask<SomeCommand, string>(new SomeCommand(2));

        var waiterRef = AnswerCreateReplyWaiter(producerAdapterProbe); // 1st round trip: create the reply waiter

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(); // 2nd round trip: enqueue
        Assert.NotNull(enqueue.Envelope.ReplyTo);
        Assert.Equal(waiterRef, enqueue.Envelope.ReplyTo);
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref); // adapter acks the enqueue

        // The business reply comes from whoever the entity was told to ReplyTo — the adapter's own
        // part ends at the enqueue ack above.
        enqueue.Envelope.ReplyTo!.Tell("reply-value");

        Assert.Equal("reply-value", await askTask);
    }

    [Fact]
    public async Task Ask_WithIdempotencyKey_CarriesKeyOnEnvelope()
    {
        // Locks down WorkflowRef.Ask's parameter order — (command, idempotencyKey,
        // cancellationToken, metadata), matching IWorkflowHandle.Request's public order.
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var askTask = workflowRef.Ask<SomeCommand, string>(new SomeCommand(2), idempotencyKey: "key-x");

        AnswerCreateReplyWaiter(producerAdapterProbe);

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        Assert.Equal("key-x", enqueue.Envelope.IdempotencyKey);
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        enqueue.Envelope.ReplyTo!.Tell("reply-value");
        Assert.Equal("reply-value", await askTask);
    }

    [Fact]
    public async Task WorkflowHandle_Request_WithIdempotencyKey_CarriesKeyOnEnvelope()
    {
        // Same lock-down as Ask_WithIdempotencyKey_CarriesKeyOnEnvelope, but through the public-facing
        // WorkflowHandle.Request -> WorkflowRef.Ask chain — WorkflowClient.Request's call into
        // _inner.Ask is what this actually exercises, so this catches a regression at the handle
        // boundary app code actually calls through, going beyond WorkflowRef alone.
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");
        var handle = new WorkflowHandle<StubWorkflow, string>(workflowRef);

        var requestTask = handle.Request<SomeCommand, string>(new SomeCommand(2), idempotencyKey: "key-y");

        AnswerCreateReplyWaiter(producerAdapterProbe);

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        Assert.Equal("key-y", enqueue.Envelope.IdempotencyKey);
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        enqueue.Envelope.ReplyTo!.Tell("reply-value");
        Assert.Equal("reply-value", await requestTask);
    }

    [Fact]
    public async Task Ask_ReplyToIsIndependentOfProducerAdapterAck()
    {
        // The producer adapter's own Done ack must not race with / short-circuit the eventual
        // business reply -- they're two entirely separate promises.
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var askTask = workflowRef.Ask<SomeCommand, string>(new SomeCommand(2));

        AnswerCreateReplyWaiter(producerAdapterProbe);

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        // Give the enqueue ack a moment to be observed without the business reply having arrived yet.
        await Task.Delay(50);
        Assert.False(askTask.IsCompleted);

        enqueue.Envelope.ReplyTo!.Tell("reply-value");
        Assert.Equal("reply-value", await askTask);
    }

    [Fact]
    public async Task Ask_WhenEntityRepliesWithFailure_FaultsTheTask()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var askTask = workflowRef.Ask<SomeCommand, string>(new SomeCommand(2));

        AnswerCreateReplyWaiter(producerAdapterProbe);

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        enqueue.Envelope.ReplyTo!.Tell(new Status.Failure(new WorkflowCommandException("nope")));

        await Assert.ThrowsAsync<WorkflowCommandException>(() => askTask);
    }

    [Fact]
    public async Task Suspend_StillSendsDirectlyToShardRegion_UnaffectedByDelivery()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var task = workflowRef.Suspend("because");

        var envelope = shardRegionProbe.ExpectMsg<WorkflowEnvelope>();
        var suspend = Assert.IsType<Suspend>(envelope.Message);
        Assert.Equal("because", suspend.Reason);
        producerAdapterProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        shardRegionProbe.LastSender.Tell(Done.Instance, shardRegionProbe.Ref);

        await task;
    }

    [Fact]
    public async Task Resume_StillSendsDirectlyToShardRegion_UnaffectedByDelivery()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var task = workflowRef.Resume();

        var envelope = shardRegionProbe.ExpectMsg<WorkflowEnvelope>();
        Assert.IsType<Resume>(envelope.Message);
        producerAdapterProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        shardRegionProbe.LastSender.Tell(Done.Instance, shardRegionProbe.Ref);

        await task;
    }

    [Fact]
    public async Task Terminate_StillSendsDirectlyToShardRegion_UnaffectedByDelivery()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var task = workflowRef.Terminate("because");

        var envelope = shardRegionProbe.ExpectMsg<WorkflowEnvelope>();
        var terminate = Assert.IsType<Terminate>(envelope.Message);
        Assert.Equal("because", terminate.Reason);
        producerAdapterProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        shardRegionProbe.LastSender.Tell(Done.Instance, shardRegionProbe.Ref);

        await task;
    }

    [Fact]
    public async Task GetStatus_SendsGetStatusCommand_ReturnsWorkflowStatus()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var task = workflowRef.GetStatus();

        var envelope = shardRegionProbe.ExpectMsg<WorkflowEnvelope>();
        Assert.IsType<GetStatus>(envelope.Message);
        producerAdapterProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        shardRegionProbe.LastSender.Tell(new WorkflowStatusReply(WorkflowStatus.Paused), shardRegionProbe.Ref);

        Assert.Equal(WorkflowStatus.Paused, await task);
    }

    [Fact]
    public async Task RunAndAwaitResult_EnqueuesCommandThenWatchesForCompletion_ReturnsFinalState()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42");

        var resultTask = workflowRef.RunAndAwaitResult(new SomeCommand(3));

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        Assert.Equal("order-42", enqueue.EntityId);
        Assert.IsType<SomeCommand>(enqueue.Envelope.Message);
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        var watchEnvelope = shardRegionProbe.ExpectMsg<WorkflowEnvelope>();
        Assert.IsType<WatchForCompletion<string>>(watchEnvelope.Message);
        shardRegionProbe.LastSender.Tell(
            new WorkflowResult<string>.Finished(WorkflowOutcome.Completed.Instance, "final-state"), shardRegionProbe.Ref);

        var result = Assert.IsType<WorkflowResult<string>.Finished>(await resultTask);
        Assert.IsType<WorkflowOutcome.Completed>(result.Outcome);
        Assert.Equal("final-state", result.State);
    }

    /// <summary>
    /// The fix for the entity relocating mid-wait: WatchForCompletion's in-memory registration on the
    /// pre-relocation incarnation is lost, so a caller that only ever asked once would hang until its
    /// own overall CancellationToken gave up. Bounding each attempt to watchRetryInterval and re-asking
    /// on a timeout is what lets a later attempt land on whichever incarnation now owns the entity.
    ///
    /// This test proves the retry mechanism itself fires on a timeout — a TestProbe standing in for
    /// the shard region, exactly like the base test above. Proving a real relocation dropping the
    /// registration mid-wait, against an actual cluster, remains open work for the chaos-test tier.
    /// </summary>
    [Fact]
    public async Task RunAndAwaitResult_RetriesWatchForCompletion_WhenNoReplyWithinInterval()
    {
        var shardRegionProbe = CreateTestProbe();
        var producerAdapterProbe = CreateTestProbe();
        var workflowRef = new WorkflowRef<StubWorkflow, string>(
            shardRegionProbe.Ref, producerAdapterProbe.Ref, "order-42", watchRetryInterval: TimeSpan.FromMilliseconds(100));

        var resultTask = workflowRef.RunAndAwaitResult(new SomeCommand(3));

        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>();
        producerAdapterProbe.LastSender.Tell(Done.Instance, producerAdapterProbe.Ref);

        // First attempt: left unanswered on purpose, so it has to time out on its own. The timeout is
        // exactly what a relocation-dropped registration looks like from WorkflowRef's own seat —
        // indistinguishable from the entity genuinely still running.
        var firstAttempt = shardRegionProbe.ExpectMsg<WorkflowEnvelope>();
        Assert.IsType<WatchForCompletion<string>>(firstAttempt.Message);

        // The retry: a second, independent WatchForCompletion, proving WorkflowRef asked again instead
        // of giving up or hanging on the first attempt's dead promise.
        var secondAttempt = shardRegionProbe.ExpectMsg<WorkflowEnvelope>(TimeSpan.FromSeconds(2));
        Assert.IsType<WatchForCompletion<string>>(secondAttempt.Message);
        shardRegionProbe.LastSender.Tell(
            new WorkflowResult<string>.Finished(WorkflowOutcome.Completed.Instance, "final-state"), shardRegionProbe.Ref);

        var result = Assert.IsType<WorkflowResult<string>.Finished>(await resultTask);
        Assert.Equal("final-state", result.State);
    }
}
