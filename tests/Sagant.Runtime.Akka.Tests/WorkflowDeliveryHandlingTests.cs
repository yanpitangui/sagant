using Sagant.Execution;
using Sagant.Settings;
using Sagant.Protocol;
using Sagant.Effects;
using Sagant.Runtime.Akka.Clustering;
using Akka.Actor;
using Akka.Delivery;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowDeliveryHandlingTests : Support.WorkflowActorTestKit
{
    public WorkflowDeliveryHandlingTests() : base(Config)
    {
    }

    // A journal/snapshot-store plugin is required here: the UpdateState-effect tests
    // (Delivery_DuplicateSeqNr_.../Delivery_RepeatIdempotencyKey_...) call Persist(), which throws
    // without one configured, leaving Confirmed never sent and those tests hanging. Matches every
    // other actor test class's config in this suite.
    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    public sealed record Ping(string Text);

    [Fact]
    public void PreStart_SendsConsumerControllerStart()
    {
        var consumerControllerProbe = CreateTestProbe();
        CreateActor("wf-1", Script(), consumerController: consumerControllerProbe.Ref);

        consumerControllerProbe.ExpectMsg<ConsumerController.Start<WorkflowEnvelope>>();
    }

    [Fact]
    public void Delivery_InvokesHandlerAndConfirms()
    {
        var confirmProbe = CreateTestProbe();
        var script = Script()
            .Command<Ping>((state, cmd) => new EffectsBuilder<TestState>().Reply("pong"));
        var actor = CreateActor("wf-2", script);

        var envelope = new WorkflowEnvelope("wf-2", new Ping("hi"));
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope, confirmProbe.Ref, "producer-1", 1L));

        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();
    }

    [Fact]
    public void Delivery_WithReplyTo_RepliesToReplyToInsteadOfSender()
    {
        var confirmProbe = CreateTestProbe();
        var replyToProbe = CreateTestProbe();
        var script = Script()
            .Command<Ping>((state, cmd) => new EffectsBuilder<TestState>().Reply("pong"));
        var actor = CreateActor("wf-3", script);

        var envelope = new WorkflowEnvelope("wf-3", new Ping("hi"), ReplyTo: replyToProbe.Ref);
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope, confirmProbe.Ref, "producer-1", 1L));

        replyToProbe.ExpectMsg<string>(reply => reply == "pong");
    }

    [Fact]
    public void Delivery_DuplicateSeqNr_SkipsHandlerButStillConfirms()
    {
        var confirmProbe = CreateTestProbe();
        var callCount = 0;
        var script = Script()
            .Command<Ping>((state, cmd) =>
            {
                callCount++;
                return new EffectsBuilder<TestState>().UpdateState(state).Reply("pong");
            });
        var actor = CreateActor("wf-4", script);

        var envelope = new WorkflowEnvelope("wf-4", new Ping("hi"));
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope, confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        // redelivery of the same seqNr from the same producer (ConsumerController retried because
        // the earlier Confirmed was lost) — must not re-invoke the handler.
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope, confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void Delivery_RepeatIdempotencyKey_ReplaysCachedReplyWithoutReinvokingHandler()
    {
        var confirmProbe = CreateTestProbe();
        var replyToProbe = CreateTestProbe();
        var callCount = 0;
        var script = Script()
            .Command<Ping>((state, cmd) =>
            {
                callCount++;
                return new EffectsBuilder<TestState>().UpdateState(state).Reply("pong");
            });
        var actor = CreateActor("wf-5", script);

        var envelope1 = new WorkflowEnvelope("wf-5", new Ping("hi"), ReplyTo: replyToProbe.Ref, IdempotencyKey: "key-1");
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope1, confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();
        replyToProbe.ExpectMsg<string>(r => r == "pong");

        // A genuinely new seqNr (caller-code retry after an ambiguous Ask timeout — NOT a transport
        // redelivery) carrying the same idempotency key must replay, not re-invoke.
        var envelope2 = new WorkflowEnvelope("wf-5", new Ping("hi"), ReplyTo: replyToProbe.Ref, IdempotencyKey: "key-1");
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope2, confirmProbe.Ref, "producer-1", 2L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();
        replyToProbe.ExpectMsg<string>(r => r == "pong");

        Assert.Equal(1, callCount);
    }

    /// <summary>
    /// <see cref="ConsumerController.Confirmed"/> must only be sent from inside <c>PersistEnvelopeThen</c>'s
    /// <c>afterPersist</c> callback, never right after calling it — <c>Persist</c> is asynchronous
    /// (queues the write and returns immediately), so confirming before <c>afterPersist</c> runs
    /// would acknowledge the delivery before the transition is durably persisted, exactly the
    /// silent-data-loss window <c>Akka.Delivery</c>'s ack exists to rule out. Every other test in
    /// this file uses plain <c>.Reply(...)</c>, which never transitions, so this is the only coverage
    /// of the transition branch specifically.
    ///
    /// Asserts actual ORDERING, not just eventual delivery: a published <see cref="WorkflowFeedItem"/> carrying <see cref="WorkflowEvent.RunFinished"/>
    /// is published from inside <c>PersistEnvelopeThen</c>'s <c>Persist</c> callback, strictly before
    /// <c>afterPersist</c> runs — so if it's subscribed on the SAME probe that's also the delivery's
    /// <c>ConfirmTo</c>, both messages are Tell'd by the very same single-threaded actor to the very
    /// same target, in program order; there's no cross-actor race to make this flaky.
    /// </summary>
    [Fact]
    public void Delivery_WithTransitionEffect_ConfirmsOnlyAfterPersistCompletes()
    {
        var confirmProbe = CreateTestProbe();
        Sys.EventStream.Subscribe(confirmProbe.Ref, typeof(WorkflowFeedItem));

        var script = Script()
            .Command<Ping>((state, cmd) => new EffectsBuilder<TestState>().Complete().ThenReply("ended"));
        var actor = CreateActor("wf-6", script);

        var envelope = new WorkflowEnvelope("wf-6", new Ping("hi"));
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope, confirmProbe.Ref, "producer-1", 1L));

        confirmProbe.FishForMessage<WorkflowFeedItem>(item => item.Event is WorkflowEvent.RunFinished);
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();
    }

    /// <summary>
    /// The built-in <see cref="GetState"/> query, with no author-registered handler at all — the
    /// script below deliberately registers no <c>GetState</c> command, so a reply here can only have
    /// come from the framework's own branch. Sent as a plain <c>Tell</c>: a read never enters the
    /// delivery pipeline, so there is no envelope, no sequence number and no confirmation.
    /// </summary>
    [Fact]
    public void GetState_RepliesWithCurrentUserStateWithNoAuthorHandlerRegistered()
    {
        var confirmProbe = CreateTestProbe();
        var setupProbe = CreateTestProbe();
        var script = Script()
            .Command<Ping>((_, _) => new EffectsBuilder<TestState>().UpdateState(new TestState { Value = "changed" }).Reply("pong"));
        var actor = CreateActor("wf-getstate-1", script);

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-getstate-1", new Ping("hi")), confirmProbe.Ref, "producer-1", 1L), setupProbe.Ref);
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        actor.Tell(new GetState(), TestActor);

        ExpectMsg<TestState>(s => s.Value == "changed");
    }

    [Fact]
    public void GetState_DispatchesImmediatelyEvenWhileStepInFlight()
    {
        var confirmProbe = CreateTestProbe();
        var setupProbe = CreateTestProbe();
        var stepGate = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => stepGate.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("started"));
        var actor = CreateActor("wf-getstate-2", script);

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-getstate-2", new StartWorkflow(1)), confirmProbe.Ref, "producer-1", 1L), setupProbe.Ref);
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        actor.Tell(new GetState(), TestActor);

        // HangingStep is still in flight (stepGate is never resolved in this test at all).
        ExpectMsg<TestState>();
    }

    public sealed record PeekQuery(string Text);

    /// <summary>
    /// The core stash guarantee: a business command arriving while a step is in flight must not
    /// dispatch (and must not be Confirmed — see <see cref="Delivery_WhileStepInFlight_LeavesTheStashedDeliveryUnconfirmed"/>
    /// for why that specifically matters) against the pre-step state. Only once the step settles
    /// does the handler see it.
    /// </summary>
    [Fact]
    public void Delivery_WhileStepInFlight_DefersHandlerUntilStepSettles()
    {
        var confirmProbe = CreateTestProbe();
        var replyToProbe = CreateTestProbe();
        var stepGate = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => stepGate.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("started"))
            .Command<Ping>((_, _) => new EffectsBuilder<TestState>().Reply("pong"));
        var actor = CreateActor("wf-stash-1", script);

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-1", new StartWorkflow(1)), confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        // HangingStep is now in flight (stepGate never resolves until told to below). A business
        // command arriving now must not dispatch yet.
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-1", new Ping("hi"), ReplyTo: replyToProbe.Ref), confirmProbe.Ref, "producer-1", 2L));
        replyToProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(300));
        confirmProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(300));

        stepGate.SetResult(new StepEffectsBuilder<TestState>().ThenComplete());

        replyToProbe.ExpectMsg<string>(r => r == "pong");
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();
    }

    /// <summary>
    /// The durability-critical half of the guarantee above, stated as its own assertion:
    /// <c>ConfirmTo</c> is never told <see cref="ConsumerController.Confirmed"/> while a delivery
    /// sits deferred. If a crash happened right here, the producer would still consider this
    /// delivery outstanding and redeliver it once this entity recovers — that's the entire
    /// durability story for this feature (see the design spec's Durability section); it depends on
    /// this specific ordering holding.
    /// </summary>
    [Fact]
    public void Delivery_WhileStepInFlight_LeavesTheStashedDeliveryUnconfirmed()
    {
        var confirmProbe = CreateTestProbe();
        var stepGate = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => stepGate.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("started"))
            .Command<Ping>((_, _) => new EffectsBuilder<TestState>().Reply("pong"));
        var actor = CreateActor("wf-stash-2", script);

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-2", new StartWorkflow(1)), confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-2", new Ping("hi")), confirmProbe.Ref, "producer-1", 2L));

        // Held long enough to be confident this isn't just a delay — the step never completes in
        // this test at all, so a Confirmed here could only mean the guard is missing entirely.
        confirmProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
    }

    /// <summary>
    /// A <c>[WorkflowQuery]</c> dispatches immediately even with a step in flight — the read path
    /// the stash guard deliberately doesn't cover, because a query returns a <c>QueryEffect</c> and
    /// so cannot join the write it exists to serialize.
    /// </summary>
    [Fact]
    public void Query_DispatchesImmediatelyEvenWhileStepInFlight()
    {
        var confirmProbe = CreateTestProbe();
        var setupProbe = CreateTestProbe();
        var stepGate = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => stepGate.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("started"))
            .Query<PeekQuery>((state, _) => new QueryEffect(new Reply.ReplyValue($"peeked-{state.Value}", null)));
        var actor = CreateActor("wf-stash-3", script);

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-3", new StartWorkflow(1)), confirmProbe.Ref, "producer-1", 1L), setupProbe.Ref);
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        actor.Tell(new PeekQuery("hi"), TestActor);

        // Replies right away — HangingStep is still in flight (stepGate is never resolved in this
        // test at all), proving nothing about the step's own progress gated this.
        ExpectMsg<string>(r => r == "peeked-initial");
    }

    /// <summary>
    /// A query handler that never returns is bounded by the workflow's own query timeout, because a
    /// caller's request timeout completes the caller's wait and sends nothing to the entity. The
    /// entity replies, frees the slot, and cancels the handler's token.
    /// </summary>
    [Fact]
    public async Task Query_ExceedingItsTimeout_FailsTheCallerAndCancelsTheHandler()
    {
        var handlerObservedCancellation = new TaskCompletionSource<bool>();
        var script = Script()
            .Query<PeekQuery>(async (_, _, ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    handlerObservedCancellation.TrySetResult(true);
                    throw;
                }

                return new QueryEffect(Reply.NoReply.Instance);
            });
        var settings = WorkflowSettings.Create().DefaultQueryTimeout(TimeSpan.FromMilliseconds(200)).Build();
        var actor = CreateActor("wf-query-timeout", script, settings);

        actor.Tell(new PeekQuery("hi"), TestActor);

        var failure = ExpectMsg<Status.Failure>(TimeSpan.FromSeconds(5));
        Assert.IsType<WorkflowQueryTimeoutException>(failure.Cause);
        Assert.True(await handlerObservedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    /// <summary>Queries carry their own caller and correlation, so several run at once and each
    /// reply reaches the right one.</summary>
    [Fact]
    public async Task Query_ConcurrentQueries_EachReplyReachesItsOwnCaller()
    {
        var firstGate = new TaskCompletionSource<QueryEffect>();
        var script = Script()
            .Query<PeekQuery>((state, query, _) => query.Text == "slow"
                ? firstGate.Task
                : Task.FromResult(new QueryEffect(new Reply.ReplyValue($"fast-{state.Value}", null))));
        var actor = CreateActor("wf-query-concurrent", script);

        var slowCaller = CreateTestProbe();
        var fastCaller = CreateTestProbe();

        actor.Tell(new PeekQuery("slow"), slowCaller.Ref);
        actor.Tell(new PeekQuery("fast"), fastCaller.Ref);

        // The second query answers while the first is still parked.
        fastCaller.ExpectMsg<string>(r => r == "fast-initial");
        slowCaller.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        firstGate.SetResult(new QueryEffect(new Reply.ReplyValue("slow-done", null)));
        slowCaller.ExpectMsg<string>(r => r == "slow-done");
        await Task.CompletedTask;
    }

    /// <summary>
    /// A stashed command must stay stashed across an entire autonomous step-chain (StepA's own
    /// effect transitions straight into StepB with no external input), not just past the first
    /// step — see the design spec's "Rejected alternative: unstash between every transition" for
    /// why unstashing mid-chain would be actively unsafe, not merely unnecessary.
    /// </summary>
    [Fact]
    public void Delivery_WhileStepChainInFlight_StaysStashedAcrossTheWholeChainNotJustTheFirstStep()
    {
        var confirmProbe = CreateTestProbe();
        var replyToProbe = CreateTestProbe();
        var stepBGate = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("StepA", (state, _) => Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(state).ThenTransitionTo(Step("StepB"))))
            .Step("StepB", (_, _) => stepBGate.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StepA")).ThenReply("started"))
            .Command<Ping>((_, _) => new EffectsBuilder<TestState>().Reply("pong"));
        var actor = CreateActor("wf-stash-4", script);

        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-4", new StartWorkflow(1)), confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();

        // StepA completes synchronously and chains straight into StepB, which then hangs. A command
        // arriving any time after Start must wait out the whole chain, not just StepA.
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(
            new WorkflowEnvelope("wf-stash-4", new Ping("hi"), ReplyTo: replyToProbe.Ref), confirmProbe.Ref, "producer-1", 2L));
        replyToProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(300));
        confirmProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(300));

        stepBGate.SetResult(new StepEffectsBuilder<TestState>().ThenComplete());

        replyToProbe.ExpectMsg<string>(r => r == "pong");
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>();
    }
}
