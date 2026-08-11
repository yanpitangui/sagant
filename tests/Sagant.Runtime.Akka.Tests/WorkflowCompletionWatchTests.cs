using Sagant.Protocol;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowCompletionWatchTests : WorkflowActorTestKit
{
    public WorkflowCompletionWatchTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    /// <summary>
    /// A parked run releases its waiters. The run has not ended and will not move until someone acts
    /// on the failure, so a caller that waited gets that failure back — where waiting on an ending
    /// that never comes would block to its own timeout and report nothing.
    /// </summary>
    [Fact]
    public void WatchForCompletion_WhenTheRunParks_NotifiedWithTheFailureHoldingIt()
    {
        var settings = Sagant.Settings.WorkflowSettings.Create()
            .StepRecovery(Step("Flaky"), Sagant.Settings.RecoverStrategy.WithMaxRetries(0).ThenPark())
            .Build();

        var actor = CreateActor(nameof(WatchForCompletion_WhenTheRunParks_NotifiedWithTheFailureHoldingIt), Script()
            .Step("Flaky", (_, _) => Task.FromException<StepEffect<TestState>>(new InvalidOperationException("gateway down")))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("Flaky")).ThenReply("accepted")), settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new WatchForCompletion<TestState>(), TestActor);

        var parked = Assert.IsType<WorkflowResult<TestState>.Parked>(
            ExpectMsg<WorkflowResult<TestState>>(TimeSpan.FromSeconds(5)));
        Assert.Contains("gateway down", parked.Cause.Message);
        Assert.Equal("Flaky", parked.Cause.StepName);
        // The convenience accessor reports a parked run's failure the same way it reports a failed
        // one's, so a caller reading Failure needs no knowledge of which case it holds.
        Assert.Same(parked.Cause, parked.Failure);
        Assert.False(parked.IsCompleted);
    }

    [Fact]
    public void WatchForCompletion_RegisteredBeforeEnd_NotifiedWithFinalStateWhenWorkflowEnds()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var actor = CreateActor(nameof(WatchForCompletion_RegisteredBeforeEnd_NotifiedWithFinalStateWhenWorkflowEnds), Script()
            .Step("Step", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Step")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new WatchForCompletion<TestState>(), TestActor);

        neverCompletes.SetResult(new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "final" }).ThenComplete());

        var final = ExpectMsg<WorkflowResult<TestState>>(TimeSpan.FromSeconds(5));
        Assert.Equal("final", final.State.Value);
    }

    [Fact]
    public void WatchForCompletion_RegisteredAfterEnd_NotifiedImmediatelyWithCurrentState()
    {
        var actor = CreateActor(nameof(WatchForCompletion_RegisteredAfterEnd_NotifiedImmediatelyWithCurrentState), Script()
            .Step("Step", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "already-done" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Step")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(5));

        actor.Tell(new WatchForCompletion<TestState>(), TestActor);
        var final = Assert.IsType<WorkflowResult<TestState>.Finished>(ExpectMsg<WorkflowResult<TestState>>());
        Assert.IsType<WorkflowOutcome.Completed>(final.Outcome);
        Assert.Equal("already-done", final.State.Value);
    }

    [Fact]
    public void WatchForCompletion_NotifiedOnTerminate_TooNotJustEnd()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var actor = CreateActor(nameof(WatchForCompletion_NotifiedOnTerminate_TooNotJustEnd), Script()
            .Step("Step", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Step")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // The watcher and the caller issuing Terminate are independent observers, so each gets its
        // own probe — asserting on one actor would pin an ordering between them that nothing
        // guarantees.
        var watcher = CreateTestProbe();
        actor.Tell(new WatchForCompletion<TestState>(), watcher.Ref);

        actor.Tell(new Terminate("operator stopped it"), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        // A terminated run still reports a result, and reports it as terminated — which is the point
        // of the outcome being typed.
        var final = Assert.IsType<WorkflowResult<TestState>.Finished>(
            watcher.ExpectMsg<WorkflowResult<TestState>>(TimeSpan.FromSeconds(5)));
        var terminated = Assert.IsType<WorkflowOutcome.Terminated>(final.Outcome);
        Assert.Equal("operator stopped it", terminated.Reason);
    }
}
