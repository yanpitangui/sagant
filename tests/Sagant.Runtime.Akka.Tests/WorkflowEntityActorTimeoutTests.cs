using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Akka.TestKit;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Uses <see cref="TestScheduler"/> (virtual time, advanced explicitly) instead of real wall-clock
/// delays, so timeout firing is deterministic and these tests don't need to actually wait.
/// </summary>
public class WorkflowEntityActorTimeoutTests : WorkflowActorTestKit
{
    public WorkflowEntityActorTimeoutTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.scheduler.implementation = "Akka.TestKit.TestScheduler, Akka.TestKit"
        akka.loglevel = OFF
        """;

    private TestScheduler Scheduler => (TestScheduler)Sys.Scheduler;

    private readonly TaskCompletionSource<StepEffect<TestState>> _neverCompletes = new();

    private Task<StepEffect<TestState>> NeverCompletingStep(TestState _, object? __) => _neverCompletes.Task;

    [Fact]
    public void StepTimeout_NoResultInTime_TreatedAsFailure_EndsWithoutRecoverStrategy()
    {
        var script = Script()
            .Step("HangingStep", NeverCompletingStep)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create().DefaultStepTimeout(TimeSpan.FromSeconds(5)).Build();

        var actor = CreateActor(nameof(StepTimeout_NoResultInTime_TreatedAsFailure_EndsWithoutRecoverStrategy), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Scheduler.Advance(TimeSpan.FromSeconds(6));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepTimeout_WithRecoverStrategy_FailsOverAfterTimeout()
    {
        var script = Script()
            .Step("HangingStep", NeverCompletingStep)
            .Step("Compensate", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "compensated" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create()
            .DefaultStepTimeout(TimeSpan.FromSeconds(5))
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(0).FailoverTo(Step("Compensate")))
            .Build();

        var actor = CreateActor(nameof(StepTimeout_WithRecoverStrategy_FailsOverAfterTimeout), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Scheduler.Advance(TimeSpan.FromSeconds(6));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("compensated", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepCompletesBeforeTimeout_TimeoutDoesNotFireSpuriously()
    {
        var script = Script()
            .Step("FastStep", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "done" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("FastStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create().DefaultStepTimeout(TimeSpan.FromSeconds(5)).Build();

        var actor = CreateActor(nameof(StepCompletesBeforeTimeout_TimeoutDoesNotFireSpuriously), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("done", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));

        // Advancing well past the step timeout after completion must not change anything further.
        Scheduler.Advance(TimeSpan.FromSeconds(10));

        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        var finalDiagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Finished, finalDiagnostics.Envelope.Status);
        Assert.Equal("done", finalDiagnostics.Envelope.UserState.Value);
    }

    [Fact]
    public void WorkflowTimeout_FiresWhileStepInFlight_TransitionsToFailoverStep()
    {
        var script = Script()
            .Step("HangingStep", NeverCompletingStep)
            .Step("AbandonStep", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "abandoned" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create().Timeout(TimeSpan.FromSeconds(30), Step("AbandonStep")).Build();

        var actor = CreateActor(nameof(WorkflowTimeout_FiresWhileStepInFlight_TransitionsToFailoverStep), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Scheduler.Advance(TimeSpan.FromSeconds(31));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("abandoned", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void WorkflowTimeout_WithoutFailoverStep_EndsWorkflow()
    {
        var script = Script()
            .Step("HangingStep", NeverCompletingStep)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create().Timeout(TimeSpan.FromSeconds(30)).Build();

        var actor = CreateActor(nameof(WorkflowTimeout_WithoutFailoverStep_EndsWorkflow), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Scheduler.Advance(TimeSpan.FromSeconds(31));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void PauseTimeout_FiresWhilePaused_TransitionsToTimeoutHandlerStep()
    {
        var pauseSettings = PauseSettings.WithTimeout(TimeSpan.FromHours(1)).TimeoutHandler(Step("AutoCancel"));
        var script = Script()
            .Step("AutoCancel", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "auto-cancelled" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().Pause(pauseSettings).ThenReply("paused"));

        var actor = CreateActor(nameof(PauseTimeout_FiresWhilePaused_TransitionsToTimeoutHandlerStep), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));

        Scheduler.Advance(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("auto-cancelled", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void WorkflowTimeout_DoesNotFireWhilePaused_PauseTimeoutStillGovernsInstead()
    {
        // A workflow-level timeout shorter than a pause window is a realistic, legitimate
        // combination (e.g. "finish within 5 minutes of active work" + "but allow up to 24h for
        // human approval") — the workflow timeout must not preempt the pause, since it's a ceiling
        // on active processing time, not on time spent waiting for a human. Regression test for a
        // real bug found building the OrderFulfillment sample: the workflow timeout used to fire
        // even while paused, jumping straight to its own failover step and skipping the pause's
        // own timeout handler entirely.
        var pauseSettings = PauseSettings.WithTimeout(TimeSpan.FromHours(1)).TimeoutHandler(Step("AutoCancel"));
        var script = Script()
            .Step("AutoCancel", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "auto-cancelled" }).ThenComplete()))
            .Step("WorkflowTimeoutFailover", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "should-not-reach-here" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().Pause(pauseSettings).ThenReply("paused"));
        var settings = WorkflowSettings.Create().Timeout(TimeSpan.FromMinutes(5), Step("WorkflowTimeoutFailover")).Build();

        var actor = CreateActor(nameof(WorkflowTimeout_DoesNotFireWhilePaused_PauseTimeoutStillGovernsInstead), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));

        // Crosses the 5-minute workflow timeout on the way to the 1-hour pause timeout.
        Scheduler.Advance(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("auto-cancelled", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }
}
