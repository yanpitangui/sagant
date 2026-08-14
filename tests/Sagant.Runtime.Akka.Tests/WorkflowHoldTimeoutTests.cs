using Akka.TestKit;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Tests.Support;
using Sagant.Settings;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// A hold that nobody comes back for. The fold decides what a hold arms; these cover the wiring that
/// carries it — settings into the planner, the planner's decision into a live timer, the timer into
/// the step the hold named — none of which the fold's own tests reach.
///
/// Virtual time on a bare actor, since <see cref="TestScheduler"/> freezes cluster gossip too.
/// </summary>
public class WorkflowHoldTimeoutTests : WorkflowActorTestKit
{
    public WorkflowHoldTimeoutTests() : base(Config)
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

    private static WorkflowSettings HeldFor(TimeSpan hold) =>
        new(null, null, null, null, Array.Empty<StepSettings>(),
            HoldTimeout: hold, HoldTimeoutStepName: "OnAbandoned");

    private WorkflowScript HoldingScript() =>
        Script()
            .Step("Work", NeverCompletingStep)
            .Step("OnAbandoned", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>()
                    .UpdateState(new TestState { Value = "abandoned" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("Work")).ThenReply("accepted"));

    private Diagnostics<TestState> Diagnose(global::Akka.Actor.IActorRef actor)
    {
        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        return ExpectMsg<Diagnostics<TestState>>();
    }

    [Fact]
    public void AnOperatorHoldNobodyReleases_RunsTheStepItNamed()
    {
        var actor = CreateActor(nameof(AnOperatorHoldNobodyReleases_RunsTheStepItNamed),
            HoldingScript(), HeldFor(TimeSpan.FromHours(2)));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Done>();
        Assert.Equal(WorkflowStatus.Suspended, Diagnose(actor).Envelope.Status);

        Scheduler.Advance(TimeSpan.FromHours(3));

        AwaitAssert(() =>
        {
            var envelope = Diagnose(actor).Envelope;
            Assert.Equal(WorkflowStatus.Finished, envelope.Status);
            Assert.Equal("abandoned", envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Releasing the hold ends its wait, so a timer that fires after the release finds nothing to do.
    /// The envelope keeps the instant it recorded, which is why the planner keys off status rather
    /// than off that field.
    /// </summary>
    [Fact]
    public void AHoldReleasedBeforeItsDeadline_NeverRunsThatStep()
    {
        var actor = CreateActor(nameof(AHoldReleasedBeforeItsDeadline_NeverRunsThatStep),
            HoldingScript(), HeldFor(TimeSpan.FromHours(2)));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Done>();
        actor.Tell(new Resume(), TestActor);
        ExpectMsg<Done>();

        Scheduler.Advance(TimeSpan.FromHours(3));

        AwaitAssert(() =>
        {
            var envelope = Diagnose(actor).Envelope;
            Assert.Equal(WorkflowStatus.Running, envelope.Status);
            Assert.NotEqual("abandoned", envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>A deployment that names no hold timeout waits for a person however long that takes,
    /// which is the default and has to stay the default.</summary>
    [Fact]
    public void WithNoHoldTimeoutConfigured_AHeldInstanceStaysHeld()
    {
        var actor = CreateActor(nameof(WithNoHoldTimeoutConfigured_AHeldInstanceStaysHeld),
            HoldingScript(), WorkflowSettings.Default);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Done>();

        Scheduler.Advance(TimeSpan.FromDays(30));

        AwaitAssert(
            () => Assert.Equal(WorkflowStatus.Suspended, Diagnose(actor).Envelope.Status),
            TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// An entity that stops while holding a deadline drops the timer with it. Leaving it armed would
    /// have it fire against an actor that has gone — which passivation makes routine, since an
    /// instance waiting on a long deadline is exactly the kind that gets unloaded.
    /// </summary>
    [Fact]
    public void AnEntityThatStops_LeavesNoTimerBehind()
    {
        var actor = CreateActor(
            nameof(AnEntityThatStops_LeavesNoTimerBehind),
            HoldingScript(), HeldFor(TimeSpan.FromHours(2)));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Done>();

        Watch(actor);
        Sys.Stop(actor);
        ExpectTerminated(actor);

        // Past the hold's instant. A timer left armed would deliver here, to nothing.
        EventFilter.DeadLetter<object>().Expect(0, () => Scheduler.Advance(TimeSpan.FromHours(3)));
    }

    /// <summary>
    /// The hold's instant is absolute and persisted, so an instance that goes away mid-hold comes back
    /// waiting out what is left of it — the same durability every other deadline has.
    /// </summary>
    [Fact]
    public void AHoldSurvivesARestart_AndFiresOnTheRemainingWait()
    {
        const string persistenceId = nameof(AHoldSurvivesARestart_AndFiresOnTheRemainingWait);
        var settings = HeldFor(TimeSpan.FromHours(2));

        var actor = CreateActor(persistenceId, HoldingScript(), settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Done>();

        Watch(actor);
        Sys.Stop(actor);
        ExpectTerminated(actor);

        var recovered = CreateActor(persistenceId, HoldingScript(), settings);
        AwaitAssert(
            () => Assert.Equal(WorkflowStatus.Suspended, Diagnose(recovered).Envelope.Status),
            TimeSpan.FromSeconds(10));

        Scheduler.Advance(TimeSpan.FromHours(3));

        AwaitAssert(() =>
        {
            var envelope = Diagnose(recovered).Envelope;
            Assert.Equal(WorkflowStatus.Finished, envelope.Status);
            Assert.Equal("abandoned", envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }
}
