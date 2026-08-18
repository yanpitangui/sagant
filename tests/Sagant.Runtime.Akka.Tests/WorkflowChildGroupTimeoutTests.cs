using Akka.Actor;
using Akka.TestKit;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// A group whose children never finish. Covers the wiring the fold's tests cannot reach: the group's
/// instant reaching <see cref="ChildGroupState.Deadline"/>, the per-group timer the actor keeps, and
/// the step the group named running when nobody reports back.
///
/// Also the reason a group's key carries a discriminator — two groups awaited at once each keep their
/// own wait, and one resolving leaves the other's alone.
/// </summary>
public class WorkflowChildGroupTimeoutTests : WorkflowActorTestKit
{
    public WorkflowChildGroupTimeoutTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.scheduler.implementation = "Akka.TestKit.TestScheduler, Akka.TestKit"
        akka.loglevel = OFF
        """;

    private TestScheduler Scheduler => (TestScheduler)Sys.Scheduler;

    private Diagnostics<TestState> Diagnose(IActorRef actor)
    {
        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        return ExpectMsg<Diagnostics<TestState>>();
    }

    /// <summary>Awaits one group whose child is never reported settled.</summary>
    private WorkflowScript WaitingScript(TimeSpan groupTimeout) =>
        Script()
            .Step("StartChildren", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().AwaitChildren(
                    new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                    options => options
                        .GroupId("items")
                        .ResumeAt(Step<ChildGroupResult>("OnResolved"))
                        .Timeout(groupTimeout, Step<ChildGroupResult>("OnLate")))))
            .Step("OnResolved", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "resolved" }).ThenComplete()))
            .Step("OnLate", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "gave-up" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

    [Fact]
    public void AGroupWhoseChildrenNeverFinish_RunsTheStepItNamed()
    {
        // Inert delivery endpoints, so the child is startable and simply never reports back — which
        // is the case a group timeout exists for.
        RegisterScriptableChild();
        var actor = CreateActor(
            nameof(AGroupWhoseChildrenNeverFinish_RunsTheStepItNamed), WaitingScript(TimeSpan.FromHours(2)));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(
            () => Assert.NotNull(Diagnose(actor).Envelope.ChildGroups),
            TimeSpan.FromSeconds(10));

        Scheduler.Advance(TimeSpan.FromHours(3));

        AwaitAssert(() =>
        {
            var envelope = Diagnose(actor).Envelope;
            Assert.Equal(WorkflowStatus.Finished, envelope.Status);
            Assert.Equal("gave-up", envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>The group's instant is recorded on the group itself, which is what a wake keyed by
    /// group id refers to.</summary>
    [Fact]
    public void AGroupWithATimeout_RecordsItsOwnDeadline()
    {
        RegisterScriptableChild();
        var actor = CreateActor(
            nameof(AGroupWithATimeout_RecordsItsOwnDeadline), WaitingScript(TimeSpan.FromHours(2)));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var group = Assert.Single(Diagnose(actor).Envelope.ChildGroups!).Value;
            Assert.NotNull(group.Deadline);
            Assert.Equal("OnLate", group.TimeoutStepName);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>A group that waits for its children however long they take is the default, and stays
    /// so: nothing is recorded and no wake is ever owed for it.</summary>
    [Fact]
    public void AGroupWithNoTimeout_RecordsNoDeadline()
    {
        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().AwaitChildren(
                    new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                    Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        RegisterScriptableChild();
        var actor = CreateActor(nameof(AGroupWithNoTimeout_RecordsNoDeadline), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var group = Assert.Single(Diagnose(actor).Envelope.ChildGroups!).Value;
            Assert.Null(group.Deadline);
            Assert.Null(group.TimeoutStepName);
        }, TimeSpan.FromSeconds(10));

        Scheduler.Advance(TimeSpan.FromDays(30));

        AwaitAssert(
            () => Assert.Equal(WorkflowStatus.Running, Diagnose(actor).Envelope.Status),
            TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// The group's instant is absolute and persisted on the group, so an instance that goes away
    /// mid-wait comes back owing what is left of it — the actor re-arms a timer per live group as it
    /// recovers.
    /// </summary>
    [Fact]
    public void AGroupsWaitSurvivesARestart()
    {
        const string persistenceId = nameof(AGroupsWaitSurvivesARestart);
        var script = WaitingScript(TimeSpan.FromHours(2));

        RegisterScriptableChild();
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        AwaitAssert(() => Assert.NotNull(Diagnose(actor).Envelope.ChildGroups), TimeSpan.FromSeconds(10));

        Watch(actor);
        Sys.Stop(actor);
        ExpectTerminated(actor);

        var recovered = CreateActor(persistenceId, script);
        AwaitAssert(() => Assert.NotNull(Diagnose(recovered).Envelope.ChildGroups), TimeSpan.FromSeconds(10));

        Scheduler.Advance(TimeSpan.FromHours(3));

        AwaitAssert(() =>
        {
            var envelope = Diagnose(recovered).Envelope;
            Assert.Equal(WorkflowStatus.Finished, envelope.Status);
            Assert.Equal("gave-up", envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// A group's key carries its id, so a wake for one names that one. Asserted directly on the
    /// fold, since it is the key an outside scheduler stores.
    /// </summary>
    [Fact]
    public void TwoGroupsAwaitedAtOnce_EachArmUnderItsOwnId()
    {
        var cause = new TransitionCause.Control("Test");
        var due = DateTimeOffset.UtcNow.AddHours(2);

        var items = WorkflowDeadlineFold.Changes(new WorkflowEvent.ChildrenAwaited(
            "items", [], Group("items", due), 1, null, cause));
        var notify = WorkflowDeadlineFold.Changes(new WorkflowEvent.ChildrenAwaited(
            "notify", [], Group("notify", due), 2, null, cause));

        Assert.Equal("items", Assert.Single(items.OfType<WorkflowDeadlineChange.Arm>()).Discriminator);
        Assert.Equal("notify", Assert.Single(notify.OfType<WorkflowDeadlineChange.Arm>()).Discriminator);

        var finalized = WorkflowDeadlineFold.Changes(new WorkflowEvent.ChildGroupFinalized("items", [], false));
        var disarm = Assert.Single(finalized.OfType<WorkflowDeadlineChange.Disarm>());
        Assert.Equal("items", disarm.Discriminator);
    }

    private static ChildGroupState Group(string groupId, DateTimeOffset deadline) =>
        new(groupId, Generation: 0, CompletionPolicy.AllSuccessful, FailurePolicy.FailFast,
            RemainingChildrenPolicy.Terminate, "OnResolved", Finalized: false, deadline, "OnLate");
}
