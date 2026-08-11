using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// <see cref="WorkflowSettings.PruneFinalizedChildren"/> — opt-in, default <c>false</c>. Every other
/// child-lifecycle test in <c>ChildWorkflowLifecycleTests</c>/<c>NestedChildWorkflowLifecycleTests</c>
/// runs with the default and already asserts finalized members stay in
/// <c>WorkflowRuntimeState.Children</c> — that coverage is the guard that the default behavior is
/// unchanged. This file covers only the opt-in-enabled behavior.
/// </summary>
public class ChildWorkflowPruningTests : WorkflowActorTestKit
{
    public ChildWorkflowPruningTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    [Fact]
    public void PruneFinalizedChildren_Enabled_DropsFinalizedGroupMembersOnceGroupResolves()
    {
        RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[]
                {
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)),
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-2", new StartWorkflow(1)),
                },
                Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        var settings = WorkflowSettings.Create().PruneFinalizedChildren().Build();
        const string persistenceId = nameof(PruneFinalizedChildren_Enabled_DropsFinalizedGroupMembersOnceGroupResolves);
        var actor = CreateActor(persistenceId, script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Completed, result: "child-1-state");
        NotifyChild(actor, GetChild(actor, "child-2"), ChildStatus.Completed, result: "child-2-state");

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Empty(diagnostics.Envelope.Children!);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void PruneFinalizedChildren_Enabled_LeavesStillPendingStragglerInPlace()
    {
        RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[]
                {
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-fails-fast", new StartWorkflow(1)),
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-straggler", new StartWorkflow(1)),
                },
                options => options.FailFast().ContinueRemaining().ResumeAt(Step<ChildGroupResult>("OnResolved")))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        var settings = WorkflowSettings.Create().PruneFinalizedChildren().Build();
        const string persistenceId = nameof(PruneFinalizedChildren_Enabled_LeavesStillPendingStragglerInPlace);
        var actor = CreateActor(persistenceId, script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // FailFast finalizes the group as soon as one member fails, regardless of the straggler still
        // being Pending — RemainingChildrenPolicy.Ignore means this parent never asks it to terminate.
        NotifyChild(actor, GetChild(actor, "child-fails-fast"), ChildStatus.Failed, failure: new WorkflowFailure("boom"));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            // The failed member reached a terminal ChildStatus in a now-finalized group — pruned.
            // The straggler is still Pending — left in place even though its group has finalized
            // around it (see PruneFinalizedGroupMembers's own doc comment).
            var remaining = Assert.Single(diagnostics.Envelope.Children!);
            Assert.Equal("child-straggler", remaining.ChildWorkflowId);
            Assert.Equal(ChildStatus.Pending, remaining.Status);
        }, TimeSpan.FromSeconds(10));
    }
}
