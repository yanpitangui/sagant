using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The parent's <c>[WorkflowChildResult]</c> handler: what a parent can do as each of its children
/// settles, rather than only once the whole group resolves.
/// </summary>
public class ChildResultHandlerTests : WorkflowActorTestKit
{
    public ChildResultHandlerTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private WorkflowScript ParentAwaiting(
        int childCount,
        Func<ChildResultContext<TestState>, ChildResultEffect<TestState>> onChildResult)
    {
        var children = Enumerable.Range(1, childCount)
            .Select(i => new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>($"child-{i}", new StartWorkflow(1)))
            .ToArray();

        return Script()
            .Step("StartChildren", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().AwaitChildren(children, Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause()))
            .OnChildResult(onChildResult)
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));
    }

    /// <summary>
    /// A parent accumulates into its own state as each child settles. Without this it hears nothing
    /// until the whole group resolves, so "3 of 10 shipped" is unanswerable while it matters.
    /// </summary>
    [Fact]
    public void EachChildSettling_RunsTheParentsHandler_AndItsStateChangeIsDurable()
    {
        RegisterScriptableChild();

        var actor = CreateActor("ChildResultAccumulate", ParentAwaiting(3, ctx =>
            ChildResultEffects.UpdateState(new TestState { Value = $"settled-{ctx.Settled}-of-{ctx.Total}" })));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Completed, result: "one");

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal("settled-1-of-3", diagnostics.Envelope.UserState.Value);
            // The group is still open: the handler observed one child without resolving anything.
            Assert.Equal(WorkflowStatus.Running, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// The handler can end a group its <c>CompletionPolicy</c> would have kept waiting on — the
    /// business call that no policy can express, e.g. one item being unavailable making the rest
    /// pointless. Resolution then follows the ordinary path, resuming at the group's resume step.
    /// </summary>
    [Fact]
    public void HandlerStoppingTheWait_ResolvesTheGroupBeforeItsPolicyWould()
    {
        RegisterScriptableChild();

        var actor = CreateActor("ChildResultStopWaiting", ParentAwaiting(3, ctx =>
            ChildResultEffects.UpdateState(new TestState { Value = "abandoned" })
                .ThenStopWaiting(GroupOutcome.Failed)));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // One of three reports, where AllSuccessful would otherwise wait for the other two.
        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Completed, result: "one");

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
            Assert.Equal("abandoned", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>A workflow that declares no handler is unaffected: its group resolves purely on
    /// policy, as it always did.</summary>
    [Fact]
    public void WithoutAHandler_TheGroupResolvesOnPolicyAlone()
    {
        RegisterScriptableChild();

        var children = new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("solo", new StartWorkflow(1)) };
        var actor = CreateActor("ChildResultAbsent", Script()
            .Step("StartChildren", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().AwaitChildren(children, Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        NotifyChild(actor, GetChild(actor, "solo"), ChildStatus.Completed, result: "done");

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            Assert.Equal(WorkflowStatus.Paused, ExpectMsg<Diagnostics<TestState>>().Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }
}
