using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Tests.Support;
using System.Collections.Concurrent;
using System.Diagnostics;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Tests;

public class ChildWorkflowTraceLinkingTests : WorkflowActorTestKit, IDisposable
{
    public ChildWorkflowTraceLinkingTests() : base(Config)
    {
        _capturedActivities = new ConcurrentBag<Activity>();
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == WorkflowDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => _capturedActivities.Add(a),
        };
        ActivitySource.AddActivityListener(_listener);
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private readonly ActivityListener _listener;
    private readonly ConcurrentBag<Activity> _capturedActivities;

    // Explicit interface impl: TestKit's own public Dispose() (which shuts down the ActorSystem)
    // isn't virtual, so a same-named public method here would just hide it — xUnit always
    // disposes via the IDisposable reference, so this guarantees both run, in the right order.
    void IDisposable.Dispose()
    {
        _listener.Dispose();
        base.Dispose();
    }

    [Fact]
    public void ChildFirstActivity_LinksBackToParentSpanAtStartTime()
    {
        const string childPersistenceId = nameof(ChildFirstActivity_LinksBackToParentSpanAtStartTime) + "-child";
        const string parentPersistenceId = nameof(ChildFirstActivity_LinksBackToParentSpanAtStartTime) + "-parent";

        var childActor = CreateActor(childPersistenceId, Script()
            .Step("Run", (state, _) => Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(state).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Run")).ThenReply("accepted")));

        var parentScript = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>(childPersistenceId, new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));
        var parentActor = CreateAltActor(parentPersistenceId, parentScript);

        var childRelay = Sys.ActorOf(Props.Create(() => new RelayProducerAdapter(childActor)));
        var parentRelay = Sys.ActorOf(Props.Create(() => new RelayProducerAdapter(parentActor)));

        var registry = WorkflowHandleRegistryProvider.Instance.Apply(Sys);
        registry.Register<ScriptableWorkflow, TestState>(CreateTestProbe().Ref, childRelay);
        registry.Register<AltScriptableWorkflow, TestState>(CreateTestProbe().Ref, parentRelay);

        parentActor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var parentStep = Assert.Single(_capturedActivities, a =>
                a.OperationName.EndsWith("Step.StartChildren") && (string?)a.GetTagItem("workflow.persistence_id") == parentPersistenceId);
            var childFirstActivity = Assert.Single(_capturedActivities, a =>
                (string?)a.GetTagItem("workflow.persistence_id") == childPersistenceId && a.OperationName.EndsWith("StartWorkflow"));

            Assert.Contains(childFirstActivity.Links, l => l.Context.TraceId == parentStep.TraceId);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ResumeStep_LinksToEveryGroupMembersFinalTrace_WhenGroupFinalizesAllSuccessful()
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

        const string persistenceId = nameof(ResumeStep_LinksToEveryGroupMembersFinalTrace_WhenGroupFinalizesAllSuccessful);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        var traceParent1 = $"00-{ActivityTraceId.CreateRandom()}-{ActivitySpanId.CreateRandom()}-01";
        var traceParent2 = $"00-{ActivityTraceId.CreateRandom()}-{ActivitySpanId.CreateRandom()}-01";
        ActivityContext.TryParse(traceParent1, null, out var context1);
        ActivityContext.TryParse(traceParent2, null, out var context2);

        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Completed, result: "child-1-state", resultTraceParent: traceParent1);
        NotifyChild(actor, GetChild(actor, "child-2"), ChildStatus.Completed, result: "child-2-state", resultTraceParent: traceParent2);

        AwaitAssert(() =>
        {
            var resumeActivity = Assert.Single(_capturedActivities, a =>
                a.OperationName.EndsWith("Step.OnResolved") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
            Assert.Contains(resumeActivity.Links, l => l.Context.TraceId == context1.TraceId);
            Assert.Contains(resumeActivity.Links, l => l.Context.TraceId == context2.TraceId);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ResumeStep_LinksOnlyToReportingMembers_StragglerNeverReportedContributesNoLink()
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

        const string persistenceId = nameof(ResumeStep_LinksOnlyToReportingMembers_StragglerNeverReportedContributesNoLink);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        var traceParent1 = $"00-{ActivityTraceId.CreateRandom()}-{ActivitySpanId.CreateRandom()}-01";
        ActivityContext.TryParse(traceParent1, null, out var context1);

        // FailFast (the default FailurePolicy) finalizes the group on child-1's failure alone —
        // child-2 stays a straggler that never sends its own notification.
        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Failed, failure: new WorkflowFailure("boom"), resultTraceParent: traceParent1);

        AwaitAssert(() =>
        {
            var resumeActivity = Assert.Single(_capturedActivities, a =>
                a.OperationName.EndsWith("Step.OnResolved") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
            Assert.Single(resumeActivity.Links);
            Assert.Contains(resumeActivity.Links, l => l.Context.TraceId == context1.TraceId);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ResumeStep_LinksToATerminatedMembersFinalTrace_SameAsCompletedOrFailed()
    {
        RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                options => options.AllCompleted().ResumeAt(Step<ChildGroupResult>("OnResolved")))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(ResumeStep_LinksToATerminatedMembersFinalTrace_SameAsCompletedOrFailed);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        var traceParent1 = $"00-{ActivityTraceId.CreateRandom()}-{ActivitySpanId.CreateRandom()}-01";
        ActivityContext.TryParse(traceParent1, null, out var context1);

        // AllCompleted accepts any terminal status, so a lone Terminated member (the outcome a
        // cascade-terminated straggler reports back with — see ApplyTerminate/SendChildLifecycleNotification)
        // finalizes the group on its own, exercising the same code path a completed/failed member does.
        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Terminated, resultTraceParent: traceParent1);

        AwaitAssert(() =>
        {
            var resumeActivity = Assert.Single(_capturedActivities, a =>
                a.OperationName.EndsWith("Step.OnResolved") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
            Assert.Contains(resumeActivity.Links, l => l.Context.TraceId == context1.TraceId);
        }, TimeSpan.FromSeconds(10));
    }
}
