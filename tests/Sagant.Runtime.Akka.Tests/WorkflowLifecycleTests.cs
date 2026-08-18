using Sagant.Protocol;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowLifecycleTests : WorkflowActorTestKit
{
    public WorkflowLifecycleTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    [Fact]
    public void Suspend_WhileStepInFlight_FreezesStatusAndPreservesStepPosition()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Suspend_WhileStepInFlight_FreezesStatusAndPreservesStepPosition), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Suspended, diagnostics.Envelope.Status);
        Assert.Equal("HangingStep", diagnostics.Envelope.CurrentStepName);
    }

    [Fact]
    public void Suspend_ThenLateStepResult_IsDiscarded()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Suspend_ThenLateStepResult_IsDiscarded), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        // The step's result arrives after suspend — must be discarded outright, via the stale epoch,
        // never applied.
        neverCompletes.SetResult(new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "should-not-apply" }).ThenComplete());

        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Suspended, diagnostics.Envelope.Status);
        Assert.NotEqual("should-not-apply", diagnostics.Envelope.UserState.Value);
    }

    [Fact]
    public void Resume_AfterSuspend_ReExecutesInFlightStepFromScratch()
    {
        var attempts = 0;
        var script = Script()
            .Step("CountingStep", (_, _) =>
            {
                attempts++;
                return attempts == 1
                    ? new TaskCompletionSource<StepEffect<TestState>>().Task // first attempt hangs forever
                    : Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = $"attempt-{attempts}" }).ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("CountingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Resume_AfterSuspend_ReExecutesInFlightStepFromScratch), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        actor.Tell(new Resume(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("attempt-2", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));

        Assert.Equal(2, attempts);
    }

    [Fact]
    public void Terminate_EndsWorkflowAndIsIdempotent()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Terminate_EndsWorkflowAndIsIdempotent), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Terminate(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);

        // idempotent: terminating an already-terminated workflow succeeds, doesn't error
        actor.Tell(new Terminate(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();
    }

    [Fact]
    public void Suspend_WhenNotRunning_ReturnsFailure()
    {
        var script = Script()
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().Complete().ThenReply("accepted"));

        var actor = CreateActor(nameof(Suspend_WhenNotRunning_ReturnsFailure), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<global::Akka.Actor.Status.Failure>();
    }

    [Fact]
    public void Resume_WhenNotSuspended_ReturnsFailure()
    {
        var actor = CreateActor(nameof(Resume_WhenNotSuspended_ReturnsFailure), Script());

        actor.Tell(new Resume(), TestActor);
        ExpectMsg<global::Akka.Actor.Status.Failure>();
    }

    [Fact]
    public void Suspend_SurvivesRecovery_StaysFrozenWithoutAutoResuming()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        const string persistenceId = nameof(Suspend_SurvivesRecovery_StaysFrozenWithoutAutoResuming);
        var actor1 = CreateActor(persistenceId, script);
        actor1.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        actor1.Tell(new Suspend(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        Watch(actor1);
        Sys.Stop(actor1);
        ExpectTerminated(actor1);

        var actor2 = CreateActor(persistenceId, script);
        AwaitAssert(() =>
        {
            actor2.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Suspended, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void GetStatus_ReturnsCurrentEngineLevelStatus_WithoutRequiringACustomCommandHandler()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(GetStatus_ReturnsCurrentEngineLevelStatus_WithoutRequiringACustomCommandHandler), script);

        // Asked before anything has been sent, so this is an entity sharding activated to answer the
        // question and nothing more.
        actor.Tell(new GetStatus(), TestActor);
        Assert.Equal(WorkflowStatus.NotStarted, ExpectMsg<WorkflowStatusReply>().Status);

        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        actor.Tell(new GetStatus(), TestActor);
        Assert.Equal(WorkflowStatus.Suspended, ExpectMsg<WorkflowStatusReply>().Status);
    }
}
