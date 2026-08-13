using Sagant.Protocol;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Guarantee E5 through the durable driver: what happens to an instance standing on a step the code
/// running underneath it no longer registers.
///
/// This is the shape of a deploy that removed a step while instances were persisted on it — the one
/// version-skew hazard the engine can see for itself, since it is the engine that goes looking for
/// the step by name.
/// </summary>
public class WorkflowUnknownStepTests : WorkflowActorTestKit
{
    public WorkflowUnknownStepTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    [Fact]
    public void TransitioningToAnUnknownStep_HoldsTheRunAtThatStep()
    {
        var script = Script()
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step<int>("Ghost"), 42)
                .ThenReply("accepted"));

        var actor = CreateActor(nameof(TransitioningToAnUnknownStep_HoldsTheRunAtThatStep), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Suspended, diagnostics.Envelope.Status);
            Assert.Equal("Ghost", diagnostics.Envelope.CurrentStepName);
            Assert.Equal(42, diagnostics.Envelope.CurrentStepInput);
            Assert.Equal("Ghost", diagnostics.Envelope.ParkedFailure!.StepName);
            Assert.Null(diagnostics.Envelope.Outcome);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// The deploy story end to end: an instance is mid-step when the process dies, comes back on a
    /// deployment missing that step, and is held; the step is deployed again and a resume carries the
    /// same run to completion with its state intact.
    /// </summary>
    [Fact]
    public void AnInstanceRecoveringOntoAMissingStep_IsHeldUntilTheStepIsDeployedAgain()
    {
        const string persistenceId = nameof(AnInstanceRecoveringOntoAMissingStep_IsHeldUntilTheStepIsDeployedAgain);

        var start = Script()
            .Step("Charge", (_, _) => new TaskCompletionSource<StepEffect<TestState>>().Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("Charge"))
                .ThenReply("accepted"));

        var withTheStep = CreateActor(persistenceId, start);
        withTheStep.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Watch(withTheStep);
        Sys.Stop(withTheStep);
        ExpectTerminated(withTheStep);

        // The deploy that dropped the step: same instance, same journal, a workflow with no "Charge".
        var withoutTheStep = CreateActor(persistenceId, Script());

        AwaitAssert(() =>
        {
            withoutTheStep.Tell(new GetDiagnostics<TestState>(), TestActor);
            var held = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Suspended, held.Envelope.Status);
            Assert.Equal("Charge", held.Envelope.CurrentStepName);
            Assert.Contains("Charge", held.Envelope.ParkedFailure!.Message);
        }, TimeSpan.FromSeconds(10));

        Watch(withoutTheStep);
        Sys.Stop(withoutTheStep);
        ExpectTerminated(withoutTheStep);

        // The deploy that put it back.
        var restored = CreateActor(persistenceId, Script()
            .Step("Charge", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>()
                .UpdateState(new TestState { Value = "charged" })
                .ThenComplete())));

        restored.Tell(new Resume(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        AwaitAssert(() =>
        {
            restored.Tell(new GetDiagnostics<TestState>(), TestActor);
            var finished = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, finished.Envelope.Status);
            Assert.Equal("charged", finished.Envelope.UserState.Value);
            Assert.Null(finished.Envelope.ParkedFailure);
        }, TimeSpan.FromSeconds(10));
    }
}
