using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowEntityActorTests : WorkflowActorTestKit
{
    public WorkflowEntityActorTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.loglevel = OFF
        """ + "\n" + RecordingSnapshotStore.Config;

    [Fact]
    public void HappyPath_CommandStartsStepChain_EndsWithReply()
    {
        var script = Script()
            .Step("ReserveInventory", (_, input) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "reserved" }).ThenTransitionTo(Step<object>("ChargePayment"), input!)))
            .Step("ChargePayment", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "charged" }).ThenComplete()))
            .Command<StartWorkflow>((_, cmd) => new EffectsBuilder<TestState>()
                .UpdateState(new TestState { Value = "starting" })
                .TransitionTo(Step<object>("ReserveInventory"), cmd.Amount)
                .ThenReply("accepted"));

        var actor = CreateActor(nameof(HappyPath_CommandStartsStepChain_EndsWithReply), script);

        actor.Tell(new StartWorkflow(42), TestActor);
        ExpectMsg<string>(msg => msg == "accepted");

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("charged", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepFailure_WithoutRecoverStrategy_EndsWorkflowWithFailureReason()
    {
        var script = Script()
            .Step("FailingStep", (_, _) => Task.FromException<StepEffect<TestState>>(new InvalidOperationException("boom")))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("FailingStep"))
                .ThenReply("accepted"));

        var actor = CreateActor(nameof(StepFailure_WithoutRecoverStrategy_EndsWorkflowWithFailureReason), script);

        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepFailure_WithRecoverStrategy_RetriesThenFailsOverToCompensationStep()
    {
        var attempts = 0;
        var script = Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempts++;
                return Task.FromException<StepEffect<TestState>>(new InvalidOperationException("flaky failure"));
            })
            .Step("Compensate", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "compensated" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("FlakyStep"))
                .ThenReply("accepted"));

        var settings = WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("Compensate")))
            .Build();
        var actor = CreateActor(nameof(StepFailure_WithRecoverStrategy_RetriesThenFailsOverToCompensationStep), script, settings);

        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("compensated", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));

        Assert.Equal(2, attempts); // 1 initial + 1 retry, then failover
    }

    [Fact]
    public void CrashRecovery_ResumesInFlightStepFromScratch()
    {
        var script = Script()
            .Step("SlowStep", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "done-after-restart" }).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("SlowStep"))
                .ThenReply("accepted"));

        const string persistenceId = nameof(CrashRecovery_ResumesInFlightStepFromScratch);
        var actor1 = CreateActor(persistenceId, script);
        actor1.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Watch(actor1);
        Sys.Stop(actor1);
        ExpectTerminated(actor1);

        var actor2 = CreateActor(persistenceId, script);

        AwaitAssert(() =>
        {
            actor2.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("done-after-restart", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void Snapshot_HappensAfterEachTransition()
    {
        var actor = CreateActor(nameof(Snapshot_HappensAfterEachTransition), Script()
            .Step("StepA", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StepA")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            Assert.NotEmpty(RecordingSnapshotStore.SavesFor(nameof(Snapshot_HappensAfterEachTransition)));
        }, TimeSpan.FromSeconds(10));
    }
}
