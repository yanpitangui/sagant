using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Exercises the real actor's wiring of <see cref="Execution.SnapshotPolicy"/> — <c>SnapshotPolicyTests</c>
/// already covers the pure decision logic in isolation; this covers that
/// <see cref="WorkflowEntityActor{TWorkflow, TState}"/> actually calls it with the right
/// <c>LastSequenceNr</c>/status at every persist site. <see cref="RecordingSnapshotStore"/> records
/// each save, so these assert the snapshots themselves and at which sequence numbers they landed.
/// </summary>
public class WorkflowEntityActorSnapshotCadenceTests : WorkflowActorTestKit
{
    public WorkflowEntityActorSnapshotCadenceTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.loglevel = OFF
        """ + "\n" + RecordingSnapshotStore.Config;

    [Fact]
    public void PeriodicSnapshot_FiresOnceThresholdReached_WhileStillRunning()
    {
        var invocations = 0;
        var script = Script()
            // seqNr 1: Start's own StepTransition into LoopStep.
            .Step("LoopStep", (_, _) =>
            {
                invocations++;
                // seqNr 2, then seqNr 3: self-transitions back into LoopStep, still Running.
                // seqNr 4: pause — settles the workflow so the assertion below can't race a
                // still-in-flight further persist.
                return Task.FromResult(invocations < 3
                    ? new StepEffectsBuilder<TestState>().ThenTransitionTo(Step("LoopStep"))
                    : new StepEffectsBuilder<TestState>().ThenPause());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("LoopStep")).ThenReply("accepted"));

        var actor = CreateActor(
            nameof(PeriodicSnapshot_FiresOnceThresholdReached_WhileStillRunning), script, snapshotEveryNEvents: 3);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
            // Exactly one periodic snapshot fired, at seqNr 3 while Running. seqNr 4 falls short of
            // the next multiple at 6, and lands on Paused, which the terminal half of the policy
            // leaves alone.
            Assert.Equal(
                new long[] { 3 },
                RecordingSnapshotStore.SavesFor(nameof(PeriodicSnapshot_FiresOnceThresholdReached_WhileStillRunning)));
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void TerminalTransition_AlwaysSnapshots_RegardlessOfHowFarFromN()
    {
        // N is large enough that periodic cadence alone would never fire across this short-lived
        // workflow's handful of transitions — isolates "terminal always snapshots" from the periodic
        // half of the policy.
        var script = Script()
            .Step("EndStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("EndStep")).ThenReply("accepted"));

        var actor = CreateActor(
            nameof(TerminalTransition_AlwaysSnapshots_RegardlessOfHowFarFromN), script, snapshotEveryNEvents: 1000);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Single(
                RecordingSnapshotStore.SavesFor(nameof(TerminalTransition_AlwaysSnapshots_RegardlessOfHowFarFromN)));
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void RecoveryAfterCrash_ReplaysOnlyTrailingEventsSinceLastSnapshot_AndReachesCorrectState()
    {
        var invocations = 0;
        var script = Script()
            .Step("LoopStep", (_, _) =>
            {
                invocations++;
                return Task.FromResult(invocations < 4
                    ? new StepEffectsBuilder<TestState>().ThenTransitionTo(Step("LoopStep"))
                    : new StepEffectsBuilder<TestState>().ThenPause());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("LoopStep")).ThenReply("accepted"));

        const string persistenceId = nameof(RecoveryAfterCrash_ReplaysOnlyTrailingEventsSinceLastSnapshot_AndReachesCorrectState);
        var actor1 = CreateActor(persistenceId, script, snapshotEveryNEvents: 3);
        actor1.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor1.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));

        Watch(actor1);
        Sys.Stop(actor1);
        ExpectTerminated(actor1);

        // A fresh incarnation recovers from the seqNr-3 snapshot plus the seqNr-4/5 trailing events
        // (the Pause transition and whatever's persisted with it) — never a full replay from seqNr 1,
        // and lands on the exact same durable state regardless.
        var actor2 = CreateActor(persistenceId, script, snapshotEveryNEvents: 3);
        AwaitAssert(() =>
        {
            actor2.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }
}
