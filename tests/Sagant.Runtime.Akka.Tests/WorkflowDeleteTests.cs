using Sagant.Protocol;
using Sagant.Effects;
using Sagant.Runtime.Akka;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Tests.Support;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Verifies <see cref="Delete"/> — physically purges the journal/snapshots for this instance rather
/// than just flipping status (see <see cref="Terminate"/>), and the business-level
/// <see cref="Sagant.Effects.Transition.DeleteTransition"/>'s convergence on the same purge.
/// </summary>
public class WorkflowDeleteTests : WorkflowActorTestKit
{
    public WorkflowDeleteTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    /// <summary>
    /// Simulates the Shard's echo of the stop message a real <c>Passivate</c> round trip produces
    /// (see <see cref="PurgeStopMessage"/>'s own doc comment) — the same convention
    /// <see cref="WorkflowGracefulShutdownTests"/> already uses for <c>GracefulShutdown</c>, the
    /// production stop message a real Shard sends directly.
    /// </summary>
    private void SimulateShardEchoAndAwaitStop(IActorRef actor)
    {
        Watch(actor);
        actor.Tell(new PurgeStopMessage(), TestActor);
        ExpectTerminated(actor);
    }

    [Fact]
    public void Delete_OnActiveWorkflow_ForceStopsAndRepliesDoneOnceConfirmed()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Delete_OnActiveWorkflow_ForceStopsAndRepliesDoneOnceConfirmed), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Delete(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();
    }

    [Fact]
    public void Delete_DoesNotStopUntilShardEchoesStopMessage()
    {
        var actor = CreateActor(nameof(Delete_DoesNotStopUntilShardEchoesStopMessage), Script());
        Watch(actor);

        actor.Tell(new Delete(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        // Purge is confirmed and Done already sent, but this actor stays alive and responsive here —
        // it waits for its Shard parent to echo the stop message back first (see PurgeStopMessage's
        // own doc comment) before it actually stops.
        actor.Tell(new GetStatus(), TestActor);
        Assert.Equal(WorkflowStatus.Deleted, ExpectMsg<WorkflowStatus>());

        actor.Tell(new PurgeStopMessage(), TestActor);
        ExpectTerminated(actor);
    }

    [Fact]
    public void Delete_ThenNewActorSamePersistenceId_RecoversAsGenuinelyFreshInstance()
    {
        var script = Script()
            .Step("Run", (state, _) => Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(state).ThenPause()))
            .Command<StartWorkflow>((_, command) =>
            {
                var state = new TestState { Value = $"amount-{command.Amount}" };
                return new EffectsBuilder<TestState>().UpdateState(state).TransitionTo(Step("Run")).ThenReply("accepted");
            });

        const string persistenceId = nameof(Delete_ThenNewActorSamePersistenceId_RecoversAsGenuinelyFreshInstance);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(42), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Delete(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();
        SimulateShardEchoAndAwaitStop(actor);

        var recovered = CreateActor(persistenceId, script);
        recovered.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Running, diagnostics.Envelope.Status);
        Assert.Null(diagnostics.Envelope.CurrentStepName);
        Assert.Equal("initial", diagnostics.Envelope.UserState.Value);
    }

    [Fact]
    public void Delete_OnAlreadyTerminalWorkflow_StillPurges()
    {
        var script = Script()
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().Complete().ThenReply("ended"));

        const string persistenceId = nameof(Delete_OnAlreadyTerminalWorkflow_StillPurges);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new GetStatus(), TestActor);
        Assert.Equal(WorkflowStatus.Finished, ExpectMsg<WorkflowStatus>());

        // Purging an already-terminal workflow's leftover data is the primary use case, not an edge
        // case — no early-return the way ApplyTerminate has for an already-terminal status.
        actor.Tell(new Delete(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();
        SimulateShardEchoAndAwaitStop(actor);

        var recovered = CreateActor(persistenceId, script);
        recovered.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Running, diagnostics.Envelope.Status);
    }

    [Fact]
    public void Delete_CascadesDeleteNotTerminate_ToTerminatePolicyChildren_LeavesAbandonPolicyChildrenAlone()
    {
        var producerAdapterProbe = RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[]
                {
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-terminate", new StartWorkflow(1), ParentClosePolicy.Terminate),
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-abandon", new StartWorkflow(1), ParentClosePolicy.Abandon),
                },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(Delete_CascadesDeleteNotTerminate_ToTerminatePolicyChildren_LeavesAbandonPolicyChildrenAlone);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));

        actor.Tell(new Delete(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        var deleted = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        Assert.Equal("child-terminate", deleted.EntityId);
        Assert.IsType<Delete>(deleted.Envelope.Message);

        // Nothing else was sent for child-abandon — the only other message this probe could receive
        // is a second cascade send, and there isn't one.
        producerAdapterProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void Delete_NotifiesWaitingParent_AsCancelledNotCompleted()
    {
        const string childPersistenceId = nameof(Delete_NotifiesWaitingParent_AsCancelledNotCompleted) + "-child";
        const string parentPersistenceId = nameof(Delete_NotifiesWaitingParent_AsCancelledNotCompleted) + "-parent";

        var childActor = CreateActor(childPersistenceId, Script()
            .Step("Wait", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Wait")).ThenReply("accepted")));

        var parentScript = Script()
            .Step("AwaitStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>(childPersistenceId, new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("AwaitStep")).ThenReply("accepted"));
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
            childActor.Tell(new GetStatus(), TestActor);
            Assert.Equal(WorkflowStatus.Paused, ExpectMsg<WorkflowStatus>());
        }, TimeSpan.FromSeconds(10));

        // The child is deleted directly (an operator cleaning it up), not reached via its own
        // business logic — the parent still needs to hear about it, but as what it actually was: a
        // child that went away without finishing, which is not the same thing as one that completed.
        childActor.Tell(new Delete(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        AwaitAssert(() =>
        {
            parentActor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            var relationship = Assert.Single(diagnostics.Envelope.Children!);
            Assert.Equal(childPersistenceId, relationship.ChildWorkflowId);
            Assert.Equal(ChildStatus.Cancelled, relationship.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void DeleteTransition_BusinessLevel_AlsoPhysicallyPurges()
    {
        var script = Script()
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().Delete("self-deleted").ThenReply("deleted"));

        const string persistenceId = nameof(DeleteTransition_BusinessLevel_AlsoPhysicallyPurges);
        var actor = CreateActor(persistenceId, script);
        Watch(actor);

        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // No external Delete command is sent here — the workflow's own step logic decided to delete
        // itself. It converges on the same purge-then-stop routine, so the actor still needs the
        // Shard's echo before it actually stops.
        actor.Tell(new PurgeStopMessage(), TestActor);
        ExpectTerminated(actor);

        var recovered = CreateActor(persistenceId, script);
        recovered.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        Assert.Equal(WorkflowStatus.Running, diagnostics.Envelope.Status);
    }

    [Fact]
    public void DeleteTransition_BusinessLevel_CascadesDeleteToTerminatePolicyChildren()
    {
        var producerAdapterProbe = RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-terminate", new StartWorkflow(1), ParentClosePolicy.Terminate) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, command) => command.Amount == 1
                ? new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted")
                : new EffectsBuilder<TestState>().Delete("self-deleted").ThenReply("deleted"));

        const string persistenceId = nameof(DeleteTransition_BusinessLevel_CascadesDeleteToTerminatePolicyChildren);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));

        actor.Tell(new StartWorkflow(2), TestActor);
        ExpectMsg<string>();

        var deleted = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        Assert.Equal("child-terminate", deleted.EntityId);
        Assert.IsType<Delete>(deleted.Envelope.Message);
    }
}
