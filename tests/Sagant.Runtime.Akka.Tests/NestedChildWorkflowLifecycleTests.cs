using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Descriptors;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Tests.Support;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// A workflow instance can be a child of one workflow and the parent of another at the same time —
/// nothing about <c>ParentRelationship</c>/<c>Children</c>/<c>AwaitChildren</c> is scoped to one
/// level. This drives a real three-level tree (grandparent → middle → leaf) through actual
/// <see cref="WorkflowEntityActor{TWorkflow, TState}"/> instances — the middle actor is both a
/// registered child of the grandparent and the parent of its own leaf child — and proves the
/// <c>ParentClosePolicy.Terminate</c> cascade actually propagates two real hops deep: grandparent
/// ends → sends a real <c>Terminate</c> to the real middle actor → the middle actor's own
/// <c>HandleTerminate</c> applies its own <c>ParentClosePolicy</c> to its own child, sending a
/// second real <c>Terminate</c> onward. See <see cref="ChildWorkflowLifecycleTests.ParentReachingEndTransition_ActuallyTerminatesARealChildActor"/>
/// for the single-hop version of this proof.
/// </summary>
public class NestedChildWorkflowLifecycleTests : WorkflowActorTestKit
{
    public NestedChildWorkflowLifecycleTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    [Fact]
    public void MiddleActor_IsSimultaneouslyAChildAndAParent_AndTerminateCascadesThroughBothHops()
    {
        const string middlePersistenceId = nameof(MiddleActor_IsSimultaneouslyAChildAndAParent_AndTerminateCascadesThroughBothHops) + "-middle";

        var leafProbe = RegisterAlt2ScriptableChild();

        var middleScript = Script()
            .Step("StartLeafChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<Alt2ScriptableWorkflow>("leaf-1", new StartWorkflow(1), ParentClosePolicy.Terminate) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartLeafChildren")).ThenReply("accepted"));
        var middleActor = CreateAltActor(middlePersistenceId, middleScript);

        var grandparentScript = Script()
            .Step("StartMiddleChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<AltScriptableWorkflow>(middlePersistenceId, new StartWorkflow(1), ParentClosePolicy.Terminate) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, command) => command.Amount == 1
                ? new EffectsBuilder<TestState>().TransitionTo(Step("StartMiddleChildren")).ThenReply("accepted")
                : new EffectsBuilder<TestState>().Complete().ThenReply("ended"));

        var middleRelay = Sys.ActorOf(Props.Create(() => new RelayProducerAdapter(middleActor)));
        var registry = WorkflowHandleRegistryProvider.Instance.Apply(Sys);
        registry.Register<AltScriptableWorkflow, TestState>(CreateTestProbe().Ref, middleRelay);

        var grandparentActor = CreateActor(nameof(MiddleActor_IsSimultaneouslyAChildAndAParent_AndTerminateCascadesThroughBothHops) + "-grandparent", grandparentScript);

        grandparentActor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // First hop of the child-start cascade, sent by the real middle actor for its own leaf.
        var leafStart = leafProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        Assert.Equal("leaf-1", leafStart.EntityId);
        Assert.IsType<StartWorkflow>(leafStart.Envelope.Message);

        AwaitAssert(() =>
        {
            middleActor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();

            // The middle actor is a child of the grandparent...
            Assert.NotNull(diagnostics.Envelope.ParentRelationship);
            Assert.Equal(middlePersistenceId, diagnostics.Envelope.ParentRelationship!.ChildWorkflowId);

            // ...and, at the very same time, the parent of its own leaf. Neither field crowds out
            // the other — this is the whole claim nesting depends on.
            var leafRelationship = Assert.Single(diagnostics.Envelope.Children!);
            Assert.Equal("leaf-1", leafRelationship.ChildWorkflowId);
            Assert.Equal(ChildStatus.Pending, leafRelationship.Status);
        }, TimeSpan.FromSeconds(10));

        // Drives the grandparent to its own EndTransition while the middle relationship is still
        // Pending — the real trigger ParentClosePolicy.Terminate cascades on, same as the
        // single-hop test, but this time the receiving side is itself a parent too.
        grandparentActor.Tell(new StartWorkflow(2), TestActor);
        ExpectMsg<string>();

        // Second hop: the middle actor's own HandleTerminate re-applies ParentClosePolicy to its
        // own child and sends a real Terminate onward, unprompted by anything except having
        // actually processed the first one.
        var leafTerminate = leafProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        Assert.Equal("leaf-1", leafTerminate.EntityId);
        Assert.IsType<Terminate>(leafTerminate.Envelope.Message);

        AwaitAssert(() =>
        {
            middleActor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }
}
