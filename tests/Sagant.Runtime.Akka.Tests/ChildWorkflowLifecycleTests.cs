using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Descriptors;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Tests.Support;
using Akka.Actor;
using Akka.Delivery;

namespace Sagant.Runtime.Akka.Tests;

public class ChildWorkflowLifecycleTests : WorkflowActorTestKit
{
    public ChildWorkflowLifecycleTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    [Fact]
    public void AwaitChildrenTransition_PersistsRelationshipAsPending_WithFrameworkGeneratedGroupId()
    {
        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(AwaitChildrenTransition_PersistsRelationshipAsPending_WithFrameworkGeneratedGroupId);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            var relationship = Assert.Single(diagnostics.Envelope.Children!);
            Assert.Equal("ScriptableWorkflow", relationship.ChildWorkflowType);
            Assert.Equal("child-1", relationship.ChildWorkflowId);
            Assert.Equal(ChildStatus.Pending, relationship.Status);
            Assert.NotNull(relationship.GroupId);
            Assert.Equal($"{persistenceId}:group:0", relationship.GroupId);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void AwaitChildrenTransition_RetriedBeforePersist_ProducesSameGroupId()
    {
        var attempts = 0;
        var script = Script()
            .Step("StartChildren", (_, _) =>
            {
                attempts++;
                if (attempts < 2)
                {
                    throw new InvalidOperationException("flaky");
                }

                return Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                    new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                    Step<ChildGroupResult>("OnResolved")));
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        var settings = WorkflowSettings.Create().DefaultStepRecovery(RecoverStrategy.WithMaxRetries(3).FailoverTo(Step("StartChildren"))).Build();
        const string persistenceId = nameof(AwaitChildrenTransition_RetriedBeforePersist_ProducesSameGroupId);
        var actor = CreateActor(persistenceId, script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            var relationship = Assert.Single(diagnostics.Envelope.Children!);
            // Confirms ChildGroupSequence was incremented exactly once total, across both attempts.
            Assert.Equal($"{persistenceId}:group:0", relationship.GroupId);
            Assert.Equal(1, diagnostics.Envelope.ChildGroupSequence);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Registers the child's producer adapter with a <see cref="global::Akka.TestKit.TestProbe"/> standing in for a real
    /// <see cref="WorkflowProducerAdapter"/>, asserting the actual <c>Enqueue</c> dispatch for the
    /// framework-generated-GroupId path — the common case, and the one whose group id
    /// <c>PersistEnvelopeThen</c> resolves once up front and reuses for both the persisted
    /// relationship and this send.
    /// </summary>
    [Fact]
    public void AwaitChildrenTransition_SendsEnqueueToRegisteredProducerAdapter_ForFrameworkGeneratedGroupId()
    {
        var producerAdapterProbe = RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(AwaitChildrenTransition_SendsEnqueueToRegisteredProducerAdapter_ForFrameworkGeneratedGroupId);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        var enqueue = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));

        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        var relationship = Assert.Single(diagnostics.Envelope.Children!);

        Assert.Equal(relationship.ChildWorkflowId, enqueue.EntityId);
        Assert.Equal(relationship.ChildWorkflowId, enqueue.Envelope.EntityId);
        Assert.IsType<StartWorkflow>(enqueue.Envelope.Message);
        Assert.Equal(relationship.RelationshipId, enqueue.Envelope.IdempotencyKey);
        Assert.Equal(relationship, enqueue.Envelope.ParentRelationship);
    }

    [Fact]
    public void ChildStartEnvelope_PersistsParentRelationship_AtomicallyWithCommandEffect()
    {
        var script = Script()
            .Command<StartWorkflow>((state, _) => new EffectsBuilder<TestState>().UpdateState(state).Reply("accepted"));

        const string persistenceId = nameof(ChildStartEnvelope_PersistsParentRelationship_AtomicallyWithCommandEffect);
        var confirmProbe = CreateTestProbe();
        var actor = CreateActor(persistenceId, script);

        var relationship = new ChildWorkflowRelationship(
            "rel-1", "ParentWorkflow", "parent-1", "ScriptableWorkflow", persistenceId, "group-0", 0,
            ChildStatus.Pending, null, null, null, ParentClosePolicy.Abandon, new StartWorkflow(1));
        var envelope = new WorkflowEnvelope(persistenceId, new StartWorkflow(1), ReplyTo: null, IdempotencyKey: "rel-1", ParentRelationship: relationship);
        actor.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(envelope, confirmProbe.Ref, "producer-1", 1L));
        confirmProbe.ExpectMsg<ConsumerController.Confirmed>(TimeSpan.FromSeconds(10));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(relationship, diagnostics.Envelope.ParentRelationship);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Two real <see cref="WorkflowEntityActor{TWorkflow, TState}"/> instances — a "parent" and a
    /// "child" — talking to each other over <see cref="RelayProducerAdapter"/> stand-ins for the real
    /// <c>ShardingProducerController</c>/<c>ConsumerController</c> pair, registered in the same
    /// <see cref="WorkflowHandleRegistry"/> both <c>SendChildStart</c> and
    /// <c>SendChildLifecycleNotification</c> resolve through. Driving the parent's own
    /// <c>AwaitChildren</c> step sends a real child-start envelope to the child (proving
    /// <c>SendChildStart</c>'s send lands and the child persists its own <c>ParentRelationship</c>);
    /// the child's step immediately reaching <c>ThenEnd()</c> sends a real
    /// <c>ChildLifecycleNotification</c> back (proving <c>SendChildLifecycleNotification</c> and the
    /// parent's <c>HandleDelivery</c> routing to <c>ApplyChildLifecycleNotification</c> both work end
    /// to end).
    /// </summary>
    [Fact]
    public void ChildReachingEndTransition_WithParentRelationshipSet_SendsLifecycleNotificationToParent()
    {
        const string childPersistenceId = nameof(ChildReachingEndTransition_WithParentRelationshipSet_SendsLifecycleNotificationToParent) + "-child";
        const string parentPersistenceId = nameof(ChildReachingEndTransition_WithParentRelationshipSet_SendsLifecycleNotificationToParent) + "-parent";

        var childActor = CreateActor(childPersistenceId, Script()
            .Step("Run", (state, _) => Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(state).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Run")).ThenReply("accepted")));

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
            parentActor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            var relationship = Assert.Single(diagnostics.Envelope.Children!);
            Assert.Equal(childPersistenceId, relationship.ChildWorkflowId);
            Assert.Equal(ChildStatus.Completed, relationship.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void AllSuccessfulGroup_ResumesOnlyOnceEveryMemberCompletes()
    {
        RegisterScriptableChild();

        var resumed = false;
        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[]
                {
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)),
                    new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-2", new StartWorkflow(1)),
                },
                Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, input) =>
            {
                resumed = true;
                var result = Assert.IsType<ChildGroupResult>(input);
                Assert.Equal(GroupOutcome.Succeeded, result.Outcome);
                return Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(AllSuccessfulGroup_ResumesOnlyOnceEveryMemberCompletes);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Completed, result: "child-1-state");

        AwaitAssert(() => Assert.False(resumed), TimeSpan.FromSeconds(2));

        NotifyChild(actor, GetChild(actor, "child-2"), ChildStatus.Completed, result: "child-2-state");

        AwaitAssert(() => Assert.True(resumed), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void FailFastWithTerminateRemaining_FinalizesImmediately_MarksStragglerTerminationRequested()
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
            .Step("OnResolved", (_, input) =>
            {
                var result = Assert.IsType<ChildGroupResult>(input);
                Assert.Equal(GroupOutcome.Failed, result.Outcome);
                return Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(FailFastWithTerminateRemaining_FinalizesImmediately_MarksStragglerTerminationRequested);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        NotifyChild(actor, GetChild(actor, "child-1"), ChildStatus.Failed, failure: new WorkflowFailure("boom"));

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            var straggler = diagnostics.Envelope.Children!.Single(c => c.ChildWorkflowId == "child-2");
            Assert.Equal(ChildStatus.TerminationRequested, straggler.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ParentReachingEndTransition_TerminatesChildrenWithTerminatePolicy_LeavesAbandonPolicyChildrenAlone()
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
            .Command<StartWorkflow>((_, command) => command.Amount == 1
                ? new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted")
                : new EffectsBuilder<TestState>().Complete().ThenReply("ended"));

        const string persistenceId = nameof(ParentReachingEndTransition_TerminatesChildrenWithTerminatePolicy_LeavesAbandonPolicyChildrenAlone);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));

        // ParentClosePolicy only applies to children still running when the parent reaches its own
        // terminal transition. Driving the parent to End through an independent command gives this
        // test that real state; completing both group members first would leave nothing to stop.
        actor.Tell(new StartWorkflow(2), TestActor);
        ExpectMsg<string>();

        var terminate = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        Assert.Equal("child-terminate", terminate.EntityId);
        Assert.IsType<Terminate>(terminate.Envelope.Message);

        // A late child report cannot revive a parent that has already reached EndTransition.
        NotifyChild(actor, GetChild(actor, "child-terminate"), ChildStatus.Completed, result: "late-state");
        NotifyChild(actor, GetChild(actor, "child-abandon"), ChildStatus.Completed, result: "late-state");

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            var terminateChild = diagnostics.Envelope.Children!.Single(c => c.ChildWorkflowId == "child-terminate");
            var abandonChild = diagnostics.Envelope.Children!.Single(c => c.ChildWorkflowId == "child-abandon");
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal(ChildStatus.TerminationRequested, terminateChild.Status);
            Assert.Equal(ChildStatus.Pending, abandonChild.Status);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Every other <c>ParentClosePolicy</c>/<c>RemainingChildrenPolicy.Terminate</c> test in this
    /// file stands the child in with a bare <see cref="TestProbe"/> (<see cref="RegisterScriptableChild"/>),
    /// which only proves the <c>Terminate</c> envelope was <em>sent</em> — never that a real receiving
    /// actor can actually act on it. This uses two real <see cref="WorkflowEntityActor{TWorkflow, TState}"/>
    /// instances, same as <see cref="ChildReachingEndTransition_WithParentRelationshipSet_SendsLifecycleNotificationToParent"/>,
    /// and drives the cascade all the way through: the child pauses (so it's still non-terminal when
    /// the parent ends), the parent's own terminal transition sends a real <c>Terminate</c> over
    /// <see cref="RelayProducerAdapter"/>, and the child's own status is asserted
    /// <see cref="WorkflowStatus.Finished"/> afterward — proving <see cref="HandleDelivery"/>'s
    /// <c>Terminate</c> branch dispatches straight to <see cref="ApplyTerminate"/>.
    /// </summary>
    [Fact]
    public void ParentReachingEndTransition_ActuallyTerminatesARealChildActor()
    {
        const string childPersistenceId = nameof(ParentReachingEndTransition_ActuallyTerminatesARealChildActor) + "-child";
        const string parentPersistenceId = nameof(ParentReachingEndTransition_ActuallyTerminatesARealChildActor) + "-parent";

        var childActor = CreateActor(childPersistenceId, Script()
            .Step("Wait", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Wait")).ThenReply("accepted")));

        var parentScript = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>(childPersistenceId, new StartWorkflow(1), ParentClosePolicy.Terminate) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, command) => command.Amount == 1
                ? new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted")
                : new EffectsBuilder<TestState>().Complete().ThenReply("ended"));
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
            childActor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));

        // Drives the parent to its own EndTransition while the child relationship is still Pending —
        // the same real trigger ParentClosePolicy.Terminate cascades on.
        parentActor.Tell(new StartWorkflow(2), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            childActor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ParentCrashesBeforeChildStartConfirmed_RecoveryRedeliversStart()
    {
        var producerAdapterProbe = RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        const string persistenceId = nameof(ParentCrashesBeforeChildStartConfirmed_RecoveryRedeliversStart);
        var actor1 = CreateActor(persistenceId, script);
        actor1.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));

        Watch(actor1);
        Sys.Stop(actor1);
        ExpectTerminated(actor1);

        var actor2 = CreateActor(persistenceId, script);
        var redelivery = producerAdapterProbe.ExpectMsg<WorkflowProducerAdapter.Enqueue>(TimeSpan.FromSeconds(10));
        Assert.Equal("child-1", redelivery.EntityId);
        Assert.IsType<StartWorkflow>(redelivery.Envelope.Message);

        AwaitAssert(() =>
        {
            actor2.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            var relationship = Assert.Single(diagnostics.Envelope.Children!);
            Assert.Equal(ChildStatus.Pending, relationship.Status);
            Assert.IsType<StartWorkflow>(relationship.Command);
        }, TimeSpan.FromSeconds(10));
    }
}
