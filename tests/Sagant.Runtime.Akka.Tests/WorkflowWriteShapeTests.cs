using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Guarantees about the writes themselves, asserted against a <see cref="RecordingJournal"/>. The
/// resulting state looks the same whether a transition wrote one batch or five, and whether a child
/// report wrote one relationship or the whole group — so these two promises are only observable here.
/// </summary>
public class WorkflowWriteShapeTests : WorkflowActorTestKit
{
    public WorkflowWriteShapeTests() : base(RecordingJournal.Config + "\nakka.loglevel = OFF")
    {
    }

    /// <summary>
    /// Guarantee H5. Each of the <c>n</c> children reports once, and each report writes a single
    /// <see cref="WorkflowEvent.ChildMemberUpdated"/> naming one member — so relationships written
    /// across the whole fan-out grow with <c>n</c>.
    ///
    /// The number to watch is relationships appended to the journal, which is exactly <c>2n</c>: the
    /// group's own event carries all <c>n</c> as it opens, then one per report. Run across a spread of
    /// group sizes, because a quadratic term stays small enough to look linear at any single size — at
    /// <c>n</c> = 32 a parent rewriting its whole child list would reach 1056 here.
    ///
    /// Scoped to the journal. Snapshots re-serialize the whole relationship list each time they run,
    /// which H5 accounts for separately; this test says nothing about them.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(32)]
    public void H5_EachChildReport_WritesOneRelationship(int childCount)
    {
        RegisterScriptableChild();

        var children = Enumerable.Range(1, childCount)
            .Select(i => new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>($"child-{i}", new StartWorkflow(1)))
            .ToArray();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().AwaitChildren(children, Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));

        var persistenceId = $"{nameof(H5_EachChildReport_WritesOneRelationship)}-{childCount}";
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        for (var i = 1; i <= childCount; i++)
        {
            NotifyChild(actor, GetChild(actor, $"child-{i}"), ChildStatus.Completed, result: $"child-{i}-state");
        }

        AwaitAssert(
            () =>
            {
                actor.Tell(new GetDiagnostics<TestState>(), TestActor);
                Assert.Equal(WorkflowStatus.Finished, ExpectMsg<Diagnostics<TestState>>().Envelope.Status);
            },
            TimeSpan.FromSeconds(10));

        var events = RecordingJournal.EventsFor(persistenceId);

        var awaited = Assert.Single(events.OfType<WorkflowEvent.ChildrenAwaited>());
        Assert.Equal(childCount, awaited.Relationships.Count);

        var reports = events.OfType<WorkflowEvent.ChildMemberUpdated>().ToList();
        Assert.Equal(childCount, reports.Count);
        Assert.Equal(childCount, reports.Select(r => r.RelationshipId).Distinct().Count());

        // Summed across every event this run wrote, so an event carrying more relationships than it
        // needs shows up here even if the two counts above still look right.
        var relationshipsWritten = events.Sum(e => e switch
        {
            WorkflowEvent.ChildrenAwaited a => a.Relationships.Count,
            WorkflowEvent.ChildMemberUpdated => 1,
            WorkflowEvent.ChildGroupFinalized f => f.TerminationRequested.Count,
            WorkflowEvent.ParentClosePolicyApplied p => p.TerminationRequested.Count,
            _ => 0,
        });

        Assert.Equal(2 * childCount, relationshipsWritten);
    }

    /// <summary>
    /// Guarantee D1. A transition that changes several facts writes them as one atomic batch, so
    /// recovery finds an instance at a transition boundary. Here the transition that opens the group
    /// also carries the state the command persisted — one batch holding both.
    /// </summary>
    [Fact]
    public void D1_ATransitionChangingSeveralFacts_WritesOneBatch()
    {
        RegisterScriptableChild();

        var script = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>("child-1", new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Step("OnResolved", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .UpdateState(new TestState { Value = "started" })
                .TransitionTo(Step("StartChildren"))
                .ThenReply("accepted"));

        const string persistenceId = nameof(D1_ATransitionChangingSeveralFacts_WritesOneBatch);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(
            () => Assert.Contains(
                RecordingJournal.BatchesFor(persistenceId),
                batch => batch.OfType<WorkflowEvent.UserStateChanged<TestState>>().Any()),
            TimeSpan.FromSeconds(10));

        var batchWithState = Assert.Single(
            RecordingJournal.BatchesFor(persistenceId),
            batch => batch.OfType<WorkflowEvent.UserStateChanged<TestState>>().Any());

        // The state the command persisted and the step it moved to are one write, so no recovery can
        // land between them.
        Assert.Contains(batchWithState, e => e is WorkflowEvent.StepStarted { StepName: "StartChildren" });
    }
}
