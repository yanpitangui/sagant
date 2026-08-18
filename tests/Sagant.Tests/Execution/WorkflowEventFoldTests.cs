using System.Collections.Immutable;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Tests.Execution;

/// <summary>
/// The fold is what stands between a live instance and its recovered self: they agree only because
/// both go through this one function. A bug here corrupts recovered state silently, so the central
/// test is the invariant that covers every case at once: folding a sequence live must equal folding
/// the same sequence on replay.
/// </summary>
public class WorkflowEventFoldTests
{
    /// <summary>Stands in wherever a test cares about an event's own fields. Every caused event
    /// names what drove it, so construction requires one.</summary>
    private static readonly TransitionCause TestCause = new TransitionCause.Control("Test");

    private sealed record OrderState(string Value);

    private static readonly DateTimeOffset Now = new(2026, 8, 12, 12, 0, 0, TimeSpan.Zero);

    private static WorkflowRuntimeState<OrderState> Fresh() =>
        new(new OrderState("initial"), CurrentStepName: null, CurrentStepInput: null,
            RetryCount: 0, Status: WorkflowStatus.Running);

    /// <summary>
    /// The invariant. A driver folds as it writes; recovery folds from scratch. Both must land on the
    /// same state, or a crash quietly changes what a workflow believes about itself.
    /// </summary>
    [Theory]
    [MemberData(nameof(EventSequences))]
    public void FoldingLive_EqualsFoldingAReplayFromScratch(WorkflowEvent[] events)
    {
        var live = Fresh();
        foreach (var e in events)
        {
            live = WorkflowEventFold.Apply(live, e);
        }

        var replayed = WorkflowEventFold.ApplyAll(Fresh(), events);

        AssertSameState(live, replayed);
    }

    /// <summary>Replaying a prefix then the remainder must match replaying the whole thing — which is
    /// what a snapshot taken partway through relies on.</summary>
    [Theory]
    [MemberData(nameof(EventSequences))]
    public void FoldingIsAssociativeAcrossASnapshotBoundary(WorkflowEvent[] events)
    {
        var whole = WorkflowEventFold.ApplyAll(Fresh(), events);

        for (var split = 0; split <= events.Length; split++)
        {
            var snapshot = WorkflowEventFold.ApplyAll(Fresh(), events.Take(split));
            var resumed = WorkflowEventFold.ApplyAll(snapshot, events.Skip(split));
            AssertSameState(whole, resumed);
        }
    }

    public static TheoryData<WorkflowEvent[]> EventSequences()
    {
        var childA = Relationship("item-a");
        var childB = Relationship("item-b");
        var group = new ChildGroupState(
            "group-1", Generation: 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Terminate, "OnDone", Finalized: false);

        return new TheoryData<WorkflowEvent[]>
        {
            // a plain run
            new WorkflowEvent[]
            {
                new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(30)),
                new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), "trace-1", TestCause),
                new WorkflowEvent.UserStateChanged<OrderState>(new OrderState("charged")),
                new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, "trace-2", TestCause),
            },
            // retries, then failure
            new WorkflowEvent[]
            {
                new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
                new WorkflowEvent.StepRetryScheduled(1, Now.AddSeconds(35), Now.AddSeconds(30), TestCause),
                new WorkflowEvent.StepRetryScheduled(2, Now.AddSeconds(65), Now.AddSeconds(60), TestCause),
                new WorkflowEvent.RunFinished(
                    new WorkflowOutcome.Failed(new WorkflowFailure("declined", StepName: "Charge", Attempts: 3)), null, TestCause),
            },
            // pause, hold, resume
            new WorkflowEvent[]
            {
                new WorkflowEvent.StepStarted("Await", null, null, null, TestCause),
                new WorkflowEvent.RunPaused("awaiting approval", Now.AddHours(24), "AutoCancel", "trace-3", TestCause),
                new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
                new WorkflowEvent.RunSuspended(TestCause),
                new WorkflowEvent.RunResumed(Now.AddSeconds(5), TestCause),
            },
            // a child group start to finish
            new WorkflowEvent[]
            {
                new WorkflowEvent.ChildrenAwaited("group-1", new[] { childA, childB }, group, 1, "trace-4", TestCause),
                new WorkflowEvent.ChildMemberUpdated(childA.RelationshipId, ChildStatus.Completed, new OrderState("a"), null, null),
                new WorkflowEvent.ChildMemberUpdated(childB.RelationshipId, ChildStatus.Failed, null, new WorkflowFailure("nope"), null),
                new WorkflowEvent.ChildGroupFinalized("group-1", Array.Empty<string>(), PruneTerminalMembers: false),
                new WorkflowEvent.StepStarted("OnDone", null, null, null, TestCause),
            },
            // finalization that prunes and requests stragglers stop
            new WorkflowEvent[]
            {
                new WorkflowEvent.ChildrenAwaited("group-1", new[] { childA, childB }, group, 1, null, TestCause),
                new WorkflowEvent.ChildMemberUpdated(childA.RelationshipId, ChildStatus.Completed, null, null, null),
                new WorkflowEvent.ChildGroupFinalized("group-1", new[] { childB.RelationshipId }, PruneTerminalMembers: true),
            },
            // delivery bookkeeping alongside a run
            new WorkflowEvent[]
            {
                new WorkflowEvent.SeqNrRecorded("producer-1", 1),
                new WorkflowEvent.IdempotencyRecorded("key-1", new Reply.ReplyValue("accepted", null)),
                new WorkflowEvent.StepStarted("Charge", 42, null, null, TestCause),
                new WorkflowEvent.SeqNrRecorded("producer-1", 2),
            },
            // started as somebody's child, then deleted
            new WorkflowEvent[]
            {
                new WorkflowEvent.ParentRelationshipSet(Relationship("self")),
                new WorkflowEvent.StepStarted("Work", null, null, null, TestCause),
                new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
                new WorkflowEvent.RunDeleted(null, TestCause),
            },
        };
    }

    // ── individual guarantees the fold is responsible for ────────────────────────────────────────

    /// <summary>Guarantee D3 is a property of the event stream: the deadline is written once, so the
    /// fold has no stickiness rule to get wrong.</summary>
    [Fact]
    public void D3_WorkflowDeadline_ComesOnlyFromItsOwnEvent()
    {
        var envelope = WorkflowEventFold.Apply(Fresh(), new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(30)));

        envelope = WorkflowEventFold.Apply(envelope, new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause));

        Assert.Equal(Now.AddMinutes(30), envelope.WorkflowDeadline);
    }

    /// <summary>Guarantee E8: purging says nothing about how the run ended.</summary>
    [Fact]
    public void E8_Deletion_LeavesAnAlreadyRecordedOutcomeInPlace()
    {
        var envelope = WorkflowEventFold.ApplyAll(Fresh(), new WorkflowEvent[]
        {
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
            new WorkflowEvent.RunDeleted(null, TestCause),
        });

        Assert.Equal(WorkflowStatus.Deleted, envelope.Status);
        Assert.IsType<WorkflowOutcome.Completed>(envelope.Outcome);
    }

    /// <summary>A suspend keeps the step name and input, which is the whole reason resume can
    /// re-execute it.</summary>
    [Fact]
    public void Suspend_PreservesWhatResumeNeeds()
    {
        var envelope = WorkflowEventFold.ApplyAll(Fresh(), new WorkflowEvent[]
        {
            new WorkflowEvent.StepStarted("Charge", 42, Now.AddSeconds(5), null, TestCause),
            new WorkflowEvent.RunSuspended(TestCause),
        });

        Assert.Equal("Charge", envelope.CurrentStepName);
        Assert.Equal(42, envelope.CurrentStepInput);
    }

    /// <summary>The point of the whole scheme: one member's report touches one member.</summary>
    [Fact]
    public void ChildMemberUpdated_TouchesOnlyThatMember()
    {
        var a = Relationship("item-a");
        var b = Relationship("item-b");
        var group = new ChildGroupState("group-1", 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Terminate, "OnDone", false);

        var envelope = WorkflowEventFold.ApplyAll(Fresh(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", new[] { a, b }, group, 1, null, TestCause),
            new WorkflowEvent.ChildMemberUpdated(a.RelationshipId, ChildStatus.Completed, null, null, null),
        });

        Assert.Equal(ChildStatus.Completed, envelope.Children![a.RelationshipId].Status);
        Assert.Equal(ChildStatus.Pending, envelope.Children![b.RelationshipId].Status);
    }

    /// <summary>A report bumps its own group's running tally and leaves every other group's alone —
    /// the fold looks the touched member's <c>GroupId</c> up before deciding which group to update.</summary>
    [Fact]
    public void ChildMemberUpdated_BumpsOnlyItsOwnGroupsTally()
    {
        var a = Relationship("item-a");
        var b = Relationship("item-b") with { RelationshipId = "parent:group-2:item-b", GroupId = "group-2" };
        var groupOne = new ChildGroupState("group-1", 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Terminate, "OnDone", false, Total: 1);
        var groupTwo = new ChildGroupState("group-2", 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Terminate, "OnDone", false, Total: 1);

        var envelope = WorkflowEventFold.ApplyAll(Fresh(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", new[] { a }, groupOne, 1, null, TestCause),
            new WorkflowEvent.ChildrenAwaited("group-2", new[] { b }, groupTwo, 2, null, TestCause),
            new WorkflowEvent.ChildMemberUpdated(a.RelationshipId, ChildStatus.Completed, null, null, null),
        });

        var updatedGroupOne = envelope.ChildGroups!["group-1"];
        Assert.Equal(1, updatedGroupOne.Settled);
        Assert.Equal(1, updatedGroupOne.Completed);

        var untouchedGroupTwo = envelope.ChildGroups!["group-2"];
        Assert.Equal(0, untouchedGroupTwo.Settled);
        Assert.Equal(0, untouchedGroupTwo.Completed);
    }

    /// <summary>
    /// Compares two envelopes by value throughout. <see cref="WorkflowRuntimeState{TState}"/> is a
    /// record, but its collection members compare by reference under the compiler-generated equality,
    /// so two structurally identical states built by different routes would otherwise look different.
    /// </summary>
    private static void AssertSameState(
        WorkflowRuntimeState<OrderState> expected, WorkflowRuntimeState<OrderState> actual)
    {
        Assert.Equal(expected with { Children = null, ChildGroups = null },
                     actual with { Children = null, ChildGroups = null });
        Assert.Equal(
            (expected.Children ?? ImmutableDictionary<string, ChildWorkflowRelationship>.Empty).Values.OrderBy(c => c.RelationshipId),
            (actual.Children ?? ImmutableDictionary<string, ChildWorkflowRelationship>.Empty).Values.OrderBy(c => c.RelationshipId));
        Assert.Equal(
            (expected.ChildGroups ?? new Dictionary<string, ChildGroupState>()).OrderBy(kv => kv.Key),
            (actual.ChildGroups ?? new Dictionary<string, ChildGroupState>()).OrderBy(kv => kv.Key));
    }

    private static ChildWorkflowRelationship Relationship(string childId) =>
        new(
            RelationshipId: $"parent:group-1:{childId}",
            ParentWorkflowType: "OrderWorkflow",
            ParentWorkflowId: "order-1",
            ChildWorkflowType: "ItemWorkflow",
            ChildWorkflowId: childId,
            GroupId: "group-1",
            Generation: 0,
            Status: ChildStatus.Pending,
            Result: null,
            Failure: null,
            TraceParent: null,
            ParentClosePolicy: ParentClosePolicy.Terminate,
            Command: new object());
}
