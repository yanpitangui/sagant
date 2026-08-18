using System.Collections.Immutable;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Tests.Execution;

/// <summary>
/// What a parent keeps once a group has resolved. The results themselves reach the resume step as the
/// group finalizes, and each child keeps its own state under its own id, so what the parent carries
/// from there is the record of each child: who it was, how it ended, and what it reported as failing.
///
/// This is what bounds a parent that starts groups in a loop, whose child map lives as long as it
/// does and is written into every snapshot it takes.
/// </summary>
public class ChildGroupResultReleaseTests
{
    private static readonly TransitionCause Cause = new TransitionCause.Control("Test");

    private sealed record ItemState(string Value);

    private static ChildWorkflowRelationship Member(string childId, string groupId = "group-1") =>
        new(
            RelationshipId: $"parent:{groupId}:{childId}",
            ParentWorkflowType: "OrderWorkflow",
            ParentWorkflowId: "order-1",
            ChildWorkflowType: "ItemWorkflow",
            ChildWorkflowId: childId,
            GroupId: groupId,
            Generation: 0,
            Status: ChildStatus.Pending,
            Result: null,
            Failure: null,
            TraceParent: null,
            ParentClosePolicy: ParentClosePolicy.Terminate,
            Command: new object());

    private static ChildGroupState Group(string groupId = "group-1") =>
        new(groupId, Generation: 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Continue, "OnDone", Finalized: false, null, null);

    private static WorkflowRuntimeState<string> Empty() =>
        new("state", null, null, 0, WorkflowStatus.Running);

    [Fact]
    public void FinalizingAGroup_ReleasesItsMembersResults_AndKeepsEverythingElse()
    {
        var member = Member("item-1");
        var envelope = WorkflowEventFold.ApplyAll(Empty(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", [member], Group(), 1, null, Cause),
            new WorkflowEvent.ChildMemberUpdated(
                member.RelationshipId, ChildStatus.Completed, new ItemState("shipped"), null, "trace-1"),
            new WorkflowEvent.ChildGroupFinalized("group-1", [], PruneTerminalMembers: false),
        });

        var kept = Assert.Single(envelope.Children!.Values);
        Assert.Null(kept.Result);
        Assert.Equal(ChildStatus.Completed, kept.Status);
        Assert.Equal("item-1", kept.ChildWorkflowId);
        Assert.Equal("trace-1", kept.ResultTraceParent);
    }

    /// <summary>A failure is how the run ended, so it stays where a reader can find it.</summary>
    [Fact]
    public void AFailedMembersFailureSurvives()
    {
        var member = Member("item-1");
        var envelope = WorkflowEventFold.ApplyAll(Empty(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", [member], Group(), 1, null, Cause),
            new WorkflowEvent.ChildMemberUpdated(
                member.RelationshipId, ChildStatus.Failed, null, new WorkflowFailure("gateway down"), null),
            new WorkflowEvent.ChildGroupFinalized("group-1", [], PruneTerminalMembers: false),
        });

        var kept = Assert.Single(envelope.Children!.Values);
        Assert.Equal(ChildStatus.Failed, kept.Status);
        Assert.Equal("gateway down", kept.Failure!.Message);
    }

    /// <summary>A straggler has reported nothing, so a group finalizing around it leaves it whole.</summary>
    [Fact]
    public void AMemberStillRunningIsLeftAlone()
    {
        var done = Member("item-1");
        var running = Member("item-2");
        var envelope = WorkflowEventFold.ApplyAll(Empty(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", [done, running], Group(), 1, null, Cause),
            new WorkflowEvent.ChildMemberUpdated(
                done.RelationshipId, ChildStatus.Completed, new ItemState("shipped"), null, null),
            new WorkflowEvent.ChildGroupFinalized("group-1", [], PruneTerminalMembers: false),
        });

        Assert.Equal(2, envelope.Children!.Count);
        Assert.Equal(ChildStatus.Pending, envelope.Children.Values.Single(c => c.ChildWorkflowId == "item-2").Status);
    }

    /// <summary>A parent awaiting two groups at once keeps the one that has yet to resolve intact.</summary>
    [Fact]
    public void AnotherGroupsResultsAreUntouched()
    {
        var first = Member("item-1");
        var second = Member("ship-1", "group-2");
        var envelope = WorkflowEventFold.ApplyAll(Empty(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", [first], Group(), 1, null, Cause),
            new WorkflowEvent.ChildrenAwaited("group-2", [second], Group("group-2"), 2, null, Cause),
            new WorkflowEvent.ChildMemberUpdated(
                first.RelationshipId, ChildStatus.Completed, new ItemState("shipped"), null, null),
            new WorkflowEvent.ChildMemberUpdated(
                second.RelationshipId, ChildStatus.Completed, new ItemState("dispatched"), null, null),
            new WorkflowEvent.ChildGroupFinalized("group-1", [], PruneTerminalMembers: false),
        });

        Assert.Null(envelope.Children!.Values.Single(c => c.GroupId == "group-1").Result);
        Assert.Equal(
            new ItemState("dispatched"),
            envelope.Children.Values.Single(c => c.GroupId == "group-2").Result);
    }

    /// <summary>Pruning drops the members outright, which is the setting's own behaviour.</summary>
    [Fact]
    public void PruningStillRemovesTerminalMembers()
    {
        var member = Member("item-1");
        var envelope = WorkflowEventFold.ApplyAll(Empty(), new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("group-1", [member], Group(), 1, null, Cause),
            new WorkflowEvent.ChildMemberUpdated(
                member.RelationshipId, ChildStatus.Completed, new ItemState("shipped"), null, null),
            new WorkflowEvent.ChildGroupFinalized("group-1", [], PruneTerminalMembers: true),
        });

        Assert.Empty(envelope.Children!.Values);
    }

    /// <summary>Nothing to release leaves the map itself alone, which is what keeps an ordinary
    /// finalization from copying a map it does not change.</summary>
    [Fact]
    public void AGroupWithNoResultsToReleaseKeepsTheSameMap()
    {
        var member = Member("item-1") with { Status = ChildStatus.Terminated };
        var children = ImmutableDictionary.CreateRange(
            new[] { new KeyValuePair<string, ChildWorkflowRelationship>(member.RelationshipId, member) });

        Assert.Same(children, ChildGroupPolicy.ReleaseFinalizedGroupResults(children, "group-1"));
    }
}
