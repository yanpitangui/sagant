using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Tests.Execution;

/// <summary>
/// A group's tally is read off <see cref="ChildGroupState"/>'s own running counters — O(1) — so
/// these tests build a group at whatever count a scenario needs and confirm <c>TallyGroup</c> folds
/// one more report into it correctly, matching what the same reports would produce read back from
/// the member list directly.
/// </summary>
public class ChildGroupTallyTests
{
    private static ChildWorkflowRelationship Member(string id, ChildStatus status) =>
        new(
            RelationshipId: id,
            ParentWorkflowType: "Parent",
            ParentWorkflowId: "parent-1",
            ChildWorkflowType: "Child",
            ChildWorkflowId: id,
            GroupId: "items",
            Generation: 0,
            Status: status,
            Result: null,
            Failure: null,
            TraceParent: null,
            ParentClosePolicy: ParentClosePolicy.Terminate,
            Command: new object());

    private static ChildGroupState Group(
        int total, int settled = 0, int failed = 0, int completed = 0,
        CompletionPolicy completion = CompletionPolicy.AllSuccessful,
        FailurePolicy failure = FailurePolicy.WaitForAll) =>
        new("items", Generation: 0, completion, failure, RemainingChildrenPolicy.Continue, "OnDone",
            Finalized: false, null, null, Total: total, Settled: settled, Failed: failed, Completed: completed);

    /// <summary>Folds <paramref name="group"/>'s tally forward by one more report, the way the actor's
    /// own fold does — the tool these tests reach for to build a group up across more than one
    /// report.</summary>
    private static ChildGroupState Fold(ChildGroupState group, ChildStatus reportedStatus)
    {
        var tally = ChildGroupPolicy.TallyGroup(group, reportedStatus);
        return group with { Settled = tally.Settled, Failed = tally.Failed, Completed = tally.Completed };
    }

    [Fact]
    public void ANewGroupsFirstReport_SettlesOneMember()
    {
        var tally = ChildGroupPolicy.TallyGroup(Group(total: 2), ChildStatus.Completed);

        Assert.Equal(2, tally.Total);
        Assert.Equal(1, tally.Settled);
        Assert.Equal(1, tally.Completed);
        Assert.Equal(0, tally.Failed);
    }

    /// <summary>A report folds onto whatever the group already counted.</summary>
    [Fact]
    public void ASecondReport_AddsToWhatTheGroupAlreadyCounted()
    {
        var tally = ChildGroupPolicy.TallyGroup(Group(total: 2, settled: 1, completed: 1), ChildStatus.Failed);

        Assert.Equal(2, tally.Total);
        Assert.Equal(2, tally.Settled);
        Assert.Equal(1, tally.Completed);
        Assert.Equal(1, tally.Failed);
    }

    [Theory]
    [InlineData(ChildStatus.Failed)]
    [InlineData(ChildStatus.Cancelled)]
    [InlineData(ChildStatus.Terminated)]
    public void EveryWayOfEndingBadlyCountsAsFailed(ChildStatus status)
    {
        var tally = ChildGroupPolicy.TallyGroup(Group(total: 1), status);

        Assert.Equal(1, tally.Failed);
        Assert.Equal(1, tally.Settled);
    }

    /// <summary>A member still running, or one asked to stop and yet to say it has, keeps the group
    /// open — it never generates a report, so the group's own Settled count stays behind Total.</summary>
    [Fact]
    public void AMemberStillGoingLeavesTheGroupUnsettled()
    {
        var tally = ChildGroupPolicy.TallyGroup(Group(total: 2), ChildStatus.Completed);

        Assert.Equal(2, tally.Total);
        Assert.Equal(1, tally.Settled);
        Assert.Null(ChildGroupPolicy.EvaluateGroupOutcome(Group(total: 2), tally));
    }

    /// <summary>
    /// The two ways of asking a settled two-member group's outcome — folding each member's report
    /// into the group's own counters one at a time, and reading the same two members back as a list —
    /// are one rule, so they answer alike.
    /// </summary>
    [Theory]
    [InlineData(CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll)]
    [InlineData(CompletionPolicy.AllSuccessful, FailurePolicy.FailFast)]
    [InlineData(CompletionPolicy.AllCompleted, FailurePolicy.WaitForAll)]
    [InlineData(CompletionPolicy.AllCompleted, FailurePolicy.FailFast)]
    public void BothWaysOfAskingAgree(CompletionPolicy completion, FailurePolicy failure)
    {
        ChildStatus[] statuses = [ChildStatus.Completed, ChildStatus.Failed, ChildStatus.Terminated, ChildStatus.Cancelled];

        foreach (var first in statuses)
        {
            foreach (var second in statuses)
            {
                var members = new[] { Member("a", first), Member("b", second) };
                var fromList = ChildGroupPolicy.EvaluateGroupOutcome(
                    Group(total: 2, completion: completion, failure: failure), members);

                var folded = Fold(Fold(Group(total: 2, completion: completion, failure: failure), first), second);
                var fromTally = ChildGroupPolicy.EvaluateGroupOutcome(
                    folded, new ChildGroupPolicy.ChildGroupTally(folded.Total, folded.Settled, folded.Failed, folded.Completed));

                Assert.Equal(fromList, fromTally);
            }
        }
    }

    /// <summary>A group told to stop at the first failure resolves while members are still running.</summary>
    [Fact]
    public void FailFastResolvesBeforeEveryMemberSettles()
    {
        var tally = ChildGroupPolicy.TallyGroup(Group(total: 2, failure: FailurePolicy.FailFast), ChildStatus.Failed);

        Assert.Equal(
            GroupOutcome.Failed,
            ChildGroupPolicy.EvaluateGroupOutcome(Group(total: 2, failure: FailurePolicy.FailFast), tally));
    }

    /// <summary>A member that failed decides the group whatever the completion policy asks for: under
    /// AllCompleted it still counts as settled, and the failure is what the group reports.</summary>
    [Fact]
    public void AFailedMemberFailsTheGroupUnderAllCompleted()
    {
        var group = Group(total: 2, settled: 1, completed: 1, completion: CompletionPolicy.AllCompleted);
        var tally = ChildGroupPolicy.TallyGroup(group, ChildStatus.Failed);

        Assert.Equal(GroupOutcome.Failed, ChildGroupPolicy.EvaluateGroupOutcome(group, tally));
    }
}
