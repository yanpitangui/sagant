using System.Collections.Immutable;
using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Tests.Execution;

/// <summary>
/// Counting a group's members is how a parent answers for that group on every report a fan-out makes,
/// so what the count says has to match what reading the members themselves says.
/// </summary>
public class ChildGroupTallyTests
{
    private static ChildWorkflowRelationship Member(string id, string groupId, ChildStatus status) =>
        new(
            RelationshipId: id,
            ParentWorkflowType: "Parent",
            ParentWorkflowId: "parent-1",
            ChildWorkflowType: "Child",
            ChildWorkflowId: id,
            GroupId: groupId,
            Generation: 0,
            Status: status,
            Result: null,
            Failure: null,
            TraceParent: null,
            ParentClosePolicy: ParentClosePolicy.Terminate,
            Command: new object());

    private static IImmutableDictionary<string, ChildWorkflowRelationship> Dict(params ChildWorkflowRelationship[] children) =>
        children.ToImmutableDictionary(c => c.RelationshipId);

    private static ChildGroupState Group(
        CompletionPolicy completion = CompletionPolicy.AllSuccessful,
        FailurePolicy failure = FailurePolicy.WaitForAll) =>
        new("items", Generation: 0, completion, failure, RemainingChildrenPolicy.Continue, "OnDone",
            Finalized: false, null, null);

    /// <summary>A parent can await several groups at once, and each answers for its own members.</summary>
    [Fact]
    public void ItCountsTheNamedGroupAlone()
    {
        var children = new[]
        {
            Member("a", "items", ChildStatus.Completed),
            Member("b", "items", ChildStatus.Pending),
            Member("c", "shipments", ChildStatus.Failed),
        };

        var tally = ChildGroupPolicy.TallyGroup(Dict(children), "items", "zzz", ChildStatus.Completed);

        Assert.Equal(2, tally.Total);
        Assert.Equal(1, tally.Settled);
        Assert.Equal(1, tally.Completed);
        Assert.Equal(0, tally.Failed);
    }

    /// <summary>The report being applied has yet to be folded in, so the count reads its status for the
    /// member it names and the persisted status for every other.</summary>
    [Fact]
    public void ItReadsTheReportedStatusForTheMemberBeingReported()
    {
        var children = new[]
        {
            Member("a", "items", ChildStatus.Completed),
            Member("b", "items", ChildStatus.Pending),
        };

        var tally = ChildGroupPolicy.TallyGroup(Dict(children), "items", "b", ChildStatus.Failed);

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
        var tally = ChildGroupPolicy.TallyGroup(Dict(Member("a", "items", status)), "items", "zzz", ChildStatus.Completed);

        Assert.Equal(1, tally.Failed);
        Assert.Equal(1, tally.Settled);
    }

    /// <summary>A member still running, or one asked to stop and yet to say it has, keeps the group
    /// open.</summary>
    [Theory]
    [InlineData(ChildStatus.Pending)]
    [InlineData(ChildStatus.TerminationRequested)]
    public void AMemberStillGoingLeavesTheGroupUnsettled(ChildStatus status)
    {
        var children = new[] { Member("a", "items", ChildStatus.Completed), Member("b", "items", status) };

        var tally = ChildGroupPolicy.TallyGroup(Dict(children), "items", "zzz", ChildStatus.Completed);

        Assert.Equal(2, tally.Total);
        Assert.Equal(1, tally.Settled);
        Assert.Null(ChildGroupPolicy.EvaluateGroupOutcome(Group(), tally));
    }

    /// <summary>
    /// The two ways of asking — counting the whole child list for one group, and reading a list that is
    /// already the group's members — are one rule, so they answer alike for the same members.
    /// </summary>
    [Theory]
    [InlineData(CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll)]
    [InlineData(CompletionPolicy.AllSuccessful, FailurePolicy.FailFast)]
    [InlineData(CompletionPolicy.AllCompleted, FailurePolicy.WaitForAll)]
    [InlineData(CompletionPolicy.AllCompleted, FailurePolicy.FailFast)]
    public void BothWaysOfAskingAgree(CompletionPolicy completion, FailurePolicy failure)
    {
        ChildStatus[] statuses =
        [
            ChildStatus.Completed, ChildStatus.Failed, ChildStatus.Pending,
            ChildStatus.Terminated, ChildStatus.Cancelled, ChildStatus.TerminationRequested,
        ];

        var group = Group(completion, failure);

        foreach (var first in statuses)
        {
            foreach (var second in statuses)
            {
                var members = new[] { Member("a", "items", first), Member("b", "items", second) };
                var fromList = ChildGroupPolicy.EvaluateGroupOutcome(group, members);
                var fromTally = ChildGroupPolicy.EvaluateGroupOutcome(
                    group, ChildGroupPolicy.TallyGroup(Dict(members), "items", "zzz", ChildStatus.Completed));

                Assert.Equal(fromList, fromTally);
            }
        }
    }

    /// <summary>A group told to stop at the first failure resolves while members are still running.</summary>
    [Fact]
    public void FailFastResolvesBeforeEveryMemberSettles()
    {
        var children = new[] { Member("a", "items", ChildStatus.Failed), Member("b", "items", ChildStatus.Pending) };
        var tally = ChildGroupPolicy.TallyGroup(Dict(children), "items", "zzz", ChildStatus.Completed);

        Assert.Equal(
            GroupOutcome.Failed,
            ChildGroupPolicy.EvaluateGroupOutcome(Group(failure: FailurePolicy.FailFast), tally));
    }

    /// <summary>A member that failed decides the group whatever the completion policy asks for: under
    /// AllCompleted it still counts as settled, and the failure is what the group reports.</summary>
    [Fact]
    public void AFailedMemberFailsTheGroupUnderAllCompleted()
    {
        var children = new[] { Member("a", "items", ChildStatus.Completed), Member("b", "items", ChildStatus.Failed) };
        var tally = ChildGroupPolicy.TallyGroup(Dict(children), "items", "zzz", ChildStatus.Completed);

        Assert.Equal(
            GroupOutcome.Failed,
            ChildGroupPolicy.EvaluateGroupOutcome(Group(CompletionPolicy.AllCompleted), tally));
    }
}
