namespace Sagant.Protocol;

using Sagant.Effects;

/// <summary>
/// Policy, finalization state, and a running tally for one <c>AwaitChildren</c> group. Per-member
/// data — who a child is, how it ended, what it returned — lives entirely on
/// <see cref="WorkflowRuntimeState{TState}.Children"/>, filtered by <see cref="GroupId"/>; see
/// <see cref="ChildWorkflowRelationship"/>'s doc comment for why member data has exactly one home.
///
/// <see cref="Total"/>/<see cref="Settled"/>/<see cref="Failed"/>/<see cref="Completed"/> are a
/// running count over that same member data — maintained by the one fold function
/// (<see cref="Execution.WorkflowEventFold"/>) that is the sole writer of both this and
/// <c>Children</c>, updating a group's tally in the same step that updates the member it concerns,
/// so the two can never read as disagreeing. Answering "has this group resolved" from these four
/// numbers costs reading them alone; scanning every member for the same answer is what
/// <c>ChildGroupPolicy.TallyGroup</c> did before this existed.
/// </summary>
/// <param name="Total">Members in this group — fixed at creation, the number of children an
/// <c>AwaitChildren</c> call started under this <see cref="GroupId"/>.</param>
/// <param name="Settled">Members that have reached a terminal status.</param>
/// <param name="Failed">Settled members that failed, were cancelled, or were terminated.</param>
/// <param name="Completed">Settled members that completed.</param>
/// <param name="Deadline">How long this group waits before the parent stops waiting for it, as an
/// absolute instant computed when the group opened. <c>null</c> for a group that waits for its
/// children however long they take, which is the default — a parent that wants a bound asks for one.
/// Same durability as the instance's other deadlines: re-armed on every activation, so it survives a
/// crash at its remaining length.</param>
/// <param name="TimeoutStepName">The step run when <paramref name="Deadline"/> passes, which decides
/// what a parent does about children that never finished. <c>null</c> exactly when
/// <paramref name="Deadline"/> is.</param>
public sealed record ChildGroupState(
    string GroupId,
    int Generation,
    CompletionPolicy CompletionPolicy,
    FailurePolicy FailurePolicy,
    RemainingChildrenPolicy RemainingChildrenPolicy,
    string ResumeStepName,
    bool Finalized,
    DateTimeOffset? Deadline = null,
    string? TimeoutStepName = null,
    int Total = 0,
    int Settled = 0,
    int Failed = 0,
    int Completed = 0);
