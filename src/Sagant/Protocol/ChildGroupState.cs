namespace Sagant.Protocol;

using Sagant.Effects;

/// <summary>
/// Policy and finalization state for one <c>AwaitChildren</c> group — never duplicates member
/// status, which lives entirely on <see cref="WorkflowRuntimeState{TState}.Children"/>, filtered by
/// <see cref="GroupId"/>. See <see cref="ChildWorkflowRelationship"/>'s doc comment for why member
/// data has exactly one home.
/// </summary>
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
    string? TimeoutStepName = null);
