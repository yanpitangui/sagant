namespace Sagant.Protocol;

using Sagant.Effects;

/// <summary>
/// Policy and finalization state for one <c>AwaitChildren</c> group — never duplicates member
/// status, which lives entirely on <see cref="WorkflowRuntimeState{TState}.Children"/>, filtered by
/// <see cref="GroupId"/>. See <see cref="ChildWorkflowRelationship"/>'s doc comment for why member
/// data has exactly one home.
/// </summary>
public sealed record ChildGroupState(
    string GroupId,
    int Generation,
    CompletionPolicy CompletionPolicy,
    FailurePolicy FailurePolicy,
    RemainingChildrenPolicy RemainingChildrenPolicy,
    string ResumeStepName,
    bool Finalized);
