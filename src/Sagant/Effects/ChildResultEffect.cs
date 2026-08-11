using Sagant.Protocol;

namespace Sagant.Effects;

/// <summary>
/// What a parent knows when one of its children settles, handed to its
/// <c>[WorkflowChildResult]</c> handler.
/// </summary>
/// <param name="State">The parent's state as it stands. Safe to write from here: a parent awaiting
/// children runs no step of its own, so nothing else can be changing state at the same moment.</param>
/// <param name="Relationship">The child this report is about — its workflow id, type, and the group
/// it belongs to.</param>
/// <param name="Status">How that child settled.</param>
/// <param name="Result">The child's final state, when it completed. <c>null</c> for a child that
/// ended any other way.</param>
/// <param name="Failure">Why the child failed, when it did. <c>null</c> otherwise.</param>
/// <param name="Settled">How many members of this group have settled, counting this one.</param>
/// <param name="Total">How many members the group has.</param>
public readonly record struct ChildResultContext<TState>(
    TState State,
    ChildWorkflowRelationship Relationship,
    ChildStatus Status,
    object? Result,
    WorkflowFailure? Failure,
    int Settled,
    int Total);

/// <summary>
/// What a parent does about a child that settled: update its own state, and optionally stop waiting
/// for the rest of the group.
///
/// Deliberately unable to transition. A report arrives while the group is still resolving, so a
/// handler that could move the workflow would be racing <c>ChildGroupPolicy</c> for control of the
/// same instant. Whether the group is over stays the policy's decision, with
/// <see cref="StopWaiting"/> as the one way a handler overrides it — and even that only says the
/// group is done, leaving what happens next to the group's resume step.
/// </summary>
/// <param name="Persistence">The state half, written in the same atomic batch as the report itself.</param>
/// <param name="StopWaiting">Non-<c>null</c> to finalize the group now with that outcome, whatever
/// its <c>CompletionPolicy</c> would otherwise have waited for. Members that have yet to settle are
/// handled by the group's <c>RemainingChildrenPolicy</c>, exactly as they are when a policy
/// finalizes a group on its own.</param>
public sealed record ChildResultEffect<TState>(
    PersistenceEffect<TState> Persistence,
    GroupOutcome? StopWaiting = null);

/// <summary>
/// Builds a <see cref="ChildResultEffect{TState}"/>. Entry point for a <c>[WorkflowChildResult]</c>
/// handler, mirroring <c>Effects</c>/<c>StepEffects</c>/<c>QueryEffects</c> for the other handler
/// kinds.
/// </summary>
public static class ChildResultEffects
{
    /// <summary>
    /// Record the report and change nothing — what a handler that only cares about some children
    /// returns for the rest.
    ///
    /// Takes the context it was handed purely so <typeparamref name="TState"/> is inferred: every
    /// other entry point here reads it from a value being passed in, and this one has none of its
    /// own.
    /// </summary>
    public static ChildResultEffect<TState> None<TState>(ChildResultContext<TState> context) =>
        new(PersistenceEffect<TState>.NoPersistence.Instance);

    /// <summary>Update the parent's state to reflect this child.</summary>
    public static ChildResultEffectBuilder<TState> UpdateState<TState>(TState newState) =>
        new(new PersistenceEffect<TState>.UpdateState(newState));

    /// <summary>Stop waiting for the rest of the group, leaving the parent's state alone.</summary>
    public static ChildResultEffect<TState> StopWaiting<TState>(GroupOutcome outcome) =>
        new(PersistenceEffect<TState>.NoPersistence.Instance, outcome);
}

/// <summary>Fluent continuation from <see cref="ChildResultEffects.UpdateState"/>, so a handler can
/// record what this child did and stop the group in one effect.</summary>
public readonly struct ChildResultEffectBuilder<TState>
{
    private readonly PersistenceEffect<TState> _persistence;

    internal ChildResultEffectBuilder(PersistenceEffect<TState> persistence) => _persistence = persistence;

    /// <summary>Finalize the group now with <paramref name="outcome"/>.</summary>
    public ChildResultEffect<TState> ThenStopWaiting(GroupOutcome outcome) => new(_persistence, outcome);

    /// <summary>Keep waiting for the group's own policy to decide.</summary>
    public ChildResultEffect<TState> ThenKeepWaiting() => new(_persistence);

    public static implicit operator ChildResultEffect<TState>(ChildResultEffectBuilder<TState> builder) =>
        new(builder._persistence);
}
