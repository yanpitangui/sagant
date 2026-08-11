using Sagant.Effects;

namespace Sagant.Protocol;

/// <summary>
/// The child-group rules, as pure functions over persisted state. They live in core because
/// every driver has to make exactly these decisions and they are the definition of what a group
/// means — not an implementation detail of how one driver happens to store it.
///
/// Nothing here schedules, sends or persists. A caller decides a group's outcome with
/// <see cref="EvaluateGroupOutcome"/>, computes the terminal-parent envelope with
/// <see cref="ApplyParentClosePolicyToChildren"/>, persists it, and only then performs the sends that
/// envelope made necessary — so a crash between the two recovers from durable relationship state,
/// which holds even where the send never left the process (see <c>docs/guarantees.md</c> D6/D7).
/// </summary>
public static class ChildGroupPolicy
{
    /// <summary>
    /// The group's outcome, or <c>null</c> while it is still unresolved. Holds guarantee H1's half of
    /// the contract: this answers "has the group resolved", and the caller's generation/finalization
    /// guard answers "has it already resumed".
    /// </summary>
    public static GroupOutcome? EvaluateGroupOutcome(ChildGroupState group, IReadOnlyList<ChildWorkflowRelationship> members)
    {
        var anyFailed = members.Any(m => m.Status is ChildStatus.Failed or ChildStatus.Cancelled or ChildStatus.Terminated);
        if (anyFailed && group.FailurePolicy == FailurePolicy.FailFast)
        {
            return GroupOutcome.Failed;
        }

        var allTerminal = members.All(m => m.Status is ChildStatus.Completed or ChildStatus.Failed or ChildStatus.Cancelled or ChildStatus.Terminated);
        if (!allTerminal)
        {
            return null;
        }

        if (anyFailed)
        {
            return GroupOutcome.Failed;
        }

        return group.CompletionPolicy switch
        {
            CompletionPolicy.AllSuccessful => members.All(m => m.Status == ChildStatus.Completed)
                ? GroupOutcome.Succeeded
                : GroupOutcome.Failed,
            CompletionPolicy.AllCompleted => GroupOutcome.Succeeded,
            _ => GroupOutcome.Succeeded,
        };
    }

    /// <summary>
    /// Produces the terminal parent envelope and the child terminations it makes necessary. Pure, so
    /// a caller persists the changed child statuses atomically with its terminal parent transition,
    /// then performs the actual sends only after that write succeeds (guarantee D6).
    /// </summary>
    public static (WorkflowRuntimeState<TState> Envelope, IReadOnlyList<ChildWorkflowRelationship> ChildrenToTerminate)
        ApplyParentClosePolicyToChildren<TState>(WorkflowRuntimeState<TState> envelope)
    {
        if (envelope.Children is not { Count: > 0 } children)
        {
            return (envelope, Array.Empty<ChildWorkflowRelationship>());
        }

        var updated = children.ToList();
        var toTerminate = new List<ChildWorkflowRelationship>();
        for (var i = 0; i < updated.Count; i++)
        {
            var child = updated[i];
            if (child.ParentClosePolicy == ParentClosePolicy.Terminate
                && child.Status is ChildStatus.Pending or ChildStatus.TerminationRequested)
            {
                var terminationRequested = child with { Status = ChildStatus.TerminationRequested };
                updated[i] = terminationRequested;
                toTerminate.Add(terminationRequested);
            }
        }

        return toTerminate.Count == 0
            ? (envelope, toTerminate)
            : (envelope with { Children = updated }, toTerminate);
    }

    /// <summary>
    /// Opt-in via <see cref="Sagant.Settings.WorkflowSettings.PruneFinalizedChildren"/>: once a group
    /// finalizes, drops that group's terminal-status members (<c>Completed</c>/<c>Failed</c>/
    /// <c>Cancelled</c>/<c>Terminated</c>) from <paramref name="children"/> — safe for
    /// <see cref="ApplyParentClosePolicyToChildren{TState}"/>, which only ever acts on a
    /// still-<c>Pending</c>/<c>TerminationRequested</c> member, but does mean diagnostics lose the
    /// historical record of a pruned child. A still-<c>Pending</c>/<c>TerminationRequested</c>
    /// straggler in the same group (see <c>RemainingChildrenPolicy.Terminate</c>) is left in place —
    /// it hasn't reached a terminal status yet, even though its group has finalized around it.
    /// </summary>
    public static IReadOnlyList<ChildWorkflowRelationship> PruneFinalizedGroupMembers(
        IReadOnlyList<ChildWorkflowRelationship> children, string finalizedGroupId) =>
        children
            .Where(c => c.GroupId != finalizedGroupId
                || c.Status is ChildStatus.Pending or ChildStatus.TerminationRequested)
            .ToList();
}
