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
    /// How a group's members stand: how many there are, how many have reached a terminal status, and
    /// how those terminal ones ended. This is everything the rules below read, which is what lets a
    /// caller answer for a group by counting its members once.
    /// </summary>
    /// <param name="Total">Members of the group.</param>
    /// <param name="Settled">Members that reached a terminal status.</param>
    /// <param name="Failed">Settled members that failed, were cancelled, or were terminated.</param>
    /// <param name="Completed">Settled members that completed.</param>
    public readonly record struct ChildGroupTally(int Total, int Settled, int Failed, int Completed);

    /// <summary>
    /// Counts one group's members in a single pass, reading <paramref name="reportedStatus"/> for
    /// <paramref name="reportedRelationshipId"/> — the status the report being applied gives it, ahead
    /// of that report being folded in.
    ///
    /// Takes the whole child list, so a caller holding the list it already has answers for the group
    /// without building a copy of it. A fan-out reports once per child, and each report asks this
    /// question, so the copy is the one worth going without.
    /// </summary>
    public static ChildGroupTally TallyGroup(
        IReadOnlyList<ChildWorkflowRelationship> children,
        string groupId,
        string reportedRelationshipId,
        ChildStatus reportedStatus)
    {
        var total = 0;
        var settled = 0;
        var failed = 0;
        var completed = 0;

        for (var i = 0; i < children.Count; i++)
        {
            var child = children[i];
            if (child.GroupId != groupId)
            {
                continue;
            }

            total++;
            var status = child.RelationshipId == reportedRelationshipId ? reportedStatus : child.Status;
            switch (status)
            {
                case ChildStatus.Completed:
                    completed++;
                    settled++;
                    break;
                case ChildStatus.Failed or ChildStatus.Cancelled or ChildStatus.Terminated:
                    failed++;
                    settled++;
                    break;
            }
        }

        return new ChildGroupTally(total, settled, failed, completed);
    }

    /// <summary>
    /// The group's outcome, or <c>null</c> while it is still unresolved. Holds guarantee H1's half of
    /// the contract: this answers "has the group resolved", and the caller's generation/finalization
    /// guard answers "has it already resumed".
    /// </summary>
    public static GroupOutcome? EvaluateGroupOutcome(ChildGroupState group, ChildGroupTally tally)
    {
        if (tally.Failed > 0 && group.FailurePolicy == FailurePolicy.FailFast)
        {
            return GroupOutcome.Failed;
        }

        if (tally.Settled != tally.Total)
        {
            return null;
        }

        if (tally.Failed > 0)
        {
            return GroupOutcome.Failed;
        }

        return group.CompletionPolicy switch
        {
            CompletionPolicy.AllSuccessful => tally.Completed == tally.Total
                ? GroupOutcome.Succeeded
                : GroupOutcome.Failed,
            CompletionPolicy.AllCompleted => GroupOutcome.Succeeded,
            _ => GroupOutcome.Succeeded,
        };
    }

    /// <inheritdoc cref="EvaluateGroupOutcome(ChildGroupState, ChildGroupTally)"/>
    /// <remarks>For a caller holding the group's members as their own list. Counts them and asks the
    /// same question, so both shapes answer by one rule.</remarks>
    public static GroupOutcome? EvaluateGroupOutcome(
        ChildGroupState group, IReadOnlyList<ChildWorkflowRelationship> members)
    {
        var settled = 0;
        var failed = 0;
        var completed = 0;

        for (var i = 0; i < members.Count; i++)
        {
            switch (members[i].Status)
            {
                case ChildStatus.Completed:
                    completed++;
                    settled++;
                    break;
                case ChildStatus.Failed or ChildStatus.Cancelled or ChildStatus.Terminated:
                    failed++;
                    settled++;
                    break;
            }
        }

        return EvaluateGroupOutcome(group, new ChildGroupTally(members.Count, settled, failed, completed));
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
