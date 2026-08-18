using System.Collections.Immutable;
using Sagant.Effects;

namespace Sagant.Protocol;

/// <summary>
/// The child-group rules, as pure functions over persisted state. They live in core because
/// every driver has to make exactly these decisions, and they are the definition of what a group
/// means, independent of how any one driver happens to store it.
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
    /// The tally after folding <paramref name="reportedStatus"/> in for the one member it names —
    /// read directly off <paramref name="group"/>'s own running counters, ahead of that report
    /// actually being folded in. O(1), straight from <see cref="ChildGroupState"/>'s own count, with
    /// no scan of the child map at all.
    /// </summary>
    public static ChildGroupTally TallyGroup(ChildGroupState group, ChildStatus reportedStatus)
    {
        var settled = group.Settled + 1;
        var failed = group.Failed;
        var completed = group.Completed;

        switch (reportedStatus)
        {
            case ChildStatus.Completed:
                completed++;
                break;
            case ChildStatus.Failed or ChildStatus.Cancelled or ChildStatus.Terminated:
                failed++;
                break;
        }

        return new ChildGroupTally(group.Total, settled, failed, completed);
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

        foreach (var member in members)
        {
            switch (member.Status)
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

        // The updates list is built by the first child that needs marking, so a parent finishing with
        // nothing left running — every group already resolved, which is the ordinary ending — reads
        // its children once and allocates the empty result alone.
        List<KeyValuePair<string, ChildWorkflowRelationship>>? updates = null;
        var toTerminate = new List<ChildWorkflowRelationship>();
        foreach (var (relationshipId, child) in children)
        {
            if (child.ParentClosePolicy != ParentClosePolicy.Terminate
                || child.Status is not (ChildStatus.Pending or ChildStatus.TerminationRequested))
            {
                continue;
            }

            var terminationRequested = child with { Status = ChildStatus.TerminationRequested };
            (updates ??= new List<KeyValuePair<string, ChildWorkflowRelationship>>())
                .Add(new KeyValuePair<string, ChildWorkflowRelationship>(relationshipId, terminationRequested));
            toTerminate.Add(terminationRequested);
        }

        return updates is null
            ? (envelope, toTerminate)
            : (envelope with { Children = children.SetItems(updates) }, toTerminate);
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
    public static IImmutableDictionary<string, ChildWorkflowRelationship> PruneFinalizedGroupMembers(
        IImmutableDictionary<string, ChildWorkflowRelationship> children, string finalizedGroupId)
    {
        var toRemove = children.Values
            .Where(c => c.GroupId == finalizedGroupId
                && c.Status is not (ChildStatus.Pending or ChildStatus.TerminationRequested))
            .Select(c => c.RelationshipId);

        return children.RemoveRange(toRemove);
    }

    /// <summary>
    /// Releases what a finalized group's terminal members carry: each one's <c>Result</c>, which is a
    /// whole child workflow's final state. The relationship stays, with who the child was, how it
    /// ended and any failure it reported, so diagnostics still show every child a parent ever started.
    ///
    /// The result has been read by the time this runs — the resume step is handed the group's members
    /// as it starts — and the child itself keeps its own state under its own id, so what this releases
    /// is a copy that has already served its purpose. The record of the report itself stays in the
    /// journal, so a reader following the event stream still sees what each child returned.
    ///
    /// This changes the slope of a parent's growth, and leaves the limit where it was: the
    /// relationship stays, so a parent's map still grows with the number of children it has ever
    /// started; what stops growing is the child state each entry carries, which is the larger half of
    /// an entry for any child whose state is more than a field or two.
    /// <see cref="Sagant.Settings.WorkflowSettings.PruneFinalizedChildren"/> is the setting that
    /// bounds the map itself, by dropping the entries outright.
    ///
    /// A member still <c>Pending</c>/<c>TerminationRequested</c> keeps everything, having yet to
    /// report anything.
    /// </summary>
    public static IImmutableDictionary<string, ChildWorkflowRelationship> ReleaseFinalizedGroupResults(
        IImmutableDictionary<string, ChildWorkflowRelationship> children, string finalizedGroupId)
    {
        List<KeyValuePair<string, ChildWorkflowRelationship>>? released = null;
        foreach (var (relationshipId, child) in children)
        {
            if (child.GroupId != finalizedGroupId || child.Result is null
                || child.Status is ChildStatus.Pending or ChildStatus.TerminationRequested)
            {
                continue;
            }

            (released ??= new List<KeyValuePair<string, ChildWorkflowRelationship>>())
                .Add(new KeyValuePair<string, ChildWorkflowRelationship>(relationshipId, child with { Result = null }));
        }

        return released is null ? children : children.SetItems(released);
    }
}
