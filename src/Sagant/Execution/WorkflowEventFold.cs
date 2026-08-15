using Sagant.Protocol;

namespace Sagant.Execution;

/// <summary>
/// Rebuilds a workflow's state from the facts recorded about it. One event at a time, applied to the
/// state before it.
///
/// This is the only place an envelope is ever produced, which is what keeps a live instance and a
/// recovered one from disagreeing: the driver folds as it persists, folds again on recovery, the
/// in-memory test harness folds too, and <see cref="WorkflowTransitionPlanner"/> folds to work out
/// what its own events amount to. Four callers, one function.
///
/// Pure: no clock, no settings, no I/O. Every value an event needs was computed when it was written
/// (see <see cref="WorkflowEvent"/>), so replaying at a different moment reproduces the original
/// state exactly.
/// </summary>
public static class WorkflowEventFold
{
    /// <summary>
    /// The state <paramref name="envelope"/> becomes once <paramref name="event"/> has happened.
    /// </summary>
    public static WorkflowRuntimeState<TState> Apply<TState>(
        WorkflowRuntimeState<TState> envelope, WorkflowEvent @event) =>
        @event switch
        {
            WorkflowEvent.UserStateChanged<TState> e => envelope with { UserState = e.State },

            WorkflowEvent.WorkflowDeadlineSet e => envelope with { WorkflowDeadline = e.Deadline },

            WorkflowEvent.StepStarted e => envelope with
            {
                Status = WorkflowStatus.Running,
                CurrentStepName = e.StepName,
                CurrentStepInput = e.Input,
                RetryCount = 0,
                StepDeadline = e.StepDeadline,
                PauseDeadline = null,
                PauseTimeoutStepName = null,
                HoldDeadline = null,
                HoldTimeoutStepName = null,
                RetryDelayUntil = null,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
            },

            WorkflowEvent.StepRetryScheduled e => envelope with
            {
                RetryCount = e.RetryCount,
                StepDeadline = e.StepDeadline,
                RetryDelayUntil = e.RetryDelayUntil,
            },

            WorkflowEvent.RunPaused e => envelope with
            {
                Status = WorkflowStatus.Paused,
                CurrentStepName = null,
                CurrentStepInput = null,
                RetryCount = 0,
                StepDeadline = null,
                PauseDeadline = e.PauseDeadline,
                PauseTimeoutStepName = e.PauseTimeoutStepName,
                RetryDelayUntil = null,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
            },

            WorkflowEvent.RunFinished e => envelope with
            {
                Status = WorkflowStatus.Finished,
                Outcome = e.Outcome,
                CurrentStepName = null,
                CurrentStepInput = null,
                RetryCount = 0,
                StepDeadline = null,
                PauseDeadline = null,
                PauseTimeoutStepName = null,
                HoldDeadline = null,
                HoldTimeoutStepName = null,
                RetryDelayUntil = null,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
            },

            // Deletion leaves any outcome the run already reached in place: purging an instance's
            // data says nothing about how its run ended (guarantee E8).
            WorkflowEvent.RunDeleted e => envelope with
            {
                Status = WorkflowStatus.Deleted,
                CurrentStepName = null,
                CurrentStepInput = null,
                RetryCount = 0,
                StepDeadline = null,
                PauseDeadline = null,
                PauseTimeoutStepName = null,
                HoldDeadline = null,
                HoldTimeoutStepName = null,
                RetryDelayUntil = null,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
            },

            // A fresh cycle: everything belonging to the one that ended is cleared, including the
            // workflow deadline so the next cycle establishes its own (guarantee D3 writes it once
            // per instance, which would otherwise expire mid-loop). UserState and the deduplication
            // ledgers are left untouched — they belong to the instance, which is still the same one.
            WorkflowEvent.RunRestarted e => envelope with
            {
                Status = WorkflowStatus.Running,
                CurrentStepName = e.StepName,
                CurrentStepInput = e.Input,
                RetryCount = 0,
                StepDeadline = e.StepDeadline,
                WorkflowDeadline = null,
                PauseDeadline = null,
                PauseTimeoutStepName = null,
                HoldDeadline = null,
                HoldTimeoutStepName = null,
                RetryDelayUntil = null,
                Outcome = null,
                Children = null,
                ChildGroups = null,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
            },

            // The step name and input survive, so a later resume knows what to re-execute.
            WorkflowEvent.RunSuspended e => envelope with
            {
                Status = WorkflowStatus.Suspended,
                HoldDeadline = e.HoldDeadline,
                HoldTimeoutStepName = e.HoldTimeoutStepName,
            },

            // Reaches the same held status, carrying what stopped it. The retry count is left where
            // the exhausted budget put it, so a reader sees how many attempts ran; resuming resets it.
            WorkflowEvent.RunParked e => envelope with
            {
                Status = WorkflowStatus.Suspended,
                ParkedFailure = e.Failure,
                StepDeadline = null,
                RetryDelayUntil = null,
                HoldDeadline = e.HoldDeadline,
                HoldTimeoutStepName = e.HoldTimeoutStepName,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
            },

            WorkflowEvent.RunResumed e => envelope with
            {
                Status = WorkflowStatus.Running,
                RetryCount = 0,
                StepDeadline = e.StepDeadline,
                RetryDelayUntil = null,
                // The run is live again, whatever comes of the retry, and the hold it was under is
                // over — so whatever that hold was waiting for stops waiting too.
                ParkedFailure = null,
                HoldDeadline = null,
                HoldTimeoutStepName = null,
            },

            WorkflowEvent.ChildrenAwaited e => envelope with
            {
                Status = WorkflowStatus.Running,
                CurrentStepName = null,
                CurrentStepInput = null,
                RetryCount = 0,
                PauseDeadline = null,
                PauseTimeoutStepName = null,
                HoldDeadline = null,
                HoldTimeoutStepName = null,
                RetryDelayUntil = null,
                LastTraceParent = e.TraceParent ?? envelope.LastTraceParent,
                ChildGroupSequence = e.NextGroupSequence,
                Children = Concat(envelope.Children, e.Relationships),
                ChildGroups = With(envelope.ChildGroups, e.GroupId, e.Group),
            },

            WorkflowEvent.ChildMemberUpdated e => envelope with
            {
                Children = UpdateMember(envelope.Children, e),
            },

            WorkflowEvent.ChildGroupFinalized e => FinalizeGroup(envelope, e),

            WorkflowEvent.ParentClosePolicyApplied e => envelope with
            {
                Children = MarkTerminationRequested(envelope.Children, e.TerminationRequested),
            },

            WorkflowEvent.ParentRelationshipSet e => envelope with { ParentRelationship = e.Relationship },

            // The ledgers are sized when the envelope is first built, so recording an entry needs no
            // settings here — see WorkflowRuntimeState's own construction.
            WorkflowEvent.SeqNrRecorded e => envelope with
            {
                HighestAppliedSeqNr = envelope.HighestAppliedSeqNr?.Record(e.ProducerId, e.SeqNr),
            },

            WorkflowEvent.IdempotencyRecorded e => envelope with
            {
                IdempotencyLedger = envelope.IdempotencyLedger?.Record(e.Key, e.Reply),
            },

            // The instance stayed where it was, so its state comes entirely from the
            // UserStateChanged sharing this batch and the fold passes the envelope through.
            WorkflowEvent.RunStayed => envelope,

            _ => envelope,
        };

    /// <summary>Folds a whole sequence, oldest first.</summary>
    public static WorkflowRuntimeState<TState> ApplyAll<TState>(
        WorkflowRuntimeState<TState> envelope, IEnumerable<WorkflowEvent> events)
    {
        foreach (var @event in events)
        {
            envelope = Apply(envelope, @event);
        }

        return envelope;
    }

    private static IReadOnlyList<ChildWorkflowRelationship> Concat(
        IReadOnlyList<ChildWorkflowRelationship>? existing, IReadOnlyList<ChildWorkflowRelationship> added)
    {
        if (existing is not { Count: > 0 })
        {
            return added;
        }

        var combined = new List<ChildWorkflowRelationship>(existing.Count + added.Count);
        combined.AddRange(existing);
        combined.AddRange(added);
        return combined;
    }

    private static IReadOnlyDictionary<string, ChildGroupState> With(
        IReadOnlyDictionary<string, ChildGroupState>? groups, string groupId, ChildGroupState group)
    {
        var updated = groups is null
            ? new Dictionary<string, ChildGroupState>()
            : new Dictionary<string, ChildGroupState>(groups);
        updated[groupId] = group;
        return updated;
    }

    private static IReadOnlyList<ChildWorkflowRelationship>? UpdateMember(
        IReadOnlyList<ChildWorkflowRelationship>? children, WorkflowEvent.ChildMemberUpdated e)
    {
        if (children is null)
        {
            return null;
        }

        var updated = new List<ChildWorkflowRelationship>(children.Count);
        foreach (var child in children)
        {
            updated.Add(child.RelationshipId == e.RelationshipId
                ? child with
                {
                    Status = e.Status,
                    Result = e.Result,
                    Failure = e.Failure,
                    ResultTraceParent = e.ResultTraceParent,
                }
                : child);
        }

        return updated;
    }

    private static IReadOnlyList<ChildWorkflowRelationship>? MarkTerminationRequested(
        IReadOnlyList<ChildWorkflowRelationship>? children, IReadOnlyList<string> relationshipIds)
    {
        if (children is null || relationshipIds.Count == 0)
        {
            return children;
        }

        var requested = relationshipIds.ToHashSet();
        var updated = new List<ChildWorkflowRelationship>(children.Count);
        foreach (var child in children)
        {
            updated.Add(requested.Contains(child.RelationshipId)
                ? child with { Status = ChildStatus.TerminationRequested }
                : child);
        }

        return updated;
    }

    private static WorkflowRuntimeState<TState> FinalizeGroup<TState>(
        WorkflowRuntimeState<TState> envelope, WorkflowEvent.ChildGroupFinalized e)
    {
        if (envelope.ChildGroups?.GetValueOrDefault(e.GroupId) is not { } group)
        {
            return envelope;
        }

        var children = envelope.Children;
        if (children is not null && e.TerminationRequested.Count > 0)
        {
            var requested = e.TerminationRequested.ToHashSet();
            var updated = new List<ChildWorkflowRelationship>(children.Count);
            foreach (var child in children)
            {
                updated.Add(requested.Contains(child.RelationshipId)
                    ? child with { Status = ChildStatus.TerminationRequested }
                    : child);
            }

            children = updated;
        }

        if (children is not null)
        {
            // A group that has resolved has handed its results to the resume step, so what the parent
            // carries from here is the record of each child. Pruning drops the terminal members
            // outright; keeping them releases the child state each one carries — see
            // ChildGroupPolicy.ReleaseFinalizedGroupResults.
            children = e.PruneTerminalMembers
                ? ChildGroupPolicy.PruneFinalizedGroupMembers(children, e.GroupId)
                : ChildGroupPolicy.ReleaseFinalizedGroupResults(children, e.GroupId);
        }

        return envelope with
        {
            Children = children,
            ChildGroups = With(envelope.ChildGroups, e.GroupId, group with
            {
                Generation = group.Generation + 1,
                Finalized = true,
            }),
        };
    }
}
