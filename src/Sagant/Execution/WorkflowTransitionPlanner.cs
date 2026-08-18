using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Settings;

namespace Sagant.Execution;

/// <summary>
/// How a runtime driver identifies the instance it is driving. Two ids, because they serve different
/// purposes: one must be globally unique for use as an opaque key, the other must be the value
/// another instance can address this one by.
/// </summary>
/// <param name="PersistenceId">Globally unique across every workflow type. Prefixes group ids and
/// relationship ids, which are only ever compared to themselves, never routed on.</param>
/// <param name="RoutableId">What another instance sends to in order to reach this one. Recorded on a
/// child relationship so the child can report back.</param>
/// <param name="WorkflowTypeName">This workflow's durable type name.</param>
public readonly record struct WorkflowInstanceIdentity(string PersistenceId, string RoutableId, string WorkflowTypeName);

/// <summary>
/// What a transition amounts to: the facts to record, and everything a driver must do once they are
/// durably written. See <see cref="WorkflowDecision"/> for why the two travel together.
///
/// The state itself is deliberately absent — a driver derives it by folding
/// <paramref name="Events"/> through <see cref="WorkflowEventFold"/>, the same function recovery
/// uses. Returning both would be two representations of one change, free to drift.
/// </summary>
public sealed record TransitionPlan<TState>(
    IReadOnlyList<WorkflowEvent> Events,
    IReadOnlyList<WorkflowDecision> AfterPersist);

/// <summary>
/// Decides what a transition means: the state it produces and the consequences that follow. Pure —
/// no clock of its own, no persistence, no scheduling, no I/O — so the rules that define what a
/// workflow *is* can be exercised directly, in memory, without a runtime underneath them.
///
/// This exists because those rules were previously written twice: once inside the durable driver's
/// persist path and once inside the in-memory test harness, with a comment asking maintainers to
/// keep the two in step by hand. Every guarantee under "Execution" and "Children" in
/// <c>docs/guarantees.md</c> is decided here now, so both drivers reach the same answer by
/// construction.
/// </summary>
public static class WorkflowTransitionPlanner
{
    /// <summary>
    /// Plans <paramref name="transition"/> against <paramref name="envelope"/>.
    /// </summary>
    /// <param name="envelope">The instance's state as it stands before this transition.</param>
    /// <param name="transition">What the handler's effect asked for.</param>
    /// <param name="persistence">The effect's persistence half. State is written only when it
    /// actually changed, so a transition that leaves state alone writes none of it.</param>
    /// <param name="now">The driver's clock reading, taken once for the whole plan so every deadline
    /// in it is relative to the same instant.</param>
    /// <param name="settings">Resolved settings — see <see cref="ResolvedWorkflowSettings"/>.</param>
    /// <param name="identity">Who this instance is; see <see cref="WorkflowInstanceIdentity"/>.</param>
    /// <param name="traceParent">The trace context this transition should be recorded under, or
    /// <c>null</c> to carry forward whatever the envelope already holds.</param>
    public static TransitionPlan<TState> Plan<TState>(
        WorkflowRuntimeState<TState> envelope,
        Transition transition,
        PersistenceEffect<TState> persistence,
        DateTimeOffset now,
        ResolvedWorkflowSettings settings,
        WorkflowInstanceIdentity identity,
        TransitionCause cause,
        string? traceParent = null)
    {
        var events = new List<WorkflowEvent>();

        if (persistence is PersistenceEffect<TState>.UpdateState updated)
        {
            events.Add(new WorkflowEvent.UserStateChanged<TState>(updated.NewState));
        }

        // Guarantee D3: written at most once per instance, on the first transition that keeps the
        // workflow going. A terminal or delete transition has no future to bound.
        if (envelope.WorkflowDeadline is null
            && settings.WorkflowTimeout is { } workflowTimeout
            && transition is Transition.StepTransition or Transition.PauseTransition or Transition.AwaitChildrenTransition)
        {
            events.Add(new WorkflowEvent.WorkflowDeadlineSet(now + workflowTimeout));
        }

        var lastTraceParent = traceParent ?? envelope.LastTraceParent;

        // Resolved before any event is folded in: this is the only point where ChildGroupSequence
        // still holds the value the id is derived from (guarantee H3).
        var groupId = transition is Transition.AwaitChildrenTransition act
            ? act.GroupId ?? $"{identity.PersistenceId}:group:{envelope.ChildGroupSequence}"
            : null;

        switch (transition)
        {
            case Transition.StepTransition st:
                events.Add(new WorkflowEvent.StepStarted(
                    st.StepName, st.Input,
                    settings.StepTimeoutFor(st.StepName) is { } stepTimeout ? now + stepTimeout : null,
                    lastTraceParent,
                    cause));
                break;

            case Transition.PauseTransition pt:
                events.Add(new WorkflowEvent.RunPaused(
                    pt.Reason,
                    now,
                    pt.Settings?.Timeout is { } pauseTimeout ? now + pauseTimeout : null,
                    pt.Settings?.TimeoutHandlerStepName,
                    lastTraceParent,
                    cause));
                break;

            case Transition.TerminalTransition tt:
                events.Add(new WorkflowEvent.RunFinished(Enrich(tt.Outcome, envelope), lastTraceParent, cause));
                break;

            case Transition.DeleteTransition:
                events.Add(new WorkflowEvent.RunDeleted(lastTraceParent, cause));
                break;

            case Transition.ParkTransition park:
                events.Add(new WorkflowEvent.RunParked(
                    park.Failure, lastTraceParent, cause,
                    HoldDeadlineFor(settings, now), settings.HoldTimeoutStepName));
                break;

            case Transition.RestartTransition rt:
                events.Add(new WorkflowEvent.RunRestarted(
                    rt.StepName, rt.Input, rt.Reason,
                    settings.StepTimeoutFor(rt.StepName) is { } restartStepTimeout ? now + restartStepTimeout : null,
                    lastTraceParent,
                    cause));
                break;

            case Transition.AwaitChildrenTransition awaitChildren:
                var relationships = BuildRelationships(awaitChildren, groupId!, identity, lastTraceParent).ToList();
                events.Add(new WorkflowEvent.ChildrenAwaited(
                    groupId!,
                    relationships,
                    new ChildGroupState(
                        groupId!, Generation: 0, awaitChildren.CompletionPolicy, awaitChildren.FailurePolicy,
                        awaitChildren.RemainingChildrenPolicy, awaitChildren.ResumeStepName, Finalized: false,
                        // Both together or neither: a deadline with no step to run would leave the
                        // parent nowhere to go when it lands.
                        awaitChildren is { Timeout: { } groupTimeout, TimeoutStepName: not null }
                            ? now + groupTimeout
                            : null,
                        awaitChildren.TimeoutStepName,
                        Total: relationships.Count),
                    awaitChildren.GroupId is null ? envelope.ChildGroupSequence + 1 : envelope.ChildGroupSequence,
                    lastTraceParent,
                    cause));
                break;
        }

        // Guarantee D6: a terminal transition records its parent-close decisions in the same batch
        // that makes the instance terminal, so the sends below are recoverable if they never happen.
        // A restart closes its children the same way, since the cycle that owns them is ending and
        // the fold drops them from the fresh envelope.
        IReadOnlyList<ChildWorkflowRelationship> childrenToClose = Array.Empty<ChildWorkflowRelationship>();
        if (transition is Transition.TerminalTransition or Transition.DeleteTransition or Transition.RestartTransition)
        {
            (_, childrenToClose) = ChildGroupPolicy.ApplyParentClosePolicyToChildren(envelope);
            if (childrenToClose.Count > 0)
            {
                events.Add(new WorkflowEvent.ParentClosePolicyApplied(
                    childrenToClose.Select(c => c.RelationshipId).ToList()));
            }
        }

        // Exactly one event per batch names what drove it. A transition event carries the cause
        // directly; a batch that moves nothing — a command updating state and staying put — gets a
        // RunStayed of its own, so every change in the stream is explained.
        if (!events.Any(e => e is WorkflowEvent.CausedEvent))
        {
            events.Insert(0, new WorkflowEvent.RunStayed(cause));
        }

        // The planner works out what its own events amount to using the very fold a driver and
        // recovery use, so the decisions below are built against exactly the state those will see.
        var next = WorkflowEventFold.ApplyAll(envelope, events);

        return new TransitionPlan<TState>(events, BuildDecisions(envelope, next, transition, now, groupId, childrenToClose));
    }

    /// <summary>
    /// Decides what a failed step attempt means: retry, fail over, or end. Holds guarantees E1 and
    /// E2 — the retry budget and the rule that a retried attempt's timeout budget starts when the
    /// attempt starts, so a backoff longer than the step timeout cannot expire it before it runs.
    /// </summary>
    /// <param name="envelope">State as it stands after the failed attempt.</param>
    /// <param name="stepName">The step that failed.</param>
    /// <param name="failureMessage">Message describing the failure, used as the end reason when the
    /// budget is exhausted and no failover is configured.</param>
    /// <param name="now">The driver's clock reading.</param>
    /// <param name="settings">Resolved settings — see <see cref="ResolvedWorkflowSettings"/>.</param>
    public static StepFailurePlan<TState> PlanStepFailure<TState>(
        WorkflowRuntimeState<TState> envelope,
        string stepName,
        string failureMessage,
        DateTimeOffset now,
        ResolvedWorkflowSettings settings,
        WorkflowFailure? failure = null,
        TimeSpan duration = default)
    {
        var strategy = settings.RecoverStrategyFor(stepName);

        if (strategy is null || envelope.RetryCount >= strategy.MaxRetries)
        {
            var exhausted = failure ?? new WorkflowFailure(
                failureMessage, StepName: stepName, Attempts: envelope.RetryCount + 1);

            // Three ways a spent budget can conclude, and a step with no strategy at all ends the run
            // on its first failure — the same route as a strategy that says so explicitly.
            return new StepFailurePlan<TState>.Conclude(strategy switch
            {
                { FailoverStepName: { } failoverStep } =>
                    new Transition.StepTransition(failoverStep, strategy.FailoverStepInput),
                { ParkOnExhaustion: true } => new Transition.ParkTransition(exhausted),
                _ => new Transition.TerminalTransition(new WorkflowOutcome.Failed(exhausted)),
            });
        }

        var retryCount = envelope.RetryCount + 1;
        var attempt = retryCount + 1;

        var backoff = strategy.BackoffForAttempt?.Invoke(attempt) ?? TimeSpan.Zero;
        if (backoff < TimeSpan.Zero)
        {
            backoff = TimeSpan.Zero;
        }

        var stepTimeout = settings.StepTimeoutFor(stepName);
        // Guarantee E2: the backoff is folded into the deadline, so the attempt's budget is measured
        // from when it actually begins.
        return new StepFailurePlan<TState>.Retry(
            new WorkflowEvent[]
            {
                new WorkflowEvent.StepRetryScheduled(
                    retryCount,
                    stepTimeout is { } t ? now + backoff + t : null,
                    backoff > TimeSpan.Zero ? now + backoff : null,
                    new TransitionCause.StepFailed(
                        stepName, envelope.RetryCount + 1, failureMessage, duration, WillRetry: true)),
            },
            backoff > TimeSpan.Zero ? now + backoff : null,
            attempt);
    }

    /// <summary>
    /// What an instance standing on a step its deployed code has stopped registering does: guarantee E5.
    /// It is held at that step, keeping its state, the step name and that step's input, so deploying
    /// the step again and calling <c>IWorkflowHandle.Resume</c> continues the run from where it stood.
    ///
    /// A driver reaches this when it goes to start a step and its dispatcher has no such name — the
    /// shape of a deploy that removed a step while instances were persisted on it. Holding turns that
    /// into a stall an operator can see and undo, and every affected run keeps everything a resume
    /// needs.
    /// </summary>
    /// <param name="stepName">The step name the instance is persisted on.</param>
    public static Transition PlanUnknownStep(string stepName) =>
        new Transition.ParkTransition(new WorkflowFailure(
            $"No step named '{stepName}' is registered on this workflow. Deploy the step and resume.",
            StepName: stepName));

    /// <summary>
    /// Whether a fired workflow-level timeout should be acted on, and what it means. Holds guarantee
    /// E3: the workflow timeout bounds active processing, so a timer arriving while the instance is
    /// paused is a stale no-op. Returns <c>null</c> when there is nothing to do.
    /// </summary>
    public static Transition? PlanWorkflowTimeout<TState>(
        WorkflowRuntimeState<TState> envelope, ResolvedWorkflowSettings settings)
    {
        if (envelope.Status is not WorkflowStatus.Running)
        {
            return null;
        }

        // The same three conclusions a spent step budget has, so one vocabulary covers both: run a
        // recovery step, hold the instance for someone to look at, or end the run. Ending is what a
        // workflow with no strategy does, because a workflow timeout is a bound its author chose —
        // reaching it is a decision the workflow already made, where a step failing is the world
        // misbehaving.
        return settings.WorkflowRecoverStrategy switch
        {
            { FailoverStepName: { } failoverStep } strategy =>
                new Transition.StepTransition(failoverStep, strategy.FailoverStepInput),
            { ParkOnExhaustion: true } => new Transition.ParkTransition(new WorkflowFailure(
                "The workflow deadline elapsed.",
                ExceptionType: typeof(TimeoutException).FullName,
                StepName: envelope.CurrentStepName)),
            _ => new Transition.TerminalTransition(WorkflowOutcome.TimedOut.Instance),
        };
    }

    /// <summary>
    /// Holds an instance where it stands, so it can be resumed later. Valid only while it is
    /// <see cref="WorkflowStatus.Running"/> — there is nothing to hold otherwise.
    ///
    /// The current step name and input stay on the envelope, since <see cref="PlanResume{TState}"/>
    /// needs them to know what to re-execute.
    /// </summary>
    public static ControlPlan<TState> PlanSuspend<TState>(
        WorkflowRuntimeState<TState> envelope,
        TransitionCause cause,
        DateTimeOffset now,
        ResolvedWorkflowSettings settings)
    {
        if (envelope.Status != WorkflowStatus.Running)
        {
            return new ControlPlan<TState>.Reject($"Cannot suspend from status {envelope.Status}.");
        }

        var holdDeadline = HoldDeadlineFor(settings, now);
        var decisions = new List<WorkflowDecision>
        {
            WorkflowDecision.RecordStatusChange.For(WorkflowStatus.Suspended),
        };

        if (holdDeadline is { } deadline)
        {
            decisions.Add(new WorkflowDecision.ArmTimer(WorkflowTimerKind.Hold, deadline));
        }

        return new ControlPlan<TState>.Apply(
            new WorkflowEvent[] { new WorkflowEvent.RunSuspended(cause, holdDeadline, settings.HoldTimeoutStepName) },
            decisions);
    }

    /// <summary>
    /// Puts a suspended instance back to work, restarting its step from the beginning: guarantee E4,
    /// so the retry count resets and any backoff that was in progress when it was suspended is
    /// dropped. The step's own deadline is measured from now, since that is when the attempt starts.
    /// </summary>
    public static ControlPlan<TState> PlanResume<TState>(
        WorkflowRuntimeState<TState> envelope, DateTimeOffset now, ResolvedWorkflowSettings settings,
        TransitionCause cause)
    {
        if (envelope.Status != WorkflowStatus.Suspended)
        {
            return new ControlPlan<TState>.Reject($"Cannot resume from status {envelope.Status}.");
        }

        var stepTimeout = envelope.CurrentStepName is { } stepName ? settings.StepTimeoutFor(stepName) : null;
        var decisions = new List<WorkflowDecision>
        {
            WorkflowDecision.RecordStatusChange.For(WorkflowStatus.Running),
        };

        if (envelope.CurrentStepName is not null)
        {
            decisions.Add(WorkflowDecision.StartStep.Instance);
        }

        return new ControlPlan<TState>.Apply(
            new WorkflowEvent[] { new WorkflowEvent.RunResumed(stepTimeout is { } t ? now + t : null, cause) },
            decisions);
    }

    /// <summary>
    /// Stops an instance where it stands, without unwinding — see
    /// <see cref="PlanCancel{TState}"/> for the graceful counterpart. Applies from any status the run
    /// is still live in; an already-finished instance is left alone.
    /// </summary>
    public static ControlPlan<TState> PlanTerminate<TState>(
        WorkflowRuntimeState<TState> envelope, string? reason, DateTimeOffset now, TransitionCause cause)
    {
        if (envelope.Status is WorkflowStatus.Finished or WorkflowStatus.Deleted)
        {
            return new ControlPlan<TState>.Reject($"Cannot terminate from status {envelope.Status}.");
        }

        var outcome = new WorkflowOutcome.Terminated(reason);
        var (_, childrenToClose) = ChildGroupPolicy.ApplyParentClosePolicyToChildren(envelope);

        var events = new List<WorkflowEvent> { new WorkflowEvent.RunFinished(outcome, envelope.LastTraceParent, cause) };
        if (childrenToClose.Count > 0)
        {
            events.Add(new WorkflowEvent.ParentClosePolicyApplied(
                childrenToClose.Select(c => c.RelationshipId).ToList()));
        }

        var decisions = new List<WorkflowDecision>
        {
            WorkflowDecision.RecordStatusChange.For(WorkflowStatus.Finished),
            new WorkflowDecision.RecordOutcome(outcome),
        };

        // The one route out of Paused that never reaches BuildDecisions — a Terminate lands here
        // directly, so this is the one other place that duration is reported from.
        if (envelope.Status == WorkflowStatus.Paused && envelope.PausedAt is { } pausedAt)
        {
            decisions.Add(new WorkflowDecision.RecordPauseDuration(now - pausedAt));
        }
        foreach (var child in childrenToClose)
        {
            decisions.Add(new WorkflowDecision.TerminateChild(child));
        }

        if (envelope.ParentRelationship is { } relationship)
        {
            decisions.Add(new WorkflowDecision.NotifyParent(relationship, outcome));
        }

        decisions.Add(WorkflowDecision.CancelTimer.For(WorkflowTimerKind.Workflow));
        decisions.Add(WorkflowDecision.CancelTimer.For(WorkflowTimerKind.Pause));
        decisions.Add(WorkflowDecision.NotifyCompletionWatchers.Instance);

        return new ControlPlan<TState>.Apply(events, decisions);
    }

    /// <summary>
    /// What a cancellation request means for an instance: unwind through the configured cancellation
    /// step, or — with none configured, or once the instance has already finished — finish straight
    /// away as <see cref="WorkflowOutcome.Cancelled"/>.
    ///
    /// Routing to a step is what makes cancellation graceful: that step runs like any other, with its
    /// own timeout and retry budget, and decides the run's final outcome itself. It normally ends with
    /// <c>ThenCancel()</c>; a compensation that hit trouble reports <c>ThenFail(...)</c>, so the
    /// recorded outcome matches what actually happened during the unwind.
    ///
    /// Returns <c>null</c> for an instance that already finished: cancellation applies to a run still
    /// in progress.
    /// </summary>
    public static Transition? PlanCancel<TState>(
        WorkflowRuntimeState<TState> envelope, string? reason, ResolvedWorkflowSettings settings)
    {
        if (envelope.Status is WorkflowStatus.Finished or WorkflowStatus.Deleted)
        {
            return null;
        }

        return settings.CancellationStepName is { } stepName
            ? new Transition.StepTransition(stepName, new WorkflowCancellation(reason))
            : new Transition.TerminalTransition(new WorkflowOutcome.Cancelled(reason));
    }

    /// <summary>
    /// Whether a fired pause timeout should be acted on. Returns <c>null</c> unless the instance is
    /// still paused with a handler step recorded — a timer that outlived its pause is a no-op.
    /// </summary>
    public static Transition? PlanPauseTimeout<TState>(WorkflowRuntimeState<TState> envelope) =>
        envelope.Status == WorkflowStatus.Paused && envelope.PauseTimeoutStepName is { } stepName
            ? new Transition.StepTransition(stepName, null)
            : null;

    /// <summary>
    /// What a held instance does when nobody came back for it: run the step its hold named, which
    /// decides whether it resumes, fails or ends. <c>null</c> once the instance has left
    /// <see cref="WorkflowStatus.Suspended"/> by any other route, so a timer that fires just after a
    /// resume finds nothing to do.
    /// </summary>
    /// <summary>
    /// What a parent does about a group whose children never finished: run the step the group named.
    /// <c>null</c> once that group has resolved, so a timer firing just after the last child reports
    /// does nothing.
    /// </summary>
    public static Transition? PlanChildGroupTimeout<TState>(
        WorkflowRuntimeState<TState> envelope, string groupId) =>
        envelope.ChildGroups?.GetValueOrDefault(groupId) is { Finalized: false, TimeoutStepName: { } stepName }
            ? new Transition.StepTransition(stepName, null)
            : null;

    public static Transition? PlanHoldTimeout<TState>(WorkflowRuntimeState<TState> envelope) =>
        envelope.Status == WorkflowStatus.Suspended && envelope.HoldTimeoutStepName is { } stepName
            ? new Transition.StepTransition(stepName, null)
            : null;

    private static IReadOnlyList<WorkflowDecision> BuildDecisions<TState>(
        WorkflowRuntimeState<TState> previous,
        WorkflowRuntimeState<TState> next,
        Transition transition,
        DateTimeOffset now,
        string? groupId,
        IReadOnlyList<ChildWorkflowRelationship> childrenToClose)
    {
        var decisions = new List<WorkflowDecision>();

        // A run beginning moves an instance off NotStarted, and landing on Running that way is the run
        // starting. The counters this feeds read a move to Running as a resume, which a first step is
        // not — step and outcome metrics are what cover a run starting. A first transition that lands
        // anywhere else still reports: a run whose opening move is a pause has genuinely paused.
        var beginning = previous.Status == WorkflowStatus.NotStarted
            && next.Status == WorkflowStatus.Running;

        // This covers every exit from Paused as well, whatever caused it: an ordinary step transition
        // driven by a business command lands on Running and reports the change from here, the same as
        // the admin Resume path does.
        if (next.Status != previous.Status && !beginning)
        {
            decisions.Add(WorkflowDecision.RecordStatusChange.For(next.Status));

            // Leaving Paused this way covers every route through Plan() itself — an ordinary
            // business-command step transition, a pause timeout's step, ending, deleting, restarting.
            // PlanTerminate is the one route out of Paused that never reaches here (it builds its own
            // decisions directly) and reports this duration itself.
            if (previous.Status == WorkflowStatus.Paused && previous.PausedAt is { } pausedAt)
            {
                decisions.Add(new WorkflowDecision.RecordPauseDuration(now - pausedAt));
            }
        }

        switch (transition)
        {
            case Transition.TerminalTransition:
                decisions.Add(new WorkflowDecision.RecordOutcome(next.Outcome!));
                foreach (var child in childrenToClose)
                {
                    // A parent that unwound gracefully lets its children do the same; every other
                    // way of finishing stops them where they are.
                    decisions.Add(next.Outcome is WorkflowOutcome.Cancelled cancelled
                        ? new WorkflowDecision.CancelChild(child, cancelled.Reason)
                        : new WorkflowDecision.TerminateChild(child));
                }

                AddParentNotification(decisions, next);
                break;

            case Transition.DeleteTransition dt:
                foreach (var child in childrenToClose)
                {
                    decisions.Add(new WorkflowDecision.DeleteChild(child));
                }

                AddParentNotification(decisions, next);
                decisions.Add(WorkflowDecision.PurgeAndStop.Instance);
                break;

            case Transition.AwaitChildrenTransition:
                foreach (var relationship in next.Children!.Values.Where(c => c.Status == ChildStatus.Pending && c.GroupId == groupId))
                {
                    decisions.Add(new WorkflowDecision.StartChild(relationship));
                }

                break;

            case Transition.RestartTransition:
                foreach (var child in childrenToClose)
                {
                    decisions.Add(new WorkflowDecision.TerminateChild(child));
                }

                decisions.Add(WorkflowDecision.ReclaimHistory.Instance);
                break;
        }

        // A step transition chains straight into the next step, so anything deferred stays deferred;
        // a restart does the same, since it continues the run at a step of its own. Every other
        // settled transition releases what was deferred (guarantee C2).
        decisions.Add(transition is Transition.StepTransition or Transition.RestartTransition
            ? WorkflowDecision.StartStep.Instance
            : WorkflowDecision.ReleaseDeferredCommands.Instance);

        if (previous.WorkflowDeadline is null && next.WorkflowDeadline is { } workflowDeadline)
        {
            decisions.Add(new WorkflowDecision.ArmTimer(WorkflowTimerKind.Workflow, workflowDeadline));
        }

        if (next.Status is WorkflowStatus.Finished or WorkflowStatus.Deleted)
        {
            decisions.Add(WorkflowDecision.CancelTimer.For(WorkflowTimerKind.Workflow));
        }

        decisions.Add(next.Status == WorkflowStatus.Paused && next.PauseDeadline is { } pauseDeadline
            ? new WorkflowDecision.ArmTimer(WorkflowTimerKind.Pause, pauseDeadline)
            : WorkflowDecision.CancelTimer.For(WorkflowTimerKind.Pause));

        // A group's own wait starts when the group opens. Keyed by the group, so a parent awaiting
        // two of them keeps a deadline for each.
        if (transition is Transition.AwaitChildrenTransition
            && groupId is not null
            && next.ChildGroups?.GetValueOrDefault(groupId) is { Deadline: { } groupDeadline })
        {
            decisions.Add(new WorkflowDecision.ArmTimer(WorkflowTimerKind.ChildGroup, groupDeadline, groupId));
        }

        // A hold runs while the instance is Suspended, by the same rule the pause timer follows: the
        // status is what decides, so leaving that status ends the wait whatever instant the envelope
        // still carries.
        decisions.Add(next.Status == WorkflowStatus.Suspended && next.HoldDeadline is { } holdDeadline
            ? new WorkflowDecision.ArmTimer(WorkflowTimerKind.Hold, holdDeadline)
            : WorkflowDecision.CancelTimer.For(WorkflowTimerKind.Hold));

        // A parked run releases its watchers too. It has not ended, but nothing it does next will
        // happen without someone acting on the failure first, so a caller waiting on it has nothing
        // left to wait for — see WorkflowResult{TState}.Parked.
        if (next.Status is WorkflowStatus.Finished or WorkflowStatus.Deleted
            || transition is Transition.ParkTransition)
        {
            decisions.Add(WorkflowDecision.NotifyCompletionWatchers.Instance);
        }

        return decisions;
    }

    /// <summary>
    /// When a hold established now stops waiting, as an absolute instant. <c>null</c> unless the
    /// settings name both a length and a step to run — a deadline with nowhere to go would release
    /// an instance into no particular step, so both are required together.
    /// </summary>
    private static DateTimeOffset? HoldDeadlineFor(ResolvedWorkflowSettings settings, DateTimeOffset now) =>
        settings is { HoldTimeout: { } timeout, HoldTimeoutStepName: not null } ? now + timeout : null;

    private static void AddParentNotification<TState>(
        List<WorkflowDecision> decisions, WorkflowRuntimeState<TState> next)
    {
        if (next.ParentRelationship is { } relationship)
        {
            // A deleted instance never finished, so it has no outcome of its own to report; the
            // parent learns only that this child is gone.
            decisions.Add(new WorkflowDecision.NotifyParent(relationship, next.Outcome));
        }
    }

    /// <summary>
    /// Fills in what a handler could not know about its own failure — which step it was raised from
    /// and how many attempts had run — leaving anything the caller already supplied untouched.
    /// </summary>
    private static WorkflowOutcome Enrich<TState>(WorkflowOutcome outcome, WorkflowRuntimeState<TState> envelope) =>
        outcome is WorkflowOutcome.Failed { Cause: { StepName: null } cause }
            ? new WorkflowOutcome.Failed(cause with
            {
                StepName = envelope.CurrentStepName,
                Attempts = cause.Attempts == 0 ? envelope.RetryCount + 1 : cause.Attempts,
            })
            : outcome;

    private static IEnumerable<ChildWorkflowRelationship> BuildRelationships(
        Transition.AwaitChildrenTransition transition,
        string groupId,
        WorkflowInstanceIdentity identity,
        string? traceParent)
    {
        foreach (var child in transition.Children)
        {
            yield return new ChildWorkflowRelationship(
                // PersistenceId, for global uniqueness across every workflow type sharing a registry.
                // Only ever compared to itself as an opaque key, never used to route anything.
                RelationshipId: $"{identity.PersistenceId}:{groupId}:{child.WorkflowId}",
                ParentWorkflowType: identity.WorkflowTypeName,
                // RoutableId, because this is what a child addresses to report back.
                ParentWorkflowId: identity.RoutableId,
                ChildWorkflowType: child.WorkflowType,
                ChildWorkflowId: child.WorkflowId,
                GroupId: groupId,
                Generation: 0,
                Status: ChildStatus.Pending,
                Result: null,
                Failure: null,
                TraceParent: traceParent,
                ParentClosePolicy: child.ParentClosePolicy,
                Command: child.Command);
        }
    }
}
