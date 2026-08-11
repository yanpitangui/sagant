using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Execution;

/// <summary>
/// One durable fact about a workflow instance. A runtime persists these; folding them over a fresh
/// envelope reproduces the instance exactly (see <see cref="WorkflowEventFold"/>).
///
/// Every event carries <em>computed facts</em>, never intent. An event says "the step started, and
/// its deadline is 12:04:31", never "the step started, work out the deadline" — because a replay
/// happens at a different moment than the original write, and recomputing a deadline against the
/// replay's clock would silently move it. Guarantee D2 promises a crash resumes the *remaining*
/// wait, and that only holds if what was written is what comes back.
///
/// The same rule keeps <see cref="WorkflowEventFold"/> free of a clock and of settings, so it is a
/// pure function of state and event — trivially testable, and identical whether it runs live or on
/// recovery.
///
/// Closed hierarchy (all cases nested sealed records) so the fold is exhaustive and the compiler
/// flags a new event that nothing handles.
///
/// <para><b>Schema obligation.</b> These are persisted, so a field may be added as optional, and a
/// new case may be introduced — but a field must never be renamed or have its type changed, and a
/// case must never be removed while any instance might still replay it. Unlike <c>TState</c>, whose
/// evolution is the consumer's responsibility, this schema is the engine's own.</para>
/// </summary>
public abstract record WorkflowEvent
{
    private WorkflowEvent()
    {
    }

    /// <summary>
    /// The workflow's own state was replaced. Separate from the transition events because the two
    /// change independently: an effect can update state without moving the workflow, and a
    /// transition can move it without touching state — in which case nothing here is written and the
    /// state is left out of that write entirely.
    /// </summary>
    public sealed record UserStateChanged<TState>(TState State) : WorkflowEvent;

    /// <summary>
    /// The workflow-wide deadline was established. Written at most once per instance, which is what
    /// makes guarantee D3's stickiness a property of the event stream: the fold applies whatever it
    /// finds, and only one write ever sets it.
    /// </summary>
    public sealed record WorkflowDeadlineSet(DateTimeOffset Deadline) : WorkflowEvent;

    /// <summary>
    /// An event that names what drove it. Exactly one event per batch carries a cause, so a consumer
    /// matching this one base type reads "why did this change" without knowing which concrete event
    /// happened to arrive. Batch boundaries are invisible to a reader of the event stream, so an
    /// event carries its own cause.
    ///
    /// The whole observability vocabulary lives on <see cref="TransitionCause"/>: step name, attempt,
    /// duration, error, retry decision, command type, and caller metadata.
    /// </summary>
    public abstract record CausedEvent(TransitionCause Cause) : WorkflowEvent;

    /// <summary>
    /// The workflow applied a change and stayed where it was — a command updating state without
    /// moving the instance. This is the outcome event for a batch that moves nothing, so every batch
    /// has exactly one <see cref="CausedEvent"/> saying what happened and why.
    /// </summary>
    public sealed record RunStayed(TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>A step began. Deadlines are absolute instants computed at write time.</summary>
    public sealed record StepStarted(
        string StepName,
        object? Input,
        DateTimeOffset? StepDeadline,
        string? TraceParent,
        TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>A step attempt failed and another will run. Carries the attempt's own deadline, with
    /// any backoff already folded in (guarantee E2).</summary>
    public sealed record StepRetryScheduled(
        int RetryCount,
        DateTimeOffset? StepDeadline,
        DateTimeOffset? RetryDelayUntil,
        TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>The workflow paused, optionally with a deadline and the step to resume through.
    /// <paramref name="Reason"/> is the pausing handler's own words, carried on the event so anything
    /// reading the event stream can say why the instance is waiting.</summary>
    public sealed record RunPaused(
        string? Reason,
        DateTimeOffset? PauseDeadline,
        string? PauseTimeoutStepName,
        string? TraceParent,
        TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>The run finished. <paramref name="Outcome"/> says how.</summary>
    public sealed record RunFinished(WorkflowOutcome Outcome, string? TraceParent, TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>The instance's data is being purged, recorded so a crash mid-purge still recovers as
    /// deleted.</summary>
    public sealed record RunDeleted(string? TraceParent, TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>
    /// A fresh cycle began under the same id, and the history before it became reclaimable.
    ///
    /// The fold resets what belongs to the finished cycle — retry count, deadlines, children — and
    /// carries forward what belongs to the instance: its state, written by the
    /// <see cref="UserStateChanged{TState}"/> sharing this batch, and its deduplication ledgers,
    /// since a producer keeps counting sequence numbers across a restart.
    ///
    /// Recorded separately from the reclamation itself, which is a driver's own act: replaying this
    /// event rebuilds the same fresh envelope whether or not the history was ever physically
    /// reclaimed, so a crash between the two costs disk and changes no state.
    /// </summary>
    public sealed record RunRestarted(
        string StepName,
        object? Input,
        string? Reason,
        DateTimeOffset? StepDeadline,
        string? TraceParent,
        TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>An operator held the instance. The current step name and input stay put, so a later
    /// resume knows what to re-execute.</summary>
    public sealed record RunSuspended(TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>
    /// A step exhausted its retry budget under a parking strategy, so the instance is held at that
    /// step with <paramref name="Failure"/> recording what stopped it. Reaches the same
    /// <c>Suspended</c> status an operator hold reaches, and resumes the same way — the difference is
    /// that this one carries a reason a reader can act on.
    /// </summary>
    public sealed record RunParked(WorkflowFailure Failure, string? TraceParent, TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>A held instance went back to work, restarting its step from the beginning
    /// (guarantee E4).</summary>
    public sealed record RunResumed(DateTimeOffset? StepDeadline, TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>A child group was started. One event carrying all <c>n</c> relationships it created,
    /// written once as the group opens.</summary>
    /// <param name="NextGroupSequence">The counter's value after this group. The event carries it
    /// because the counter advances only for a generated id — a caller naming its own group leaves it
    /// alone (guarantee H3), and only the planner knows which case this was.</param>
    public sealed record ChildrenAwaited(
        string GroupId,
        IReadOnlyList<ChildWorkflowRelationship> Relationships,
        ChildGroupState Group,
        int NextGroupSequence,
        string? TraceParent,
        TransitionCause Cause) : CausedEvent(Cause);

    /// <summary>One child reported its own terminal outcome. The small, frequent event this whole
    /// scheme exists for: each one names the single member it concerns, so a group of <c>n</c>
    /// children costs <c>n</c> relationship writes across the whole fan-out (guarantee H5).</summary>
    public sealed record ChildMemberUpdated(
        string RelationshipId,
        ChildStatus Status,
        object? Result,
        WorkflowFailure? Failure,
        string? ResultTraceParent) : WorkflowEvent;

    /// <summary>A group's policy resolved. <paramref name="TerminationRequested"/> names members the
    /// group asked to stop as it finalized.</summary>
    public sealed record ChildGroupFinalized(
        string GroupId,
        IReadOnlyList<string> TerminationRequested,
        bool PruneTerminalMembers) : WorkflowEvent;

    /// <summary>
    /// A terminal transition applied <c>ParentClosePolicy</c> to the children it owns, naming those
    /// it asked to stop. One event for the whole set, written in the same batch that makes the
    /// instance terminal — which is guarantee D6.
    /// </summary>
    public sealed record ParentClosePolicyApplied(IReadOnlyList<string> TerminationRequested) : WorkflowEvent;

    /// <summary>This instance was started as another workflow's child.</summary>
    public sealed record ParentRelationshipSet(ChildWorkflowRelationship Relationship) : WorkflowEvent;

    /// <summary>A delivered message was applied, recorded so a redelivery of it is recognised.</summary>
    public sealed record SeqNrRecorded(string ProducerId, long SeqNr) : WorkflowEvent;

    /// <summary>A caller-supplied idempotency key was applied, with the reply to replay for a
    /// repeat.</summary>
    public sealed record IdempotencyRecorded(string Key, Reply Reply) : WorkflowEvent;
}
