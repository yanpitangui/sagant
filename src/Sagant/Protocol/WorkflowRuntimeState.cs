namespace Sagant.Protocol;

using Sagant.Idempotency;

/// <summary>
/// Coarse-grained lifecycle status of a workflow instance. How a finished run *ended* is
/// <see cref="WorkflowOutcome"/>'s job alone; this enum stops at "it's over."
/// </summary>
public enum WorkflowStatus
{
    /// <summary>
    /// Nothing has run under this id. An instance addressed before anything was written to it reports
    /// this, so a caller can tell an absent run from a live one.
    ///
    /// This is the zero value deliberately: a status that arrives as <c>default</c> — an unset field, a
    /// value type that was never assigned, an id whose history is gone — reports the run as absent, a
    /// definitive answer a caller can act on immediately, distinct from every in-progress status the
    /// enum defines.
    /// </summary>
    NotStarted = 0,

    /// <summary>Executing, or about to.</summary>
    Running = 1,

    /// <summary>Waiting on something outside itself, by its own decision.</summary>
    Paused = 2,

    /// <summary>Held by an operator. Resumes where it left off.</summary>
    Suspended = 3,

    /// <summary>
    /// The run is over. <see cref="WorkflowRuntimeState{TState}.Outcome"/> is non-null exactly when
    /// the status is this, and says how it ended.
    /// </summary>
    Finished = 4,

    /// <summary>
    /// The instance's persisted data has been purged. Orthogonal to how the run ended: an instance
    /// deleted after finishing still carries its outcome, one deleted mid-run carries none.
    /// </summary>
    Deleted = 5,
}

/// <summary>
/// Everything a runtime knows about one workflow instance: runtime-owned bookkeeping alongside
/// <typeparamref name="TState"/>. This is what a handler sees and what every decision is made from.
///
/// Derived, never written directly: a transition persists the facts it changed as
/// <see cref="Execution.WorkflowEvent"/>s, and this is what folding those facts produces (see
/// <see cref="Execution.WorkflowEventFold"/>). A runtime may store a copy as a snapshot to shorten
/// replay, which is a caching decision that leaves the events the source of truth.
/// </summary>
public sealed record WorkflowRuntimeState<TState>(
    TState UserState,
    string? CurrentStepName,
    object? CurrentStepInput,
    int RetryCount,
    WorkflowStatus Status,
    /// <summary>
    /// How this run finished, or <c>null</c> while it is still going. Non-null exactly when
    /// <paramref name="Status"/> is <see cref="WorkflowStatus.Finished"/> — and preserved through a
    /// later <see cref="WorkflowStatus.Deleted"/>, since deletion says nothing about how the run
    /// ended.
    /// </summary>
    WorkflowOutcome? Outcome = null,
    /// <summary>
    /// Why this instance is being held: a step exhausted its retry budget under
    /// <see cref="Settings.RecoverStrategy.ParkOnExhaustion"/>, or it stands on a step the running
    /// deployment has no code for (guarantee E5). Readable while the run waits, so whoever decides
    /// whether to resume can see what to fix first.
    ///
    /// Distinct from <see cref="Outcome"/>, which says how a run <em>ended</em>: a parked run has
    /// not ended, and clears this the moment it resumes.
    /// </summary>
    WorkflowFailure? ParkedFailure = null,
    DateTimeOffset? StepDeadline = null,
    DateTimeOffset? WorkflowDeadline = null,
    DateTimeOffset? PauseDeadline = null,
    string? PauseTimeoutStepName = null,
    /// <summary>
    /// How long this instance stays held before it decides for itself, set when an operator holds it
    /// or a parked failure stops it. Same durability as the deadlines above: an absolute instant,
    /// re-armed on every activation, so a hold outlives a crash and a relocation at its
    /// <em>remaining</em> length.
    ///
    /// A hold that waits for a person indefinitely leaves this <c>null</c>, which is the default: an
    /// instance is released by a command, and a deadline is what a deployment adds when it wants one
    /// to stop waiting eventually.
    /// </summary>
    DateTimeOffset? HoldDeadline = null,
    /// <summary>The step run when <see cref="HoldDeadline"/> passes, releasing the instance into
    /// whatever that step decides.</summary>
    string? HoldTimeoutStepName = null,
    string? LastTraceParent = null,
    /// <summary>Set while waiting out a <c>RecoverStrategy.BackoffForAttempt</c> delay before a
    /// retry — same "persist an absolute deadline, re-arm a live timer on reactivation" durability
    /// as <see cref="StepDeadline"/>/<see cref="WorkflowDeadline"/>/<see cref="PauseDeadline"/>, so
    /// a crash or the runtime relocating this instance mid-wait resumes the *remaining* delay from
    /// where it left off. <c>null</c> once the retry actually starts.</summary>
    DateTimeOffset? RetryDelayUntil = null,
    /// <summary>
    /// Closes transport-level redelivery (a runtime's delivery mechanism retrying a send because its
    /// prior acknowledgment was lost, e.g. this entity crashed before persisting) with no
    /// caller-facing API: keyed by the sending producer's id, value is the highest sequence number
    /// this instance has already durably applied for that producer. A redelivered seqNr at or below
    /// the recorded value is a genuine duplicate — skip the handler, re-confirm, don't re-persist.
    /// Bounded via <see cref="SeqNrLedger"/> — see that type's own doc comment for why an evicted
    /// producer id is safe to forget. Which runtime, if any, actually produces redelivery is not this
    /// core layer's concern — see the runtime driver's own docs for its producer/consumer wiring.
    /// </summary>
    SeqNrLedger? HighestAppliedSeqNr = null,
    /// <summary>
    /// Closes the ambiguous-<c>Ask</c>-timeout caller-retry case: a caller-supplied idempotency key
    /// on a repeat send replays the cached <see cref="Idempotency.IdempotencyLedger"/> reply, with
    /// the command handler left uninvoked. <c>null</c> until the first key-bearing command this
    /// instance ever handles — constructed lazily (see <c>WorkflowEntityActor</c>) since most
    /// workflow instances never use idempotency keys at all.
    /// </summary>
    IdempotencyLedger? IdempotencyLedger = null,
    /// <summary>
    /// Every child workflow this instance has ever started, across every group, regardless of
    /// whether that group has since finalized — the lifetime-scoped list <c>ParentClosePolicy</c>
    /// operates over. See <see cref="ChildWorkflowRelationship"/>'s own doc comment for why this is
    /// the single source of truth for every parent/child relationship this instance holds.
    /// </summary>
    IReadOnlyList<ChildWorkflowRelationship>? Children = null,
    /// <summary>
    /// One entry per active-or-recently-finalized <c>AwaitChildren</c> group this instance has
    /// created, keyed by <c>GroupId</c>. Holds policy + finalization state only — member status
    /// lives on <see cref="Children"/>, filtered by <c>GroupId</c>, never duplicated here.
    /// </summary>
    IReadOnlyDictionary<string, ChildGroupState>? ChildGroups = null,
    /// <summary>
    /// A simple persisted counter, incremented once per <c>AwaitChildren</c> call that didn't supply
    /// an explicit group id — same category of mechanism as <c>RetryCount</c>: computed at persist
    /// time by the runtime driver, never by the step body, which is what makes the resulting
    /// <c>GroupId</c> retry-safe (a step retried before this counter's increment was itself
    /// persisted reads the same, still-unincremented value on its next attempt).
    /// </summary>
    int ChildGroupSequence = 0,
    /// <summary>
    /// Set once, when this instance was started *as a child* of another workflow — never touched
    /// again afterward. <c>null</c> for a workflow with no parent (the overwhelmingly common case).
    /// Read at this instance's own terminal transition to durably notify whichever workflow is
    /// waiting on it.
    /// </summary>
    ChildWorkflowRelationship? ParentRelationship = null);
