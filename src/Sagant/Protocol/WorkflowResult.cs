namespace Sagant.Protocol;

/// <summary>
/// What a caller who ran a workflow and waited gets back: the point at which there is nothing more
/// for them to wait on, and the state as of then.
///
/// Three ways a wait ends, so this is a closed hierarchy a caller switches over exhaustively. A run
/// that <see cref="Finished"/> is over. A run that <see cref="Parked"/> is alive and holding on a
/// failure, and resumes only once someone acts on it — this comes back the moment that becomes true,
/// releasing the caller as soon as waiting further would achieve nothing. A run that
/// <see cref="Waiting"/> is alive and holding on purpose, no failure involved — a business pause or an
/// operator hold — and releases the caller the same way, immediately, however long the hold itself
/// ends up lasting.
///
/// A failed run comes back as a value: a workflow that could not charge an order is an ordinary
/// business result the caller decides about, never an exceptional condition in the caller's own
/// control flow.
/// </summary>
public abstract record WorkflowResult<TState>
{
    // Private, so the cases below — which reach it by being nested — are the only ones there are.
    private WorkflowResult(TState state) => State = state;

    /// <summary>The workflow's state as of the transition this result describes. Present either way:
    /// a failed, terminated, parked or waiting run still has whatever state it reached.</summary>
    public TState State { get; }

    /// <summary>Whether the run reached its own successful conclusion.</summary>
    public bool IsCompleted => this is Finished { Outcome: WorkflowOutcome.Completed };

    /// <summary>
    /// The failure behind this result: why a run failed, or why a parked one is being held.
    /// <c>null</c> for a run that ended any other way, including <see cref="Waiting"/> — a hold with
    /// no failure behind it has nothing here to report.
    /// </summary>
    public WorkflowFailure? Failure => this switch
    {
        Finished { Outcome: WorkflowOutcome.Failed failed } => failed.Cause,
        Parked parked => parked.Cause,
        _ => null,
    };

    /// <summary>The run is over. <see cref="Outcome"/> says how.</summary>
    public sealed record Finished : WorkflowResult<TState>
    {
        public Finished(WorkflowOutcome outcome, TState state)
            : base(state) => Outcome = outcome;

        /// <summary>How the run ended.</summary>
        public WorkflowOutcome Outcome { get; }
    }

    /// <summary>
    /// The run is held at its current step — by a spent retry budget under
    /// <see cref="Settings.RecoverStrategy.ParkOnExhaustion"/>, or by that step being missing from
    /// the running deployment (guarantee E5) — and stays there until <c>IWorkflowHandle.Resume</c>
    /// retries it.
    ///
    /// The instance is still alive and still owns its id, so a caller seeing this reports the
    /// failure onward and leaves the run where it is; resuming it later picks up from that step.
    /// </summary>
    public sealed record Parked : WorkflowResult<TState>
    {
        public Parked(WorkflowFailure cause, TState state)
            : base(state) => Cause = cause;

        /// <summary>What stopped the step this run is held at.</summary>
        public WorkflowFailure Cause { get; }
    }

    /// <summary>
    /// The run is holding on purpose — <see cref="WorkflowStatus.Paused"/> (a business
    /// <c>ThenPause</c>) or <see cref="WorkflowStatus.Suspended"/> with no <c>ParkedFailure</c> (an
    /// operator <c>Suspend</c>) — as opposed to <see cref="Parked"/>, which is <c>Suspended</c>
    /// specifically because something failed. Both routes here read the same way: nothing went wrong,
    /// the run is simply waiting for a command, an operator, or its own deadline to pass.
    /// </summary>
    public sealed record Waiting : WorkflowResult<TState>
    {
        public Waiting(WorkflowStatus status, WorkflowWaitReason reason, TState state)
            : base(state)
        {
            Status = status;
            Reason = reason;
        }

        /// <summary><see cref="WorkflowStatus.Paused"/> or <see cref="WorkflowStatus.Suspended"/> —
        /// carried for completeness; the two read the same way, so most callers have no reason to
        /// switch on this.</summary>
        public WorkflowStatus Status { get; }

        /// <summary>Why the run is holding, and what would release it on its own if anything would.</summary>
        public WorkflowWaitReason Reason { get; }
    }
}
