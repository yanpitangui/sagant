namespace Sagant.Protocol;

/// <summary>
/// What a caller who ran a workflow and waited gets back: the point at which there is nothing more
/// for them to wait on, and the state as of then.
///
/// Two ways a wait ends, so this is a closed hierarchy a caller switches over exhaustively. A run
/// that <see cref="Finished"/> is over. A run that <see cref="Parked"/> is alive and holding, and
/// resumes only once someone acts on the failure it carries — waiting longer would achieve nothing,
/// which is why it comes back rather than blocking to a timeout.
///
/// A failed run comes back as a value rather than an exception, because a workflow that could not
/// charge an order is an ordinary business result the caller decides about, never an exceptional
/// condition in the caller's own control flow.
/// </summary>
public abstract record WorkflowResult<TState>
{
    // Private, so the cases below — which reach it by being nested — are the only ones there are.
    private WorkflowResult(TState state) => State = state;

    /// <summary>The workflow's state as of the transition this result describes. Present either way:
    /// a failed, terminated or parked run still has whatever state it reached.</summary>
    public TState State { get; }

    /// <summary>Whether the run reached its own successful conclusion.</summary>
    public bool IsCompleted => this is Finished { Outcome: WorkflowOutcome.Completed };

    /// <summary>
    /// The failure behind this result: why a run failed, or why a parked one is being held.
    /// <c>null</c> for a run that ended any other way.
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
    /// The run is held at the step that exhausted its retry budget under
    /// <see cref="Settings.RecoverStrategy.ParkOnExhaustion"/>, and stays there until
    /// <c>IWorkflowHandle.Resume</c> retries it.
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
}
