namespace Sagant.Protocol;

/// <summary>
/// How a workflow run finished. Non-null exactly when
/// <see cref="WorkflowRuntimeState{TState}.Status"/> is <see cref="WorkflowStatus.Finished"/>, so
/// "did this run finish, and how?" is one question with one answer.
///
/// Closed hierarchy (all cases nested sealed records) so callers pattern-match exhaustively, and each
/// case carries what is specific to it. The set
/// matches what comparable engines distinguish — Temporal's
/// Completed/Failed/TimedOut/Canceled/Terminated, Step Functions' SUCCEEDED/FAILED/TIMED_OUT/ABORTED.
///
/// Deletion is deliberately absent: purging an instance's data is orthogonal to how its run ended.
/// A workflow deleted after completing is <see cref="WorkflowStatus.Deleted"/> carrying
/// <see cref="Completed"/>; one deleted mid-run carries no outcome at all.
/// </summary>
public abstract record WorkflowOutcome
{
    private WorkflowOutcome()
    {
    }

    /// <summary>The workflow reached its own successful conclusion.</summary>
    public sealed record Completed : WorkflowOutcome
    {
        public static readonly Completed Instance = new();
    }

    /// <summary>
    /// The workflow failed. Either a step exhausted its retry budget with no failover configured, or
    /// a handler failed the run deliberately.
    /// </summary>
    public sealed record Failed(WorkflowFailure Cause) : WorkflowOutcome;

    /// <summary>
    /// The workflow-level deadline elapsed with no recover strategy configured.
    ///
    /// Only the workflow-level deadline produces this. A step timeout becomes a step failure and
    /// flows through the retry budget like any other, surfacing as <see cref="Failed"/>; a pause
    /// timeout transitions into its configured handler step and is not terminal at all.
    /// </summary>
    public sealed record TimedOut : WorkflowOutcome
    {
        public static readonly TimedOut Instance = new();
    }

    /// <summary>
    /// Stopped from outside, gracefully: the workflow was given the chance to unwind through its
    /// configured cancellation step before finishing.
    ///
    /// Distinct from <see cref="Terminated"/> because the intent differs — a cancelled run was asked
    /// to stop and did so on its own terms; a terminated one was stopped regardless. The distinction
    /// survives even where the workflow had nothing to unwind, so a caller can always tell which was
    /// asked for.
    /// </summary>
    public sealed record Cancelled(string? Reason) : WorkflowOutcome;

    /// <summary>
    /// Stopped from outside, abruptly. Whatever step was running is cancelled and the run finishes
    /// without unwinding. Reach for <see cref="Cancelled"/> when the workflow should get to
    /// compensate first.
    /// </summary>
    public sealed record Terminated(string? Reason) : WorkflowOutcome;
}
