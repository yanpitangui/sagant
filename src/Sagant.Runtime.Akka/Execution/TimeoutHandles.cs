using Akka.Actor;

namespace Sagant.Runtime.Akka.Execution;

/// <summary>
/// Bundles <see cref="WorkflowEntityActor{TWorkflow, TState}"/>'s five live timeout handles — one
/// <see cref="ICancelable"/> per kind of deadline it arms (step, workflow, pause, retry backoff
/// delay, graceful-shutdown grace window). Arming a timer stays on the actor (it needs the actor's
/// own <c>IWorkflowTimeoutScheduler</c>/<c>Self</c>/message type, which varies per call site) — this
/// collaborator's job is owning the handles themselves and the two multi-cancel sequences
/// (<see cref="CancelForSuspend"/>/<see cref="CancelForTerminate"/>) that would otherwise be
/// hand-copied at every call site that needs them.
/// </summary>
internal sealed class TimeoutHandles
{
    public ICancelable? Step { get; set; }

    public ICancelable? Workflow { get; set; }

    public ICancelable? Pause { get; set; }

    public ICancelable? RetryDelay { get; set; }

    public ICancelable? GracefulShutdownDeadline { get; set; }

    public void CancelStep() => Step?.Cancel();

    public void CancelWorkflow() => Workflow?.Cancel();

    public void CancelPause() => Pause?.Cancel();

    public void CancelRetryDelay() => RetryDelay?.Cancel();

    public void CancelGracefulShutdownDeadline() => GracefulShutdownDeadline?.Cancel();

    /// <summary>Suspend invalidates the current step attempt and any retry backoff wait, but leaves
    /// the workflow/pause timers alone — a suspended workflow isn't Paused, and its workflow-level
    /// deadline keeps ticking (see <c>HandleWorkflowTimedOut</c>'s own doc comment).</summary>
    public void CancelForSuspend()
    {
        CancelStep();
        CancelRetryDelay();
    }

    /// <summary>Terminate is the workflow's own end — every live timer this instance could have
    /// armed becomes moot at once.</summary>
    public void CancelForTerminate()
    {
        CancelStep();
        CancelWorkflow();
        CancelPause();
        CancelRetryDelay();
    }
}
