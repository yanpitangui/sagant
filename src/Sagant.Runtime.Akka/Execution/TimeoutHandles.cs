using Akka.Actor;

namespace Sagant.Runtime.Akka.Execution;

/// <summary>
/// Bundles <see cref="WorkflowEntityActor{TWorkflow, TState}"/>'s live timeout handles — one
/// <see cref="ICancelable"/> per kind of deadline it arms (step, workflow, pause, hold, retry
/// backoff delay, graceful-shutdown grace window). Arming a timer stays on the actor (it needs the actor's
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

    public ICancelable? Hold { get; set; }

    /// <summary>One handle per awaited child group, keyed by group id — a dictionary, since an
    /// instance can await several groups at once, each with its own wait.</summary>
    private readonly Dictionary<string, ICancelable> _childGroups = new();

    public void SetChildGroup(string groupId, ICancelable handle)
    {
        CancelChildGroup(groupId);
        _childGroups[groupId] = handle;
    }

    public void CancelChildGroup(string groupId)
    {
        if (_childGroups.Remove(groupId, out var existing))
        {
            existing.Cancel();
        }
    }

    public void CancelAllChildGroups()
    {
        foreach (var handle in _childGroups.Values)
        {
            handle.Cancel();
        }

        _childGroups.Clear();
    }

    public ICancelable? RetryDelay { get; set; }

    public ICancelable? GracefulShutdownDeadline { get; set; }

    public void CancelStep() => Step?.Cancel();

    public void CancelWorkflow() => Workflow?.Cancel();

    public void CancelPause() => Pause?.Cancel();

    public void CancelHold() => Hold?.Cancel();

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

    /// <summary>
    /// Every timer this incarnation armed, dropped at once.
    ///
    /// What an incarnation ending means for a deadline is nothing: each one is a persisted absolute
    /// instant that the next activation arms afresh. Leaving them armed instead would have them fire
    /// against an actor that has gone, which the scheduler holds until they do.
    /// </summary>
    public void CancelAll() => CancelForTerminate();

    /// <summary>Terminate is the workflow's own end — every live timer this instance could have
    /// armed becomes moot at once.</summary>
    public void CancelForTerminate()
    {
        CancelStep();
        CancelWorkflow();
        CancelPause();
        CancelHold();
        CancelRetryDelay();
        CancelAllChildGroups();
    }
}
