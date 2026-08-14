namespace Sagant.Execution;

/// <summary>
/// What one event does to the set of deadlines an instance is waiting on. Closed hierarchy, so a
/// reader handles both cases and the compiler flags a third if one ever appears.
/// </summary>
public abstract record WorkflowDeadlineChange
{
    private WorkflowDeadlineChange()
    {
    }

    /// <summary>The instance now waits on <paramref name="Kind"/> until <paramref name="DueUtc"/>.
    /// Replaces whatever instant that kind was previously waiting on.</summary>
    /// <param name="Discriminator">Which one, for a kind an instance holds several of — the group id
    /// for <see cref="WorkflowTimerKind.ChildGroup"/>. <c>null</c> otherwise.</param>
    public sealed record Arm(
        WorkflowTimerKind Kind, DateTimeOffset DueUtc, string? Discriminator = null) : WorkflowDeadlineChange;

    /// <summary>The instance waits on <paramref name="Kind"/> no more.</summary>
    /// <param name="Discriminator">As <see cref="Arm"/>.</param>
    public sealed record Disarm(
        WorkflowTimerKind Kind, string? Discriminator = null) : WorkflowDeadlineChange;
}

/// <summary>
/// Reads an instance's deadlines out of its own event stream.
///
/// A pure function of one event, holding no clock and no state, so the answer is the same whether it
/// runs as the event is written or during a replay years later — the same property
/// <see cref="WorkflowEventFold"/> rests on, for the same reason. It reads the absolute instants the
/// events already carry, which is what makes a replayed arm land on the instant the original write
/// computed.
///
/// It reports what <see cref="WorkflowTransitionPlanner"/> arms, which is narrower than what
/// <see cref="WorkflowEventFold"/> stores. The planner arms a pause timer while the status is
/// <see cref="WorkflowStatus.Paused"/> and cancels the workflow timer once the run is terminal, so
/// those two conditions decide what is live. The envelope meanwhile keeps
/// <see cref="WorkflowRuntimeState{TState}.WorkflowDeadline"/> past the end, because guarantee
/// <c>D3</c> makes it sticky and a reader of the history is entitled to see what bounded the run, and
/// keeps <see cref="WorkflowRuntimeState{TState}.PauseDeadline"/> past a resume, because the instant
/// stays true of the pause that recorded it.
///
/// So every event that moves the status out of <see cref="WorkflowStatus.Paused"/> disarms the pause
/// wake, whatever the envelope still holds. An event added to
/// <see cref="WorkflowEventFold"/> that writes a status belongs in the switch below.
/// </summary>
public static class WorkflowDeadlineFold
{
    private static readonly IReadOnlyList<WorkflowDeadlineChange> None = [];

    private static readonly IReadOnlyList<WorkflowDeadlineChange> ClearsAll =
    [
        new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Workflow),
        new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Pause),
        new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Hold),
    ];

    /// <summary>Leaving a pause or a hold for somewhere the instance is running again clears both,
    /// since it can be in only one of them and is now in neither.</summary>
    private static readonly IReadOnlyList<WorkflowDeadlineChange> ClearsWaits =
    [
        new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Pause),
        new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Hold),
    ];

    /// <summary>
    /// What <paramref name="event"/> changes about the instance's deadlines. Empty for the events
    /// that leave both alone, which is most of them.
    /// </summary>
    public static IReadOnlyList<WorkflowDeadlineChange> Changes(WorkflowEvent @event) => @event switch
    {
        // Written at most once per instance, and the fold applies whatever it finds (D3).
        WorkflowEvent.WorkflowDeadlineSet e =>
            [new WorkflowDeadlineChange.Arm(WorkflowTimerKind.Workflow, e.Deadline)],

        // A pause with a timeout waits on an instant; one without waits on a command, so it has
        // nothing for a wake to fire and clears any instant a previous pause left behind.
        WorkflowEvent.RunPaused { PauseDeadline: { } deadline } =>
            [new WorkflowDeadlineChange.Arm(WorkflowTimerKind.Pause, deadline)],
        WorkflowEvent.RunPaused => ClearsWaits,

        // A hold that names a deadline arms it; one released by a command alone clears whatever the
        // previous wait left behind.
        WorkflowEvent.RunSuspended { HoldDeadline: { } suspendedUntil } =>
            [new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Pause),
             new WorkflowDeadlineChange.Arm(WorkflowTimerKind.Hold, suspendedUntil)],
        WorkflowEvent.RunParked { HoldDeadline: { } parkedUntil } =>
            [new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Pause),
             new WorkflowDeadlineChange.Arm(WorkflowTimerKind.Hold, parkedUntil)],
        WorkflowEvent.RunSuspended => ClearsWaits,
        WorkflowEvent.RunParked => ClearsWaits,

        // Back to running, by any of the routes that lead there. Both waits end; the workflow
        // deadline they were counting down against carries on.
        WorkflowEvent.StepStarted => ClearsWaits,
        WorkflowEvent.RunResumed => ClearsWaits,

        // A fresh cycle drops the finished cycle's deadlines; the next transition establishes its own.
        WorkflowEvent.RunRestarted => ClearsAll,

        // Terminal: the run is over, so nothing about it is worth waking for.
        WorkflowEvent.RunFinished => ClearsAll,
        WorkflowEvent.RunDeleted => ClearsAll,

        // A group that names a deadline is something a wake can fire, keyed by the group so two
        // awaited at once keep their own.
        WorkflowEvent.ChildrenAwaited { Group.Deadline: { } groupDeadline } e =>
        [
            new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Pause),
            new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.Hold),
            new WorkflowDeadlineChange.Arm(WorkflowTimerKind.ChildGroup, groupDeadline, e.GroupId),
        ],

        WorkflowEvent.ChildrenAwaited => ClearsWaits,

        // A group that has resolved stops being worth waking for.
        WorkflowEvent.ChildGroupFinalized e =>
            [new WorkflowDeadlineChange.Disarm(WorkflowTimerKind.ChildGroup, e.GroupId)],

        _ => None,
    };
}
