using Sagant.Protocol;

namespace Sagant.Execution;

/// <summary>Which of a workflow instance's deadlines a timer decision refers to.</summary>
public enum WorkflowTimerKind
{
    /// <summary>The workflow-wide deadline (<see cref="WorkflowRuntimeState{TState}.WorkflowDeadline"/>).</summary>
    Workflow,

    /// <summary>The pause deadline (<see cref="WorkflowRuntimeState{TState}.PauseDeadline"/>).</summary>
    Pause,

    /// <summary>
    /// The deadline on a held instance (<see cref="WorkflowRuntimeState{TState}.HoldDeadline"/>) —
    /// how long an operator hold or a parked failure waits before the workflow decides for itself.
    /// One per instance, since an instance is held once or not at all.
    /// </summary>
    Hold,

    /// <summary>
    /// The deadline on one awaited child group (<see cref="ChildGroupState.Deadline"/>) — how long a
    /// parent waits for children that may never finish. An instance can await several groups, so a
    /// key for this kind carries the group id in its discriminator.
    /// </summary>
    ChildGroup,
}

/// <summary>
/// One thing a runtime driver must do as a consequence of a transition, decided by
/// <see cref="WorkflowTransitionPlanner"/> and carried out by the driver.
///
/// Every decision reaches a driver inside a <see cref="TransitionPlan{TState}"/>, alongside the
/// envelope the driver has not written yet — so there is no shape in which a driver can act on one
/// before the write. That makes guarantee D1 a property of the type itself.
///
/// Closed hierarchy (all cases nested sealed records) so a driver pattern-matches exhaustively and
/// the compiler tells it when a new decision appears. A driver that genuinely cannot perform a
/// decision — the in-memory test harness has no children to start and nothing to purge — ignores it;
/// what it must not do is invent one the planner didn't emit.
/// </summary>
public abstract record WorkflowDecision
{
    private WorkflowDecision()
    {
    }

    /// <summary>The instance's status changed; report it. Emitted only for a genuine change, so a
    /// driver does not need to compare against the previous status itself.</summary>
    public sealed record RecordStatusChange(WorkflowStatus Status) : WorkflowDecision
    {
        private static readonly RecordStatusChange[] ByStatus =
        [
            new(WorkflowStatus.NotStarted),
            new(WorkflowStatus.Running),
            new(WorkflowStatus.Paused),
            new(WorkflowStatus.Suspended),
            new(WorkflowStatus.Finished),
            new(WorkflowStatus.Deleted),
        ];

        /// <summary>
        /// The decision reporting <paramref name="status"/>, held as one value per status.
        ///
        /// A decision carrying only an enum is the same value every time it is made, and a plan makes
        /// one on most transitions — see <see cref="CancelTimer.For"/>, which shares its reasoning.
        /// </summary>
        public static RecordStatusChange For(WorkflowStatus status) => ByStatus[(int)status];
    }

    /// <summary>The run finished; report how. Separate from
    /// <see cref="RecordStatusChange"/> because the outcome is the dimension worth reporting here —
    /// every finished run shares the same status, so that alone says nothing.</summary>
    public sealed record RecordOutcome(WorkflowOutcome Outcome) : WorkflowDecision;

    /// <summary>The instance left <see cref="WorkflowStatus.Paused"/>; report how long it waited.
    /// Emitted alongside <see cref="RecordStatusChange"/> whenever a transition's previous status was
    /// <c>Paused</c>, whatever route led out of it — a business-command step transition, a pause
    /// timeout, ending, deleting, restarting, or an operator <c>Terminate</c>.</summary>
    public sealed record RecordPauseDuration(TimeSpan Duration) : WorkflowDecision;

    /// <summary>Arm a live timer for a deadline the envelope now carries.</summary>
    /// <param name="Discriminator">Which deadline of <paramref name="Kind"/>, for a kind an instance
    /// holds several of. The group id for <see cref="WorkflowTimerKind.ChildGroup"/>, <c>null</c>
    /// otherwise.</param>
    public sealed record ArmTimer(
        WorkflowTimerKind Kind, DateTimeOffset Deadline, string? Discriminator = null) : WorkflowDecision;

    /// <summary>Cancel a live timer whose deadline has stopped applying.</summary>
    /// <param name="Discriminator">As <see cref="ArmTimer"/>. <c>null</c> for a kind an instance holds
    /// one of, which cancels that one.</param>
    public sealed record CancelTimer(
        WorkflowTimerKind Kind, string? Discriminator = null) : WorkflowDecision
    {
        private static readonly CancelTimer[] ByKind =
        [
            new(WorkflowTimerKind.Workflow),
            new(WorkflowTimerKind.Pause),
            new(WorkflowTimerKind.Hold),
            new(WorkflowTimerKind.ChildGroup),
        ];

        /// <summary>
        /// The decision cancelling the single timer of <paramref name="kind"/> an instance holds, held
        /// as one value per kind.
        ///
        /// Every settled transition emits two of these — the pause timer and the hold timer are
        /// cancelled whenever the status they belong to is left — so they are the decisions a plan
        /// makes most often, and each carries an enum and nothing else. A kind an instance holds
        /// several of names which one through <see cref="CancelTimer(WorkflowTimerKind, string)"/>,
        /// where the discriminator makes each value its own.
        /// </summary>
        public static CancelTimer For(WorkflowTimerKind kind) => ByKind[(int)kind];
    }

    /// <summary>Begin executing <see cref="WorkflowRuntimeState{TState}.CurrentStepName"/>.</summary>
    public sealed record StartStep : WorkflowDecision
    {
        public static readonly StartStep Instance = new();
    }

    /// <summary>The step chain has settled, so commands deferred while it ran may now be dispatched
    /// (guarantee C2's release side).</summary>
    public sealed record ReleaseDeferredCommands : WorkflowDecision
    {
        public static readonly ReleaseDeferredCommands Instance = new();
    }

    /// <summary>Send this relationship's child-start command. Emitted for a relationship the
    /// envelope has just recorded as <see cref="ChildStatus.Pending"/> (guarantee D7).</summary>
    public sealed record StartChild(ChildWorkflowRelationship Relationship) : WorkflowDecision;

    /// <summary>Terminate a child, per <c>ParentClosePolicy</c> or <c>RemainingChildrenPolicy</c>.</summary>
    public sealed record TerminateChild(ChildWorkflowRelationship Relationship) : WorkflowDecision;

    /// <summary>
    /// Cancel a child, cascading a parent's own graceful stop. Distinct from
    /// <see cref="TerminateChild"/> so a child gets the same chance to unwind that its parent took —
    /// cascading a cancel as a terminate would compensate the parent and abandon the children, which
    /// is the worse half of both.
    /// </summary>
    public sealed record CancelChild(ChildWorkflowRelationship Relationship, string? Reason) : WorkflowDecision;

    /// <summary>Delete a child, cascading a parent's own delete through its owned subtree.</summary>
    public sealed record DeleteChild(ChildWorkflowRelationship Relationship) : WorkflowDecision;

    /// <summary>
    /// Report this instance's fate to the parent waiting on it. <paramref name="Outcome"/> is
    /// <c>null</c> when the instance was deleted without ever finishing — the parent learns the child
    /// is gone, which is all there is to learn.
    /// </summary>
    public sealed record NotifyParent(ChildWorkflowRelationship Relationship, WorkflowOutcome? Outcome) : WorkflowDecision;

    /// <summary>Physically purge everything persisted for this instance, then stop.</summary>
    public sealed record PurgeAndStop : WorkflowDecision
    {
        public static readonly PurgeAndStop Instance = new();
    }

    /// <summary>
    /// Reclaim the history recorded before this restart, keeping the instance alive.
    ///
    /// A driver does this by recording the fresh envelope in a form that stands on its own, then
    /// releasing what came before it. Purely reclamation: the envelope this restart produced is
    /// already durable by the time this runs, so a driver that never gets to it, or fails at it,
    /// leaves an instance that recovers to exactly the same state and simply keeps its old rows.
    /// A driver with no history to reclaim ignores it.
    /// </summary>
    public sealed record ReclaimHistory : WorkflowDecision
    {
        public static readonly ReclaimHistory Instance = new();
    }

    /// <summary>The instance reached a terminal status; release anyone awaiting its completion.</summary>
    public sealed record NotifyCompletionWatchers : WorkflowDecision
    {
        public static readonly NotifyCompletionWatchers Instance = new();
    }
}
