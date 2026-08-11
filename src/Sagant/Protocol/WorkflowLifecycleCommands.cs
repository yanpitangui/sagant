namespace Sagant.Protocol;

/// <summary>
/// Operator-level admin controls, handled directly by <see cref="WorkflowEntityActor{TWorkflow, TState}"/>
/// for every workflow type uniformly — distinct from business-level <c>Effects.Pause()</c>.
/// </summary>
public sealed record Suspend(string? Reason = null);

/// <summary>
/// Resumes a <see cref="Suspend"/>ed workflow. If a step was in flight when suspended, it is
/// re-executed from scratch (a fresh epoch — any late result from the pre-suspend attempt was
/// already discarded).
/// </summary>
public sealed record Resume;

/// <summary>Permanently stops the workflow. Idempotent — terminating an already-terminal workflow succeeds.</summary>
public sealed record Terminate(string? Reason = null);

/// <summary>
/// Asks a workflow to stop, letting it unwind first. Routes to the step named by
/// <see cref="Settings.WorkflowSettings.CancellationStepName"/>, which runs like any other step —
/// its own timeout, its own retry budget — and decides the run's final outcome. With no cancellation
/// step configured there is nothing to unwind, so the run finishes immediately.
///
/// Either way the run reports <see cref="WorkflowOutcome.Cancelled"/>, not
/// <see cref="WorkflowOutcome.Terminated"/>: the two say different things about what was asked for,
/// and that distinction is worth keeping even when the effect happened to be the same.
///
/// Reach for <see cref="Terminate"/> instead when the workflow must stop regardless of whether it
/// can unwind cleanly.
/// </summary>
public sealed record Cancel(string? Reason = null);

/// <summary>
/// Force-stops the workflow if still active, then physically purges everything persisted for it —
/// snapshots. <see cref="Terminate"/> keeps every persisted event around so
/// <c>GetStatus</c>/diagnostics keep working against a terminated instance; this goes further and
/// erases it. Works at any status, including an already-terminal one (<see cref="Terminate"/>d,
/// ended, or previously deleted via the business-level <c>Transition.DeleteTransition</c>) — cleaning
/// up an already-finished workflow's leftover data is the primary use case this command exists for.
/// Cascades to every child this instance owns under <c>ParentClosePolicy.Terminate</c>, sending each
/// of them <see cref="Delete"/> in turn: deleting a parent purges its owned subtree. After a
/// successful delete the workflow id is fully reusable — the next message sent to it starts a
/// genuinely new instance with no memory of the old one.
/// </summary>
public sealed record Delete(string? Reason = null);
