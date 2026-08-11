namespace Sagant.Protocol;

/// <summary>
/// A child workflow relationship's lifecycle state, from its parent's point of view.
/// <see cref="Pending"/> covers "not yet terminal, nothing decided about it" uniformly — no
/// separate "Started"/"confirmed sent" state, since child-start delivery is safely redeliverable
/// regardless of prior delivery attempts, so no state is needed to record that one was made.
/// <see cref="TerminationRequested"/> is the one other non-terminal value: set durably the moment
/// the runtime decides this child should stop (fail-fast + <c>RemainingChildrenPolicy.Terminate</c>,
/// or <c>ParentClosePolicy.Terminate</c>) — recovery redelivers the actual <c>Terminate</c> send to
/// any relationship still in this status, exactly like it redelivers child-start for
/// <see cref="Pending"/>. A group can legitimately finalize with a member still
/// <see cref="Pending"/> or <see cref="TerminationRequested"/> — the parent's resume step never
/// waits for the straggler to actually confirm it stopped.
/// </summary>
public enum ChildStatus
{
    Pending,
    TerminationRequested,
    Completed,
    Failed,
    Cancelled,
    Terminated,
}
