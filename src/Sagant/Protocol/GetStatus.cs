namespace Sagant.Protocol;

/// <summary>
/// Engine-level lifecycle status query — Running/Paused/Suspended/Ended/Deleted/Terminated. Public:
/// this is engine-owned, workflow-agnostic information (<c>GetDiagnostics</c> stays
/// internal/test-only), so the engine exposes it directly with no custom command handler needed on
/// every workflow. Exposing <c>TState</c> itself needs exactly that kind of handler, since only the
/// workflow author knows its shape.
/// </summary>
public sealed record GetStatus;

/// <summary>
/// What an instance answers <see cref="GetStatus"/> with.
///
/// A named type carries this: this reply crosses nodes, and a bare enum goes on the wire as the
/// number behind it and arrives as one, so the asker gets an integer where it expected a status.
/// Carrying it in a record gives the value a type the other side can rebuild it from.
///
/// Held inside the engine — <see cref="Sagant.Clients.IWorkflowHandle.GetStatus"/> unwraps it, so
/// application code still sees a <see cref="WorkflowStatus"/>.
/// </summary>
public sealed record WorkflowStatusReply(WorkflowStatus Status);
