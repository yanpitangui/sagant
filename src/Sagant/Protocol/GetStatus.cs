namespace Sagant.Protocol;

/// <summary>
/// Engine-level lifecycle status query — Running/Paused/Suspended/Ended/Deleted/Terminated. Public
/// (unlike <c>GetDiagnostics</c>, which stays internal/test-only): this is engine-owned
/// information, not business-specific, so unlike <c>TState</c> the engine can expose it directly
/// without requiring a custom command handler on every workflow.
/// </summary>
public sealed record GetStatus;

/// <summary>
/// What an instance answers <see cref="GetStatus"/> with.
///
/// A named type rather than the enum on its own, because this reply crosses nodes: a bare enum goes on
/// the wire as the number behind it and arrives as one, so the asker gets an integer where it expected
/// a status. Carrying it in a record gives the value a type the other side can rebuild it from.
///
/// Held inside the engine — <see cref="Sagant.Clients.IWorkflowHandle.GetStatus"/> unwraps it, so
/// application code still sees a <see cref="WorkflowStatus"/>.
/// </summary>
public sealed record WorkflowStatusReply(WorkflowStatus Status);
