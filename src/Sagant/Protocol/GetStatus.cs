namespace Sagant.Protocol;

/// <summary>
/// Engine-level lifecycle status query — Running/Paused/Suspended/Ended/Deleted/Terminated. Public
/// (unlike <c>GetDiagnostics</c>, which stays internal/test-only): this is engine-owned
/// information, not business-specific, so unlike <c>TState</c> the engine can expose it directly
/// without requiring a custom command handler on every workflow.
/// </summary>
public sealed record GetStatus;
