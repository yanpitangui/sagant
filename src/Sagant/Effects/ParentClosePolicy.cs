namespace Sagant.Effects;

/// <summary>
/// What happens to a child workflow when its parent reaches a terminal status, set per-child at
/// <c>Child&lt;TWorkflow&gt;</c> call time, matching Temporal's own per-child-start
/// <c>ParentClosePolicy</c> model. No cooperative <c>Cancel</c> value — this project's
/// <c>Terminate</c> is already unconditional, bypassing business code entirely (same as an
/// operator-invoked Terminate). A cooperative cancel primitive is a distinct mechanism, out of
/// this enum's scope.
/// </summary>
public enum ParentClosePolicy
{
    /// <summary>The child keeps running independently of its parent's own lifecycle — the
    /// default.</summary>
    Abandon,
    /// <summary>The child is sent <c>Terminate</c> when the parent reaches any terminal status.</summary>
    Terminate,
}
