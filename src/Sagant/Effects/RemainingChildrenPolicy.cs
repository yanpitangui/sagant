namespace Sagant.Effects;

/// <summary>
/// What happens to still-running members once <see cref="FailurePolicy.FailFast"/> finalizes a
/// group's outcome. Independent of <see cref="FailurePolicy"/> — "we know the outcome" and "stop the
/// stragglers" are separate decisions a workflow author might want to make differently.
/// </summary>
public enum RemainingChildrenPolicy
{
    /// <summary>Send <c>Terminate</c> to every non-terminal member — fire-and-forget, the resume
    /// step doesn't wait for acknowledgement.</summary>
    Terminate,
    /// <summary>Leave still-running members alone; they run to their own natural conclusion
    /// independently of this group's already-decided outcome.</summary>
    Continue,
}
