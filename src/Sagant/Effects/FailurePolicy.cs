namespace Sagant.Effects;

/// <summary>
/// When the group's outcome is considered known. Independent of
/// <see cref="RemainingChildrenPolicy"/> — this only answers "should I keep waiting," not "what
/// happens to the stragglers."
/// </summary>
public enum FailurePolicy
{
    /// <summary>The group finalizes the moment a failure makes the configured
    /// <see cref="CompletionPolicy"/> impossible to satisfy — doesn't wait for remaining members.</summary>
    FailFast,
    /// <summary>The group only finalizes once every member has reached a terminal status.</summary>
    WaitForAll,
}
