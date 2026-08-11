namespace Sagant.Effects;

/// <summary>
/// What "the group succeeded" means. Deliberately small — <c>AnySuccessful</c>/<c>FirstCompleted</c>
/// (race semantics) aren't included; nothing in this project's own worked examples motivates them
/// yet, and they're easy to add later without touching either of these.
/// </summary>
public enum CompletionPolicy
{
    /// <summary>Every member must reach <see cref="Protocol.ChildStatus.Completed"/>.</summary>
    AllSuccessful,
    /// <summary>Every member must reach *a* terminal status, success or not — the resume step
    /// inspects individual outcomes itself.</summary>
    AllCompleted,
}
