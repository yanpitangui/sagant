namespace Sagant.Protocol;

/// <summary>
/// Wraps an <see cref="EffectsBuilder{TState}.Error"/> message when a command handler's effect
/// carries an error reply — the caller's <c>Ask</c> fails with this, carrying that message.
/// </summary>
public sealed class WorkflowCommandException : Exception
{
    public WorkflowCommandException(string message) : base(message)
    {
    }
}
