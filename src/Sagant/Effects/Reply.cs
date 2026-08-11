namespace Sagant.Effects;

/// <summary>
/// What a command handler sends back to the caller. Only <see cref="EffectsBuilder{TState}"/>
/// (command handlers) produces these — step handlers cannot reply, enforced by
/// <see cref="StepEffect{TState}"/> simply having no <c>Reply</c> member.
/// </summary>
public abstract record Reply
{
    private Reply()
    {
    }

    public sealed record ReplyValue(object? Value, object? Metadata) : Reply;

    public sealed record ErrorValue(string Message) : Reply;

    public sealed record NoReply : Reply
    {
        public static readonly NoReply Instance = new();
    }
}
