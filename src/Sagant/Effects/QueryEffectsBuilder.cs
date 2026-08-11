namespace Sagant.Effects;

/// <summary>
/// Builder for query-handler effects. Obtain via <see cref="Workflow{TState}.QueryEffects"/>.
/// Mirrors <see cref="EffectsBuilder{TState}"/>'s reply surface and stops there — the state and
/// transition methods a command handler has are absent, which is what makes a query read-only (see
/// <see cref="QueryEffect"/>).
///
/// Stateless, so a single shared <see cref="Instance"/> serves every call site: a query effect is
/// fully described by its terminal <see cref="Reply{TReply}(TReply)"/> call, with nothing accumulated
/// beforehand.
/// </summary>
public sealed class QueryEffectsBuilder
{
    internal static readonly QueryEffectsBuilder Instance = new();

    private QueryEffectsBuilder()
    {
    }

    public QueryEffect Reply<TReply>(TReply value) =>
        new(new global::Sagant.Effects.Reply.ReplyValue(value, null));

    public QueryEffect Reply<TReply>(TReply value, object metadata) =>
        new(new global::Sagant.Effects.Reply.ReplyValue(value, metadata));

    public QueryEffect Error(string message) =>
        new(new global::Sagant.Effects.Reply.ErrorValue(message));
}
