namespace Sagant.Effects;

/// <summary>
/// Returned by <see cref="EffectsBuilder{TState}"/>'s transition-producing methods
/// (<c>Pause</c>/<c>TransitionTo</c>/<c>End</c>/<c>Delete</c>). Usable directly as a terminal
/// <see cref="CommandEffect{TState}"/> (implicit conversion, reply = <c>NoReply</c>), or continue
/// with <see cref="ThenReply{TReply}(TReply)"/> to also reply to the caller in the same effect.
/// </summary>
public sealed record TransitionalEffect<TState>(PersistenceEffect<TState> Persistence, Transition Transition)
{
    public CommandEffect<TState> ThenReply<TReply>(TReply value) =>
        new(Persistence, Transition, new global::Sagant.Effects.Reply.ReplyValue(value, null));

    public CommandEffect<TState> ThenReply<TReply>(TReply value, object metadata) =>
        new(Persistence, Transition, new global::Sagant.Effects.Reply.ReplyValue(value, metadata));

    public static implicit operator CommandEffect<TState>(TransitionalEffect<TState> transitional) =>
        new(transitional.Persistence, transitional.Transition, global::Sagant.Effects.Reply.NoReply.Instance);
}
