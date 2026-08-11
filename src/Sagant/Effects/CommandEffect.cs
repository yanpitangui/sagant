namespace Sagant.Effects;

/// <summary>
/// The effect a command handler (a public method on <see cref="Workflow{TState}"/> that returns
/// this type) produces: optionally a new state, a transition, and optionally a reply to the caller.
/// </summary>
public sealed record CommandEffect<TState>(PersistenceEffect<TState> Persistence, Transition Transition, Reply Reply);
