namespace Sagant.Effects;

/// <summary>
/// The effect a step handler produces: optionally a new state and a transition. No reply — steps
/// are internal orchestration, never a direct response to an external caller.
/// </summary>
public sealed record StepEffect<TState>(PersistenceEffect<TState> Persistence, Transition Transition);
