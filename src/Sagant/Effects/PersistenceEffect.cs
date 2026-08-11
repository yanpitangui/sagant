namespace Sagant.Effects;

/// <summary>
/// Whether an effect writes new state. Coarse by design: an effect either replaces the whole state
/// or leaves it alone.
/// </summary>
public abstract record PersistenceEffect<TState>
{
    private PersistenceEffect()
    {
    }

    public sealed record UpdateState(TState NewState) : PersistenceEffect<TState>;

    public sealed record NoPersistence : PersistenceEffect<TState>
    {
        public static readonly NoPersistence Instance = new();
    }
}
