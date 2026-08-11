using Sagant.Effects;

namespace Sagant.Idempotency;

/// <summary>
/// Bounded, immutable dedup ledger for caller-supplied idempotency keys — a fixed-size ring buffer
/// of (key, reply) pairs, oldest evicted first once at capacity. Persisted as part of
/// <c>WorkflowRuntimeState</c> so a repeat key replays the cached <see cref="Reply"/> without
/// re-invoking the command handler.
/// Deliberately has no runtime dependency of its own — a plain, immutable value with `with`-style
/// updates via <see cref="Record"/>, unit-testable with no execution-engine infrastructure at all,
/// fitting the same "runtime-agnostic core" layering as the rest of <c>Sagant</c>.
/// Bounded: a workflow instance only receives so many distinct retriable commands in its lifetime,
/// so a fixed window is sufficient, and a genuinely-late retry past that window simply re-executes
/// the handler — an accepted tradeoff of keeping the ledger's size bounded.
/// </summary>
public sealed class IdempotencyLedger
{
    public int Capacity { get; }

    /// <summary>Oldest first.</summary>
    public IReadOnlyList<string> Order { get; }

    public IReadOnlyDictionary<string, Reply> Entries { get; }

    /// <summary><see cref="Empty"/> and <see cref="Record"/> are the intended way to construct a
    /// sensible instance — this constructor exists so a serializer pairing public properties with a
    /// matching constructor can rehydrate this type directly.</summary>
    public IdempotencyLedger(int capacity, IReadOnlyList<string> order, IReadOnlyDictionary<string, Reply> entries)
    {
        Capacity = capacity;
        Order = order;
        Entries = entries;
    }

    public static IdempotencyLedger Empty(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "Idempotency ledger capacity must be positive — a non-positive capacity would silently disable dedup.");
        }

        return new IdempotencyLedger(capacity, Array.Empty<string>(), new Dictionary<string, Reply>());
    }

    public bool TryGetCachedReply(string key, out Reply reply) => Entries.TryGetValue(key, out reply!);

    /// <summary>
    /// Returns a new ledger with <paramref name="key"/> recorded against <paramref name="reply"/>.
    /// Re-recording an already-present key updates its reply and keeps its original position —
    /// only a genuinely new key can trigger eviction.
    /// </summary>
    public IdempotencyLedger Record(string key, Reply reply)
    {
        if (Entries.ContainsKey(key))
        {
            var updatedEntries = new Dictionary<string, Reply>(Entries) { [key] = reply };
            return new IdempotencyLedger(Capacity, Order, updatedEntries);
        }

        var newOrder = Order.Append(key).ToList();
        var newEntries = new Dictionary<string, Reply>(Entries) { [key] = reply };

        if (newOrder.Count > Capacity)
        {
            var oldest = newOrder[0];
            newOrder.RemoveAt(0);
            newEntries.Remove(oldest);
        }

        return new IdempotencyLedger(Capacity, newOrder, newEntries);
    }
}
