namespace Sagant.Protocol;

/// <summary>
/// Bounded, immutable dedup ledger for transport-level redelivery — a fixed-size ring buffer of
/// (producer id, highest applied sequence number) pairs, oldest evicted first once at capacity.
/// Persisted as part of <see cref="WorkflowRuntimeState{TState}"/> so a redelivered seqNr at or below
/// the recorded value is recognized as a genuine duplicate without re-invoking the handler. Which
/// runtime, if any, actually produces redelivery is not this core layer's concern — see the runtime
/// driver's own docs for its producer/consumer wiring.
/// Deliberately has no runtime dependency of its own — a plain, immutable value with `with`-style
/// updates via <see cref="Record"/>, unit-testable with no execution-engine infrastructure at all,
/// fitting the same "runtime-agnostic core" layering as the rest of <c>Sagant</c>.
/// A producer id is minted fresh per producer incarnation (one per sending process's lifetime), so a
/// long-lived workflow instance would otherwise accumulate one permanently-stale entry per redeploy
/// of the sending node, forever. Bounding this the same way as <see cref="Idempotency.IdempotencyLedger"/>
/// closes that off: an evicted producer id is safe to forget outright — a producer that's gone never
/// redelivers again under that identity, so eviction here never lets a genuine duplicate through,
/// only forgets a producer that could no longer ask anyway.
/// Unlike <see cref="Idempotency.IdempotencyLedger"/>, re-recording an existing producer id DOES move
/// it to the freshest position: every message from a still-active producer touches its own entry, so
/// true LRU (recency of use, not just first-seen order) is what keeps an actively-sending producer
/// from being evicted out from under itself while genuinely-stale producers still age out.
/// </summary>
public sealed class SeqNrLedger
{
    public int Capacity { get; }

    /// <summary>Oldest first.</summary>
    public IReadOnlyList<string> Order { get; }

    public IReadOnlyDictionary<string, long> Entries { get; }

    /// <summary><see cref="Empty"/> and <see cref="Record"/> are the intended way to construct a
    /// sensible instance — this constructor exists so a serializer pairing public properties with a
    /// matching constructor can rehydrate this type directly.</summary>
    public SeqNrLedger(int capacity, IReadOnlyList<string> order, IReadOnlyDictionary<string, long> entries)
    {
        Capacity = capacity;
        Order = order;
        Entries = entries;
    }

    public static SeqNrLedger Empty(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "SeqNr dedup ledger capacity must be positive — a non-positive capacity would silently disable dedup.");
        }

        return new SeqNrLedger(capacity, Array.Empty<string>(), new Dictionary<string, long>());
    }

    public bool TryGetHighest(string producerId, out long seqNr) => Entries.TryGetValue(producerId, out seqNr);

    /// <summary>
    /// Returns a new ledger with <paramref name="producerId"/> recorded against <paramref name="seqNr"/>,
    /// moved to the freshest position regardless of whether it was already present — see this type's
    /// own doc comment for why that differs from <see cref="Idempotency.IdempotencyLedger.Record"/>.
    /// </summary>
    public SeqNrLedger Record(string producerId, long seqNr)
    {
        var newOrder = Order.Where(id => id != producerId).Append(producerId).ToList();
        var newEntries = new Dictionary<string, long>(Entries) { [producerId] = seqNr };

        if (newOrder.Count > Capacity)
        {
            var oldest = newOrder[0];
            newOrder.RemoveAt(0);
            newEntries.Remove(oldest);
        }

        return new SeqNrLedger(Capacity, newOrder, newEntries);
    }
}
