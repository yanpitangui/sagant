namespace Sagant.Protocol;

/// <summary>
/// Bounded, immutable dedup ledger for transport-level redelivery — a fixed-capacity, copy-on-write
/// buffer of (producer id, highest applied sequence number) pairs, oldest evicted first once full.
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
/// redelivers again under that identity, so eviction here never lets a genuine duplicate through; it
/// only forgets a producer with nothing left to ask.
/// Re-recording an existing producer id moves it to the freshest position: every message from a
/// still-active producer touches its own entry, so what an entry's position tracks is when it was
/// last used — true LRU — which is what keeps an actively-sending producer from being evicted while
/// genuinely-stale producers still age out. (<see cref="Idempotency.IdempotencyLedger.Record"/> keeps
/// a re-recorded key's original position — see that method's own doc comment for why the two ledgers
/// differ there.)
///
/// Backed by two arrays, oldest-first, holding exactly what is currently recorded — <see cref="Record"/>
/// finds the touched producer with a plain indexed scan and writes a new pair of arrays sized to what
/// the result actually holds. At the capacities this ledger runs at (a handful to a few dozen
/// entries), that scan-and-copy costs less than a dictionary's own hashing and rehashing.
/// </summary>
public sealed class SeqNrLedger
{
    public int Capacity { get; }

    /// <summary>How many entries are currently recorded, at most <see cref="Capacity"/>.</summary>
    public int Count { get; }

    /// <summary>Oldest first, for indices <c>[0, Count)</c>.</summary>
    public IReadOnlyList<string> ProducerIds { get; }

    /// <summary><see cref="ProducerIds"/>[i]'s recorded sequence number is this list's same index,
    /// for i in <c>[0, Count)</c>.</summary>
    public IReadOnlyList<long> SeqNrs { get; }

    /// <summary><see cref="Empty"/> and <see cref="Record"/> are the intended way to construct a
    /// sensible instance — this constructor exists so a serializer pairing public properties with a
    /// matching constructor can rehydrate this type directly.</summary>
    public SeqNrLedger(int capacity, IReadOnlyList<string> producerIds, IReadOnlyList<long> seqNrs, int count)
    {
        Capacity = capacity;
        ProducerIds = producerIds;
        SeqNrs = seqNrs;
        Count = count;
    }

    public static SeqNrLedger Empty(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "SeqNr dedup ledger capacity must be positive — a non-positive capacity would silently disable dedup.");
        }

        return new SeqNrLedger(capacity, Array.Empty<string>(), Array.Empty<long>(), 0);
    }

    public bool TryGetHighest(string producerId, out long seqNr)
    {
        for (var i = 0; i < Count; i++)
        {
            if (ProducerIds[i] == producerId)
            {
                seqNr = SeqNrs[i];
                return true;
            }
        }

        seqNr = default;
        return false;
    }

    /// <summary>
    /// Returns a new ledger with <paramref name="producerId"/> recorded against <paramref name="seqNr"/>,
    /// moved to the freshest position regardless of whether it was already present — see this type's
    /// own doc comment for why that differs from <see cref="Idempotency.IdempotencyLedger.Record"/>.
    /// </summary>
    public SeqNrLedger Record(string producerId, long seqNr)
    {
        var existingIndex = -1;
        for (var i = 0; i < Count; i++)
        {
            if (ProducerIds[i] == producerId)
            {
                existingIndex = i;
                break;
            }
        }

        var dropOldest = existingIndex < 0 && Count == Capacity;
        var newIds = new string[Math.Min(Count + 1, Capacity)];
        var newSeqNrs = new long[newIds.Length];

        var writeIndex = 0;
        for (var i = dropOldest ? 1 : 0; i < Count; i++)
        {
            if (i == existingIndex)
            {
                continue;
            }

            newIds[writeIndex] = ProducerIds[i];
            newSeqNrs[writeIndex] = SeqNrs[i];
            writeIndex++;
        }

        newIds[writeIndex] = producerId;
        newSeqNrs[writeIndex] = seqNr;
        writeIndex++;

        return new SeqNrLedger(Capacity, newIds, newSeqNrs, writeIndex);
    }
}
