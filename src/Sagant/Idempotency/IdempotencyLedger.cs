using Sagant.Effects;

namespace Sagant.Idempotency;

/// <summary>
/// Bounded, immutable dedup ledger for caller-supplied idempotency keys — a fixed-capacity,
/// copy-on-write buffer of (key, reply) pairs, oldest evicted first once full. Persisted as part of
/// <c>WorkflowRuntimeState</c> so a repeat key replays the cached <see cref="Reply"/> without
/// re-invoking the command handler.
/// Deliberately has no runtime dependency of its own — a plain, immutable value with `with`-style
/// updates via <see cref="Record"/>, unit-testable with no execution-engine infrastructure at all,
/// fitting the same "runtime-agnostic core" layering as the rest of <c>Sagant</c>.
/// Bounded: a workflow instance only receives so many distinct retriable commands in its lifetime,
/// so a fixed window is sufficient, and a genuinely-late retry past that window simply re-executes
/// the handler — an accepted tradeoff of keeping the ledger's size bounded.
///
/// Backed by two arrays, oldest-first, holding exactly what is currently recorded — <see cref="Record"/>
/// finds the touched key with a plain indexed scan and writes what the result actually holds. At the
/// capacities this ledger runs at (a handful to a few dozen entries), that scan-and-copy costs less
/// than a dictionary's own hashing and rehashing. Re-recording an already-present key shares its
/// existing <see cref="Keys"/> array outright — its position never changes, so only
/// <see cref="Replies"/> needs a fresh copy.
/// </summary>
public sealed class IdempotencyLedger
{
    public int Capacity { get; }

    /// <summary>How many entries are currently recorded, at most <see cref="Capacity"/>.</summary>
    public int Count { get; }

    /// <summary>Oldest first, for indices <c>[0, Count)</c>.</summary>
    public IReadOnlyList<string> Keys { get; }

    /// <summary><see cref="Keys"/>[i]'s cached reply is this list's same index, for i in
    /// <c>[0, Count)</c>.</summary>
    public IReadOnlyList<Reply> Replies { get; }

    /// <summary><see cref="Empty"/> and <see cref="Record"/> are the intended way to construct a
    /// sensible instance — this constructor exists so a serializer pairing public properties with a
    /// matching constructor can rehydrate this type directly.</summary>
    public IdempotencyLedger(int capacity, IReadOnlyList<string> keys, IReadOnlyList<Reply> replies, int count)
    {
        Capacity = capacity;
        Keys = keys;
        Replies = replies;
        Count = count;
    }

    public static IdempotencyLedger Empty(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "Idempotency ledger capacity must be positive — a non-positive capacity would silently disable dedup.");
        }

        return new IdempotencyLedger(capacity, Array.Empty<string>(), Array.Empty<Reply>(), 0);
    }

    public bool TryGetCachedReply(string key, out Reply reply)
    {
        for (var i = 0; i < Count; i++)
        {
            if (Keys[i] == key)
            {
                reply = Replies[i];
                return true;
            }
        }

        reply = default!;
        return false;
    }

    /// <summary>
    /// Returns a new ledger with <paramref name="key"/> recorded against <paramref name="reply"/>.
    /// Re-recording an already-present key updates its reply and keeps its original position —
    /// only a genuinely new key can trigger eviction.
    /// </summary>
    public IdempotencyLedger Record(string key, Reply reply)
    {
        var existingIndex = -1;
        for (var i = 0; i < Count; i++)
        {
            if (Keys[i] == key)
            {
                existingIndex = i;
                break;
            }
        }

        if (existingIndex >= 0)
        {
            var updatedReplies = new Reply[Count];
            for (var i = 0; i < Count; i++)
            {
                updatedReplies[i] = Replies[i];
            }

            updatedReplies[existingIndex] = reply;
            return new IdempotencyLedger(Capacity, Keys, updatedReplies, Count);
        }

        var dropOldest = Count == Capacity;
        var newKeys = new string[Math.Min(Count + 1, Capacity)];
        var newReplies = new Reply[newKeys.Length];

        var writeIndex = 0;
        for (var i = dropOldest ? 1 : 0; i < Count; i++)
        {
            newKeys[writeIndex] = Keys[i];
            newReplies[writeIndex] = Replies[i];
            writeIndex++;
        }

        newKeys[writeIndex] = key;
        newReplies[writeIndex] = reply;
        writeIndex++;

        return new IdempotencyLedger(Capacity, newKeys, newReplies, writeIndex);
    }
}
