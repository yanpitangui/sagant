using Sagant.Protocol;

namespace Sagant.Tests.Protocol;

public class SeqNrLedgerTests
{
    [Fact]
    public void TryGetHighest_UnknownProducer_ReturnsFalse()
    {
        var ledger = SeqNrLedger.Empty(capacity: 3);
        Assert.False(ledger.TryGetHighest("producer-1", out _));
    }

    [Fact]
    public void Record_ThenTryGetHighest_ReturnsStoredSeqNr()
    {
        var ledger = SeqNrLedger.Empty(capacity: 3).Record("producer-1", 5);

        Assert.True(ledger.TryGetHighest("producer-1", out var seqNr));
        Assert.Equal(5, seqNr);
    }

    [Fact]
    public void Record_AtCapacity_EvictsLeastRecentlyUsedProducer()
    {
        var ledger = SeqNrLedger.Empty(capacity: 2)
            .Record("producer-1", 1)
            .Record("producer-2", 1)
            .Record("producer-3", 1);

        Assert.False(ledger.TryGetHighest("producer-1", out _)); // evicted, least recently used
        Assert.True(ledger.TryGetHighest("producer-2", out _));
        Assert.True(ledger.TryGetHighest("producer-3", out _));
    }

    [Fact]
    public void Record_ExistingProducer_MovesItToFreshestPosition()
    {
        // Unlike IdempotencyLedger, re-recording an existing key must bump its position — an
        // actively-sending producer touches its own entry on every message, so eviction should track
        // recency of use, not just first-seen order.
        var ledger = SeqNrLedger.Empty(capacity: 2)
            .Record("producer-1", 1)
            .Record("producer-2", 1)
            .Record("producer-1", 2) // producer-1 touched again — now freshest, producer-2 is oldest
            .Record("producer-3", 1); // must evict producer-2, not producer-1

        Assert.True(ledger.TryGetHighest("producer-1", out var seqNr));
        Assert.Equal(2, seqNr);
        Assert.False(ledger.TryGetHighest("producer-2", out _)); // evicted
        Assert.True(ledger.TryGetHighest("producer-3", out _));
    }

    [Fact]
    public void Empty_NonPositiveCapacity_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => SeqNrLedger.Empty(capacity: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => SeqNrLedger.Empty(capacity: -1));
    }
}
