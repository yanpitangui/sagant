using Sagant.Effects;
using Sagant.Idempotency;

namespace Sagant.Tests.Idempotency;

public class IdempotencyLedgerTests
{
    [Fact]
    public void TryGetCachedReply_UnknownKey_ReturnsFalse()
    {
        var ledger = IdempotencyLedger.Empty(capacity: 3);
        Assert.False(ledger.TryGetCachedReply("missing", out _));
    }

    [Fact]
    public void Record_ThenTryGetCachedReply_ReturnsStoredReply()
    {
        var ledger = IdempotencyLedger.Empty(capacity: 3);
        var reply = new Reply.ReplyValue("value-1", null);

        var recorded = ledger.Record("key-1", reply);

        Assert.True(recorded.TryGetCachedReply("key-1", out var cached));
        Assert.Same(reply, cached);
    }

    [Fact]
    public void Record_AtCapacity_EvictsOldestKey()
    {
        var ledger = IdempotencyLedger.Empty(capacity: 2)
            .Record("key-1", new Reply.ReplyValue("v1", null))
            .Record("key-2", new Reply.ReplyValue("v2", null))
            .Record("key-3", new Reply.ReplyValue("v3", null));

        Assert.False(ledger.TryGetCachedReply("key-1", out _)); // evicted, oldest
        Assert.True(ledger.TryGetCachedReply("key-2", out _));
        Assert.True(ledger.TryGetCachedReply("key-3", out _));
    }

    [Fact]
    public void Record_SameKeyTwice_DoesNotGrowLedgerOrDuplicateEviction()
    {
        var secondReply = new Reply.ReplyValue("v1-again", null);
        var ledger = IdempotencyLedger.Empty(capacity: 2)
            .Record("key-1", new Reply.ReplyValue("v1", null))
            .Record("key-1", secondReply)
            .Record("key-2", new Reply.ReplyValue("v2", null));

        // key-1 must still be present — re-recording the same key must not have evicted itself
        // via the capacity-2 ring buffer treating it as two separate entries.
        Assert.True(ledger.TryGetCachedReply("key-1", out var cached));
        Assert.Same(secondReply, cached); // re-recording updates the cached reply to the latest one
        Assert.True(ledger.TryGetCachedReply("key-2", out _));
    }

    [Fact]
    public void Empty_NonPositiveCapacity_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => IdempotencyLedger.Empty(capacity: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => IdempotencyLedger.Empty(capacity: -1));
    }
}
