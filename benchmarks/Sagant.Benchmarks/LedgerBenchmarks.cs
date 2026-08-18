using BenchmarkDotNet.Attributes;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Idempotency;

namespace Sagant.Benchmarks;

/// <summary>
/// What one delivered command costs the dedup ledgers, at capacity — the path G12 in
/// <c>docs/deferred-work.md</c> names. Both ledgers sit on every business command a driver applies,
/// a per-message cost, where <see cref="ChildFanOutBenchmarks"/> measures a per-fan-out one.
/// </summary>
[MemoryDiagnoser]
public class LedgerBenchmarks
{
    private SeqNrLedger _seqNrLedger = null!;
    private IdempotencyLedger _idempotencyLedger = null!;

    [GlobalSetup]
    public void Setup()
    {
        var seqNr = SeqNrLedger.Empty(16);
        for (var i = 0; i < 16; i++)
        {
            seqNr = seqNr.Record($"producer-{i}", i);
        }

        _seqNrLedger = seqNr;

        var idempotency = IdempotencyLedger.Empty(50);
        for (var i = 0; i < 50; i++)
        {
            idempotency = idempotency.Record($"key-{i}", new Reply.ReplyValue($"value-{i}", null));
        }

        _idempotencyLedger = idempotency;
    }

    /// <summary>At capacity, so every call evicts one entry — the most expensive case.</summary>
    [Benchmark]
    public SeqNrLedger SeqNrLedger_Record() => _seqNrLedger.Record("producer-new", 999);

    /// <summary>At capacity, no idempotency key supplied — the common case (most commands carry
    /// none).</summary>
    [Benchmark]
    public IdempotencyLedger IdempotencyLedger_Record() =>
        _idempotencyLedger.Record("key-new", new Reply.ReplyValue("value-new", null));
}
