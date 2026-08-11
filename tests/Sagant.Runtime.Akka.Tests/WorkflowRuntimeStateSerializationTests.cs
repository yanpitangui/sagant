using Sagant.Effects;
using Sagant.Idempotency;
using Sagant.Protocol;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Round-trips <see cref="WorkflowRuntimeState{TState}"/> through the real serializer Akka.Persistence
/// picks for it — every other test in this suite runs against <c>WithInMemoryJournal</c>, which
/// stores the persisted CLR object directly and never actually serializes it, so a type that's fine
/// in-memory can still be unrecoverable against a real journal (Postgres, SQL Server, ...). Guards
/// <see cref="SeqNrLedger"/> and <see cref="IdempotencyLedger"/> specifically: both expose their
/// backing collections as plain public properties with a public matching constructor, exactly what
/// Akka's default Newtonsoft-based serializer needs — it can't deserialize a type with no
/// public/default constructor (e.g. <c>System.Collections.Immutable.ImmutableDictionary</c>) once
/// reference-preservation is involved.
/// </summary>
public class WorkflowRuntimeStateSerializationTests
{
    [Fact]
    public void RoundTripsThroughRealSerializer_IncludingHighestAppliedSeqNrAndIdempotencyLedger()
    {
        using var system = ActorSystem.Create("workflow-runtime-state-serialization-test");

        var seqNrLedger = SeqNrLedger.Empty(4).Record("producer-1", 5).Record("producer-2", 12);
        var ledger = IdempotencyLedger.Empty(4).Record("key-1", new Reply.ReplyValue("ok", null));
        var state = new WorkflowRuntimeState<string>(
            UserState: "hello",
            CurrentStepName: "ReserveInventoryStep",
            CurrentStepInput: null,
            RetryCount: 1,
            Status: WorkflowStatus.Running,
            HighestAppliedSeqNr: seqNrLedger,
            IdempotencyLedger: ledger);

        var serializer = system.Serialization.FindSerializerFor(state);
        var bytes = serializer.ToBinary(state);
        var roundTripped = (WorkflowRuntimeState<string>)serializer.FromBinary(bytes, state.GetType());

        Assert.Equal(state.UserState, roundTripped.UserState);
        Assert.Equal(state.CurrentStepName, roundTripped.CurrentStepName);
        Assert.Equal(state.RetryCount, roundTripped.RetryCount);
        Assert.Equal(state.Status, roundTripped.Status);
        Assert.NotNull(roundTripped.HighestAppliedSeqNr);
        Assert.True(roundTripped.HighestAppliedSeqNr!.TryGetHighest("producer-1", out var highest1));
        Assert.Equal(5, highest1);
        Assert.True(roundTripped.HighestAppliedSeqNr!.TryGetHighest("producer-2", out var highest2));
        Assert.Equal(12, highest2);
        Assert.NotNull(roundTripped.IdempotencyLedger);
        Assert.True(roundTripped.IdempotencyLedger!.TryGetCachedReply("key-1", out var reply));
        var replyValue = Assert.IsType<Reply.ReplyValue>(reply);
        Assert.Equal("ok", replyValue.Value);
    }
}
