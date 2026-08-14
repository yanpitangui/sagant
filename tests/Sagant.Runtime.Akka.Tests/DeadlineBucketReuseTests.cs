using Akka.Actor;
using Akka.TestKit.Xunit2;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Deadlines;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// What a bucket does once it has dealt with everything it held.
///
/// A bucket covers a slice of time rather than one deadline, so every deadline falling inside that
/// slice addresses the same entity — and a schedule firing several times a minute re-arms into the
/// slice it just fired out of. An entity that stops itself loses whatever <c>ClusterSharding</c>
/// routes to it while it goes, which for a bucket is ordinary traffic rather than a rare race: the
/// schedule that just woke is arming its next deadline right then. Losing that arm leaves the
/// schedule asleep with nothing left to wake it.
///
/// So a drained bucket stays put and goes quiet, and idle passivation reaps it on the Shard's own
/// handshake, which holds anything still addressed to it.
/// </summary>
public class DeadlineBucketReuseTests : TestKit
{
    /// <summary>Answers every wake, so a bucket empties promptly and gets on with draining.</summary>
    private sealed class WakingClient : IWorkflowClient
    {
        public IWorkflowHandle<TWorkflow> For<TWorkflow>(string entityId) where TWorkflow : class =>
            throw new NotSupportedException("A bucket addresses instances by type name.");

        public IWorkflowHandle For(string workflowType, string entityId) => new Handle(entityId);

        private sealed class Handle(string entityId) : IWorkflowHandle
        {
            public string EntityId => entityId;

            public Task<Done> Wake(WorkflowTimerKind kind, TimeSpan? timeout = null, CancellationToken ct = default) =>
                Task.FromResult(Done.Instance);

            public ValueTask Send<TCommand>(
                TCommand command, CancellationToken cancellationToken = default, string? idempotencyKey = null,
                IReadOnlyDictionary<string, string>? metadata = null) where TCommand : notnull =>
                throw new NotSupportedException();

            public Task<WorkflowStatus> GetStatus(TimeSpan? timeout = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<TReply> Request<TCommand, TReply>(
                TCommand command, TimeSpan? timeout = null, CancellationToken cancellationToken = default,
                string? idempotencyKey = null, IReadOnlyDictionary<string, string>? metadata = null)
                where TCommand : notnull => throw new NotSupportedException();

            public Task<TReply> Query<TQuery, TReply>(
                TQuery query, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
                where TQuery : notnull => throw new NotSupportedException();

            public Task<Done> Suspend(string? reason = null, TimeSpan? t = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Resume(TimeSpan? t = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Terminate(string? reason = null, TimeSpan? t = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Cancel(string? reason = null, TimeSpan? t = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Delete(string? reason = null, TimeSpan? t = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<WorkflowResult<TState>> RunAndAwaitResult<TState>(
                object command, TimeSpan timeout, string? idempotencyKey = null, CancellationToken ct = default) =>
                throw new NotSupportedException();
        }
    }

    public DeadlineBucketReuseTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = WARNING
        """;

    /// <summary>
    /// Driven directly rather than through a shard region, since what is being pinned down is what the
    /// bucket does about its own lifetime — a region in front of it would answer a later question by
    /// starting a fresh incarnation, hiding the very thing this asks about.
    /// </summary>
    [Fact]
    public void ABucketThatDealtWithEverythingItHeld_StaysForTheShardToReap()
    {
        var bucket = Sys.ActorOf(DeadlineBucketActor.Props(
            "202608142029", new WorkflowDeadlineSettings(), new WakingClient(), TimeProvider.System));

        Watch(bucket);

        // Already due, so it fires at once, the wake is answered, and the bucket has nothing left.
        bucket.Tell(new BucketCommands.Place(
            new WorkflowDeadlineKey("SleepingWorkflow", "drains-it", WorkflowTimerKind.Pause),
            DateTimeOffset.UtcNow.AddSeconds(-1)));

        ExpectMsg<Done>();

        // Long enough to cover firing, the wake, the drain record and the journal delete behind it.
        ExpectNoMsg(TimeSpan.FromSeconds(3));

        // Still here: an entity that stopped itself would have dropped whatever arrived while it went.
        bucket.Tell(BucketCommands.GetCount.Instance);
        Assert.Equal(0, ExpectMsg<int>());
    }
}
