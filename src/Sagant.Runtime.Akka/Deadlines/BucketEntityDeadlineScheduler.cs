using Akka.Actor;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>
/// <see cref="IWorkflowDeadlineScheduler"/> over sharded bucket entities: the implementation to reach
/// for once the number of waiting instances outgrows an in-memory index.
///
/// A deadline is written into the entity holding the slice of time it falls in, so the only buckets
/// ever resident are the ones near now. Memory therefore tracks how many deadlines are due soon,
/// where <see cref="InMemoryDeadlineScheduler"/>'s tracks how many exist at all. A million instances
/// waiting on next month cost a million journal rows and nothing in memory.
///
/// <para><see cref="DisarmAsync"/> does nothing, and that is the point. Finding a placed deadline
/// again would need a key-to-bucket mapping — the very index this exists to avoid — so a deadline
/// that moves is placed again in its new bucket and the old entry is left to expire on its own. The
/// wake it eventually causes activates an instance that re-derives its own deadline and goes quiet
/// again. A stale wake costs one activation; see <see cref="DeadlineBucketActor"/>.</para>
/// </summary>
public sealed class BucketEntityDeadlineScheduler : IWorkflowDeadlineScheduler
{
    private readonly IActorRef _buckets;
    private readonly TimeSpan _timeout;

    internal BucketEntityDeadlineScheduler(IActorRef buckets, TimeSpan timeout)
    {
        _buckets = buckets;
        _timeout = timeout;
    }

    public Task ArmAsync(
        WorkflowDeadlineKey key, DateTimeOffset dueUtc, CancellationToken cancellationToken = default) =>
        _buckets.Ask<Done>(
            new BucketEnvelope(DeadlineBucket.For(dueUtc), new BucketCommands.Place(key, dueUtc)),
            _timeout, cancellationToken);

    /// <summary>
    /// Answers without doing anything. A stale entry wakes an instance that has already moved on,
    /// which is a no-op by design — so leaving it costs one activation, where finding it would cost
    /// an index proportional to every waiting instance.
    /// </summary>
    public Task DisarmAsync(WorkflowDeadlineKey key, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    /// <summary>How many deadlines the bucket covering <paramref name="instant"/> holds. For
    /// diagnostics and tests.</summary>
    public Task<int> CountInBucketAsync(DateTimeOffset instant, CancellationToken cancellationToken = default) =>
        _buckets.Ask<int>(
            new BucketEnvelope(DeadlineBucket.For(instant), BucketCommands.GetCount.Instance),
            _timeout, cancellationToken);
}
