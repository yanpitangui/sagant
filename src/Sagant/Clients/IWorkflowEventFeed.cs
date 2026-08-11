using Sagant.Protocol;

namespace Sagant.Clients;

/// <summary>
/// Reads the events a runtime recorded for its workflows, so a consumer can build whatever view of
/// them it needs — a searchable table, an audit log, a dashboard's backing store.
///
/// Every method is a stateless per-caller read: a subscription opens on the call, resumes from
/// whatever <see cref="WorkflowFeedPosition"/> is passed, and disposes when the token fires. There is
/// no host, no daemon, and no offset store here — the position travels with each item, and a consumer
/// commits it in the same transaction as its own write. That is what turns at-least-once delivery
/// into exactly-once effect: an offset kept anywhere else is a second write with no shared
/// transaction, leaving duplicates or gaps on a crash between the two.
///
/// Where a consumer needs exactly one runner across replicas, that is a property of how it deploys
/// its own projection.
/// </summary>
public interface IWorkflowEventFeed
{
    /// <summary>
    /// Live tail, completing when <paramref name="cancellationToken"/> fires.
    ///
    /// Ordering holds within a single workflow: its events arrive in the order they were written.
    /// Across workflows there is no ordering at all, so a consumer tolerates a child's events
    /// arriving before its parent's and encodes no cross-instance invariant.
    /// </summary>
    /// <param name="tag">Which stream to read, or <c>null</c> for every workflow the runtime records.
    /// A runtime publishes a small, fixed set — see its own documentation.</param>
    /// <param name="from">Where to resume, or <c>null</c> to start at the beginning.</param>
    IAsyncEnumerable<WorkflowFeedItem> Subscribe(
        string? tag = null, WorkflowFeedPosition? from = null, CancellationToken cancellationToken = default);

    /// <summary>Everything recorded so far, completing at the end as it stands when the read
    /// reaches it. Separate from <see cref="Subscribe"/> because whether an enumeration ever
    /// terminates is too consequential to hide behind a flag.</summary>
    IAsyncEnumerable<WorkflowFeedItem> Read(
        string? tag = null, WorkflowFeedPosition? from = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// One workflow's own events, in sequence-number order, completing at its end.
    ///
    /// Sequence numbers here are dense and monotonic per instance, which makes this the primitive a
    /// consumer reconciles with: holding a high-water mark per workflow (the same one it uses to
    /// recognise a duplicate), it reads forward from that mark to collect anything a live
    /// subscription missed.
    /// </summary>
    IAsyncEnumerable<WorkflowFeedItem> ReadEntity(
        string entityId, long fromSequenceNr = 0, CancellationToken cancellationToken = default);
}
