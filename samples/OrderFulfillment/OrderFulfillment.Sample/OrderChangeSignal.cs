namespace OrderFulfillment.Sample;

/// <summary>
/// This replica's own "something changed, go re-read Postgres" bell — replaces the old in-memory
/// <c>OrderStore.Changed</c> event now that the actual read-model data lives in
/// <see cref="OrderReadModelRepository"/>'s shared tables instead of a per-replica dictionary. Every
/// replica's <see cref="WorkflowEventLoggerActor"/> already receives every
/// <c>WorkflowFeedItem</c> via the cluster-wide pub-sub bridge regardless of storage backend, so
/// firing this locally after each write is enough to drive that replica's own SSE stream — no
/// Postgres <c>LISTEN</c>/<c>NOTIFY</c> needed on top of it.
/// </summary>
public sealed class OrderChangeSignal
{
    public event Action? Changed;

    public void Raise() => Changed?.Invoke();
}
