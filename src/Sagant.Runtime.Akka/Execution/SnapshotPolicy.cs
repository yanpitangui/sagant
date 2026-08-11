using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Execution;

/// <summary>
/// Decides when <see cref="WorkflowEntityActor{TWorkflow, TState}"/> takes a snapshot: always once
/// the just-persisted batch made the workflow terminal (<see cref="WorkflowStatus.Finished"/> or
/// <see cref="WorkflowStatus.Deleted"/>), and otherwise once
/// <see cref="_everyNEvents"/> events have accumulated since the last one. Bounds recovery replay
/// depth while cutting snapshot-store writes roughly <c>everyNEvents</c>-fold.
///
/// The threshold is a distance from the last snapshot, so a batch of any size crosses it: one
/// transition can write several events at once (a fan-out over <c>n</c> children writes <c>n</c>
/// member updates plus its group event), and a batch that jumps the sequence number from 8 to 13
/// still triggers at a threshold of 10.
///
/// Stateful by design: a snapshot save completes asynchronously, so the decision records its own
/// baseline immediately and the next several batches stay quiet while that save is still in flight.
/// Pure decision logic otherwise, with no Akka or persistence dependency of its own — the actor
/// supplies its <c>LastSequenceNr</c>, which <c>ReceivePersistentActor</c> already tracks.
/// </summary>
internal sealed class SnapshotPolicy(int everyNEvents)
{
    private readonly int _everyNEvents = everyNEvents;
    private long _lastSnapshotSequenceNr;

    /// <summary>
    /// Whether to snapshot now, given the status the instance reached and the sequence number the
    /// last write landed on. Saying yes moves the baseline, so each threshold crossing asks for one
    /// snapshot.
    /// </summary>
    public bool ShouldSnapshot(WorkflowStatus status, long lastSequenceNr)
    {
        if (!IsTerminal(status) && lastSequenceNr - _lastSnapshotSequenceNr < _everyNEvents)
        {
            return false;
        }

        _lastSnapshotSequenceNr = lastSequenceNr;
        return true;
    }

    /// <summary>
    /// Adopts a snapshot that already exists as the baseline, called when recovery is offered one.
    /// A run that recovers with a snapshot at sequence 40 and a short journal tail then waits for a
    /// full threshold's worth of new events.
    /// </summary>
    public void RecordSnapshot(long sequenceNr) => _lastSnapshotSequenceNr = sequenceNr;

    private static bool IsTerminal(WorkflowStatus status) =>
        status is WorkflowStatus.Finished or WorkflowStatus.Deleted;
}
