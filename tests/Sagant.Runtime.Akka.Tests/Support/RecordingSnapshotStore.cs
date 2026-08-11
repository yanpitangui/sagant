using System.Collections.Concurrent;
using Akka.Persistence;
using Akka.Persistence.Snapshot;

namespace Sagant.Runtime.Akka.Tests.Support;

/// <summary>
/// An in-memory snapshot store that keeps a record of what was saved to it, so a test can assert
/// about snapshot cadence: how many snapshots an instance took, and at which sequence numbers.
///
/// Snapshot cadence is invisible to a test that only reads resulting state — a workflow looks
/// identical whether it snapshotted once or five times along the way. Recording the saves is what
/// makes the policy assertable, and it keeps that observation in the test project where it belongs.
///
/// Enable with the HOCON in <see cref="Config"/>. Records are keyed by persistence id, and every
/// test using this store picks its own, so recordings stay isolated while test classes run in
/// parallel.
/// </summary>
public sealed class RecordingSnapshotStore : MemorySnapshotStore
{
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<long>> Saves = new();

    public const string Config = """
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.recording"
        akka.persistence.snapshot-store.recording {
            class = "Sagant.Runtime.Akka.Tests.Support.RecordingSnapshotStore, Sagant.Runtime.Akka.Tests"
            plugin-dispatcher = "akka.actor.default-dispatcher"
        }
        """;

    /// <summary>The sequence number of every snapshot saved for <paramref name="persistenceId"/>,
    /// oldest first.</summary>
    public static IReadOnlyList<long> SavesFor(string persistenceId) =>
        Saves.TryGetValue(persistenceId, out var saves) ? saves.ToArray() : Array.Empty<long>();

    protected override Task SaveAsync(
        SnapshotMetadata metadata, object snapshot, CancellationToken cancellationToken)
    {
        Saves.GetOrAdd(metadata.PersistenceId, _ => new ConcurrentQueue<long>()).Enqueue(metadata.SequenceNr);
        return base.SaveAsync(metadata, snapshot, cancellationToken);
    }
}
