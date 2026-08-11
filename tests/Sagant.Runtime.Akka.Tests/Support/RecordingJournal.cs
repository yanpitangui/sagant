using System.Collections.Concurrent;
using System.Collections.Immutable;
using Akka.Persistence;
using Akka.Persistence.Journal;

namespace Sagant.Runtime.Akka.Tests.Support;

/// <summary>
/// An in-memory journal that keeps a record of what was written to it, so a test can assert about
/// the shape of an instance's writes: how many batches, and what each batch contained.
///
/// This exists because two guarantees are about writes themselves. D1 promises a transition is one
/// atomic batch, and H5 promises a fan-out's cost grows with the size of the group — both are
/// invisible to a test that only reads the resulting state, which looks identical either way.
///
/// Enable with <c>akka.persistence.journal.plugin = "akka.persistence.journal.recording"</c> and the
/// HOCON in <see cref="Config"/>. Records are keyed by persistence id, and every test using this
/// journal picks its own, so recordings stay isolated while test classes run in parallel.
/// </summary>
public sealed class RecordingJournal : MemoryJournal
{
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<IReadOnlyList<object>>> Batches = new();

    public const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.recording"
        akka.persistence.journal.recording {
            class = "Sagant.Runtime.Akka.Tests.Support.RecordingJournal, Sagant.Runtime.Akka.Tests"
            plugin-dispatcher = "akka.actor.default-dispatcher"
        }
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        """;

    /// <summary>Every batch written for <paramref name="persistenceId"/>, oldest first. One entry per
    /// atomic write, holding that write's payloads in order.</summary>
    public static IReadOnlyList<IReadOnlyList<object>> BatchesFor(string persistenceId) =>
        Batches.TryGetValue(persistenceId, out var batches) ? batches.ToArray() : Array.Empty<IReadOnlyList<object>>();

    /// <summary>Every payload written for <paramref name="persistenceId"/>, flattened across batches.</summary>
    public static IReadOnlyList<object> EventsFor(string persistenceId) =>
        BatchesFor(persistenceId).SelectMany(b => b).ToArray();

    protected override Task<IImmutableList<Exception>> WriteMessagesAsync(
        IEnumerable<AtomicWrite> messages, CancellationToken cancellationToken)
    {
        foreach (var write in messages)
        {
            // A real journal strips Tagged and stores the event with its tags alongside, so record
            // the event these assertions are about.
            var payloads = ((IImmutableList<IPersistentRepresentation>)write.Payload)
                .Select(p => p.Payload is Tagged tagged ? tagged.Payload : p.Payload)
                .ToArray();

            Batches.GetOrAdd(write.PersistenceId, _ => new ConcurrentQueue<IReadOnlyList<object>>()).Enqueue(payloads);
        }

        return base.WriteMessagesAsync(messages, cancellationToken);
    }
}
