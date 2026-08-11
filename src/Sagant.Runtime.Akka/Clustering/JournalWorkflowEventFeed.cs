using Akka;
using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Reads recorded workflow events back out of the <see cref="Akka.Persistence"/> journal.
///
/// The journal keeps a live instance's events — <c>WorkflowEntityActor</c> snapshots to bound how
/// far recovery replays and leaves the events themselves in place — so a projection built from this
/// can be rebuilt whenever a read model needs reshaping.
///
/// That rebuild reaches back to the instance's most recent restart, which is as far as its history
/// goes: a restart reclaims the cycle it closed (guarantee <c>E11</c>), so a workflow that restarts
/// holds only its current cycle. See <c>V5</c> for what that means for a projection built over one.
///
/// Requires a journal whose read journal implements the query interfaces it uses; the in-memory
/// journal and <c>Akka.Persistence.Sql</c> both do.
/// </summary>
public sealed class JournalWorkflowEventFeed : IWorkflowEventFeed
{
    private readonly IReadJournal _readJournal;
    private readonly IMaterializer _materializer;

    public JournalWorkflowEventFeed(IReadJournal readJournal, IMaterializer materializer)
    {
        _readJournal = readJournal;
        _materializer = materializer;
    }

    /// <summary>Resolves the read journal <paramref name="readJournalPluginId"/> names on
    /// <paramref name="system"/> — e.g. <c>akka.persistence.query.journal.sql</c>.</summary>
    public static JournalWorkflowEventFeed For(ActorSystem system, string readJournalPluginId) =>
        new(PersistenceQuery.Get(system).ReadJournalFor<IReadJournal>(readJournalPluginId),
            system.Materializer());

    public IAsyncEnumerable<WorkflowFeedItem> Subscribe(
        string? tag = null, WorkflowFeedPosition? from = null, CancellationToken cancellationToken = default) =>
        Run(Query<IEventsByTagQuery>().EventsByTag(tag ?? WorkflowEventTags.All, ToOffset(from)), cancellationToken);

    public IAsyncEnumerable<WorkflowFeedItem> Read(
        string? tag = null, WorkflowFeedPosition? from = null, CancellationToken cancellationToken = default) =>
        Run(Query<ICurrentEventsByTagQuery>().CurrentEventsByTag(tag ?? WorkflowEventTags.All, ToOffset(from)), cancellationToken);

    /// <summary>
    /// Reads by the routable entity id — the value <c>IWorkflowClient.For</c> takes — which the
    /// journal does not key by. Persistence ids carry a workflow-type prefix
    /// (<c>{WorkflowTypeName}-{entityId}</c>), so the matching ones are resolved first.
    ///
    /// An id can legitimately belong to more than one workflow type, and every match is read: a
    /// caller asking about "order-17" wants what happened to order-17, whichever workflows ran under
    /// that name.
    /// </summary>
    public async IAsyncEnumerable<WorkflowFeedItem> ReadEntity(
        string entityId,
        long fromSequenceNr = 0,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var persistenceIds = Query<ICurrentPersistenceIdsQuery>()
            .CurrentPersistenceIds()
            .RunAsAsyncEnumerable(_materializer);

        var matching = new List<string>();
        await foreach (var persistenceId in persistenceIds.WithCancellation(cancellationToken))
        {
            if (WorkflowPersistenceId.EntityIdOf(persistenceId) == entityId)
            {
                matching.Add(persistenceId);
            }
        }

        foreach (var persistenceId in matching)
        {
            var source = Query<ICurrentEventsByPersistenceIdQuery>()
                .CurrentEventsByPersistenceId(persistenceId, fromSequenceNr, long.MaxValue);

            await foreach (var item in Run(source, cancellationToken))
            {
                yield return item;
            }
        }
    }

    private async IAsyncEnumerable<WorkflowFeedItem> Run(
        Source<EventEnvelope, NotUsed> source,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Delivery bookkeeping records how a message arrived: transport detail, which the journal
        // keeps so deduplication survives a crash. Skipping it here is what keeps this transport
        // carrying the same sequence the in-process publish does.
        var items = source
            .Where(e => e.Event is WorkflowEvent and not (WorkflowEvent.SeqNrRecorded or WorkflowEvent.IdempotencyRecorded))
            .Select(ToItem)
            .RunAsAsyncEnumerable(_materializer);

        await foreach (var item in items.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    private static WorkflowFeedItem ToItem(EventEnvelope envelope) => new(
        Position: new WorkflowFeedPosition(envelope.Offset.ToString()!),
        EntityId: WorkflowPersistenceId.EntityIdOf(envelope.PersistenceId),
        WorkflowType: WorkflowPersistenceId.WorkflowTypeOf(envelope.PersistenceId),
        SequenceNr: envelope.SequenceNr,
        Timestamp: JournalTimestamp.Read(envelope.Timestamp),
        Event: (WorkflowEvent)envelope.Event);

    /// <summary>An absent position starts at the beginning; a supplied one resumes past what it
    /// names.</summary>
    private static Offset ToOffset(WorkflowFeedPosition? from) =>
        from is { } position && long.TryParse(position.Value, out var sequence)
            ? global::Akka.Persistence.Query.Offset.Sequence(sequence)
            : global::Akka.Persistence.Query.Offset.NoOffset();

    private TQuery Query<TQuery>() where TQuery : class, IReadJournal =>
        _readJournal as TQuery
        ?? throw new NotSupportedException(
            $"The configured read journal ({_readJournal.GetType().Name}) implements no {typeof(TQuery).Name}, " +
            "which this feed reads through. Configure a journal whose read journal supports it.");
}
