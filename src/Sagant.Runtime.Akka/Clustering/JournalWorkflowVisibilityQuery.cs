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
/// Answers lifecycle questions by replaying each instance's own events through
/// <see cref="WorkflowEventFold"/> — the same fold the entity actor applies as it writes and again on
/// recovery. A listed record and a live entity therefore agree by construction, with no second
/// representation of state to drift.
///
/// It needs no store, no schema, and no migration, which suits a modest fleet: every listing is a
/// scan across persistence ids. A deployment that outgrows that implements
/// <see cref="IWorkflowVisibilityQuery"/> over its own projected table, and its callers do not
/// change.
///
/// <see cref="WorkflowVisibilityFilter.WorkflowType"/> is applied to the persistence id before any
/// event is read, so narrowing to one workflow type costs nothing.
/// </summary>
public sealed class JournalWorkflowVisibilityQuery : IWorkflowVisibilityQuery
{
    private readonly IReadJournal _readJournal;
    private readonly IMaterializer _materializer;

    public JournalWorkflowVisibilityQuery(IReadJournal readJournal, IMaterializer materializer)
    {
        _readJournal = readJournal;
        _materializer = materializer;
    }

    /// <summary>Resolves the read journal <paramref name="readJournalPluginId"/> names on
    /// <paramref name="system"/> — e.g. <c>akka.persistence.query.journal.sql</c>.</summary>
    public static JournalWorkflowVisibilityQuery For(ActorSystem system, string readJournalPluginId) =>
        new(PersistenceQuery.Get(system).ReadJournalFor<IReadJournal>(readJournalPluginId),
            system.Materializer());

    public async Task<WorkflowVisibilityRecord?> GetAsync(
        string entityId, CancellationToken cancellationToken = default)
    {
        await foreach (var persistenceId in PersistenceIds(cancellationToken))
        {
            if (WorkflowPersistenceId.EntityIdOf(persistenceId) == entityId)
            {
                return await BuildAsync(persistenceId, cancellationToken);
            }
        }

        return null;
    }

    public async IAsyncEnumerable<WorkflowVisibilityRecord> ListAsync(
        WorkflowVisibilityFilter filter,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var returned = 0;

        await foreach (var persistenceId in PersistenceIds(cancellationToken))
        {
            if (filter.WorkflowType is { } type && WorkflowPersistenceId.WorkflowTypeOf(persistenceId) != type)
            {
                continue;
            }

            if (filter.EntityIdPrefix is { } prefix
                && !WorkflowPersistenceId.EntityIdOf(persistenceId).StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var record = await BuildAsync(persistenceId, cancellationToken);
            if (record is null || !Matches(record, filter))
            {
                continue;
            }

            yield return record;

            if (filter.Limit is { } limit && ++returned >= limit)
            {
                yield break;
            }
        }
    }

    private static bool Matches(WorkflowVisibilityRecord record, WorkflowVisibilityFilter filter) =>
        (filter.Statuses is not { Count: > 0 } || filter.Statuses.Contains(record.Status))
        && (filter.StartedAfter is not { } after || record.StartedAt >= after)
        && (filter.StartedBefore is not { } before || record.StartedAt <= before);

    /// <summary>
    /// Folds one instance's whole event stream into its current state.
    ///
    /// <c>TState</c> is deliberately absent: this reports the lifecycle every workflow shares, so it
    /// folds into an envelope of <see cref="object"/> and reads only the fields that hold for any
    /// workflow. A <c>UserStateChanged&lt;TState&gt;</c> passes through the fold's default arm
    /// untouched, which is exactly right here.
    /// </summary>
    private async Task<WorkflowVisibilityRecord?> BuildAsync(
        string persistenceId, CancellationToken cancellationToken)
    {
        var envelope = new WorkflowRuntimeState<object>(
            UserState: null!, CurrentStepName: null, CurrentStepInput: null,
            RetryCount: 0, Status: WorkflowStatus.NotStarted);

        DateTimeOffset? startedAt = null;
        DateTimeOffset? endedAt = null;
        var seen = false;

        var source = Query<ICurrentEventsByPersistenceIdQuery>()
            .CurrentEventsByPersistenceId(persistenceId, 0, long.MaxValue);

        await foreach (var recorded in source.RunAsAsyncEnumerable(_materializer).WithCancellation(cancellationToken))
        {
            if (recorded.Event is not WorkflowEvent @event)
            {
                continue;
            }

            seen = true;
            var at = JournalTimestamp.Read(recorded.Timestamp);
            startedAt ??= at;

            envelope = WorkflowEventFold.Apply(envelope, @event);

            if (@event is WorkflowEvent.RunFinished or WorkflowEvent.RunDeleted)
            {
                endedAt = at;
            }
        }

        if (!seen)
        {
            return null;
        }

        return new WorkflowVisibilityRecord(
            EntityId: WorkflowPersistenceId.EntityIdOf(persistenceId),
            WorkflowType: WorkflowPersistenceId.WorkflowTypeOf(persistenceId),
            Status: envelope.Status,
            Outcome: envelope.Outcome,
            CurrentStepName: envelope.CurrentStepName,
            Attempt: envelope.RetryCount + 1,
            StartedAt: startedAt!.Value,
            EndedAt: endedAt,
            LastTraceParent: envelope.LastTraceParent);
    }

    /// <summary>
    /// Every persistence id the journal knows.
    ///
    /// A journal that keeps its own id metadata still lists an instance whose events were purged, so
    /// a caller may see an id that yields no record — which <see cref="BuildAsync"/> reports as
    /// <c>null</c> and both callers here skip.
    /// </summary>
    private async IAsyncEnumerable<string> PersistenceIds(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var ids = Query<ICurrentPersistenceIdsQuery>()
            .CurrentPersistenceIds()
            .RunAsAsyncEnumerable(_materializer);

        await foreach (var id in ids.WithCancellation(cancellationToken))
        {
            yield return id;
        }
    }

    private TQuery Query<TQuery>() where TQuery : class, IReadJournal =>
        _readJournal as TQuery
        ?? throw new NotSupportedException(
            $"The configured read journal ({_readJournal.GetType().Name}) implements no {typeof(TQuery).Name}, " +
            "which this query reads through. Configure a journal whose read journal supports it.");
}
