using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Sagant.Execution;
using Akka.Util;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>
/// Reads every instance's deadlines out of the journal and records them with an
/// <see cref="IWorkflowDeadlineScheduler"/>.
///
/// Deriving arms from the events themselves is what keeps the index honest. An instance that
/// recorded a deadline and then went quiet is in the stream whether or not anything else happened,
/// so an arm cannot go missing between the write and the recording — the failure that would leave an
/// instance nothing ever wakes. It is also why enabling this on a running deployment needs no
/// migration: reading the stream from its start finds every instance already waiting.
///
/// It follows the deadline-shard tags alone (see
/// <see cref="WorkflowEventTags.ForDeadlineShard"/>), so it reads the small fraction of the journal
/// that moves a deadline. One instance takes every shard while the volume is modest; splitting them
/// across several readers is a matter of giving each a subset.
///
/// A deadline nearer than <see cref="WorkflowDeadlineSettings.ExternalArmThreshold"/> is left alone:
/// the instance holding it stays resident long enough to fire it itself.
/// </summary>
public sealed class WorkflowDeadlineProjection
{
    private readonly IReadJournal _readJournal;
    private readonly IMaterializer _materializer;
    private readonly IWorkflowDeadlineScheduler _scheduler;
    private readonly WorkflowDeadlineSettings _settings;
    private readonly TimeProvider _timeProvider;
    private readonly ILoggingAdapter _log;

    public WorkflowDeadlineProjection(
        IReadJournal readJournal,
        IMaterializer materializer,
        IWorkflowDeadlineScheduler scheduler,
        WorkflowDeadlineSettings settings,
        TimeProvider timeProvider,
        ILoggingAdapter log)
    {
        _readJournal = readJournal;
        _materializer = materializer;
        _scheduler = scheduler;
        _settings = settings;
        _timeProvider = timeProvider;
        _log = log;
    }

    /// <summary>
    /// Reads the history once to work out what is still waiting, records that, and then follows the
    /// stream live from where the history ended.
    ///
    /// The two phases exist because replaying history one event at a time would <em>act</em> on it:
    /// an old pause would be recorded as a deadline in a bucket whose slice is long past, which fires
    /// at once — waking an instance that finished months ago, before the event retiring that deadline
    /// has even been read. On a journal of any age that is a wake for a large share of everything
    /// that ever ran. Folding first and recording afterwards asks the same question with none of that:
    /// what, at the end of all this, is still waiting.
    /// </summary>
    /// <param name="from">Where to resume, or <c>null</c> to read the history from the start. A
    /// caller holding a checkpoint passes it, which is what keeps a restart from re-reading a journal
    /// that only grows.</param>
    /// <param name="onProgress">Called with each offset applied, so a caller can record how far this
    /// has got. Called after the change it names is recorded, so a crash between the two repeats an
    /// arm rather than skipping one.</param>
    public async Task<IKillSwitch> RunAsync(
        Offset? from = null,
        Func<Offset, Task>? onProgress = null,
        CancellationToken cancellationToken = default)
    {
        var start = from ?? Offset.Sequence(0);

        if (from is null)
        {
            start = await BackfillAsync(cancellationToken);
        }

        return RunLive(start, onProgress, cancellationToken);
    }

    /// <summary>
    /// Folds the history into the set of deadlines still waiting at the end of it, records those, and
    /// answers with the offset the fold reached.
    ///
    /// <c>CurrentEventsByTag</c> rather than the live query, because this wants the history as it
    /// stands and an end to it. Nothing is recorded while folding: an instance that paused and later
    /// finished contributes nothing, which is most of a journal's history.
    /// </summary>
    private async Task<Offset> BackfillAsync(CancellationToken cancellationToken)
    {
        var waiting = new Dictionary<WorkflowDeadlineKey, DateTimeOffset>();
        var reached = Offset.Sequence(0);
        var read = 0L;

        var source = Query<ICurrentEventsByTagQuery>()
            .CurrentEventsByTag(WorkflowEventTags.Deadline, Offset.Sequence(0));

        await foreach (var envelope in source.RunAsAsyncEnumerable(_materializer).WithCancellation(cancellationToken))
        {
            reached = envelope.Offset;
            read++;

            if (envelope.Event is not WorkflowEvent @event)
            {
                continue;
            }

            var workflowType = WorkflowPersistenceId.WorkflowTypeOf(envelope.PersistenceId);
            var entityId = WorkflowPersistenceId.EntityIdOf(envelope.PersistenceId);

            foreach (var change in WorkflowDeadlineFold.Changes(@event))
            {
                switch (change)
                {
                    case WorkflowDeadlineChange.Arm arm:
                        waiting[new WorkflowDeadlineKey(workflowType, entityId, arm.Kind, arm.Discriminator)] = arm.DueUtc;
                        break;
                    case WorkflowDeadlineChange.Disarm disarm:
                        waiting.Remove(new WorkflowDeadlineKey(workflowType, entityId, disarm.Kind, disarm.Discriminator));
                        break;
                }
            }
        }

        _log.Info(
            "Deadline backfill read {0} events and found {1} instances still waiting", read, waiting.Count);

        foreach (var (key, dueUtc) in waiting)
        {
            await ArmAsync(key, dueUtc);
        }

        return reached;
    }

    /// <summary>Follows the stream from <paramref name="from"/> until cancelled.</summary>
    private IKillSwitch RunLive(Offset from, Func<Offset, Task>? onProgress, CancellationToken cancellationToken)
    {
        var lanes = _settings.ProjectionLanes;

        // Resumed from wherever it reached rather than from the start, so a failure costs the events
        // since the last recorded position instead of the whole journal.
        var resumeFrom = from;

        var lanesMerged = (Source<Offset, NotUsed>)RestartSource.OnFailuresWithBackoff(
            () => Query<IEventsByTagQuery>().EventsByTag(WorkflowEventTags.Deadline, resumeFrom),
            RestartSettings.Create(
                minBackoff: TimeSpan.FromSeconds(1),
                maxBackoff: TimeSpan.FromSeconds(30),
                randomFactor: 0.2))
            // Hashed rather than round-robined, so every event of one instance lands in the same lane
            // and stays in the order the journal holds it. MurmurHash for the same reason sharding
            // uses it: string.GetHashCode() is randomized per process, which would move an instance
            // between lanes on a restart and let two of its events run concurrently.
            .GroupBy(lanes, e => (int)((uint)MurmurHash.StringHash(e.PersistenceId) % (uint)lanes))
            // One at a time within a lane, so the last arm an instance recorded is the one that wins.
            .SelectAsync(1, async e =>
            {
                await ApplyAsync(e);
                return e.Offset;
            })
            .MergeSubstreams();

        // After the merge, so one switch stops every lane at once.
        var (killSwitch, completion) = lanesMerged
            .ViaMaterialized(KillSwitches.Single<Offset>(), Keep.Right)
            .ToMaterialized(
                Sink.ForEach<Offset>(offset =>
                {
                    // Held so a restart resumes here, and handed to the caller so it can outlive this
                    // process. Recorded after the change it names, which makes a crash between the two
                    // repeat an arm rather than skip one.
                    resumeFrom = offset;
                    onProgress?.Invoke(offset);
                }),
                Keep.Both)
            .Run(_materializer);

        completion.ContinueWith(
            t => _log.Error(
                t.Exception,
                "Deadline projection stopped. Deadlines already recorded still fire; ones recorded "
                + "from here are not, until this is running again."),
            TaskContinuationOptions.OnlyOnFaulted);

        cancellationToken.Register(() => killSwitch.Shutdown());
        return killSwitch;
    }

    /// <summary>
    /// Records what one event does to its instance's deadlines. Every path answers, so the stream
    /// advances past an event whose change is already recorded.
    /// </summary>
    private async Task ApplyAsync(EventEnvelope envelope)
    {
        if (envelope.Event is not WorkflowEvent @event)
        {
            return;
        }

        var changes = WorkflowDeadlineFold.Changes(@event);
        if (changes.Count == 0)
        {
            return;
        }

        var workflowType = WorkflowPersistenceId.WorkflowTypeOf(envelope.PersistenceId);
        var entityId = WorkflowPersistenceId.EntityIdOf(envelope.PersistenceId);

        foreach (var change in changes)
        {
            switch (change)
            {
                case WorkflowDeadlineChange.Arm arm:
                    await ArmAsync(
                        new WorkflowDeadlineKey(workflowType, entityId, arm.Kind, arm.Discriminator),
                        arm.DueUtc);
                    break;

                case WorkflowDeadlineChange.Disarm disarm:
                    await _scheduler.DisarmAsync(
                        new WorkflowDeadlineKey(workflowType, entityId, disarm.Kind, disarm.Discriminator));
                    break;
            }
        }
    }

    private async Task ArmAsync(WorkflowDeadlineKey key, DateTimeOffset dueUtc)
    {
        if (dueUtc - _timeProvider.GetUtcNow() <= _settings.ExternalArmThreshold)
        {
            // Near enough that the instance holding it is still resident and fires it itself. Any
            // earlier arm for this kind is retired, since this instant is the one that now applies.
            await _scheduler.DisarmAsync(key);
            return;
        }

        await _scheduler.ArmAsync(key, dueUtc);
    }

    private TQuery Query<TQuery>() where TQuery : class, IReadJournal =>
        _readJournal as TQuery
        ?? throw new NotSupportedException(
            $"The configured read journal ({_readJournal.GetType().Name}) implements no {typeof(TQuery).Name}, " +
            "which the deadline projection reads through. Configure a journal whose read journal supports it.");
}
