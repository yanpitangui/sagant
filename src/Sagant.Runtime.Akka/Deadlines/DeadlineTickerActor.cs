using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>Recorded once a bucket has been poked, so a restart resumes where this left off.</summary>
internal sealed record BucketPoked(string BucketId);

/// <summary>
/// Pokes each bucket as its slice arrives, so the entity holding that slice's deadlines recovers,
/// fires them and deletes itself.
///
/// A cluster singleton, and the only durable state outside the buckets themselves: the last bucket it
/// poked. That one fact is what makes a gap recoverable — a process down for an hour walks the hour
/// of buckets it missed rather than skipping to the present, so the deadlines inside them fire late
/// instead of never.
///
/// It knows nothing about which buckets hold anything. Poking an empty one costs an entity that
/// recovers with nothing and stops, which is the trade that removes the index: the ticker needs no
/// knowledge of what is scheduled, only of what time it is.
/// </summary>
internal sealed class DeadlineTickerActor : ReceivePersistentActor
{
    private sealed record Tick
    {
        public static readonly Tick Instance = new();
    }

    private readonly WorkflowDeadlineSettings _settings;
    private readonly IActorRef _buckets;
    private readonly TimeProvider _timeProvider;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private DateTimeOffset _lastPoked;
    private ICancelable? _tick;

    public override string PersistenceId => "sagant-deadline-ticker";

    public DeadlineTickerActor(
        WorkflowDeadlineSettings settings, IActorRef buckets, TimeProvider timeProvider)
    {
        _settings = settings;
        _buckets = buckets;
        _timeProvider = timeProvider;

        // See DeadlineBucketActor: a journal hands recovery the Tagged wrapper rather than the
        // payload, so both shapes are handled.
        Recover<Tagged>(t =>
        {
            if (t.Payload is BucketPoked poked)
            {
                ApplyPoked(poked.BucketId);
            }
        });
        Recover<BucketPoked>(e => ApplyPoked(e.BucketId));

        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is string bucketId && DeadlineBucket.TryParse(bucketId, out var start))
            {
                _lastPoked = start;
            }
        });

        Recover<RecoveryCompleted>(_ =>
        {
            // A ticker with no history starts from the current slice: buckets written before it ever
            // ran belong to a deployment that had no ticker, and their deadlines are on guarantee
            // D8's terms already.
            if (_lastPoked == default)
            {
                _lastPoked = DeadlineBucket.Truncate(_timeProvider.GetUtcNow()) - DeadlineBucket.Interval;
            }

            Tock();
            ScheduleTick();
        });

        Command<Tick>(_ =>
        {
            Tock();
            ScheduleTick();
        });

        Command<SaveSnapshotSuccess>(msg =>
            DeleteMessages(msg.Metadata.SequenceNr));
        Command<SaveSnapshotFailure>(msg =>
            _log.Warning(msg.Cause, "{0}: snapshot failed; the journal keeps growing until one lands", PersistenceId));
        Command<DeleteMessagesSuccess>(_ => { });
        Command<DeleteMessagesFailure>(msg =>
            _log.Warning(msg.Cause, "{0}: could not release poked-bucket history", PersistenceId));
    }

    public static Props Props(
        WorkflowDeadlineSettings settings, IActorRef buckets, TimeProvider timeProvider) =>
        global::Akka.Actor.Props.Create(() => new DeadlineTickerActor(settings, buckets, timeProvider));

    private void ApplyPoked(string bucketId)
    {
        if (DeadlineBucket.TryParse(bucketId, out var start))
        {
            _lastPoked = start;
        }
    }

    /// <summary>
    /// Pokes every bucket owed since the last one, oldest first, and records how far it got. The
    /// record lands after the pokes, so a crash between the two repeats them — and a repeated poke
    /// reaches a bucket that has already drained itself, which costs an activation and changes
    /// nothing.
    /// </summary>
    private void Tock()
    {
        var now = _timeProvider.GetUtcNow();
        var owed = DeadlineBucket.Between(_lastPoked, now, _settings.MaxBucketCatchUp);
        if (owed.Count == 0)
        {
            return;
        }

        foreach (var bucketId in owed)
        {
            _buckets.Tell(new BucketEnvelope(bucketId, BucketCommands.Poke.Instance));
        }

        var last = owed[^1];
        Persist(new Tagged(new BucketPoked(last), [WorkflowEventTags.Internal]), tagged =>
        {
            var e = (BucketPoked)tagged.Payload;
            if (DeadlineBucket.TryParse(e.BucketId, out var start))
            {
                _lastPoked = start;
            }

            // Only the latest matters, so the history behind it is noise. Snapshotting each pass keeps
            // recovery to one read whatever the ticker's age.
            SaveSnapshot(e.BucketId);
        });
    }

    private void ScheduleTick()
    {
        _tick?.Cancel();

        // Aligned to the next slice boundary, so a bucket is poked as its slice opens rather than
        // drifting further into it with every pass.
        var now = _timeProvider.GetUtcNow();
        var nextBoundary = DeadlineBucket.Truncate(now) + DeadlineBucket.Interval;
        _tick = Context.System.Scheduler.ScheduleTellOnceCancelable(
            nextBoundary - now, Self, Tick.Instance, Self);
    }

    protected override void PostStop()
    {
        _tick?.Cancel();
        base.PostStop();
    }
}
