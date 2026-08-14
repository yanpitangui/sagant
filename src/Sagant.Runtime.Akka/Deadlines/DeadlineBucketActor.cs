using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Akka.Streams;
using Akka.Streams.Dsl;
using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>Recorded when a deadline is placed in this bucket.</summary>
internal sealed record DeadlinePlaced(WorkflowDeadlineKey Key, DateTimeOffset DueUtc);

/// <summary>Recorded once every deadline in this bucket has been dealt with.</summary>
internal sealed record BucketDrained;

/// <summary>What a bucket accepts.</summary>
internal static class BucketCommands
{
    /// <summary>Put a deadline in this bucket.</summary>
    public sealed record Place(WorkflowDeadlineKey Key, DateTimeOffset DueUtc);

    /// <summary>Fire whatever is due. Sent by the ticker as the bucket's slice arrives.</summary>
    public sealed record Poke
    {
        public static readonly Poke Instance = new();
    }

    /// <summary>How many deadlines this bucket holds. For tests and diagnostics.</summary>
    public sealed record GetCount
    {
        public static readonly GetCount Instance = new();
    }
}

/// <summary>
/// Holds the deadlines due inside one slice of time, and wakes each instance as its own arrives.
///
/// One entity per slice, through <c>ClusterSharding</c>, so only the buckets near now are ever
/// resident: a deadline six months out sits in a bucket that stays passivated for six months and
/// costs a row in the journal until then. That is what makes this the implementation to reach for
/// once the number of waiting instances outgrows an in-memory index.
///
/// <para><b>Placement needs no index, and removal needs no message.</b> A deadline's bucket is its
/// own instant truncated, so nothing has to remember where a key was put. A deadline that moves is
/// placed again in its new bucket and the old entry is left where it is — the wake it eventually
/// causes activates an instance that re-derives its own deadline and goes quiet again, costing one
/// activation. So <see cref="IWorkflowDeadlineScheduler.DisarmAsync"/> has nothing to do here, which
/// is what keeps the key-to-bucket mapping an in-memory index needs from existing at all.</para>
///
/// <para>The re-arm obligation is met inside the bucket instead: an entry stays until its wake is
/// answered, retried on a backoff, and the bucket deletes itself once every entry is settled or has
/// run out of attempts. That bounds a dropped wake to this bucket's lifetime.</para>
/// </summary>
internal sealed class DeadlineBucketActor : ReceivePersistentActor
{
    private sealed record Retry;

    private sealed record WakesSettled(
        IReadOnlyList<WorkflowDeadlineKey> Settled, IReadOnlyList<WorkflowDeadlineKey> Failed);

    private readonly WorkflowDeadlineSettings _settings;
    private readonly IWorkflowClient _client;
    private readonly TimeProvider _timeProvider;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private readonly Dictionary<WorkflowDeadlineKey, DateTimeOffset> _deadlines = new();
    private readonly Dictionary<WorkflowDeadlineKey, int> _attempts = new();
    private ICancelable? _retry;
    private bool _firing;

    public override string PersistenceId { get; }

    public DeadlineBucketActor(
        string bucketId, WorkflowDeadlineSettings settings, IWorkflowClient client, TimeProvider timeProvider)
    {
        PersistenceId = "sagant-deadline-bucket-" + bucketId;
        _settings = settings;
        _client = client;
        _timeProvider = timeProvider;

        // Recovery is offered whatever was written, and what was written is Tagged — a journal hands
        // the wrapper back rather than the payload inside it, the same way WorkflowEntityActor's own
        // recovery reads one. Both shapes are handled so this holds whichever a journal does.
        Recover<Tagged>(t => ApplyRecovered(t.Payload));
        Recover<DeadlinePlaced>(e => ApplyRecovered(e));
        Recover<BucketDrained>(e => ApplyRecovered(e));
        Recover<RecoveryCompleted>(_ => OnRecovered());

        Command<BucketCommands.Place>(HandlePlace);
        Command<BucketCommands.Poke>(_ => Fire());
        Command<Retry>(_ => Fire());
        Command<WakesSettled>(HandleWakesSettled);
        Command<BucketCommands.GetCount>(_ => Sender.Tell(_deadlines.Count));
        Command<DeleteMessagesSuccess>(_ => Context.Stop(Self));
        Command<DeleteMessagesFailure>(msg =>
        {
            _log.Warning(msg.Cause, "{0}: draining failed to delete journal messages; stopping anyway", PersistenceId);
            Context.Stop(Self);
        });
    }

    public static Props Props(
        string bucketId, WorkflowDeadlineSettings settings, IWorkflowClient client, TimeProvider timeProvider) =>
        global::Akka.Actor.Props.Create(() => new DeadlineBucketActor(bucketId, settings, client, timeProvider));

    /// <summary>
    /// A bucket that comes back holding deadlines is one whose slice has arrived, or one restarted
    /// partway through firing. Either way there is work, and <see cref="Fire"/> decides what is due.
    /// </summary>
    private void OnRecovered()
    {
        if (_deadlines.Count > 0)
        {
            Fire();
        }
    }

    private void HandlePlace(BucketCommands.Place place)
    {
        // Already held at this instant, so the write is redundant. A projection replaying its stream
        // from the start repeats every arm it has already made, which is why this is worth checking.
        if (_deadlines.TryGetValue(place.Key, out var held) && held == place.DueUtc)
        {
            Sender.Tell(Done.Instance);
            return;
        }

        var replyTo = Sender;
        Persist(Internal(new DeadlinePlaced(place.Key, place.DueUtc)), tagged =>
        {
            var e = (DeadlinePlaced)tagged.Payload;
            _deadlines[e.Key] = e.DueUtc;
            replyTo.Tell(Done.Instance);

            // The bucket takes responsibility for its own contents the moment it holds any. A slice
            // the ticker has already passed gets no second poke, and a deadline arriving mid-slice is
            // due before the next boundary — this bucket is alive right now, so it holds the timer
            // for both. A slice still far off passivates before firing, and the ticker's poke is what
            // brings it back.
            Fire();
        });
    }

    private void Fire()
    {
        if (_firing)
        {
            return;
        }

        var now = _timeProvider.GetUtcNow();
        var due = _deadlines.Where(d => d.Value <= now).Select(d => d.Key).ToList();

        if (due.Count == 0)
        {
            if (_deadlines.Count == 0)
            {
                Drain();
                return;
            }

            // Nothing due yet: come back when the earliest one is. A bucket poked at its slice's
            // start still holds deadlines spread across the slice.
            ScheduleRetry(_deadlines.Values.Min() - now);
            return;
        }

        _firing = true;

        // The materializer is read here, on the actor, because Context is the actor's and the work
        // below leaves it.
        var materializer = Context.Materializer();

        // Piped rather than awaited, so the result arrives as a message on this actor's own thread and
        // a failure arrives the same way. A pass that ended without reporting would leave the bucket
        // believing it is still firing, and nothing would ever fire it again.
        WakeAllAsync(due, materializer).PipeTo(
            Self,
            success: settled => settled,
            failure: ex =>
            {
                _log.Warning(ex, "{0}: a firing pass ended early; what it did not settle is tried again", PersistenceId);
                return new WakesSettled([], due);
            });
    }

    /// <summary>
    /// Wakes each due instance off the actor thread, then reports back through <see cref="Self"/> so
    /// every change to this bucket's state happens on the actor. One that answers is settled; one
    /// that fails counts an attempt and stays for the next pass, so a wake lost in transit is retried
    /// inside this bucket's lifetime.
    ///
    /// The two lists are written from the stream and read on the actor after it completes, and the
    /// stage collecting them runs one element at a time, so each is touched by one thread at a time.
    /// </summary>
    private async Task<WakesSettled> WakeAllAsync(
        IReadOnlyList<WorkflowDeadlineKey> due, IMaterializer materializer)
    {
        var settled = new List<WorkflowDeadlineKey>();
        var failed = new List<WorkflowDeadlineKey>();

        // One wake per instance, however many of its deadlines came due together. The kind a wake
        // names is diagnostic: activation re-arms every deadline the instance holds and fires whatever
        // has passed, so an instance awaiting five child groups and a workflow deadline is served by
        // the same single activation the first of them would have caused.
        var byInstance = due
            .GroupBy(k => (k.WorkflowType, k.EntityId))
            .Select(g => (Instance: g.First(), Keys: g.ToList()))
            .ToList();

        await Source.From(byInstance)
            // Rate first, then concurrency: an instance answers as soon as it has recovered and only
            // then starts the work its deadline asked for, so the answer says nothing about the load
            // that follows. Throttle accounts for that; SelectAsyncUnordered adapts to how slow
            // recovery currently is, and unordered so one slow instance holds no other one back.
            .Throttle(_settings.MaxWakesPerSecond, TimeSpan.FromSeconds(1), _settings.WakeBurst, ThrottleMode.Shaping)
            .SelectAsyncUnordered(_settings.MaxWakesInFlight, async entry =>
            {
                var (instance, keys) = entry;
                try
                {
                    await _client.For(instance.WorkflowType, instance.EntityId)
                        .Wake(instance.Kind, _settings.WakeTimeout);
                    return (Keys: keys, Ok: true);
                }
                catch (Exception ex)
                {
                    _log.Debug(
                        ex, "{0}: wake for {1}/{2} did not complete",
                        PersistenceId, instance.WorkflowType, instance.EntityId);
                    return (Keys: keys, Ok: false);
                }
            })
            .RunForeach(
                // One answer settles every deadline it woke for, since the activation it caused fired
                // all of them.
                result => (result.Ok ? settled : failed).AddRange(result.Keys),
                materializer);

        return new WakesSettled(settled, failed);
    }

    private void HandleWakesSettled(WakesSettled message)
    {
        _firing = false;

        foreach (var key in message.Settled)
        {
            _deadlines.Remove(key);
            _attempts.Remove(key);
        }

        foreach (var key in message.Failed)
        {
            var attempts = _attempts.GetValueOrDefault(key) + 1;
            if (attempts >= _settings.MaxWakeAttempts)
            {
                _log.Warning(
                    "{0}: giving up on {1}/{2} after {3} attempts; the instance fires this deadline "
                    + "whenever something next activates it",
                    PersistenceId, key.WorkflowType, key.EntityId, attempts);
                _deadlines.Remove(key);
                _attempts.Remove(key);
            }
            else
            {
                _attempts[key] = attempts;
            }
        }

        if (_deadlines.Count == 0)
        {
            Drain();
        }
        else
        {
            ScheduleRetry(_settings.RetryBackoff);
        }
    }

    private void ScheduleRetry(TimeSpan delay)
    {
        _retry?.Cancel();
        _retry = Context.System.Scheduler.ScheduleTellOnceCancelable(
            delay > TimeSpan.Zero ? delay : TimeSpan.Zero, Self, new Retry(), Self);
    }

    /// <summary>
    /// Records that this bucket is finished, then releases everything it wrote. The record is what
    /// makes recovery after a crash mid-delete land on an empty bucket rather than firing the same
    /// deadlines a second time.
    /// </summary>
    private void Drain() => Persist(Internal(new BucketDrained()), _ =>
    {
        _deadlines.Clear();
        DeleteMessages(LastSequenceNr);
    });

    private void ApplyRecovered(object @event)
    {
        switch (@event)
        {
            case DeadlinePlaced placed:
                _deadlines[placed.Key] = placed.DueUtc;
                break;
            case BucketDrained:
                _deadlines.Clear();
                break;
        }
    }

    /// <summary>Every event this writes carries the engine's internal tag — see
    /// <see cref="WorkflowEventTags.Internal"/>.</summary>
    private static Tagged Internal(object @event) => new(@event, [WorkflowEventTags.Internal]);

    protected override void PostStop()
    {
        _retry?.Cancel();
        base.PostStop();
    }
}
