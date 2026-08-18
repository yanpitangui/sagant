using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Sagant.Runtime.Akka.Clustering;
using Akka.Persistence.Query;
using Akka.Streams;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>How far the projection has read. Recorded so a restart resumes from here.</summary>
internal sealed record ProjectionAdvanced(long Offset);

/// <summary>
/// Owns the deadline projection and remembers how far it has read.
///
/// A cluster singleton, for two reasons that both come down to the journal. One reader means the
/// stream is read once, total, across the cluster, and one reader means a single position to
/// remember — two readers would each hold their own, and neither would be the truth.
///
/// The position is what keeps a restart cheap. Reading from the start is right exactly once, when
/// there is no position yet and instances may already be waiting; every start after that resumes
/// from the recorded position, so the cost of coming back is only the events since the last one.
/// </summary>
internal sealed class DeadlineProjectionHostActor : ReceivePersistentActor
{
    private sealed record Started(IKillSwitch KillSwitch);

    private sealed record StartFailed(Exception Cause);

    private readonly WorkflowDeadlineSettings _settings;
    private readonly Func<WorkflowDeadlineProjection> _projectionFactory;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private long? _offset;
    private IKillSwitch? _killSwitch;
    private long _sinceSnapshot;

    public override string PersistenceId => "sagant-deadline-projection";

    public DeadlineProjectionHostActor(
        WorkflowDeadlineSettings settings, Func<WorkflowDeadlineProjection> projectionFactory)
    {
        _settings = settings;
        _projectionFactory = projectionFactory;

        // See DeadlineBucketActor: a journal that indexes tags lifts them off on the way to recovery
        // and hands back the plain payload; one with no tag support hands back the Tagged wrapper as
        // written. Both shapes are handled here.
        Recover<Tagged>(t =>
        {
            if (t.Payload is ProjectionAdvanced advanced)
            {
                _offset = advanced.Offset;
            }
        });
        Recover<ProjectionAdvanced>(e => _offset = e.Offset);
        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is long offset)
            {
                _offset = offset;
            }
        });
        Recover<RecoveryCompleted>(_ => Start());

        Command<ProjectionAdvanced>(HandleAdvanced);
        Command<Started>(started => _killSwitch = started.KillSwitch);
        Command<StartFailed>(failed =>
            _log.Error(
                failed.Cause,
                "{0}: the deadline projection could not start. Deadlines already recorded still fire; "
                + "ones recorded from here are not, until this is running again.",
                PersistenceId));
        Command<SaveSnapshotSuccess>(msg => DeleteMessages(msg.Metadata.SequenceNr));
        Command<SaveSnapshotFailure>(msg =>
            _log.Warning(msg.Cause, "{0}: snapshot failed; the journal keeps growing until one lands", PersistenceId));
        Command<DeleteMessagesSuccess>(_ => { });
        Command<DeleteMessagesFailure>(msg =>
            _log.Warning(msg.Cause, "{0}: could not release recorded positions", PersistenceId));
    }

    public static Props Props(
        WorkflowDeadlineSettings settings, Func<WorkflowDeadlineProjection> projectionFactory) =>
        global::Akka.Actor.Props.Create(() => new DeadlineProjectionHostActor(settings, projectionFactory));

    private void Start()
    {
        var self = Self;
        var resumeFrom = _offset is { } offset ? Offset.Sequence(offset) : null;

        if (resumeFrom is null)
        {
            _log.Info(
                "{0}: no recorded position, so the history is read once to find instances already waiting",
                PersistenceId);
        }

        _projectionFactory()
            .RunAsync(
                resumeFrom,
                onProgress: applied => Task.FromResult(Advance(self, applied)))
            .PipeTo(self, success: sw => new Started(sw), failure: ex => new StartFailed(ex));
    }

    /// <summary>
    /// Reports an applied offset to this actor, so the position is recorded on the actor's own
    /// thread, the same as everything else it holds.
    /// </summary>
    private static NotUsed Advance(IActorRef self, Offset applied)
    {
        if (applied is Sequence sequence)
        {
            self.Tell(new ProjectionAdvanced(sequence.Value));
        }

        return NotUsed.Instance;
    }

    private void HandleAdvanced(ProjectionAdvanced advanced)
    {
        // Only ever forward. A stream that restarted from a position already passed would otherwise
        // walk the recorded one backwards.
        if (_offset is { } held && advanced.Offset <= held)
        {
            return;
        }

        Persist(new Tagged(advanced, [WorkflowEventTags.Internal]), tagged =>
        {
            var e = (ProjectionAdvanced)tagged.Payload;
            _offset = e.Offset;

            // Only the latest position matters, so the history behind it is noise. Snapshotting keeps
            // recovery to one read however long this has been running.
            if (++_sinceSnapshot >= _settings.ProjectionCheckpointEvery)
            {
                _sinceSnapshot = 0;
                SaveSnapshot(e.Offset);
            }
        });
    }

    protected override void PostStop()
    {
        _killSwitch?.Shutdown();
        base.PostStop();
    }
}
