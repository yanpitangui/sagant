using System.Collections.Concurrent;
using Akka.Event;
using Akka.Persistence.Query;
using Akka.Persistence.Query.InMemory;
using Akka.Persistence.Journal;
using Akka.Streams;
using Akka.TestKit.Xunit2;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Deadlines;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The projection reading deadlines out of the journal. Its lanes are what let unrelated instances
/// proceed in parallel, and the ordering inside one lane is the whole reason they are hashed rather
/// than round-robined: applying an instance's disarm before its arm leaves a wake for a deadline
/// that is gone, and nothing later notices.
/// </summary>
public class WorkflowDeadlineProjectionTests : TestKit
{
    public WorkflowDeadlineProjectionTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private static readonly TransitionCause Cause = new TransitionCause.Control("Test");

    /// <summary>Records what the projection asked for, in the order it asked.</summary>
    private sealed class RecordingScheduler : IWorkflowDeadlineScheduler
    {
        public ConcurrentQueue<(WorkflowDeadlineKey Key, DateTimeOffset? Due)> Calls { get; } = new();

        public Task ArmAsync(WorkflowDeadlineKey key, DateTimeOffset dueUtc, CancellationToken ct = default)
        {
            Calls.Enqueue((key, dueUtc));
            return Task.CompletedTask;
        }

        public Task DisarmAsync(WorkflowDeadlineKey key, CancellationToken ct = default)
        {
            Calls.Enqueue((key, null));
            return Task.CompletedTask;
        }
    }

    /// <summary>Writes tagged events straight into the journal, standing in for the entity actor.</summary>
    private sealed class Writer(string persistenceId) : global::Akka.Persistence.UntypedPersistentActor
    {
        public override string PersistenceId => persistenceId;

        protected override void OnCommand(object message)
        {
            var sender = Sender;
            Persist(
                new Tagged(message, WorkflowEventTags.ForDeadlineEvent("TestWorkflow")),
                _ => sender.Tell(Done.Instance, Self));
        }

        protected override void OnRecover(object message)
        {
        }
    }

    private void Write(string entityId, params WorkflowEvent[] events)
    {
        var writer = Sys.ActorOf(global::Akka.Actor.Props.Create(() => new Writer($"TestWorkflow-{entityId}")));
        foreach (var e in events)
        {
            writer.Tell(e, TestActor);
            ExpectMsg<Done>(TimeSpan.FromSeconds(5));
        }
    }

    private WorkflowDeadlineProjection Projection(RecordingScheduler scheduler, int lanes) =>
        new(PersistenceQuery.Get(Sys).ReadJournalFor<InMemoryReadJournal>(InMemoryReadJournal.Identifier),
            Sys.Materializer(),
            scheduler,
            new WorkflowDeadlineSettings { ExternalArmThreshold = TimeSpan.FromSeconds(1), ProjectionLanes = lanes },
            TimeProvider.System,
            Logging.GetLogger(Sys, typeof(WorkflowDeadlineProjectionTests)));

    private static List<(WorkflowDeadlineKey Key, DateTimeOffset? Due)> PauseCalls(
        RecordingScheduler scheduler, string entityId) =>
        scheduler.Calls
            .Where(c => c.Key.EntityId == entityId && c.Key.Kind == WorkflowTimerKind.Pause)
            .ToList();

    /// <summary>
    /// The invariant the lanes exist to protect. An instance's arm and the disarm that follows it are
    /// applied in that order, so the last word about that instance is the one that stands — with one
    /// lane and with many, since hashing puts one instance's events in one lane either way.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    public void AnInstancesOwnEventsAreAppliedInOrder(int lanes)
    {
        var scheduler = new RecordingScheduler();

        // Started before the writes, so these arrive through the live phase. The backfill folds
        // history instead of replaying it, so an instance that paused and resumed before this ran
        // contributes nothing — which is a different question, covered below.
        Projection(scheduler, lanes).RunAsync().Wait();

        Write("order-ordered",
            new WorkflowEvent.RunPaused("waiting", DateTimeOffset.UtcNow.AddHours(4), "OnTimeout", null, Cause),
            new WorkflowEvent.RunResumed(null, Cause));

        // Resuming ends the pause and any hold at once, so the resume contributes two disarms — the
        // pause ones are what this is about.
        AwaitAssert(
            () => Assert.Equal(2, PauseCalls(scheduler, "order-ordered").Count),
            TimeSpan.FromSeconds(10));

        var forInstance = PauseCalls(scheduler, "order-ordered");
        Assert.NotNull(forInstance[0].Due);
        Assert.Null(forInstance[1].Due);
    }

    /// <summary>
    /// Several instances at once, each of whose events must stay in their own order. This is what a
    /// round-robin partition would break while still looking correct for a single instance.
    /// </summary>
    [Fact]
    public void ManyInstancesInterleaved_EachKeepsItsOwnOrder()
    {
        var scheduler = new RecordingScheduler();
        var ids = Enumerable.Range(0, 12).Select(i => $"order-{i}").ToList();

        Projection(scheduler, lanes: 16).RunAsync().Wait();

        foreach (var id in ids)
        {
            Write(id,
                new WorkflowEvent.RunPaused("waiting", DateTimeOffset.UtcNow.AddHours(4), "OnTimeout", null, Cause),
                new WorkflowEvent.RunResumed(null, Cause));
        }

        AwaitAssert(
            () => Assert.All(ids, id => Assert.Equal(2, PauseCalls(scheduler, id).Count)),
            TimeSpan.FromSeconds(30));

        foreach (var id in ids)
        {
            var forInstance = PauseCalls(scheduler, id);
            Assert.NotNull(forInstance[0].Due);
            Assert.Null(forInstance[1].Due);
        }
    }

    /// <summary>
    /// A deadline inside the threshold belongs to the instance's own timer, so the projection retires
    /// whatever was recorded for it rather than recording a new one.
    /// </summary>
    [Fact]
    public void ADeadlineInsideTheThreshold_IsRetiredRatherThanRecorded()
    {
        var scheduler = new RecordingScheduler();
        Projection(scheduler, lanes: 4).RunAsync().Wait();

        Write("order-near",
            new WorkflowEvent.RunPaused("waiting", DateTimeOffset.UtcNow.AddMilliseconds(200), "OnTimeout", null, Cause));

        AwaitAssert(
            () => Assert.Contains(scheduler.Calls, c => c.Key.EntityId == "order-near"),
            TimeSpan.FromSeconds(10));

        Assert.All(PauseCalls(scheduler, "order-near"), c => Assert.Null(c.Due));
    }

    /// <summary>
    /// Reading from the start of the journal is what lets this be switched on for a deployment that
    /// is already running: instances that recorded a deadline before it existed are found.
    /// </summary>
    [Fact]
    public void ItFindsDeadlinesWrittenBeforeItStarted()
    {
        var scheduler = new RecordingScheduler();
        Write("order-backfill",
            new WorkflowEvent.RunPaused("waiting", DateTimeOffset.UtcNow.AddHours(4), "OnTimeout", null, Cause));

        // Started only after the write, which is the case a running deployment presents.
        Projection(scheduler, lanes: 4).RunAsync().Wait();

        // Recording is at-least-once: the live phase resumes at the offset the fold reached, and a
        // journal whose offset includes that event applies it a second time. An arm carries the
        // instant it is due, so a repeat records the same instant and the bucket holding it already
        // treats that as the write it has (see DeadlineBucketActor.HandlePlace). What this asserts is
        // that the instance was found and every arm for it names a deadline.
        AwaitAssert(
            () => Assert.NotEmpty(PauseCalls(scheduler, "order-backfill")),
            TimeSpan.FromSeconds(10));

        Assert.All(PauseCalls(scheduler, "order-backfill"), c => Assert.NotNull(c.Due));
    }

    /// <summary>
    /// The reason the history is folded rather than replayed. An instance that paused and later
    /// finished is not waiting for anything, so nothing is recorded for it — where replaying its
    /// events one at a time would record the pause, wake it, and only then read the event saying it
    /// had finished. Over a journal of any age that is a wake for a large share of everything that
    /// ever ran.
    /// </summary>
    [Fact]
    public void HistoryThatEndedIsNotRecordedAtAll()
    {
        var scheduler = new RecordingScheduler();

        Write("order-done",
            new WorkflowEvent.RunPaused("waiting", DateTimeOffset.UtcNow.AddHours(4), "OnTimeout", null, Cause),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, Cause));

        Projection(scheduler, lanes: 4).RunAsync().Wait();

        Thread.Sleep(500);
        Assert.DoesNotContain(scheduler.Calls, c => c.Key.EntityId == "order-done");
    }

    /// <summary>
    /// Resuming from a recorded position leaves what came before it alone, which is what keeps a
    /// restart from costing a journal that only grows.
    /// </summary>
    [Fact]
    public void ResumingFromAPosition_LeavesEarlierHistoryAlone()
    {
        var seen = new RecordingScheduler();
        Write("order-early",
            new WorkflowEvent.RunPaused("waiting", DateTimeOffset.UtcNow.AddHours(4), "OnTimeout", null, Cause));

        // Reads the history, so this one knows the instance.
        Projection(seen, lanes: 4).RunAsync().Wait();
        AwaitAssert(
            () => Assert.Contains(seen.Calls, c => c.Key.EntityId == "order-early"),
            TimeSpan.FromSeconds(10));

        // Resumed past everything written so far, so the same instance is not seen again.
        var resumed = new RecordingScheduler();
        Projection(resumed, lanes: 4).RunAsync(Offset.Sequence(long.MaxValue - 1)).Wait();

        Thread.Sleep(500);
        Assert.DoesNotContain(resumed.Calls, c => c.Key.EntityId == "order-early");
    }

    /// <summary>A group's arm carries its id, so two groups on one instance stay apart.</summary>
    [Fact]
    public void AGroupsArmCarriesItsGroupId()
    {
        var scheduler = new RecordingScheduler();
        Projection(scheduler, lanes: 4).RunAsync().Wait();

        var group = new ChildGroupState(
            "items", Generation: 0, Sagant.Effects.CompletionPolicy.AllSuccessful,
            Sagant.Effects.FailurePolicy.FailFast, Sagant.Effects.RemainingChildrenPolicy.Terminate,
            "OnDone", Finalized: false, DateTimeOffset.UtcNow.AddHours(4), "OnLate");

        Write("order-groups", new WorkflowEvent.ChildrenAwaited("items", [], group, 1, null, Cause));

        AwaitAssert(() =>
        {
            var arm = Assert.Single(scheduler.Calls.Where(c =>
                c.Key.EntityId == "order-groups" && c.Key.Kind == WorkflowTimerKind.ChildGroup && c.Due is not null));
            Assert.Equal("items", arm.Key.Discriminator);
        }, TimeSpan.FromSeconds(10));
    }
}
