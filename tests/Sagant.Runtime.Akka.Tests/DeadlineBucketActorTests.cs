using Akka.Actor;
using Akka.TestKit.Xunit2;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Deadlines;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The bucket holding one slice of time, driven directly here, with no sharding in between — its
/// recovery path, its retry of a wake nobody answered, and the point at which it lets an entry go.
/// None of that runs in the end-to-end test, which only ever sees a bucket that works.
/// </summary>
public class DeadlineBucketActorTests : TestKit
{
    public DeadlineBucketActorTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private static readonly DateTimeOffset Noon = new(2026, 8, 14, 12, 0, 0, TimeSpan.Zero);

    private static WorkflowDeadlineSettings Settings(int attempts = 5) => new()
    {
        RetryBackoff = TimeSpan.FromMilliseconds(50),
        MaxRetryBackoff = TimeSpan.FromMilliseconds(100),
        WakeTimeout = TimeSpan.FromSeconds(2),
        MaxWakeAttempts = attempts,
    };

    private static WorkflowDeadlineKey Key(string entityId) =>
        new("TestWorkflow", entityId, WorkflowTimerKind.Pause);

    private IActorRef Bucket(
        string bucketId, IWorkflowClient client, WorkflowDeadlineSettings? settings = null,
        FixedClock? clock = null) =>
        Sys.ActorOf(DeadlineBucketActor.Props(
            bucketId, settings ?? Settings(), client, clock ?? new FixedClock(Noon)));

    /// <summary>A clock a test moves explicitly, so "due" is decided by the test itself, immune to
    /// wall-clock drift during it.</summary>
    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        private long _ticks = now.UtcTicks;

        public override DateTimeOffset GetUtcNow() =>
            new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

        public void Set(DateTimeOffset at) => Interlocked.Exchange(ref _ticks, at.UtcTicks);
    }

    [Fact]
    public void ADeadlineAlreadyDue_WakesItsInstance()
    {
        var client = new RecordingWakeClient();
        var bucket = Bucket("202608141200", client);

        bucket.Tell(new BucketCommands.Place(Key("order-1"), Noon.AddMinutes(-1)), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(() => Assert.Contains("order-1", client.Woken), TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// A bucket takes responsibility for its own contents the moment it holds any: the ticker has no
    /// reason to come back to a slice it has already passed, so an entry arriving afterwards would
    /// otherwise sit there.
    /// </summary>
    [Fact]
    public void ADeadlinePlacedIntoAnAlreadyPokedSlice_StillWakes()
    {
        var client = new RecordingWakeClient();
        var bucket = Bucket("202608141200", client);

        bucket.Tell(BucketCommands.Poke.Instance);
        bucket.Tell(new BucketCommands.Place(Key("order-late"), Noon.AddMinutes(-1)), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(() => Assert.Contains("order-late", client.Woken), TimeSpan.FromSeconds(5));
    }

    /// <summary>An entry whose wake goes unanswered stays and is tried again, which is what bounds a
    /// wake lost in transit to this bucket's lifetime.</summary>
    [Fact]
    public void AWakeThatFails_IsTriedAgain()
    {
        var client = new RecordingWakeClient { FailUntilAttempt = 3 };
        var bucket = Bucket("202608141200", client);

        bucket.Tell(new BucketCommands.Place(Key("order-flaky"), Noon.AddMinutes(-1)), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(
            () => Assert.True(client.Attempts >= 3, $"only {client.Attempts} attempts"),
            TimeSpan.FromSeconds(10));
        AwaitAssert(() => Assert.Contains("order-flaky", client.Woken), TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Attempts are bounded, so an instance that never answers stops costing wakes. It falls back to
    /// guarantee <c>D8</c>: its deadline fires whenever something next activates it.
    /// </summary>
    [Fact]
    public void AWakeThatNeverSucceeds_IsGivenUpOnAfterItsAttempts()
    {
        var client = new RecordingWakeClient { FailUntilAttempt = int.MaxValue };
        var bucket = Bucket("202608141200", client, Settings(attempts: 3));

        bucket.Tell(new BucketCommands.Place(Key("order-dead"), Noon.AddMinutes(-1)), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(
            () => Assert.True(client.Attempts >= 3, $"only {client.Attempts} attempts"),
            TimeSpan.FromSeconds(10));

        // Stops climbing once the budget is spent — no retrying forever.
        var settled = client.Attempts;
        Thread.Sleep(500);
        Assert.True(client.Attempts <= settled + 1, $"kept trying: {settled} then {client.Attempts}");
    }

    /// <summary>
    /// Placing the same key at the same instant twice is the ordinary case — a projection replaying
    /// its stream repeats every arm it has already made — and must not write a second time.
    /// </summary>
    [Fact]
    public void PlacingTheSameDeadlineTwice_IsAnswered()
    {
        var client = new RecordingWakeClient();
        var bucket = Bucket("202608141300", client);
        var due = Noon.AddHours(1);

        bucket.Tell(new BucketCommands.Place(Key("order-1"), due), TestActor);
        ExpectMsg<Done>();
        bucket.Tell(new BucketCommands.Place(Key("order-1"), due), TestActor);
        ExpectMsg<Done>();

        bucket.Tell(BucketCommands.GetCount.Instance, TestActor);
        Assert.Equal(1, ExpectMsg<int>());
    }

    /// <summary>
    /// A bucket that comes back holding a deadline whose instant has passed fires it, which is what
    /// makes a slice survive the process that wrote it.
    /// </summary>
    [Fact]
    public void ABucketThatRecoversHoldingADueDeadline_WakesIt()
    {
        const string bucketId = "202608141200";

        // Placed ahead of the clock, so it is written and nothing fires — the entry is still there
        // when the process holding it goes away, which is the case recovery exists for.
        var first = Bucket(bucketId, new RecordingWakeClient(), clock: new FixedClock(Noon));
        first.Tell(new BucketCommands.Place(Key("order-restart"), Noon.AddMinutes(1)), TestActor);
        ExpectMsg<Done>();

        Watch(first);
        Sys.Stop(first);
        ExpectTerminated(first);

        // Comes back past the instant it was holding, so recovery alone is what fires it.
        var client = new RecordingWakeClient();
        Bucket(bucketId, client, clock: new FixedClock(Noon.AddMinutes(2)));

        AwaitAssert(() => Assert.Contains("order-restart", client.Woken), TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Disarming does nothing, deliberately. Finding a placed deadline again would need the
    /// key-to-bucket mapping the design exists to avoid, so a deadline that moves is placed again in
    /// its new bucket and the old entry is left to expire — the wake it eventually causes activates an
    /// instance that re-derives its own deadline and goes quiet again. Pinned here because it reads
    /// like an oversight and is not.
    /// </summary>
    [Fact]
    public async Task Disarming_LeavesTheEntryWhereItIs()
    {
        var client = new RecordingWakeClient();
        var clock = new FixedClock(Noon);
        var bucket = Bucket("202608141300", client, clock: clock);
        var scheduler = new BucketEntityDeadlineScheduler(bucket, TimeSpan.FromSeconds(5));

        bucket.Tell(new BucketCommands.Place(Key("order-moved"), Noon.AddHours(1)), TestActor);
        ExpectMsg<Done>();

        await scheduler.DisarmAsync(Key("order-moved"));

        bucket.Tell(BucketCommands.GetCount.Instance, TestActor);
        Assert.Equal(1, ExpectMsg<int>());
    }

    /// <summary>
    /// One wake per instance however many of its deadlines came due together — activation re-arms
    /// every deadline it holds, so the rest would find nothing to do.
    /// </summary>
    [Fact]
    public void SeveralDeadlinesOfOneInstance_ProduceOneWake()
    {
        var client = new RecordingWakeClient();
        var clock = new FixedClock(Noon);
        var bucket = Bucket("202608141200", client, clock: clock);

        // Placed while still ahead of the clock, so all three are held before any of them is due —
        // otherwise each placement would start a firing pass of its own and the coalescing this is
        // about would never be exercised.
        foreach (var kind in new[] { WorkflowTimerKind.Pause, WorkflowTimerKind.Workflow, WorkflowTimerKind.Hold })
        {
            bucket.Tell(
                new BucketCommands.Place(new WorkflowDeadlineKey("TestWorkflow", "order-many", kind), Noon.AddMinutes(1)),
                TestActor);
            ExpectMsg<Done>();
        }

        clock.Set(Noon.AddMinutes(2));
        bucket.Tell(BucketCommands.Poke.Instance);

        AwaitAssert(() => Assert.Contains("order-many", client.Woken), TimeSpan.FromSeconds(5));
        Thread.Sleep(300);
        Assert.Equal(1, client.Woken.Count(w => w == "order-many"));
    }
}
