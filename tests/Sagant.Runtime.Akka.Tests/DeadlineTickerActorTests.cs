using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Sagant.Runtime.Akka.Deadlines;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The ticker that pokes each slice as it arrives. Its one durable fact is the last bucket it
/// reached, and that fact is what makes a gap recoverable — a process down for an hour walks every
/// slice across that hour on its way back to the present, so the deadlines inside those slices still
/// fire, late but not lost.
/// </summary>
public class DeadlineTickerActorTests : TestKit
{
    public DeadlineTickerActorTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private static readonly DateTimeOffset Noon = new(2026, 8, 14, 12, 0, 0, TimeSpan.Zero);

    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        private long _ticks = now.UtcTicks;

        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

        public void Set(DateTimeOffset at) => Interlocked.Exchange(ref _ticks, at.UtcTicks);
    }

    private static IReadOnlyList<string> PokedBuckets(TestProbe buckets, int atMost, TimeSpan within)
    {
        var seen = new List<string>();
        var deadline = DateTime.UtcNow + within;
        while (seen.Count < atMost && DateTime.UtcNow < deadline)
        {
            var envelope = buckets.ReceiveOne(TimeSpan.FromMilliseconds(200)) as BucketEnvelope;
            if (envelope is not null)
            {
                seen.Add(envelope.BucketId);
            }
        }

        return seen;
    }

    [Fact]
    public void OnStart_ItPokesTheSliceNowFallsIn()
    {
        var buckets = CreateTestProbe();
        Sys.ActorOf(DeadlineTickerActor.Props(
            new WorkflowDeadlineSettings(), buckets.Ref, new FixedClock(Noon)));

        var poked = PokedBuckets(buckets, atMost: 1, TimeSpan.FromSeconds(5));
        Assert.Equal([DeadlineBucket.For(Noon)], poked);
    }

    /// <summary>
    /// The gap is walked in full. A ticker that jumped straight to the present would leave every
    /// slice it missed unpoked, and every deadline inside them would fire only when something else
    /// happened to activate its instance.
    /// </summary>
    [Fact]
    public void AfterAGap_ItWalksEverySliceItMissed()
    {
        var buckets = CreateTestProbe();
        var clock = new FixedClock(Noon);
        var ticker = Sys.ActorOf(DeadlineTickerActor.Props(
            new WorkflowDeadlineSettings(), buckets.Ref, clock));

        Assert.Equal([DeadlineBucket.For(Noon)], PokedBuckets(buckets, 1, TimeSpan.FromSeconds(5)));

        // Gone for five slices, then back.
        Watch(ticker);
        Sys.Stop(ticker);
        ExpectTerminated(ticker);

        clock.Set(Noon.AddMinutes(5));
        Sys.ActorOf(DeadlineTickerActor.Props(
            new WorkflowDeadlineSettings(), buckets.Ref, clock));

        var poked = PokedBuckets(buckets, 5, TimeSpan.FromSeconds(10));
        Assert.Equal(
            new[] { 1, 2, 3, 4, 5 }.Select(m => DeadlineBucket.For(Noon.AddMinutes(m))),
            poked);
    }

    /// <summary>
    /// A backlog is taken in bounded passes, so one pass after a long outage stays finite. What is
    /// left over is picked up by the next.
    /// </summary>
    [Fact]
    public void AVeryLongGap_IsWalkedInBoundedPasses()
    {
        var buckets = CreateTestProbe();
        var clock = new FixedClock(Noon);
        var settings = new WorkflowDeadlineSettings { MaxBucketCatchUp = 3 };
        var ticker = Sys.ActorOf(DeadlineTickerActor.Props(settings, buckets.Ref, clock));

        Assert.Equal([DeadlineBucket.For(Noon)], PokedBuckets(buckets, 1, TimeSpan.FromSeconds(5)));

        Watch(ticker);
        Sys.Stop(ticker);
        ExpectTerminated(ticker);

        clock.Set(Noon.AddHours(1));
        Sys.ActorOf(DeadlineTickerActor.Props(settings, buckets.Ref, clock));

        // One pass takes three, oldest first — the whole hour's worth waits for the next.
        var poked = PokedBuckets(buckets, 3, TimeSpan.FromSeconds(10));
        Assert.Equal(
            new[] { 1, 2, 3 }.Select(m => DeadlineBucket.For(Noon.AddMinutes(m))),
            poked);
    }

    /// <summary>
    /// A ticker with no history starts from the current slice. Buckets written before one ever ran
    /// belong to a deployment that had no ticker, and walking back to find them has no end.
    /// </summary>
    [Fact]
    public void WithNoHistory_ItDoesNotWalkBackwards()
    {
        var buckets = CreateTestProbe();
        Sys.ActorOf(DeadlineTickerActor.Props(
            new WorkflowDeadlineSettings(), buckets.Ref, new FixedClock(Noon)));

        var poked = PokedBuckets(buckets, atMost: 3, TimeSpan.FromSeconds(2));
        Assert.Equal([DeadlineBucket.For(Noon)], poked);
    }
}
