using Sagant.Scheduling;

namespace Sagant.Scheduling.Tests;

/// <summary>
/// The arithmetic every schedule rests on. Each spec is a pure function of the previous occurrence,
/// so these need no workflow, no clock and no runtime — which is the point: the awkward cases are
/// calendar cases, and they are cheapest to pin down here.
/// </summary>
public class ScheduleSpecTests
{
    private static readonly DateTimeOffset Noon = new(2026, 8, 14, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void EverySpec_CountsFromThePreviousOccurrence()
    {
        var spec = new EverySpec(TimeSpan.FromMinutes(15));

        Assert.Equal(Noon.AddMinutes(15), spec.NextAfter(Noon));
        Assert.Equal(Noon.AddMinutes(30), spec.NextAfter(Noon.AddMinutes(15)));
    }

    /// <summary>
    /// A zero interval would answer with the instant it was given, so a schedule computing its next
    /// occurrence would never move. Rejected where it is written rather than where it would spin.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void EverySpec_RejectsAnIntervalThatCouldNotAdvance(int seconds) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new EverySpec(TimeSpan.FromSeconds(seconds)));

    /// <summary>Sub-second intervals are allowed: they never reach the durable scheduler, since the
    /// instance holding one stays resident and its own timer fires it.</summary>
    [Fact]
    public void EverySpec_AllowsSubSecondIntervals() =>
        Assert.Equal(
            Noon.AddMilliseconds(250),
            new EverySpec(TimeSpan.FromMilliseconds(250)).NextAfter(Noon));

    [Fact]
    public void OnceAtSpec_FiresOnceAndThenHasNothingLeft()
    {
        var spec = new OnceAtSpec(Noon.AddHours(1));

        Assert.Equal(Noon.AddHours(1), spec.NextAfter(Noon));
        Assert.Null(spec.NextAfter(Noon.AddHours(1)));
        Assert.Null(spec.NextAfter(Noon.AddHours(2)));
    }

    [Fact]
    public void CronSpec_ReadsStandardFiveFieldExpressions() =>
        Assert.Equal(
            new DateTimeOffset(2026, 8, 15, 2, 0, 0, TimeSpan.Zero),
            new CronSpec("0 2 * * *", TimeZoneInfo.Utc).NextAfter(Noon));

    [Fact]
    public void CronSpec_ReadsSixFieldExpressionsWithSeconds() =>
        Assert.Equal(
            Noon.AddSeconds(30),
            new CronSpec("30 * * * * *", TimeZoneInfo.Utc).NextAfter(Noon));

    [Fact]
    public void DailyAtSpec_FiresAtThatWallClockTime() =>
        Assert.Equal(
            new DateTimeOffset(2026, 8, 15, 2, 30, 0, TimeSpan.Zero),
            new DailyAtSpec(new TimeOnly(2, 30), TimeZoneInfo.Utc).NextAfter(Noon));

    /// <summary>
    /// The case that makes a calendar spec worth a dependency rather than hand-rolling. In New York,
    /// 2026-03-08 runs 01:59:59 straight to 03:00:00, so a 02:30 daily schedule has no 02:30 that
    /// day — and the answer has to be a real instant, not a time that did not happen.
    /// </summary>
    [Fact]
    public void DailyAtSpec_HandlesADayWhereThatTimeDoesNotExist()
    {
        var newYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        var spec = new DailyAtSpec(new TimeOnly(2, 30), newYork);

        // Just after the 2026-03-07 occurrence, so the next one falls on the day the hour is skipped.
        var next = spec.NextAfter(new DateTimeOffset(2026, 3, 7, 8, 0, 0, TimeSpan.Zero));

        Assert.NotNull(next);
        var local = TimeZoneInfo.ConvertTime(next!.Value, newYork);
        Assert.True(
            local.Date > new DateTime(2026, 3, 7),
            $"expected an occurrence after the skipped hour, got {local:O}");
    }

    /// <summary>
    /// The other half: on 2026-11-01 New York runs 01:00–02:00 twice, so a 01:30 daily schedule has
    /// two candidate instants. One occurrence is the right answer, whichever it picks.
    /// </summary>
    [Fact]
    public void DailyAtSpec_FiresOnceOnADayWhereThatTimeHappensTwice()
    {
        var newYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        var spec = new DailyAtSpec(new TimeOnly(1, 30), newYork);

        var first = spec.NextAfter(new DateTimeOffset(2026, 10, 31, 12, 0, 0, TimeSpan.Zero));
        Assert.NotNull(first);

        var second = spec.NextAfter(first!.Value);
        Assert.NotNull(second);
        Assert.True(second > first, "occurrences must advance");

        var firstLocal = TimeZoneInfo.ConvertTime(first.Value, newYork);
        var secondLocal = TimeZoneInfo.ConvertTime(second!.Value, newYork);
        Assert.NotEqual(firstLocal.Date, secondLocal.Date);
    }

    /// <summary>
    /// Strict monotonicity is what lets a schedule walk forward past a gap and terminate. Asserted
    /// across every spec, since one that answered with its own input would loop.
    /// </summary>
    [Theory]
    [MemberData(nameof(EverySpecShipped))]
    public void EverySpecShipped_AdvancesStrictly(IScheduleSpec spec)
    {
        var cursor = Noon;
        for (var i = 0; i < 50; i++)
        {
            var next = spec.NextAfter(cursor);
            if (next is null)
            {
                return;
            }

            Assert.True(next > cursor, $"{spec.GetType().Name} answered {next:O} from {cursor:O}");
            cursor = next.Value;
        }
    }

    public static TheoryData<IScheduleSpec> EverySpecShipped() => new()
    {
        new EverySpec(TimeSpan.FromSeconds(1)),
        new EverySpec(TimeSpan.FromDays(30)),
        new OnceAtSpec(Noon.AddDays(1)),
        new CronSpec("0 2 * * *", TimeZoneInfo.Utc),
        new CronSpec("*/5 * * * *", TimeZoneInfo.Utc),
        new DailyAtSpec(new TimeOnly(2, 30), TimeZoneInfo.Utc),
        new DailyAtSpec(new TimeOnly(2, 30), TimeZoneInfo.FindSystemTimeZoneById("America/New_York")),
    };
}
