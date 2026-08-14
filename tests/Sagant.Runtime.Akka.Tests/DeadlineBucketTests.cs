using Sagant.Runtime.Akka.Deadlines;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Bucket naming and the sequence a ticker walks. This is the whole index the bucket scheduler has:
/// a deadline's home is its own instant truncated, so nothing records where anything was put. Both
/// halves are arithmetic, and both are load-bearing — a naming that drifted would lose deadlines, and
/// a sequence that skipped would leave a slice unpoked.
/// </summary>
public class DeadlineBucketTests
{
    private static readonly DateTimeOffset Noon = new(2026, 8, 14, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void EveryInstantInASlice_NamesTheSameBucket()
    {
        var start = DeadlineBucket.For(Noon);

        Assert.Equal(start, DeadlineBucket.For(Noon.AddMilliseconds(1)));
        Assert.Equal(start, DeadlineBucket.For(Noon + DeadlineBucket.Interval - TimeSpan.FromTicks(1)));
        Assert.NotEqual(start, DeadlineBucket.For(Noon + DeadlineBucket.Interval));
    }

    [Fact]
    public void ABucketName_ReadsBackAsItsOwnStart()
    {
        Assert.True(DeadlineBucket.TryParse(DeadlineBucket.For(Noon.AddSeconds(37)), out var start));
        Assert.Equal(Noon, start);
    }

    [Fact]
    public void ANameThatIsNotABucket_IsRejected()
    {
        Assert.False(DeadlineBucket.TryParse("not-a-bucket", out _));
        Assert.False(DeadlineBucket.TryParse(string.Empty, out _));
    }

    /// <summary>
    /// Offsets are normalised away, so two nodes in different time zones name the same bucket for the
    /// same instant — which is what keeps one slice to one entity.
    /// </summary>
    [Fact]
    public void TheSameInstantInAnotherOffset_NamesTheSameBucket() =>
        Assert.Equal(
            DeadlineBucket.For(Noon),
            DeadlineBucket.For(Noon.ToOffset(TimeSpan.FromHours(-5))));

    [Fact]
    public void CatchingUp_WalksEverySliceMissed()
    {
        var owed = DeadlineBucket.Between(Noon, Noon.AddMinutes(3), max: 100);

        Assert.Equal(
            new[]
            {
                DeadlineBucket.For(Noon.AddMinutes(1)),
                DeadlineBucket.For(Noon.AddMinutes(2)),
                DeadlineBucket.For(Noon.AddMinutes(3)),
            },
            owed);
    }

    /// <summary>The slice already poked is behind us; the one now open is the last one owed.</summary>
    [Fact]
    public void CatchingUp_ExcludesTheSliceAlreadyPokedAndIncludesTheCurrentOne()
    {
        Assert.Empty(DeadlineBucket.Between(Noon, Noon.AddSeconds(30), max: 100));
        Assert.Equal(
            [DeadlineBucket.For(Noon.AddMinutes(1))],
            DeadlineBucket.Between(Noon, Noon.AddMinutes(1).AddSeconds(30), max: 100));
    }

    /// <summary>A long gap is walked in bounded passes, so one pass after a long outage stays
    /// finite and the remainder is picked up by the next.</summary>
    [Fact]
    public void CatchingUp_IsCappedPerPass()
    {
        var owed = DeadlineBucket.Between(Noon, Noon.AddDays(1), max: 10);

        Assert.Equal(10, owed.Count);
        Assert.Equal(DeadlineBucket.For(Noon.AddMinutes(1)), owed[0]);
        Assert.Equal(DeadlineBucket.For(Noon.AddMinutes(10)), owed[^1]);
    }
}
