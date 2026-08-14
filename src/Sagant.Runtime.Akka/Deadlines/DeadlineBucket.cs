using System.Globalization;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>
/// Names the bucket an instant falls in, and walks the sequence of them.
///
/// A bucket is a slice of time holding every deadline due inside it. Truncating an instant to its
/// bucket is what lets a wake be found without an index: whatever is due at 14:32 is in the bucket
/// named for 14:32, so nothing has to remember where it was put.
///
/// Pure string and arithmetic, so the naming and the sequence are testable without a cluster.
/// </summary>
internal static class DeadlineBucket
{
    /// <summary>
    /// How much time one bucket covers. Sets both the granularity of a wake and how many entities the
    /// ticker touches: a shorter interval fires nearer the deadline and pokes more buckets, and each
    /// instance's own timer covers the remainder of the slice either way, so the wake still lands on
    /// the instant rather than on the boundary.
    /// </summary>
    public static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);

    /// <summary>The bucket <paramref name="instant"/> belongs to, as an entity id.</summary>
    public static string For(DateTimeOffset instant) =>
        Truncate(instant).UtcDateTime.ToString("yyyyMMddHHmm", CultureInfo.InvariantCulture);

    /// <summary><paramref name="instant"/> rounded down to its bucket's start.</summary>
    public static DateTimeOffset Truncate(DateTimeOffset instant) =>
        new(instant.UtcDateTime.Ticks - (instant.UtcDateTime.Ticks % Interval.Ticks), TimeSpan.Zero);

    /// <summary>The instant a bucket id names, for a ticker working out which buckets it owes.
    /// </summary>
    public static bool TryParse(string bucketId, out DateTimeOffset start)
    {
        if (DateTime.TryParseExact(
                bucketId, "yyyyMMddHHmm", CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
        {
            start = new DateTimeOffset(parsed, TimeSpan.Zero);
            return true;
        }

        start = default;
        return false;
    }

    /// <summary>
    /// Every bucket from <paramref name="after"/> up to and including the one <paramref name="now"/>
    /// falls in, oldest first, capped at <paramref name="max"/>.
    ///
    /// This is how a ticker catches up: it holds the last bucket it poked, so a process that was down
    /// for an hour walks the hour it missed rather than skipping to the present. The cap bounds one
    /// pass, and the remainder is picked up by the next.
    /// </summary>
    public static IReadOnlyList<string> Between(DateTimeOffset after, DateTimeOffset now, int max)
    {
        var buckets = new List<string>();
        var cursor = Truncate(after) + Interval;
        var last = Truncate(now);

        while (cursor <= last && buckets.Count < max)
        {
            buckets.Add(For(cursor));
            cursor += Interval;
        }

        return buckets;
    }
}
