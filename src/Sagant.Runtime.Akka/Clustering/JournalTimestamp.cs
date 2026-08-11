namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Reads the timestamp an <c>EventEnvelope</c> carries, whose unit varies by journal plugin: the SQL
/// plugins record Unix milliseconds, while the in-memory journal records <c>DateTime.Ticks</c>.
///
/// The two ranges never overlap for any real instant — ticks for any date past 1970 run to roughly
/// 6.2×10^17, four orders of magnitude beyond the largest millisecond value
/// <see cref="DateTimeOffset"/> accepts — so the magnitude identifies the unit unambiguously.
/// </summary>
internal static class JournalTimestamp
{
    /// <summary>Largest value <see cref="DateTimeOffset.FromUnixTimeMilliseconds"/> accepts, which is
    /// year 9999. Anything above it is a tick count.</summary>
    private const long MaxUnixMilliseconds = 253402300799999L;

    public static DateTimeOffset Read(long value) =>
        value > MaxUnixMilliseconds
            ? new DateTimeOffset(value, TimeSpan.Zero)
            : DateTimeOffset.FromUnixTimeMilliseconds(value);
}
