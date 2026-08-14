using Cronos;

namespace Sagant.Scheduling;

/// <summary>
/// When a schedule fires next.
///
/// A pure function of the previous occurrence, holding no clock of its own, so the answer is the same
/// whether it runs as a schedule fires or during a replay years later — the same property the engine's
/// own folds rest on. That is what lets a schedule compute one absolute instant, record it, and be
/// woken for exactly that.
///
/// Implementations must be strictly monotonic: an answer at or before <c>previous</c> would leave a
/// schedule computing the same occurrence forever. Every one shipped here rejects a spec that could
/// do so at construction.
/// </summary>
public interface IScheduleSpec
{
    /// <summary>The first occurrence strictly after <paramref name="previous"/>, or <c>null</c> when
    /// the schedule has no more.</summary>
    DateTimeOffset? NextAfter(DateTimeOffset previous);
}

/// <summary>Fires at a fixed spacing — "every fifteen minutes", counted from the previous
/// occurrence rather than from when it actually ran, so a slow fire does not push the schedule
/// later.</summary>
public sealed record EverySpec : IScheduleSpec
{
    public EverySpec(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(interval),
                "Must be greater than zero: an interval of zero would compute the same occurrence forever.");
        }

        Interval = interval;
    }

    public TimeSpan Interval { get; }

    public DateTimeOffset? NextAfter(DateTimeOffset previous) => previous + Interval;
}

/// <summary>
/// Fires once a day at a wall-clock time in a named zone — "02:00 in Europe/Lisbon", which is a
/// different instant in summer than in winter.
/// </summary>
public sealed record DailyAtSpec : IScheduleSpec
{
    private readonly CronExpression _expression;

    public DailyAtSpec(TimeOnly at, TimeZoneInfo zone)
    {
        At = at;
        Zone = zone;
        _expression = CronExpression.Parse($"{at.Minute} {at.Hour} * * *");
    }

    public TimeOnly At { get; }

    public TimeZoneInfo Zone { get; }

    public DateTimeOffset? NextAfter(DateTimeOffset previous) =>
        _expression.GetNextOccurrence(previous, Zone, inclusive: false);
}

/// <summary>
/// Fires on a cron expression in a named zone. Standard five-field expressions, and six-field with
/// seconds leading.
///
/// The zone is where the awkward cases live: on a spring-forward boundary an hour is deleted, and on
/// a fall-back one it repeats. Cronos resolves both, so a schedule reading "02:30 daily" behaves the
/// same way a person reading it would expect on the two days a year it is ambiguous.
/// </summary>
public sealed record CronSpec : IScheduleSpec
{
    private readonly CronExpression _expression;

    public CronSpec(string expression, TimeZoneInfo zone)
    {
        Expression = expression;
        Zone = zone;
        _expression = CronExpression.Parse(
            expression,
            expression.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length >= 6
                ? CronFormat.IncludeSeconds
                : CronFormat.Standard);
    }

    public string Expression { get; }

    public TimeZoneInfo Zone { get; }

    public DateTimeOffset? NextAfter(DateTimeOffset previous) =>
        _expression.GetNextOccurrence(previous, Zone, inclusive: false);
}

/// <summary>Fires once, at a fixed instant — a delayed start rather than a recurrence.</summary>
public sealed record OnceAtSpec(DateTimeOffset At) : IScheduleSpec
{
    public DateTimeOffset? NextAfter(DateTimeOffset previous) => At > previous ? At : null;
}
