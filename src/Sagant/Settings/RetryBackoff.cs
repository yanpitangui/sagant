namespace Sagant.Settings;

/// <summary>
/// Ready-made <see cref="RecoverStrategy.BackoffForAttempt"/> implementations. Each factory here
/// just returns a plain <c>Func&lt;int, TimeSpan&gt;</c> — there's no interface to implement, so a
/// fully custom strategy is exactly as easy to write as picking one of these: e.g.
/// <c>.WithBackoff(attempt => TimeSpan.FromSeconds(attempt * attempt))</c>.
/// </summary>
public static class RetryBackoff
{
    /// <summary>Same delay before every retry, regardless of attempt number.</summary>
    public static Func<int, TimeSpan> Fixed(TimeSpan delay) => _ => delay;

    /// <summary>
    /// <paramref name="baseDelay"/> * <paramref name="multiplier"/>^(attempt-1), capped at
    /// <paramref name="maxDelay"/> if given. With <paramref name="jitter"/> (default on), the
    /// final delay is randomized to somewhere between 50% and 100% of that value — decorrelates
    /// retries across many entities failing around the same time (e.g. a dependency outage),
    /// avoiding a thundering-herd retry spike when it recovers.
    /// </summary>
    public static Func<int, TimeSpan> Exponential(
        TimeSpan baseDelay, double multiplier = 2.0, TimeSpan? maxDelay = null, bool jitter = true)
    {
        return attempt =>
        {
            var raw = baseDelay.TotalMilliseconds * Math.Pow(multiplier, Math.Max(0, attempt - 1));
            var capped = maxDelay is { } max ? Math.Min(raw, max.TotalMilliseconds) : raw;
            var final = jitter ? capped * (0.5 + Random.Shared.NextDouble() * 0.5) : capped;
            return TimeSpan.FromMilliseconds(final);
        };
    }
}
