using Sagant.Descriptors;
using Sagant.Settings;

namespace Sagant.Tests;

/// <summary>
/// <see cref="RetryBackoff"/>'s factories return plain <c>Func&lt;int, TimeSpan&gt;</c> delegates —
/// no ActorSystem, no scheduler, nothing Akka-specific. Same "the interesting part is a pure
/// function" story as <see cref="WorkflowTestHarnessTests"/>: the actor-level wiring (does the
/// engine actually wait, persist, resume-after-restart) is covered separately in
/// <see cref="RetryBackoffActorTests"/>, but the backoff *math* itself needs none of that.
/// </summary>
public class RetryBackoffTests
{
    [Fact]
    public void Fixed_ReturnsSameDelayRegardlessOfAttempt()
    {
        var backoff = RetryBackoff.Fixed(TimeSpan.FromSeconds(5));

        Assert.Equal(TimeSpan.FromSeconds(5), backoff(1));
        Assert.Equal(TimeSpan.FromSeconds(5), backoff(2));
        Assert.Equal(TimeSpan.FromSeconds(5), backoff(50));
    }

    [Fact]
    public void Exponential_NoJitter_GrowsByMultiplierPerAttempt()
    {
        var backoff = RetryBackoff.Exponential(TimeSpan.FromSeconds(1), multiplier: 2.0, jitter: false);

        Assert.Equal(TimeSpan.FromSeconds(1), backoff(1));
        Assert.Equal(TimeSpan.FromSeconds(2), backoff(2));
        Assert.Equal(TimeSpan.FromSeconds(4), backoff(3));
        Assert.Equal(TimeSpan.FromSeconds(8), backoff(4));
    }

    [Fact]
    public void Exponential_NoJitter_RespectsMaxDelayCap()
    {
        var backoff = RetryBackoff.Exponential(
            TimeSpan.FromSeconds(1), multiplier: 2.0, maxDelay: TimeSpan.FromSeconds(5), jitter: false);

        Assert.Equal(TimeSpan.FromSeconds(4), backoff(3));
        Assert.Equal(TimeSpan.FromSeconds(5), backoff(4)); // would be 8s uncapped
        Assert.Equal(TimeSpan.FromSeconds(5), backoff(10)); // stays capped, doesn't keep growing
    }

    [Fact]
    public void Exponential_WithJitter_StaysWithinHalfToFullOfUncappedValue()
    {
        var backoff = RetryBackoff.Exponential(TimeSpan.FromSeconds(1), multiplier: 2.0, jitter: true);

        for (var attempt = 1; attempt <= 5; attempt++)
        {
            var uncapped = TimeSpan.FromSeconds(Math.Pow(2, attempt - 1));
            var delay = backoff(attempt);
            Assert.True(delay >= uncapped * 0.5, $"attempt {attempt}: {delay} was below the 50% floor of {uncapped}");
            Assert.True(delay <= uncapped, $"attempt {attempt}: {delay} exceeded the uncapped value {uncapped}");
        }
    }

    [Fact]
    public void RecoverStrategy_WithBackoff_SetsBackoffForAttemptWithoutTouchingOtherFields()
    {
        Func<int, TimeSpan> backoff = attempt => TimeSpan.FromSeconds(attempt);

        var strategy = RecoverStrategy.WithMaxRetries(2).FailoverTo(Ref.Step<DocWorkflowFor<string>>("Compensate")).WithBackoff(backoff);

        Assert.Equal(2, strategy.MaxRetries);
        Assert.Equal("Compensate", strategy.FailoverStepName);
        Assert.Same(backoff, strategy.BackoffForAttempt);
    }

    [Fact]
    public void RecoverStrategy_WithoutWithBackoff_BackoffForAttemptIsNull()
    {
        var strategy = RecoverStrategy.WithMaxRetries(2).FailoverTo(Ref.Step<DocWorkflowFor<string>>("Compensate"));

        Assert.Null(strategy.BackoffForAttempt);
    }
}
