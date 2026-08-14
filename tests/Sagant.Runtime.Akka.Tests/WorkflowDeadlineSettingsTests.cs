using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Deadlines;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The settings behind the deadline machinery, and the two numbers whose relationship decides whether
/// a deadline is ever recorded at all.
/// </summary>
public class WorkflowDeadlineSettingsTests
{
    /// <summary>
    /// The threshold has to sit below the passivation window, or a deadline landing between the two is
    /// left to an instance that is no longer there to fire it. Both defaults ship from this repo, so
    /// their relationship is worth pinning rather than leaving to whoever edits one of them.
    /// </summary>
    [Fact]
    public void TheDefaultThreshold_SitsBelowTheDefaultPassivationWindow() =>
        Assert.True(
            new WorkflowDeadlineSettings().ExternalArmThreshold
            < WorkflowClusterShardingExtensions.DefaultPassivateIdleEntityAfter,
            "a deadline landing between the two would be left to an instance that has gone away");

    /// <summary>
    /// Passivation is on by default, which is what the whole deadline scheme exists to make safe. A
    /// change back to holding every instance resident is a memory decision a deployment makes, not one
    /// this package makes for it.
    /// </summary>
    [Fact]
    public void IdlePassivation_IsOnByDefault() =>
        Assert.True(
            WorkflowClusterShardingExtensions.DefaultPassivateIdleEntityAfter > TimeSpan.Zero,
            "instances would stay resident until terminal");

    [Theory]
    [MemberData(nameof(RejectedSettings))]
    public void ASettingThatCouldNotWork_IsRejectedWhereItIsWritten(WorkflowDeadlineSettings settings) =>
        Assert.Throws<ArgumentOutOfRangeException>(settings.Validate);

    public static TheoryData<WorkflowDeadlineSettings> RejectedSettings() => new()
    {
        new WorkflowDeadlineSettings { ExternalArmThreshold = TimeSpan.Zero },
        new WorkflowDeadlineSettings { ExternalArmThreshold = TimeSpan.FromSeconds(-1) },
        new WorkflowDeadlineSettings { MaxWakesPerSecond = 0 },
        new WorkflowDeadlineSettings { WakeBurst = 0 },
        new WorkflowDeadlineSettings { MaxWakesInFlight = 0 },
        new WorkflowDeadlineSettings { MaxWakesPerTick = 0 },
        new WorkflowDeadlineSettings { RetryBackoff = TimeSpan.Zero },
        // A ceiling under the base would make the backoff shrink with each attempt.
        new WorkflowDeadlineSettings
        {
            RetryBackoff = TimeSpan.FromMinutes(5),
            MaxRetryBackoff = TimeSpan.FromMinutes(1),
        },
        new WorkflowDeadlineSettings { MaxWakeAttempts = 0 },
        new WorkflowDeadlineSettings { ProjectionLanes = 0 },
        new WorkflowDeadlineSettings { MaxBucketCatchUp = 0 },
    };

    [Fact]
    public void TheDefaults_AreAcceptable() => new WorkflowDeadlineSettings().Validate();
}
