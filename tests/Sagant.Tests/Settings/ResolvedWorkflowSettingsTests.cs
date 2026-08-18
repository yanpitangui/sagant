using Sagant.Descriptors;
using Sagant.Settings;

namespace Sagant.Tests.Settings;

/// <summary>
/// Override layering is guarantee E6's other half: "step-specific value, else default" has to mean
/// the same thing to every driver, so it is decided here, once, and every resolution site reads
/// that single answer.
/// </summary>
public class ResolvedWorkflowSettingsTests
{
    [Fact]
    public void StepTimeoutFor_StepWithOverride_PrefersTheOverride()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Create()
            .DefaultStepTimeout(TimeSpan.FromSeconds(5))
            .StepTimeout(Ref.Step<DocWorkflowFor<string>, NoInput>("Slow"), TimeSpan.FromMinutes(2))
            .Build());

        Assert.Equal(TimeSpan.FromMinutes(2), resolved.StepTimeoutFor("Slow"));
    }

    [Fact]
    public void StepTimeoutFor_StepWithoutOverride_FallsBackToDefault()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Create()
            .DefaultStepTimeout(TimeSpan.FromSeconds(5))
            .StepTimeout(Ref.Step<DocWorkflowFor<string>, NoInput>("Slow"), TimeSpan.FromMinutes(2))
            .Build());

        Assert.Equal(TimeSpan.FromSeconds(5), resolved.StepTimeoutFor("Other"));
    }

    [Fact]
    public void StepTimeoutFor_NoDefaultAndNoOverride_IsNull()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Default);

        Assert.Null(resolved.StepTimeoutFor("Anything"));
    }

    /// <summary>A step registering only a recover strategy must still fall through to the default
    /// timeout — the two overrides layer independently, each one its own optional entry.</summary>
    [Fact]
    public void StepTimeoutFor_StepWithOnlyARecoverStrategyOverride_StillUsesTheDefaultTimeout()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Create()
            .DefaultStepTimeout(TimeSpan.FromSeconds(5))
            .StepRecovery(Ref.Step<DocWorkflowFor<string>, NoInput>("Flaky"), RecoverStrategy.WithMaxRetries(2).FailoverTo(Ref.Step<DocWorkflowFor<string>>("Cleanup")))
            .Build());

        Assert.Equal(TimeSpan.FromSeconds(5), resolved.StepTimeoutFor("Flaky"));
        Assert.Equal(2, resolved.RecoverStrategyFor("Flaky")!.MaxRetries);
    }

    [Fact]
    public void RecoverStrategyFor_StepWithoutOverride_FallsBackToDefault()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(1).FailoverTo(Ref.Step<DocWorkflowFor<string>>("DefaultCleanup")))
            .StepRecovery(Ref.Step<DocWorkflowFor<string>, NoInput>("Flaky"), RecoverStrategy.WithMaxRetries(4).FailoverTo(Ref.Step<DocWorkflowFor<string>>("Cleanup")))
            .Build());

        Assert.Equal("DefaultCleanup", resolved.RecoverStrategyFor("Other")!.FailoverStepName);
        Assert.Equal("Cleanup", resolved.RecoverStrategyFor("Flaky")!.FailoverStepName);
    }

    private sealed record SlowQuery;

    [Fact]
    public void QueryTimeoutFor_QueryWithOverride_PrefersTheOverride()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Create()
            .DefaultQueryTimeout(TimeSpan.FromSeconds(2))
            .QueryTimeout<SlowQuery>(TimeSpan.FromSeconds(45))
            .Build());

        Assert.Equal(TimeSpan.FromSeconds(45), resolved.QueryTimeoutFor(nameof(SlowQuery)));
        Assert.Equal(TimeSpan.FromSeconds(2), resolved.QueryTimeoutFor("OtherQuery"));
    }

    /// <summary>Guarantee Q2: a query is always bounded, so this never resolves to null even with
    /// nothing configured at all.</summary>
    [Fact]
    public void QueryTimeoutFor_NothingConfigured_FallsBackToTheBuiltInBound()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Default);

        Assert.Equal(WorkflowSettings.BuiltInQueryTimeout, resolved.QueryTimeoutFor("AnyQuery"));
    }

    [Fact]
    public void From_CarriesScalarSettingsThrough()
    {
        var resolved = ResolvedWorkflowSettings.From(WorkflowSettings.Create()
            .Timeout(TimeSpan.FromMinutes(10))
            .IdempotencyLedgerCapacity(7)
            .SeqNrDedupCapacity(3)
            .PruneFinalizedChildren()
            .Build());

        Assert.Equal(TimeSpan.FromMinutes(10), resolved.WorkflowTimeout);
        Assert.Equal(7, resolved.IdempotencyLedgerCapacity);
        Assert.Equal(3, resolved.SeqNrDedupCapacity);
        Assert.True(resolved.PruneFinalizedChildren);
    }
}
