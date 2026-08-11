using Sagant.Descriptors;
using Sagant.Settings;

namespace Sagant.Tests;

public class WorkflowSettingsTests
{
    [Fact]
    public void Build_WithNothingSet_ReturnsAllDefaults()
    {
        var settings = WorkflowSettings.Create().Build();

        Assert.Null(settings.WorkflowTimeout);
        Assert.Null(settings.WorkflowRecoverStrategy);
        Assert.Null(settings.DefaultStepTimeout);
        Assert.Null(settings.DefaultStepRecoverStrategy);
        Assert.Empty(settings.StepSettings);
        Assert.Equal(50, settings.IdempotencyLedgerCapacity);
        Assert.Equal(16, settings.SeqNrDedupCapacity);
        Assert.False(settings.PruneFinalizedChildren);
    }

    [Fact]
    public void Timeout_WithoutFailover_SetsWorkflowTimeoutOnly()
    {
        var settings = WorkflowSettings.Create()
            .Timeout(TimeSpan.FromSeconds(10))
            .Build();

        Assert.Equal(TimeSpan.FromSeconds(10), settings.WorkflowTimeout);
        Assert.Null(settings.WorkflowRecoverStrategy);
    }

    [Fact]
    public void Timeout_WithFailoverStep_SetsWorkflowRecoverStrategy()
    {
        var settings = WorkflowSettings.Create()
            .Timeout(TimeSpan.FromSeconds(10), Ref.Step<DocWorkflowFor<string>>("TimeoutHandlerStep"))
            .Build();

        Assert.NotNull(settings.WorkflowRecoverStrategy);
        Assert.Equal(0, settings.WorkflowRecoverStrategy!.MaxRetries);
        Assert.Equal("TimeoutHandlerStep", settings.WorkflowRecoverStrategy.FailoverStepName);
    }

    [Fact]
    public void DefaultStepTimeout_And_DefaultStepRecovery_AreCaptured()
    {
        var recover = RecoverStrategy.WithMaxRetries(1).FailoverTo(Ref.Step<DocWorkflowFor<string>>("FailoverStep"));

        var settings = WorkflowSettings.Create()
            .DefaultStepTimeout(TimeSpan.FromSeconds(2))
            .DefaultStepRecovery(recover)
            .Build();

        Assert.Equal(TimeSpan.FromSeconds(2), settings.DefaultStepTimeout);
        Assert.Equal(recover, settings.DefaultStepRecoverStrategy);
    }

    [Fact]
    public void StepTimeout_And_StepRecovery_ForSameStep_MergeIntoOneStepSettingsEntry()
    {
        var recover = RecoverStrategy.WithMaxRetries(2).FailoverTo(Ref.Step<DocWorkflowFor<string>>("CompensateStep"));

        var settings = WorkflowSettings.Create()
            .StepTimeout(Ref.Step<DocWorkflowFor<string>, NoInput>("ChargePayment"), TimeSpan.FromSeconds(5))
            .StepRecovery(Ref.Step<DocWorkflowFor<string>, NoInput>("ChargePayment"), recover)
            .Build();

        var stepSettings = Assert.Single(settings.StepSettings);
        Assert.Equal("ChargePayment", stepSettings.StepName);
        Assert.Equal(TimeSpan.FromSeconds(5), stepSettings.Timeout);
        Assert.Equal(recover, stepSettings.RecoverStrategy);
    }

    [Fact]
    public void StepTimeout_ForDifferentSteps_ProducesSeparateEntries()
    {
        var settings = WorkflowSettings.Create()
            .StepTimeout(Ref.Step<DocWorkflowFor<string>, NoInput>("StepA"), TimeSpan.FromSeconds(1))
            .StepTimeout(Ref.Step<DocWorkflowFor<string>, NoInput>("StepB"), TimeSpan.FromSeconds(2))
            .Build();

        Assert.Equal(2, settings.StepSettings.Count);
        Assert.Contains(settings.StepSettings, s => s.StepName == "StepA" && s.Timeout == TimeSpan.FromSeconds(1));
        Assert.Contains(settings.StepSettings, s => s.StepName == "StepB" && s.Timeout == TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void IdempotencyLedgerCapacity_IsCaptured()
    {
        var settings = WorkflowSettings.Create()
            .IdempotencyLedgerCapacity(100)
            .Build();

        Assert.Equal(100, settings.IdempotencyLedgerCapacity);
    }

    [Fact]
    public void IdempotencyLedgerCapacity_NonPositiveCapacity_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => WorkflowSettings.Create().IdempotencyLedgerCapacity(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => WorkflowSettings.Create().IdempotencyLedgerCapacity(-1));
    }

    [Fact]
    public void SeqNrDedupCapacity_IsCaptured()
    {
        var settings = WorkflowSettings.Create()
            .SeqNrDedupCapacity(32)
            .Build();

        Assert.Equal(32, settings.SeqNrDedupCapacity);
    }

    [Fact]
    public void SeqNrDedupCapacity_NonPositiveCapacity_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => WorkflowSettings.Create().SeqNrDedupCapacity(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => WorkflowSettings.Create().SeqNrDedupCapacity(-1));
    }

    [Fact]
    public void PruneFinalizedChildren_DefaultsToFalse_AndCanBeEnabled()
    {
        var enabled = WorkflowSettings.Create().PruneFinalizedChildren().Build();
        var explicitlyDisabled = WorkflowSettings.Create().PruneFinalizedChildren(false).Build();

        Assert.True(enabled.PruneFinalizedChildren);
        Assert.False(explicitlyDisabled.PruneFinalizedChildren);
    }
}
