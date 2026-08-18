using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Tests.Execution;

/// <summary>
/// The decisions that carry an enum and nothing else are held as one value per case, built once and
/// shared, so a plan reads that value directly on every call. What this relies on is that shared
/// value being equal to a freshly built one — a driver pattern-matches decisions and compares them,
/// and a test asserts on them.
/// </summary>
public class WorkflowDecisionTests
{
    [Theory]
    [InlineData(WorkflowTimerKind.Workflow)]
    [InlineData(WorkflowTimerKind.Pause)]
    [InlineData(WorkflowTimerKind.Hold)]
    [InlineData(WorkflowTimerKind.ChildGroup)]
    public void AHeldCancelTimerCarriesItsKind(WorkflowTimerKind kind)
    {
        var held = WorkflowDecision.CancelTimer.For(kind);

        Assert.Equal(kind, held.Kind);
        Assert.Null(held.Discriminator);
        Assert.Equal(new WorkflowDecision.CancelTimer(kind), held);
        Assert.Same(held, WorkflowDecision.CancelTimer.For(kind));
    }

    [Theory]
    [InlineData(WorkflowStatus.NotStarted)]
    [InlineData(WorkflowStatus.Running)]
    [InlineData(WorkflowStatus.Paused)]
    [InlineData(WorkflowStatus.Suspended)]
    [InlineData(WorkflowStatus.Finished)]
    [InlineData(WorkflowStatus.Deleted)]
    public void AHeldStatusChangeCarriesItsStatus(WorkflowStatus status)
    {
        var held = WorkflowDecision.RecordStatusChange.For(status);

        Assert.Equal(status, held.Status);
        Assert.Equal(new WorkflowDecision.RecordStatusChange(status), held);
        Assert.Same(held, WorkflowDecision.RecordStatusChange.For(status));
    }

    /// <summary>A group's cancellation names which group, so it is its own value each time.</summary>
    [Fact]
    public void ACancelTimerNamingAGroupIsItsOwnValue()
    {
        var first = new WorkflowDecision.CancelTimer(WorkflowTimerKind.ChildGroup, "items");
        var second = new WorkflowDecision.CancelTimer(WorkflowTimerKind.ChildGroup, "shipments");

        Assert.NotEqual(first, second);
        Assert.NotEqual(WorkflowDecision.CancelTimer.For(WorkflowTimerKind.ChildGroup), first);
    }
}
