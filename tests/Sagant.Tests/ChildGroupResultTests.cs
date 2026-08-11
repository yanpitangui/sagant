using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Tests;

public class ChildGroupResultTests
{
    private sealed class FakeInventoryWorkflow : IWorkflowTypeInfo
    {
        static string IWorkflowTypeInfo.WorkflowTypeName => "FakeInventoryWorkflow";
    }

    private sealed class FakePaymentWorkflow : IWorkflowTypeInfo
    {
        static string IWorkflowTypeInfo.WorkflowTypeName => "FakePaymentWorkflow";
    }

    private sealed record InventoryState(bool Reserved);

    private static ChildGroupResult BuildResult(params ChildWorkflowRelationship[] members) =>
        new(GroupOutcome.Failed, members);

    [Fact]
    public void Get_Completed_ReturnsState()
    {
        var result = BuildResult(new ChildWorkflowRelationship(
            "rel-1", "Parent", "p-1", "FakeInventoryWorkflow", "inv-1", "group-0", 0,
            ChildStatus.Completed, new InventoryState(true), null, null, ParentClosePolicy.Abandon, new object()));

        var state = result.Get<FakeInventoryWorkflow, InventoryState>("inv-1");

        Assert.True(state.Reserved);
    }

    [Fact]
    public void Get_UnknownWorkflowId_ThrowsChildNotInGroupException()
    {
        var result = BuildResult();

        Assert.Throws<ChildNotInGroupException>(() => result.Get<FakeInventoryWorkflow, InventoryState>("nope"));
    }

    [Fact]
    public void Get_WrongWorkflowType_ThrowsChildWorkflowTypeMismatchException()
    {
        var result = BuildResult(new ChildWorkflowRelationship(
            "rel-1", "Parent", "p-1", "FakePaymentWorkflow", "pay-1", "group-0", 0,
            ChildStatus.Completed, new object(), null, null, ParentClosePolicy.Abandon, new object()));

        Assert.Throws<ChildWorkflowTypeMismatchException>(() => result.Get<FakeInventoryWorkflow, InventoryState>("pay-1"));
    }

    [Fact]
    public void Get_Failed_ThrowsChildResultNotAvailableException()
    {
        var result = BuildResult(new ChildWorkflowRelationship(
            "rel-1", "Parent", "p-1", "FakeInventoryWorkflow", "inv-1", "group-0", 0,
            ChildStatus.Failed, null, new WorkflowFailure("boom"), null, ParentClosePolicy.Abandon, new object()));

        var ex = Assert.Throws<ChildResultNotAvailableException>(() => result.Get<FakeInventoryWorkflow, InventoryState>("inv-1"));
        Assert.Equal(ChildStatus.Failed, ex.Status);
    }

    [Fact]
    public void Get_Pending_ThrowsChildResultNotAvailableException()
    {
        var result = BuildResult(new ChildWorkflowRelationship(
            "rel-1", "Parent", "p-1", "FakeInventoryWorkflow", "inv-1", "group-0", 0,
            ChildStatus.Pending, null, null, null, ParentClosePolicy.Abandon, new object()));

        var ex = Assert.Throws<ChildResultNotAvailableException>(() => result.Get<FakeInventoryWorkflow, InventoryState>("inv-1"));
        Assert.Equal(ChildStatus.Pending, ex.Status);
    }

    [Fact]
    public void TryGet_NeverThrows_ReturnsFalseForEveryFailureReason()
    {
        var result = BuildResult(
            new ChildWorkflowRelationship("rel-1", "Parent", "p-1", "FakePaymentWorkflow", "pay-1", "group-0", 0,
                ChildStatus.Completed, new object(), null, null, ParentClosePolicy.Abandon, new object()),   // wrong type for the lookup below
            new ChildWorkflowRelationship("rel-2", "Parent", "p-1", "FakeInventoryWorkflow", "inv-failed", "group-0", 0,
                ChildStatus.Failed, null, new WorkflowFailure("boom"), null, ParentClosePolicy.Abandon, new object()));

        Assert.False(result.TryGet<FakeInventoryWorkflow, InventoryState>("does-not-exist", out _));
        Assert.False(result.TryGet<FakeInventoryWorkflow, InventoryState>("pay-1", out _));       // wrong type
        Assert.False(result.TryGet<FakeInventoryWorkflow, InventoryState>("inv-failed", out _));  // not completed
    }

    [Fact]
    public void GetFailure_ReturnsNullForNonFailedStatus_ReturnsFailureForFailed()
    {
        var result = BuildResult(
            new ChildWorkflowRelationship("rel-1", "Parent", "p-1", "FakeInventoryWorkflow", "inv-1", "group-0", 0,
                ChildStatus.Completed, new InventoryState(true), null, null, ParentClosePolicy.Abandon, new object()),
            new ChildWorkflowRelationship("rel-2", "Parent", "p-1", "FakePaymentWorkflow", "pay-1", "group-0", 0,
                ChildStatus.Failed, null, new WorkflowFailure("card declined"), null, ParentClosePolicy.Abandon, new object()));

        Assert.Null(result.GetFailure("inv-1"));
        Assert.Equal("card declined", result.GetFailure("pay-1")!.Message);
    }

    [Fact]
    public void GetAll_HomogeneousGroup_ReturnsEveryMemberTyped()
    {
        var result = BuildResult(
            new ChildWorkflowRelationship("rel-1", "Parent", "p-1", "FakeInventoryWorkflow", "inv-1", "group-0", 0,
                ChildStatus.Completed, new InventoryState(true), null, null, ParentClosePolicy.Abandon, new object()),
            new ChildWorkflowRelationship("rel-2", "Parent", "p-1", "FakeInventoryWorkflow", "inv-2", "group-0", 0,
                ChildStatus.Completed, new InventoryState(false), null, null, ParentClosePolicy.Abandon, new object()));

        var all = result.GetAll<FakeInventoryWorkflow, InventoryState>();

        Assert.Equal(2, all.Count);
        Assert.True(all["inv-1"].Reserved);
        Assert.False(all["inv-2"].Reserved);
    }

    [Fact]
    public void GetAll_MixedTypes_ThrowsChildWorkflowTypeMismatchExceptionForTheFirstMismatch()
    {
        var result = BuildResult(
            new ChildWorkflowRelationship("rel-1", "Parent", "p-1", "FakeInventoryWorkflow", "inv-1", "group-0", 0,
                ChildStatus.Completed, new InventoryState(true), null, null, ParentClosePolicy.Abandon, new object()),
            new ChildWorkflowRelationship("rel-2", "Parent", "p-1", "FakePaymentWorkflow", "pay-1", "group-0", 0,
                ChildStatus.Completed, new object(), null, null, ParentClosePolicy.Abandon, new object()));

        Assert.Throws<ChildWorkflowTypeMismatchException>(() => result.GetAll<FakeInventoryWorkflow, InventoryState>());
    }
}
