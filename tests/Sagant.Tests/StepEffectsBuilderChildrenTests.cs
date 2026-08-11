using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Tests;

public class StepEffectsBuilderChildrenTests
{
    private sealed class FakeInventoryWorkflow : IWorkflowTypeInfo
    {
        static string IWorkflowTypeInfo.WorkflowTypeName => "FakeInventoryWorkflow";
    }

    private sealed record ReserveInventory(int Quantity);

    [Fact]
    public void AwaitChildrenTransition_CarriesChildrenAndPolicies()
    {
        var children = new[] { new ChildStart("InventoryWorkflow", "inv-1", new object()) };

        var transition = new Transition.AwaitChildrenTransition(
            GroupId: null, children, CompletionPolicy.AllSuccessful, FailurePolicy.FailFast,
            RemainingChildrenPolicy.Terminate, "OnResolved");

        Assert.Null(transition.GroupId);
        Assert.Single(transition.Children);
        Assert.Equal("InventoryWorkflow", transition.Children[0].WorkflowType);
        Assert.Equal("OnResolved", transition.ResumeStepName);
    }

    [Fact]
    public void Child_ProducesChildStartWithResolvedWorkflowType()
    {
        var start = new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("inv-1", new ReserveInventory(5));

        Assert.Equal("FakeInventoryWorkflow", start.WorkflowType);
        Assert.Equal("inv-1", start.WorkflowId);
        Assert.IsType<ReserveInventory>(start.Command);
    }

    [Fact]
    public void AwaitChildren_DuplicateWorkflowIdInSameGroup_ThrowsImmediately()
    {
        var children = new[]
        {
            new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("dup-id", new ReserveInventory(1)),
            new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("dup-id", new ReserveInventory(2)),
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            new StepEffectsBuilder<string>().AwaitChildren(children, Ref.Step<DocWorkflowFor<string>, ChildGroupResult>("OnResolved")));

        Assert.Contains("dup-id", ex.Message);
    }

    [Fact]
    public void AwaitChildren_CommonCase_ProducesDefaultPolicies()
    {
        var children = new[] { new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("inv-1", new ReserveInventory(1)) };

        var effect = new StepEffectsBuilder<string>().AwaitChildren(children, Ref.Step<DocWorkflowFor<string>, ChildGroupResult>("OnResolved"));

        var transition = Assert.IsType<Transition.AwaitChildrenTransition>(effect.Transition);
        Assert.Null(transition.GroupId);
        Assert.Equal(CompletionPolicy.AllSuccessful, transition.CompletionPolicy);
        Assert.Equal(FailurePolicy.FailFast, transition.FailurePolicy);
        Assert.Equal(RemainingChildrenPolicy.Terminate, transition.RemainingChildrenPolicy);
        Assert.Equal("OnResolved", transition.ResumeStepName);
    }

    [Fact]
    public void AwaitChildren_ConfiguredCase_OverridesDefaults()
    {
        var children = new[] { new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("inv-1", new ReserveInventory(1)) };

        var effect = new StepEffectsBuilder<string>().AwaitChildren(
            children,
            options => options.AllCompleted().WaitForAll().ContinueRemaining().ResumeAt(Ref.Step<DocWorkflowFor<string>, ChildGroupResult>("OnResolved")).GroupId("shipments"));

        var transition = Assert.IsType<Transition.AwaitChildrenTransition>(effect.Transition);
        Assert.Equal("shipments", transition.GroupId);
        Assert.Equal(CompletionPolicy.AllCompleted, transition.CompletionPolicy);
        Assert.Equal(FailurePolicy.WaitForAll, transition.FailurePolicy);
        Assert.Equal(RemainingChildrenPolicy.Continue, transition.RemainingChildrenPolicy);
    }

    [Fact]
    public void AwaitChildren_ConfiguredCase_MissingResumeAt_ThrowsInvalidOperationException()
    {
        var children = new[] { new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("inv-1", new ReserveInventory(1)) };

        Assert.Throws<InvalidOperationException>(() =>
            new StepEffectsBuilder<string>().AwaitChildren(children, options => options.FailFast()));
    }

    [Fact]
    public void Child_DefaultsToAbandonParentClosePolicy()
    {
        var start = new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>("inv-1", new ReserveInventory(1));

        Assert.Equal(ParentClosePolicy.Abandon, start.ParentClosePolicy);
    }

    [Fact]
    public void Child_AcceptsExplicitParentClosePolicy()
    {
        var start = new StepEffectsBuilder<string>().Child<FakeInventoryWorkflow>(
            "inv-1", new ReserveInventory(1), ParentClosePolicy.Terminate);

        Assert.Equal(ParentClosePolicy.Terminate, start.ParentClosePolicy);
    }

    [Fact]
    public void AwaitChildren_HomogeneousOverload_ProducesSameTransitionShapeAsGeneralPath()
    {
        var effect = new StepEffectsBuilder<string>().AwaitChildren<FakeInventoryWorkflow, DocWorkflowFor<string>>(
            new (string, object)[] { ("inv-1", new ReserveInventory(1)), ("inv-2", new ReserveInventory(2)) },
            Ref.Step<DocWorkflowFor<string>, ChildGroupResult>("OnResolved"));

        var transition = Assert.IsType<Transition.AwaitChildrenTransition>(effect.Transition);
        Assert.Equal(2, transition.Children.Count);
        Assert.All(transition.Children, c => Assert.Equal("FakeInventoryWorkflow", c.WorkflowType));
        Assert.Equal(new[] { "inv-1", "inv-2" }, transition.Children.Select(c => c.WorkflowId));
    }
}
