using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Testing;

namespace Sagant.Tests;

/// <summary>
/// A child workflow can itself start and await children of its own — nothing about
/// <c>AwaitChildren</c>/<c>ParentRelationship</c> is scoped to one level. This exercises a
/// three-level tree (grandparent → middle → leaf) through <see cref="WorkflowTestHarness{TWorkflow, TState}"/>:
/// the middle harness is registered as a child of the grandparent via <c>WithChild</c>, and is
/// itself given its own child via the same method — proving the harness composes recursively, with
/// no special-casing for "a workflow that is simultaneously someone's child and someone's parent."
/// </summary>
public class WorkflowTestHarnessNestedChildrenTests
{
    [Fact]
    public async Task GrandchildResolution_PropagatesUpThroughMiddleParentToGrandparent()
    {
        var leaf = new WorkflowTestHarness<LeafWorkflow, LeafState>(new LeafWorkflow());
        var middle = new WorkflowTestHarness<MiddleWorkflow, MiddleState>(new MiddleWorkflow())
            .WithChild("leaf-1", leaf);
        var grandparent = new WorkflowTestHarness<GrandparentWorkflow, GrandparentState>(new GrandparentWorkflow())
            .WithChild("middle-1", middle);

        await grandparent.RunStep(GrandparentWorkflow.Steps.StartMiddleChildren);
        Assert.Equal(WorkflowStatus.Running, grandparent.Status);

        await middle.RunStep(MiddleWorkflow.Steps.StartLeafChildren);
        Assert.Equal(WorkflowStatus.Running, middle.Status);

        await leaf.RunUntilStop(new StartChild());
        Assert.IsType<WorkflowOutcome.Completed>(leaf.Outcome);

        // The middle harness is a child of grandparent (registered via WithChild above) and a
        // parent of leaf (registered via its own WithChild) at the same time — resolving leaf's
        // lifecycle against it exercises exactly that dual role.
        await middle.DeliverChildLifecycle("leaf-1");
        Assert.IsType<WorkflowOutcome.Completed>(middle.Outcome);
        Assert.True(middle.State.LeafCompleted);

        await grandparent.DeliverChildLifecycle("middle-1");
        Assert.IsType<WorkflowOutcome.Completed>(grandparent.Outcome);
        Assert.True(grandparent.State.MiddleSawLeafComplete);
    }

    private sealed record GrandparentState(bool MiddleSawLeafComplete = false);
    private sealed record MiddleState(bool LeafCompleted = false);
    private sealed record LeafState(bool Completed = false);
    private sealed record StartChild;

    private sealed class GrandparentWorkflow : Workflow<GrandparentState>, IWorkflowStepDispatcher<GrandparentState>, IWorkflowCommandDispatcher<GrandparentState>, IWorkflowQueryDispatcher<GrandparentState>, IWorkflowChildResultDispatcher<GrandparentState>, IWorkflowTypeInfo
    {
        public static class Steps
        {
            public static readonly StepRef<GrandparentWorkflow, NoInput> StartMiddleChildren = new("StartMiddleChildren");
            public static readonly StepRef<GrandparentWorkflow, ChildGroupResult> OnResolved = new("OnResolved");
        }

        public override GrandparentState EmptyState() => new();
        static string IWorkflowTypeInfo.WorkflowTypeName => "NestedGrandparent";
        public override string WorkflowTypeName => "NestedGrandparent";

        bool IWorkflowStepDispatcher<GrandparentState>.TryGetStep(string stepName, out StepDescriptor<GrandparentState> descriptor)
        {
            if (stepName == "StartMiddleChildren")
            {
                descriptor = new StepDescriptor<GrandparentState>(stepName, typeof(NoInput), static (w, _, _) => ((GrandparentWorkflow)w).StartMiddleChildrenStep());
                return true;
            }
            if (stepName == "OnResolved")
            {
                descriptor = new StepDescriptor<GrandparentState>(stepName, typeof(ChildGroupResult), static (w, ctx, input) => ((GrandparentWorkflow)w).OnResolvedStep((ChildGroupResult)input!, ctx));
                return true;
            }
            descriptor = default;
            return false;
        }

        IReadOnlyCollection<string> IWorkflowStepDispatcher<GrandparentState>.StepNames => new[] { "StartMiddleChildren", "OnResolved" };

        bool IWorkflowQueryDispatcher<GrandparentState>.TryGetQuery(Type queryType, out QueryDescriptor<GrandparentState> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<GrandparentState>.TryGetChildResultHandler(out ChildResultDescriptor<GrandparentState> descriptor) { descriptor = default; return false; }
        bool IWorkflowCommandDispatcher<GrandparentState>.TryGetHandler(Type _, out CommandDescriptor<GrandparentState> descriptor) { descriptor = default; return false; }

        private Task<StepEffect<GrandparentState>> StartMiddleChildrenStep() => Task.FromResult(StepEffects.AwaitChildren(
            new[] { new StepEffectsBuilder<GrandparentState>().Child<MiddleWorkflow>("middle-1", new StartChild()) },
            options => options.GroupId("group-1").AllSuccessful().ResumeAt(Ref.Step<DocWorkflowFor<string>, ChildGroupResult>("OnResolved"))));

        private Task<StepEffect<GrandparentState>> OnResolvedStep(ChildGroupResult result, StepContext<GrandparentState> ctx)
        {
            Assert.Equal(GroupOutcome.Succeeded, result.Outcome);
            var middleState = result.Get<MiddleWorkflow, MiddleState>("middle-1");
            return Task.FromResult(StepEffects.UpdateState(ctx.State with { MiddleSawLeafComplete = middleState.LeafCompleted }).ThenComplete());
        }
    }

    private sealed class MiddleWorkflow : Workflow<MiddleState>, IWorkflowStepDispatcher<MiddleState>, IWorkflowCommandDispatcher<MiddleState>, IWorkflowQueryDispatcher<MiddleState>, IWorkflowChildResultDispatcher<MiddleState>, IWorkflowTypeInfo
    {
        public static class Steps
        {
            public static readonly StepRef<MiddleWorkflow, NoInput> StartLeafChildren = new("StartLeafChildren");
            public static readonly StepRef<MiddleWorkflow, ChildGroupResult> OnResolved = new("OnResolved");
        }

        public override MiddleState EmptyState() => new();
        static string IWorkflowTypeInfo.WorkflowTypeName => "NestedMiddle";
        public override string WorkflowTypeName => "NestedMiddle";

        bool IWorkflowStepDispatcher<MiddleState>.TryGetStep(string stepName, out StepDescriptor<MiddleState> descriptor)
        {
            if (stepName == "StartLeafChildren")
            {
                descriptor = new StepDescriptor<MiddleState>(stepName, typeof(NoInput), static (w, _, _) => ((MiddleWorkflow)w).StartLeafChildrenStep());
                return true;
            }
            if (stepName == "OnResolved")
            {
                descriptor = new StepDescriptor<MiddleState>(stepName, typeof(ChildGroupResult), static (w, ctx, input) => ((MiddleWorkflow)w).OnResolvedStep((ChildGroupResult)input!, ctx));
                return true;
            }
            descriptor = default;
            return false;
        }

        IReadOnlyCollection<string> IWorkflowStepDispatcher<MiddleState>.StepNames => new[] { "StartLeafChildren", "OnResolved" };

        bool IWorkflowQueryDispatcher<MiddleState>.TryGetQuery(Type queryType, out QueryDescriptor<MiddleState> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<MiddleState>.TryGetChildResultHandler(out ChildResultDescriptor<MiddleState> descriptor) { descriptor = default; return false; }
        bool IWorkflowCommandDispatcher<MiddleState>.TryGetHandler(Type commandType, out CommandDescriptor<MiddleState> descriptor)
        {
            if (commandType == typeof(StartChild))
            {
                descriptor = new CommandDescriptor<MiddleState>(commandType, commandType.Name, static (w, ctx, _) => ((MiddleWorkflow)w).Effects.TransitionTo(Steps.StartLeafChildren));
                return true;
            }
            descriptor = default;
            return false;
        }

        private Task<StepEffect<MiddleState>> StartLeafChildrenStep() => Task.FromResult(StepEffects.AwaitChildren(
            new[] { new StepEffectsBuilder<MiddleState>().Child<LeafWorkflow>("leaf-1", new StartChild()) },
            options => options.GroupId("group-1").AllSuccessful().ResumeAt(Ref.Step<DocWorkflowFor<string>, ChildGroupResult>("OnResolved"))));

        private Task<StepEffect<MiddleState>> OnResolvedStep(ChildGroupResult result, StepContext<MiddleState> ctx)
        {
            Assert.Equal(GroupOutcome.Succeeded, result.Outcome);
            var leafState = result.Get<LeafWorkflow, LeafState>("leaf-1");
            return Task.FromResult(StepEffects.UpdateState(ctx.State with { LeafCompleted = leafState.Completed }).ThenComplete());
        }
    }

    private sealed class LeafWorkflow : Workflow<LeafState>, IWorkflowStepDispatcher<LeafState>, IWorkflowCommandDispatcher<LeafState>, IWorkflowQueryDispatcher<LeafState>, IWorkflowChildResultDispatcher<LeafState>, IWorkflowTypeInfo
    {
        public static class Steps { public static readonly StepRef<LeafWorkflow, NoInput> Run = new("Run"); }

        public override LeafState EmptyState() => new();
        static string IWorkflowTypeInfo.WorkflowTypeName => "NestedLeaf";
        public override string WorkflowTypeName => "NestedLeaf";

        bool IWorkflowStepDispatcher<LeafState>.TryGetStep(string stepName, out StepDescriptor<LeafState> descriptor)
        {
            if (stepName == "Run")
            {
                descriptor = new StepDescriptor<LeafState>(stepName, typeof(NoInput), static (w, _, _) => ((LeafWorkflow)w).RunStep());
                return true;
            }
            descriptor = default;
            return false;
        }

        IReadOnlyCollection<string> IWorkflowStepDispatcher<LeafState>.StepNames => new[] { "Run" };

        bool IWorkflowQueryDispatcher<LeafState>.TryGetQuery(Type queryType, out QueryDescriptor<LeafState> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<LeafState>.TryGetChildResultHandler(out ChildResultDescriptor<LeafState> descriptor) { descriptor = default; return false; }
        bool IWorkflowCommandDispatcher<LeafState>.TryGetHandler(Type commandType, out CommandDescriptor<LeafState> descriptor)
        {
            if (commandType == typeof(StartChild))
            {
                descriptor = new CommandDescriptor<LeafState>(commandType, commandType.Name, static (w, ctx, _) => ((LeafWorkflow)w).Effects.TransitionTo(Steps.Run));
                return true;
            }
            descriptor = default;
            return false;
        }

        private Task<StepEffect<LeafState>> RunStep() => Task.FromResult(StepEffects.UpdateState(new LeafState(true)).ThenComplete());
    }
}
