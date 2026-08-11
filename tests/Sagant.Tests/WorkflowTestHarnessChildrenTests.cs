using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Testing;

namespace Sagant.Tests;

public class WorkflowTestHarnessChildrenTests
{
    [Fact]
    public async Task AwaitChildren_ParentWaitsUntilExplicitlyDelivered_ThenResumes()
    {
        var child = new WorkflowTestHarness<ChildWorkflow, ChildState>(new ChildWorkflow());
        var parent = new WorkflowTestHarness<ParentWorkflow, ParentState>(new ParentWorkflow())
            .WithChild("child-1", child);

        await parent.RunStep(ParentWorkflow.Steps.StartChildren);

        Assert.Equal(WorkflowStatus.Running, parent.Status);
        Assert.Equal(0, parent.State.ResumeCount);

        await child.RunUntilStop(new StartChild());
        Assert.IsType<WorkflowOutcome.Completed>(child.Outcome);

        await parent.DeliverChildLifecycle("child-1");

        Assert.Equal(1, parent.State.ResumeCount);
        Assert.IsType<WorkflowOutcome.Completed>(parent.Outcome);
    }

    [Fact]
    public async Task RedeliverChildLifecycle_AfterGroupFinalized_IsANoOp()
    {
        var child = new WorkflowTestHarness<ChildWorkflow, ChildState>(new ChildWorkflow());
        var parent = new WorkflowTestHarness<ParentWorkflow, ParentState>(new ParentWorkflow())
            .WithChild("child-1", child);

        await parent.RunStep(ParentWorkflow.Steps.StartChildren);
        await child.RunUntilStop(new StartChild());
        await parent.DeliverChildLifecycle("child-1");
        await parent.RedeliverChildLifecycle("child-1");

        Assert.Equal(1, parent.State.ResumeCount);
    }

    private sealed record ParentState(int ResumeCount = 0);
    private sealed record ChildState(bool Completed = false);
    private sealed record StartChild;

    private sealed class ParentWorkflow : Workflow<ParentState>, IWorkflowStepDispatcher<ParentState>, IWorkflowCommandDispatcher<ParentState>, IWorkflowQueryDispatcher<ParentState>, IWorkflowChildResultDispatcher<ParentState>, IWorkflowTypeInfo
    {
        public static class Steps
        {
            public static readonly StepRef<ParentWorkflow, NoInput> StartChildren = new("StartChildren");
            public static readonly StepRef<ParentWorkflow, ChildGroupResult> OnResolved = new("OnResolved");
        }

        public override ParentState EmptyState() => new();
        static string IWorkflowTypeInfo.WorkflowTypeName => "HarnessParent";
        public override string WorkflowTypeName => "HarnessParent";

        bool IWorkflowStepDispatcher<ParentState>.TryGetStep(string stepName, out StepDescriptor<ParentState> descriptor)
        {
            if (stepName == "StartChildren")
            {
                descriptor = new StepDescriptor<ParentState>(stepName, typeof(NoInput), static (w, _, _) => ((ParentWorkflow)w).StartChildrenStep());
                return true;
            }
            if (stepName == "OnResolved")
            {
                descriptor = new StepDescriptor<ParentState>(stepName, typeof(ChildGroupResult), static (w, ctx, input) => ((ParentWorkflow)w).OnResolvedStep((ChildGroupResult)input!, ctx));
                return true;
            }
            descriptor = default;
            return false;
        }

        IReadOnlyCollection<string> IWorkflowStepDispatcher<ParentState>.StepNames => new[] { "StartChildren", "OnResolved" };

        bool IWorkflowQueryDispatcher<ParentState>.TryGetQuery(Type queryType, out QueryDescriptor<ParentState> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<ParentState>.TryGetChildResultHandler(out ChildResultDescriptor<ParentState> descriptor) { descriptor = default; return false; }
        bool IWorkflowCommandDispatcher<ParentState>.TryGetHandler(Type _, out CommandDescriptor<ParentState> descriptor) { descriptor = default; return false; }

        private Task<StepEffect<ParentState>> StartChildrenStep() => Task.FromResult(StepEffects.AwaitChildren(
            new[] { new StepEffectsBuilder<ParentState>().Child<ChildWorkflow>("child-1", new StartChild()) },
            options => options.GroupId("group-1").AllSuccessful().ResumeAt(Ref.Step<DocWorkflowFor<ParentState>, ChildGroupResult>("OnResolved"))));

        private Task<StepEffect<ParentState>> OnResolvedStep(ChildGroupResult result, StepContext<ParentState> ctx)
        {
            Assert.Equal(GroupOutcome.Succeeded, result.Outcome);
            Assert.True(result.Get<ChildWorkflow, ChildState>("child-1").Completed);
            return Task.FromResult(StepEffects.UpdateState(ctx.State with { ResumeCount = ctx.State.ResumeCount + 1 }).ThenComplete());
        }
    }

    private sealed class ChildWorkflow : Workflow<ChildState>, IWorkflowStepDispatcher<ChildState>, IWorkflowCommandDispatcher<ChildState>, IWorkflowQueryDispatcher<ChildState>, IWorkflowChildResultDispatcher<ChildState>, IWorkflowTypeInfo
    {
        public static class Steps { public static readonly StepRef<ChildWorkflow, NoInput> Run = new("Run"); }

        public override ChildState EmptyState() => new();
        static string IWorkflowTypeInfo.WorkflowTypeName => "HarnessChild";
        public override string WorkflowTypeName => "HarnessChild";

        bool IWorkflowStepDispatcher<ChildState>.TryGetStep(string stepName, out StepDescriptor<ChildState> descriptor)
        {
            if (stepName == "Run")
            {
                descriptor = new StepDescriptor<ChildState>(stepName, typeof(NoInput), static (w, _, _) => ((ChildWorkflow)w).RunStep());
                return true;
            }
            descriptor = default;
            return false;
        }

        IReadOnlyCollection<string> IWorkflowStepDispatcher<ChildState>.StepNames => new[] { "Run" };

        bool IWorkflowQueryDispatcher<ChildState>.TryGetQuery(Type queryType, out QueryDescriptor<ChildState> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<ChildState>.TryGetChildResultHandler(out ChildResultDescriptor<ChildState> descriptor) { descriptor = default; return false; }
        bool IWorkflowCommandDispatcher<ChildState>.TryGetHandler(Type commandType, out CommandDescriptor<ChildState> descriptor)
        {
            if (commandType == typeof(StartChild))
            {
                descriptor = new CommandDescriptor<ChildState>(commandType, commandType.Name, static (w, ctx, _) => ((ChildWorkflow)w).Effects.TransitionTo(Steps.Run));
                return true;
            }
            descriptor = default;
            return false;
        }

        private Task<StepEffect<ChildState>> RunStep() => Task.FromResult(StepEffects.UpdateState(new ChildState(true)).ThenComplete());
    }
}
