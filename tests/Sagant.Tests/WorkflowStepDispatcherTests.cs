using Sagant.Effects;
using Sagant.Descriptors;

namespace Sagant.Tests;

public class WorkflowStepDispatcherTests
{
    // Stand-in for generator output: a partial class implementing IWorkflowStepDispatcher<TState>
    // by hand, proving the dispatch contract works before the generator emits it automatically.
    private sealed class FakeWorkflow : Workflow<string>, IWorkflowStepDispatcher<string>
    {
        public override string EmptyState() => string.Empty;

        private static readonly System.Collections.Generic.Dictionary<string, StepDescriptor<string>> Descriptors = new()
        {
            ["DoThing"] = new StepDescriptor<string>(
                "DoThing",
                typeof(int),
                static (workflow, ctx, input) => ((FakeWorkflow)workflow).DoThingStep((int)input!, ctx)),
        };

        bool IWorkflowStepDispatcher<string>.TryGetStep(string stepName, out StepDescriptor<string> descriptor) =>
            Descriptors.TryGetValue(stepName, out descriptor);

        System.Collections.Generic.IReadOnlyCollection<string> IWorkflowStepDispatcher<string>.StepNames => Descriptors.Keys;

        public Task<StepEffect<string>> DoThingStep(int input, StepContext<string> ctx) =>
            Task.FromResult(StepEffects.UpdateState($"did-thing-{input}-from-{ctx.State}").ThenComplete());
    }

    [Fact]
    public void TryGetStep_KnownStepName_ReturnsDescriptorWithMatchingInputType()
    {
        IWorkflowStepDispatcher<string> dispatcher = new FakeWorkflow();

        Assert.True(dispatcher.TryGetStep("DoThing", out var descriptor));
        Assert.Equal("DoThing", descriptor.Name);
        Assert.Equal(typeof(int), descriptor.InputType);
    }

    [Fact]
    public void TryGetStep_UnknownStepName_ReturnsFalse()
    {
        IWorkflowStepDispatcher<string> dispatcher = new FakeWorkflow();

        Assert.False(dispatcher.TryGetStep("Nope", out _));
    }

    [Fact]
    public async Task Descriptor_Invoke_CallsTheUnderlyingStepMethodOnGivenInstance()
    {
        var workflow = new FakeWorkflow();
        IWorkflowStepDispatcher<string> dispatcher = workflow;
        dispatcher.TryGetStep("DoThing", out var descriptor);

        var effect = await descriptor.Invoke(workflow, "initial", 5, attempt: 1, CancellationToken.None);

        var persistence = Assert.IsType<PersistenceEffect<string>.UpdateState>(effect.Persistence);
        Assert.Equal("did-thing-5-from-initial", persistence.NewState);
    }

    [Fact]
    public void StepNames_ReturnsAllRegisteredStepNames()
    {
        IWorkflowStepDispatcher<string> dispatcher = new FakeWorkflow();

        Assert.Equal(new[] { "DoThing" }, dispatcher.StepNames);
    }
}
