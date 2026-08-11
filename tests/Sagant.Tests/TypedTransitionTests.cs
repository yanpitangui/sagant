using Sagant.Settings;
using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Tests;

public class TypedTransitionTests
{
    // Stand-in for what the source generator emits for a workflow class — proves the hand-written
    // typed API (StepRef/NoInput/typed TransitionTo overloads) works before the generator exists.
    private sealed class FakeWorkflow : Workflow<string>
    {
        public override string EmptyState() => string.Empty;

        public static class Steps
        {
            public static readonly StepRef<FakeWorkflow, int> WithInput = new("WithInput");
            public static readonly StepRef<FakeWorkflow, NoInput> WithoutInput = new("WithoutInput");
        }

        public Task<StepEffect<string>> WithInputStep(int input) =>
            Task.FromResult(StepEffects.ThenComplete());

        public Task<StepEffect<string>> WithoutInputStep() =>
            Task.FromResult(StepEffects.ThenComplete());
    }

    [Fact]
    public void EffectsBuilder_TransitionTo_TypedStepWithInput_ProducesCorrectStepTransition()
    {
        var effect = new EffectsBuilder<string>().TransitionTo(FakeWorkflow.Steps.WithInput, 7);

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Equal("WithInput", transition.StepName);
        Assert.Equal(7, transition.Input);
    }

    [Fact]
    public void EffectsBuilder_TransitionTo_TypedStepWithoutInput_ProducesNullInput()
    {
        var effect = new EffectsBuilder<string>().TransitionTo(FakeWorkflow.Steps.WithoutInput);

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Equal("WithoutInput", transition.StepName);
        Assert.Null(transition.Input);
    }

    [Fact]
    public void StepEffectsBuilder_ThenTransitionTo_TypedStepWithInput_ProducesCorrectStepTransition()
    {
        var effect = new StepEffectsBuilder<string>().ThenTransitionTo(FakeWorkflow.Steps.WithInput, 3);

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Equal("WithInput", transition.StepName);
        Assert.Equal(3, transition.Input);
    }

    [Fact]
    public void StepEffectsBuilder_ThenTransitionTo_TypedStepWithoutInput_ProducesNullInput()
    {
        var effect = new StepEffectsBuilder<string>().ThenTransitionTo(FakeWorkflow.Steps.WithoutInput);

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Equal("WithoutInput", transition.StepName);
        Assert.Null(transition.Input);
    }

    [Fact]
    public void RecoverStrategy_FailoverTo_TypedStepWithInput_ProducesCorrectStrategy()
    {
        var strategy = RecoverStrategy.WithMaxRetries(2).FailoverTo(FakeWorkflow.Steps.WithInput, 9);

        Assert.Equal("WithInput", strategy.FailoverStepName);
        Assert.Equal(9, strategy.FailoverStepInput);
    }

    [Fact]
    public void WorkflowSettingsBuilder_StepTimeout_AcceptsTypedStepRef()
    {
        var settings = WorkflowSettings.Create()
            .StepTimeout(FakeWorkflow.Steps.WithInput, System.TimeSpan.FromSeconds(5))
            .Build();

        var stepSettings = Assert.Single(settings.StepSettings);
        Assert.Equal("WithInput", stepSettings.StepName);
    }

    [Fact]
    public void WorkflowSettingsBuilder_Timeout_AcceptsTypedStepRefWithoutInput()
    {
        var settings = WorkflowSettings.Create()
            .Timeout(System.TimeSpan.FromMinutes(1), FakeWorkflow.Steps.WithoutInput)
            .Build();

        Assert.NotNull(settings.WorkflowRecoverStrategy);
        Assert.Equal("WithoutInput", settings.WorkflowRecoverStrategy!.FailoverStepName);
        Assert.Null(settings.WorkflowRecoverStrategy.FailoverStepInput);
    }

    [Fact]
    public void WorkflowSettingsBuilder_Timeout_AcceptsTypedStepRefWithInput()
    {
        var settings = WorkflowSettings.Create()
            .Timeout(System.TimeSpan.FromMinutes(1), FakeWorkflow.Steps.WithInput, 7)
            .Build();

        Assert.NotNull(settings.WorkflowRecoverStrategy);
        Assert.Equal("WithInput", settings.WorkflowRecoverStrategy!.FailoverStepName);
        Assert.Equal(7, settings.WorkflowRecoverStrategy.FailoverStepInput);
    }
}
