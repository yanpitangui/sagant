using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Tests;

public class StepEffectsBuilderTests
{
    [Fact]
    public void ThenTransitionTo_WithInput_ProducesStepTransition()
    {
        var effect = new StepEffectsBuilder<string>().ThenTransitionTo(Ref.Step<DocWorkflowFor<string>, object>("DepositStep"), "deposit-input");

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Equal("DepositStep", transition.StepName);
        Assert.Equal("deposit-input", transition.Input);
        Assert.IsType<PersistenceEffect<string>.NoPersistence>(effect.Persistence);
    }

    [Fact]
    public void UpdateState_ThenThenTransitionTo_CarriesPersistence()
    {
        var effect = new StepEffectsBuilder<string>().UpdateState("withdrawn").ThenTransitionTo(Ref.Step<DocWorkflowFor<string>>("DepositStep"));

        var persistence = Assert.IsType<PersistenceEffect<string>.UpdateState>(effect.Persistence);
        Assert.Equal("withdrawn", persistence.NewState);
    }

    [Fact]
    public void ThenPause_ThenEnd_ThenDelete_ProduceExpectedTransitions()
    {
        var pauseEffect = new StepEffectsBuilder<string>().ThenPause("needs review");
        var endEffect = new StepEffectsBuilder<string>().ThenComplete();
        var deleteEffect = new StepEffectsBuilder<string>().ThenDelete("cleanup");

        Assert.Equal("needs review", Assert.IsType<Transition.PauseTransition>(pauseEffect.Transition).Reason);
        Assert.NotNull(Assert.IsType<Transition.TerminalTransition>(endEffect.Transition).Outcome);
        Assert.Equal("cleanup", Assert.IsType<Transition.DeleteTransition>(deleteEffect.Transition).Reason);
    }
}
