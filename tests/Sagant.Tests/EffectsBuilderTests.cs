using Sagant.Descriptors;
using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;

namespace Sagant.Tests;

public class EffectsBuilderTests
{
    [Fact]
    public void TransitionTo_WithInput_ProducesStepTransitionAndNoPersistence()
    {
        var effect = new EffectsBuilder<string>().TransitionTo(Ref.Step<DocWorkflowFor<string>, object>("ChargePayment"), 42);

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Equal("ChargePayment", transition.StepName);
        Assert.Equal(42, transition.Input);
        Assert.IsType<PersistenceEffect<string>.NoPersistence>(effect.Persistence);
    }

    [Fact]
    public void TransitionTo_WithoutThenReply_ConvertsImplicitlyToNoReply()
    {
        CommandEffect<string> effect = new EffectsBuilder<string>().TransitionTo(Ref.Step<DocWorkflowFor<string>, object>("ChargePayment"), 42);

        Assert.IsType<Reply.NoReply>(effect.Reply);
    }

    [Fact]
    public void TransitionTo_ThenReply_AttachesReplyToFinalEffect()
    {
        var effect = new EffectsBuilder<string>().TransitionTo(Ref.Step<DocWorkflowFor<string>, object>("ChargePayment"), 42).ThenReply("accepted");

        Assert.IsType<Transition.StepTransition>(effect.Transition);
        var reply = Assert.IsType<Reply.ReplyValue>(effect.Reply);
        Assert.Equal("accepted", reply.Value);
    }

    [Fact]
    public void TransitionTo_WithoutInput_ProducesNullInput()
    {
        var effect = new EffectsBuilder<string>().TransitionTo(Ref.Step<DocWorkflowFor<string>>("ChargePayment"));

        var transition = Assert.IsType<Transition.StepTransition>(effect.Transition);
        Assert.Null(transition.Input);
    }

    [Fact]
    public void UpdateState_ThenTransitionTo_CarriesUpdateStateIntoPersistence()
    {
        var effect = new EffectsBuilder<string>().UpdateState("new-state").TransitionTo(Ref.Step<DocWorkflowFor<string>>("NextStep"));

        var persistence = Assert.IsType<PersistenceEffect<string>.UpdateState>(effect.Persistence);
        Assert.Equal("new-state", persistence.NewState);
    }

    [Fact]
    public void Pause_NoArgs_ProducesPauseTransitionWithNullReasonAndSettings()
    {
        var effect = new EffectsBuilder<string>().Pause();

        var transition = Assert.IsType<Transition.PauseTransition>(effect.Transition);
        Assert.Null(transition.Reason);
        Assert.Null(transition.Settings);
    }

    [Fact]
    public void Pause_WithReason_SetsReason()
    {
        var effect = new EffectsBuilder<string>().Pause("awaiting approval");

        var transition = Assert.IsType<Transition.PauseTransition>(effect.Transition);
        Assert.Equal("awaiting approval", transition.Reason);
    }

    [Fact]
    public void Pause_WithSettings_SetsSettingsAndDerivesReason()
    {
        var settings = PauseSettings.WithTimeout(System.TimeSpan.FromHours(1))
            .WithReason("manual review")
            .TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AutoCancel"));

        var effect = new EffectsBuilder<string>().Pause(settings);

        var transition = Assert.IsType<Transition.PauseTransition>(effect.Transition);
        Assert.Equal(settings, transition.Settings);
        Assert.Equal("manual review", transition.Reason);
    }

    [Fact]
    public void End_And_Delete_ProduceExpectedTransitions()
    {
        var endEffect = new EffectsBuilder<string>().Complete();
        var deleteEffect = new EffectsBuilder<string>().Delete("cleanup");

        Assert.IsType<WorkflowOutcome.Completed>(Assert.IsType<Transition.TerminalTransition>(endEffect.Transition).Outcome);
        Assert.Equal("cleanup", Assert.IsType<Transition.DeleteTransition>(deleteEffect.Transition).Reason);
    }

    [Fact]
    public void Reply_ProducesNoTransitionAndReplyValue()
    {
        var effect = new EffectsBuilder<string>().Reply(99);

        Assert.IsType<Transition.NoTransition>(effect.Transition);
        var reply = Assert.IsType<Reply.ReplyValue>(effect.Reply);
        Assert.Equal(99, reply.Value);
        Assert.Null(reply.Metadata);
    }

    [Fact]
    public void Error_ProducesErrorReply()
    {
        var effect = new EffectsBuilder<string>().Error("something broke");

        var reply = Assert.IsType<Reply.ErrorValue>(effect.Reply);
        Assert.Equal("something broke", reply.Message);
    }
}
