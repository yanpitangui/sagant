using Sagant.Descriptors;
using Sagant.Settings;
namespace Sagant.Effects;

/// <summary>
/// Fluent builder for command-handler effects. Obtain via <see cref="Workflow{TState}.Effects"/>.
/// </summary>
public sealed class EffectsBuilder<TState>
{
    private PersistenceEffect<TState> _persistence = PersistenceEffect<TState>.NoPersistence.Instance;

    public EffectsBuilder<TState> UpdateState(TState newState)
    {
        _persistence = new PersistenceEffect<TState>.UpdateState(newState);
        return this;
    }

    public TransitionalEffect<TState> Pause() =>
        Build(new Transition.PauseTransition(null, null));

    public TransitionalEffect<TState> Pause(string reason) =>
        Build(new Transition.PauseTransition(reason, null));

    public TransitionalEffect<TState> Pause(PauseSettings settings) =>
        Build(new Transition.PauseTransition(settings.Reason, settings));

    public TransitionalEffect<TState> TransitionTo<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, TInput input)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.StepTransition(step.Name, input));

    public TransitionalEffect<TState> TransitionTo<TWorkflow>(StepRef<TWorkflow, NoInput> step)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.StepTransition(step.Name, null));

    /// <summary>
    /// Begin a fresh cycle at <paramref name="step"/>, reclaiming the history recorded so far — see
    /// <see cref="Transition.RestartTransition"/> for what an instance keeps across one.
    /// </summary>
    public TransitionalEffect<TState> RestartAt<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, TInput input, string? reason = null)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.RestartTransition(step.Name, input, reason));

    /// <inheritdoc cref="RestartAt{TWorkflow, TInput}"/>
    public TransitionalEffect<TState> RestartAt<TWorkflow>(StepRef<TWorkflow, NoInput> step, string? reason = null)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.RestartTransition(step.Name, null, reason));

    /// <summary>Finish successfully.</summary>
    public TransitionalEffect<TState> Complete() =>
        Build(new Transition.TerminalTransition(global::Sagant.Protocol.WorkflowOutcome.Completed.Instance));

    /// <summary>
    /// Finish as failed, with <paramref name="message"/> describing why. The runtime fills in which
    /// step this came from and how many attempts had run.
    /// </summary>
    public TransitionalEffect<TState> Fail(string message) =>
        Build(new Transition.TerminalTransition(
            new global::Sagant.Protocol.WorkflowOutcome.Failed(new global::Sagant.Protocol.WorkflowFailure(message))));

    /// <summary>
    /// Finish as cancelled — the run was asked to stop and has now unwound. Normally the last thing a
    /// cancellation step does.
    /// </summary>
    public TransitionalEffect<TState> Cancel() =>
        Build(new Transition.TerminalTransition(new global::Sagant.Protocol.WorkflowOutcome.Cancelled(null)));

    /// <summary>Finish as cancelled, recording why.</summary>
    public TransitionalEffect<TState> Cancel(string reason) =>
        Build(new Transition.TerminalTransition(new global::Sagant.Protocol.WorkflowOutcome.Cancelled(reason)));

    /// <summary>
    /// Finish as failed, capturing <paramref name="exception"/> — its type, stack trace and whole
    /// inner chain — so a caller inspecting the failure later sees what was actually thrown rather
    /// than a flattened message.
    /// </summary>
    public TransitionalEffect<TState> Fail(Exception exception) =>
        Build(new Transition.TerminalTransition(
            new global::Sagant.Protocol.WorkflowOutcome.Failed(
                global::Sagant.Protocol.WorkflowFailure.FromException(exception))));

    public TransitionalEffect<TState> Delete() =>
        Build(new Transition.DeleteTransition(null));

    public TransitionalEffect<TState> Delete(string reason) =>
        Build(new Transition.DeleteTransition(reason));

    public CommandEffect<TState> Reply<TReply>(TReply value) =>
        new(_persistence, Transition.NoTransition.Instance, new global::Sagant.Effects.Reply.ReplyValue(value, null));

    public CommandEffect<TState> Reply<TReply>(TReply value, object metadata) =>
        new(_persistence, Transition.NoTransition.Instance, new global::Sagant.Effects.Reply.ReplyValue(value, metadata));

    public CommandEffect<TState> Error(string message) =>
        new(_persistence, Transition.NoTransition.Instance, new global::Sagant.Effects.Reply.ErrorValue(message));

    private TransitionalEffect<TState> Build(Transition transition) =>
        new(_persistence, transition);
}
