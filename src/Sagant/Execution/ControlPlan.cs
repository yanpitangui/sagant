using Sagant.Protocol;

namespace Sagant.Execution;

/// <summary>
/// What an operator control command means for an instance. Exactly one of the two cases, decided by
/// <see cref="WorkflowTransitionPlanner"/>.
///
/// Control commands sit outside the <c>Transition</c> model: they are an operator overriding what a
/// workflow is doing, with no handler behind them and no effect to apply. They still record events
/// and carry consequences, and they can be rejected outright — which is the case
/// <see cref="TransitionPlan{TState}"/> has no room for, and why this is its own shape.
/// </summary>
public abstract record ControlPlan<TState>
{
    private ControlPlan(){ }

    /// <summary>
    /// The command applies. <paramref name="Events"/> records it; <paramref name="AfterPersist"/> is
    /// what follows once they are durable, on the same terms as
    /// <see cref="TransitionPlan{TState}"/>'s decisions.
    /// </summary>
    public sealed record Apply(
        IReadOnlyList<WorkflowEvent> Events,
        IReadOnlyList<WorkflowDecision> AfterPersist) : ControlPlan<TState>;

    /// <summary>
    /// The command doesn't apply from the instance's current status — suspending something already
    /// suspended, resuming something that was never suspended. <paramref name="Reason"/> is the
    /// message a caller sees.
    /// </summary>
    public sealed record Reject(string Reason) : ControlPlan<TState>;
}
