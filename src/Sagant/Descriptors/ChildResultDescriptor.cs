using Sagant.Effects;

namespace Sagant.Descriptors;

/// <summary>
/// Zero-reflection binding of a workflow's <c>[WorkflowChildResult]</c> method to a compiled
/// invoker. Emitted by the source generator, at most one per workflow.
///
/// Synchronous, like <see cref="CommandDescriptor{TState}"/> and for the same reason: the effect it
/// returns is written in the same atomic batch as the child report that triggered it, so there is
/// nothing for a driver to await between deciding and persisting.
/// </summary>
public readonly struct ChildResultDescriptor<TState>
{
    private readonly Func<Workflow<TState>, ChildResultContext<TState>, ChildResultEffect<TState>> _invoke;

    public ChildResultDescriptor(
        Func<Workflow<TState>, ChildResultContext<TState>, ChildResultEffect<TState>> invoke) => _invoke = invoke;

    /// <summary>Runs the handler against <paramref name="context"/>.</summary>
    public ChildResultEffect<TState> Invoke(Workflow<TState> workflow, ChildResultContext<TState> context) =>
        _invoke(workflow, context);
}

/// <summary>
/// Generated lookup for a workflow's child-result handler. Implemented by the source generator on
/// every workflow class, reporting <c>false</c> where none is declared — which is the common case,
/// and the one that costs a parent nothing.
/// </summary>
public interface IWorkflowChildResultDispatcher<TState>
{
    bool TryGetChildResultHandler(out ChildResultDescriptor<TState> descriptor);
}
