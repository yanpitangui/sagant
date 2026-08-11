using Sagant.Descriptors;

namespace Sagant.Tests;

/// <summary>
/// Step references for tests that exercise a builder without a real workflow class behind it.
///
/// The authoring API names a step only through <see cref="StepRef{TWorkflow, TInput}"/>, which the
/// source generator emits per <c>[WorkflowStep]</c> method. A test asserting what a builder produces
/// has no such class, so it constructs the reference directly — the same escape a hand-written
/// dispatcher uses.
/// </summary>
internal static class Ref
{
    /// <summary>A reference to a step taking no input.</summary>
    public static StepRef<TWorkflow, NoInput> Step<TWorkflow>(string name) => new(name);

    /// <summary>A reference to a step taking <typeparamref name="TInput"/>.</summary>
    public static StepRef<TWorkflow, TInput> Step<TWorkflow, TInput>(string name) => new(name);
}

/// <summary>Stands in as the workflow a bare builder's steps belong to.</summary>
internal sealed class DocWorkflowFor<TState> : Workflow<TState>
{
    // Never instantiated for its state — this type exists only so a StepRef can name a
    // workflow at compile time.
    public override TState EmptyState() => default!;
}
