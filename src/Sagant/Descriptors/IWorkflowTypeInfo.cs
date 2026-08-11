namespace Sagant.Descriptors;

/// <summary>
/// A workflow's durable type name, resolvable from just the type parameter — no instance required.
/// Needed at call sites that have only a generic <c>TWorkflow</c> and nothing constructed yet:
/// <c>StepEffectsBuilder&lt;TState&gt;.Child&lt;TWorkflow&gt;</c> (describing a child to start,
/// before it exists) and a runtime driver's workflow-type registry (host-startup registration,
/// before any instance of that type has ever run). <c>Sagant.SourceGenerators.StepRegistryGenerator</c>
/// implements this on every generated workflow class, using the same compile-time string literal it
/// already emits for the instance-level <see cref="Workflow{TState}.WorkflowTypeName"/> override —
/// zero reflection either way.
/// </summary>
public interface IWorkflowTypeInfo
{
    static abstract string WorkflowTypeName { get; }
}
