namespace Sagant.Descriptors;

/// <summary>
/// A statically-typed reference to a workflow step: its durable name plus the input type it
/// expects. Instances are emitted by the source generator (one per <c>[WorkflowStep]</c> method,
/// under <c>TWorkflow.Steps</c>) so <c>TransitionTo</c>/<c>FailoverTo</c>/<c>StepTimeout</c> calls
/// are checked against the step's actual input type at compile time.
/// </summary>
public readonly record struct StepRef<TWorkflow, TInput>(string Name);
