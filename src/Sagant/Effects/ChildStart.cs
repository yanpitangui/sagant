namespace Sagant.Effects;

/// <summary>
/// One child to start — the internal, runtime-facing shape. Workflow authors build these via
/// <c>StepEffectsBuilder{TState}.Child{TWorkflow}</c>, never this constructor directly.
/// <see cref="WorkflowId"/> doubles as the lookup key
/// <c>ChildGroupResult</c> uses later — no separate key concept, the id the caller already chose is
/// how they find that child's outcome.
/// </summary>
public readonly record struct ChildStart(
    string WorkflowType, string WorkflowId, object Command, ParentClosePolicy ParentClosePolicy = ParentClosePolicy.Abandon);
