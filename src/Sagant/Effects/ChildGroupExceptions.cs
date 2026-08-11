namespace Sagant.Effects;

using Sagant.Protocol;

/// <summary>A <c>ChildGroupResult.Get</c>/<c>GetAll</c> call named a <c>workflowId</c> that isn't a
/// member of this group at all — almost always a typo'd id.</summary>
public sealed class ChildNotInGroupException(string workflowId)
    : Exception($"'{workflowId}' is not a member of this child group.")
{
    public string WorkflowId { get; } = workflowId;
}

/// <summary>A <c>ChildGroupResult.Get</c>/<c>GetAll</c> call's <c>TWorkflow</c> doesn't match the
/// member's actual persisted <c>WorkflowType</c> — surfaces as this named exception at the call site,
/// the same fail-loud philosophy <c>WorkflowRef.RunAndAwaitResult&lt;TResultState&gt;</c> already uses
/// for its own runtime type check.</summary>
public sealed class ChildWorkflowTypeMismatchException(string workflowId, string expectedType, string actualType)
    : Exception($"Child '{workflowId}' is a '{actualType}', not the requested '{expectedType}'.")
{
    public string WorkflowId { get; } = workflowId;
    public string ExpectedWorkflowType { get; } = expectedType;
    public string ActualWorkflowType { get; } = actualType;
}

/// <summary>A <c>ChildGroupResult.Get</c> call's member exists and matches the requested workflow
/// type, but never reached <see cref="ChildStatus.Completed"/> — covers <c>Failed</c>/<c>Cancelled</c>/
/// <c>Terminated</c>/still-<c>Pending</c> uniformly (see <see cref="ChildStatus"/>'s own doc comment
/// for the reasoning behind grouping <c>Pending</c> here).</summary>
public sealed class ChildResultNotAvailableException(string workflowId, ChildStatus status)
    : Exception($"Child '{workflowId}' has no result available — its status is '{status}', not Completed.")
{
    public string WorkflowId { get; } = workflowId;
    public ChildStatus Status { get; } = status;
}
