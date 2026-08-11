using Sagant.Protocol;

namespace Sagant.Clients;

/// <summary>
/// A workflow instance's lifecycle at a glance — the fields every workflow has, whatever its
/// <c>TState</c> holds. Business fields belong in a consumer's own projection, which reads them off
/// <see cref="WorkflowEvent"/>'s state events.
/// </summary>
/// <param name="EntityId">The routable id, ready to hand back to
/// <see cref="IWorkflowClient.For{TWorkflow}"/>.</param>
/// <param name="StartedAt">When the instance recorded its first event.</param>
/// <param name="EndedAt">When it finished, or <c>null</c> while it is still running.</param>
/// <param name="LastTraceParent">The trace this run last recorded under, so a listing can link
/// straight into it.</param>
public sealed record WorkflowVisibilityRecord(
    string EntityId,
    string WorkflowType,
    WorkflowStatus Status,
    WorkflowOutcome? Outcome,
    string? CurrentStepName,
    int Attempt,
    DateTimeOffset StartedAt,
    DateTimeOffset? EndedAt,
    string? LastTraceParent);

/// <summary>
/// Which instances a listing should return. Every field narrows the result; leaving one unset leaves
/// that dimension unconstrained.
/// </summary>
public sealed record WorkflowVisibilityFilter(
    string? WorkflowType = null,
    IReadOnlyCollection<WorkflowStatus>? Statuses = null,
    DateTimeOffset? StartedAfter = null,
    DateTimeOffset? StartedBefore = null,
    string? EntityIdPrefix = null,
    int? Limit = null);

/// <summary>
/// Answers "what is the state of things" across instances — the questions
/// <see cref="IWorkflowClient.For{TWorkflow}"/> cannot, because it needs an id the caller already
/// holds. Listing every running order, everything that failed overnight, or anything stuck on one
/// step all start here.
///
/// A runtime's own implementation derives these by reading recorded events, which suits a modest
/// fleet and needs no extra storage. Because this is an interface, a deployment that outgrows that
/// points it at its own projected table and its callers do not change.
///
/// <see cref="IWorkflowEventFeed"/> is the counterpart: this reports state, that reports what
/// happened, including a single instance's history.
/// </summary>
public interface IWorkflowVisibilityQuery
{
    /// <summary>One instance, or <c>null</c> when nothing was ever recorded under
    /// <paramref name="entityId"/>.</summary>
    Task<WorkflowVisibilityRecord?> GetAsync(string entityId, CancellationToken cancellationToken = default);

    /// <summary>Every instance matching <paramref name="filter"/>.</summary>
    IAsyncEnumerable<WorkflowVisibilityRecord> ListAsync(
        WorkflowVisibilityFilter filter, CancellationToken cancellationToken = default);
}
