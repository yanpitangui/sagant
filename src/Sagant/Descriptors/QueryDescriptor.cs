using System.Diagnostics;
using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Descriptors;

/// <summary>
/// Zero-reflection binding of a query's runtime <see cref="Type"/> to a compiled invoker.
/// Emitted by the source generator, one per <c>[WorkflowQuery]</c> method.
///
/// Returns a <c>Task</c> even for a handler declared synchronously — the generator wraps that case,
/// so a runtime driver has one shape to drive regardless of how the handler was written.
/// </summary>
public readonly struct QueryDescriptor<TState>
{
    private readonly Func<Workflow<TState>, QueryContext<TState>, object, Task<QueryEffect>> _invoke;

    public QueryDescriptor(
        Type queryType, string queryTypeName, Func<Workflow<TState>, QueryContext<TState>, object, Task<QueryEffect>> invoke)
    {
        QueryType = queryType;
        QueryTypeName = queryTypeName;
        _invoke = invoke;
    }

    public Type QueryType { get; }

    /// <summary>
    /// The query type's display name, emitted by the source generator as a compile-time string
    /// literal — the same treatment <see cref="Workflow{TState}.WorkflowTypeName"/> and
    /// <see cref="StepDescriptor{TState}.Name"/> get, and for the same reason: every span name and
    /// metric tag reads from here, so none of them needs a runtime type lookup. A generic query type
    /// reads as it's written in source.
    /// </summary>
    public string QueryTypeName { get; }

    /// <summary>
    /// Builds this invocation's <see cref="QueryContext{TState}"/> from <paramref name="state"/> and
    /// <paramref name="cancellationToken"/>, then invokes the handler inside an
    /// <see cref="Activity"/> span on <see cref="WorkflowDiagnostics.ActivitySource"/> — the one
    /// sanctioned way any runtime drives a query, and therefore the one place its tracing and its
    /// <c>sagant.query.duration</c> metric live. Same split as
    /// <see cref="StepDescriptor{TState}.Invoke"/>: the runtime supplies the context this span sits
    /// in and any tags only it knows; everything else is owned here.
    ///
    /// The span's lifetime is tied to the returned <c>Task</c> by the <c>using</c> wrapping the
    /// <c>await</c> below, so it closes correctly whichever thread resumes the handler — the same
    /// mechanism a step's span needs, and for the same reason: ambient <see cref="Activity.Current"/>
    /// is <c>AsyncLocal</c>-scoped and does not survive a message-passing hop.
    /// </summary>
    public async Task<QueryEffect> Invoke(
        Workflow<TState> workflow,
        TState state,
        object query,
        CancellationToken cancellationToken,
        string workflowId = "",
        ActivityContext parentContext = default,
        IEnumerable<ActivityLink>? links = null,
        Action<Activity>? configureActivity = null)
    {
        using var activity = WorkflowDiagnostics.ActivitySource.StartActivity(
            $"{workflow.WorkflowTypeName}.Query.{QueryTypeName}", ActivityKind.Server, parentContext, tags: null, links: links);
        if (activity is not null)
        {
            configureActivity?.Invoke(activity);
        }

        // Every invocation goes through here, so this reads the clock as a raw timestamp: reading it
        // twice and comparing answers the duration question directly, with no object to allocate.
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var result = await _invoke(workflow, new QueryContext<TState>(state, cancellationToken, workflowId), query).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
            WorkflowDiagnostics.RecordQueryDuration(workflow.WorkflowTypeName, QueryTypeName, Stopwatch.GetElapsedTime(startedAt), "ok");
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddTag("exception.type", ex.GetType().FullName);
            activity?.AddTag("exception.message", ex.Message);
            WorkflowDiagnostics.RecordQueryDuration(
                workflow.WorkflowTypeName, QueryTypeName, Stopwatch.GetElapsedTime(startedAt),
                ex is OperationCanceledException ? "cancelled" : "error");
            throw;
        }
    }
}

/// <summary>
/// Implemented (by the source generator) on every workflow class that declares
/// <c>[WorkflowQuery]</c> methods. Lets the runtime dispatch an incoming query to the right handler
/// without reflection — required for NativeAOT/trimming, same as the step and command tables.
/// Always emitted alongside those two, even when a workflow declares no queries at all, so a runtime
/// driver can depend on all three being present.
/// </summary>
public interface IWorkflowQueryDispatcher<TState>
{
    bool TryGetQuery(Type queryType, out QueryDescriptor<TState> descriptor);
}
