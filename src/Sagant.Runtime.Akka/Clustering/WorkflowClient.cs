using Sagant.Clients;
using Sagant.Descriptors;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Clustering;

internal sealed class WorkflowClient : IWorkflowClient
{
    private readonly WorkflowHandleRegistry _registry;

    public WorkflowClient(WorkflowHandleRegistry registry) => _registry = registry;

    public IWorkflowHandle<TWorkflow> For<TWorkflow>(string entityId) where TWorkflow : class =>
        _registry.Resolve<TWorkflow>(entityId);

    public IWorkflowHandle For(string workflowType, string entityId) =>
        _registry.Resolve(workflowType, entityId);
}

internal sealed class WorkflowHandle<TWorkflow, TState> : IWorkflowHandle<TWorkflow>
    where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>
{
    private readonly WorkflowRef<TWorkflow, TState> _inner;

    public WorkflowHandle(WorkflowRef<TWorkflow, TState> inner) => _inner = inner;

    public string EntityId => _inner.EntityId;

    public ValueTask Send<TCommand>(
        TCommand command, CancellationToken cancellationToken = default, string? idempotencyKey = null,
        IReadOnlyDictionary<string, string>? metadata = null) where TCommand : notnull =>
        // WorkflowRef.Send has no cancellationToken parameter of its own because the underlying
        // enqueue Ask isn't cancellable yet — cancellationToken is accepted here to satisfy the
        // interface but isn't forwarded, a known gap in cancellation support on this path.
        _inner.Send(command, idempotencyKey, metadata);

    public Task<TReply> Request<TCommand, TReply>(
        TCommand command, TimeSpan? timeout = null, CancellationToken cancellationToken = default,
        string? idempotencyKey = null, IReadOnlyDictionary<string, string>? metadata = null)
        where TCommand : notnull =>
        _inner.Ask<TCommand, TReply>(command, idempotencyKey, timeout, cancellationToken, metadata);

    public Task<TReply> Query<TQuery, TReply>(TQuery query, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        where TQuery : notnull =>
        _inner.Query<TQuery, TReply>(query, timeout, cancellationToken);

    public Task<Done> Suspend(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.Suspend(reason, timeout, cancellationToken);

    public Task<Done> Resume(TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.Resume(timeout, cancellationToken);

    public Task<Done> Terminate(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.Terminate(reason, timeout, cancellationToken);

    public Task<Done> Cancel(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.Cancel(reason, timeout, cancellationToken);

    public Task<Done> Delete(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.Delete(reason, timeout, cancellationToken);

    public Task<WorkflowStatus> GetStatus(TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.GetStatus(timeout, cancellationToken);

    public Task<Done> Wake(WorkflowTimerKind kind, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _inner.Wake(kind, timeout, cancellationToken);

    public Task<WorkflowResult<TResultState>> RunAndAwaitResult<TResultState>(
        object command, TimeSpan timeout, string? idempotencyKey = null, CancellationToken cancellationToken = default)
    {
        if (typeof(TResultState) != typeof(TState))
        {
            throw new InvalidOperationException(
                $"RunAndAwaitResult<{typeof(TResultState).Name}> was called against a workflow whose actual " +
                $"state type is {typeof(TState).Name} — these must match.");
        }

        return _inner.RunAndAwaitResult(command, timeout, idempotencyKey, cancellationToken).ContinueWith(
            t =>
            {
                // Re-wrapped in the caller's own state type, which the guard above has already
                // established is the same type. The case is preserved: a caller switching over the
                // result sees what the entity reported.
                var state = (TResultState)(object)t.Result.State!;
                return t.Result switch
                {
                    WorkflowResult<TState>.Finished finished =>
                        (WorkflowResult<TResultState>)new WorkflowResult<TResultState>.Finished(finished.Outcome, state),
                    WorkflowResult<TState>.Parked parked =>
                        new WorkflowResult<TResultState>.Parked(parked.Cause, state),
                    _ => throw new InvalidOperationException(
                        $"Unrecognised {nameof(WorkflowResult<TState>)} case {t.Result.GetType().Name}."),
                };
            },
            cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
    }
}
