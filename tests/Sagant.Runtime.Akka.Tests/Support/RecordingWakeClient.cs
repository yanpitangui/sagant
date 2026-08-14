using System.Collections.Concurrent;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Tests.Support;

/// <summary>
/// An <see cref="IWorkflowClient"/> that records which instances were woken, and can refuse to answer
/// for a while — the two things a deadline test needs to observe. Only the string-keyed resolution
/// and <c>Wake</c> are implemented, since that is the whole of what a scheduler uses.
/// </summary>
public sealed class RecordingWakeClient : IWorkflowClient
{
    private readonly ConcurrentQueue<string> _woken = new();
    private int _attempts;

    /// <summary>Entity ids woken, in the order their wakes were answered.</summary>
    public IReadOnlyCollection<string> Woken => _woken;

    /// <summary>How many wakes have been attempted, answered or otherwise.</summary>
    public int Attempts => Volatile.Read(ref _attempts);

    /// <summary>Refuse every wake before this attempt number, so a test can watch one be retried.
    /// </summary>
    public int FailUntilAttempt { get; init; }

    public IWorkflowHandle<TWorkflow> For<TWorkflow>(string entityId) where TWorkflow : class =>
        throw new NotSupportedException("A deadline scheduler addresses an instance by type name.");

    public IWorkflowHandle For(string workflowType, string entityId) => new Handle(this, entityId);

    private sealed class Handle(RecordingWakeClient owner, string entityId) : IWorkflowHandle
    {
        public string EntityId => entityId;

        public Task<Done> Wake(WorkflowTimerKind kind, TimeSpan? timeout = null, CancellationToken ct = default)
        {
            var attempt = Interlocked.Increment(ref owner._attempts);
            if (attempt < owner.FailUntilAttempt)
            {
                return Task.FromException<Done>(new TimeoutException($"attempt {attempt} refused"));
            }

            owner._woken.Enqueue(entityId);
            return Task.FromResult(Done.Instance);
        }

        public ValueTask Send<TCommand>(
            TCommand command, CancellationToken cancellationToken = default, string? idempotencyKey = null,
            IReadOnlyDictionary<string, string>? metadata = null) where TCommand : notnull =>
            throw new NotSupportedException();

        public Task<TReply> Request<TCommand, TReply>(
            TCommand command, TimeSpan? timeout = null, CancellationToken cancellationToken = default,
            string? idempotencyKey = null, IReadOnlyDictionary<string, string>? metadata = null)
            where TCommand : notnull => throw new NotSupportedException();

        public Task<TReply> Query<TQuery, TReply>(
            TQuery query, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
            where TQuery : notnull => throw new NotSupportedException();

        public Task<Done> Suspend(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<Done> Resume(TimeSpan? timeout = null, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<Done> Terminate(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<Done> Cancel(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<Done> Delete(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<WorkflowStatus> GetStatus(TimeSpan? timeout = null, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<WorkflowResult<TState>> RunAndAwaitResult<TState>(
            object command, TimeSpan timeout, string? idempotencyKey = null, CancellationToken ct = default) =>
            throw new NotSupportedException();
    }
}
