using Sagant.Protocol;

namespace Sagant.Clients;

/// <summary>
/// A handle to one workflow instance, obtained via <see cref="IWorkflowClient.For{TWorkflow}"/>.
/// The full surface for talking to that instance — sending commands, waiting on replies, and
/// controlling its lifecycle.
/// </summary>
public interface IWorkflowHandle<TWorkflow>
{
    string EntityId { get; }

    /// <summary>
    /// Fire-and-forget: delivers <paramref name="command"/> with no reply. <c>ValueTask</c> rather
    /// than <c>void</c> or <c>Task</c> — a runtime backed by real I/O (e.g. a durable queue) needs
    /// to actually await delivery, but the reference runtime completes this synchronously
    /// (an in-memory send), so <c>ValueTask</c> avoids a <c>Task</c> allocation on what's
    /// meant to be a hot path — see <see cref="Request{TCommand, TReply}"/> for the send-and-wait
    /// counterpart.
    /// </summary>
    /// <param name="command">The command to deliver.</param>
    /// <param name="idempotencyKey">
    /// Optional, caller-supplied. Entirely opt-in — omit it and every call is treated as distinct.
    /// If supplied and this workflow instance has already handled a command with the same key, the
    /// cached outcome from the first call is returned without invoking the handler again. Meant for
    /// a caller retrying after an ambiguous failure (e.g. a timeout where it's unclear whether the
    /// command was actually delivered) to safely resend without risking a duplicate side effect.
    /// Backed by a capacity-bounded ledger per workflow instance — only the most recent
    /// <see cref="Sagant.Settings.WorkflowSettings.IdempotencyLedgerCapacity"/> keys are remembered,
    /// oldest evicted first, so a key resent long after many other commands have gone through is no
    /// longer deduplicated.
    /// </param>
    /// <param name="cancellationToken">Cancels waiting for delivery to complete.</param>
    /// <param name="metadata">
    /// Optional, caller-supplied context recorded against whatever this command causes — an acting
    /// user, a correlation id, a source system. The engine carries it through to anything reading the
    /// workflow's events and interprets none of it.
    ///
    /// It lives inside a serialized event, so it is unindexed and invisible to a query over columns:
    /// its job is to reach a projection, which lifts whatever it cares about into columns of its own.
    /// It also persists for as long as the instance's events do, so anything erasable on request
    /// belongs elsewhere.
    /// </param>
    ValueTask Send<TCommand>(
        TCommand command, CancellationToken cancellationToken = default, string? idempotencyKey = null,
        IReadOnlyDictionary<string, string>? metadata = null) where TCommand : notnull;

    /// <summary>Sends <paramref name="command"/> and waits for a typed reply — the counterpart to
    /// <see cref="Send{TCommand}"/>'s fire-and-forget.</summary>
    /// <param name="command">The command to deliver.</param>
    /// <param name="idempotencyKey">
    /// Optional, caller-supplied. Entirely opt-in — omit it and every call is treated as distinct.
    /// If supplied and this workflow instance has already handled a command with the same key, the
    /// cached reply from the first call is returned without invoking the handler again. Meant for
    /// a caller retrying after an ambiguous failure (e.g. a timeout where it's
    /// unclear whether the command was actually delivered) to safely resend without risking a
    /// duplicate side effect. Backed by a capacity-bounded ledger per workflow instance — only the
    /// most recent <see cref="Sagant.Settings.WorkflowSettings.IdempotencyLedgerCapacity"/> keys are
    /// remembered, oldest evicted first, so a key resent long after many other commands have gone
    /// through is no longer deduplicated.
    /// </param>
    /// <param name="timeout">How long to wait for the reply before timing out.</param>
    /// <param name="cancellationToken">Cancels waiting for the reply.</param>
    /// <param name="metadata">Caller-supplied context recorded against whatever this command causes.
    /// See <see cref="Send{TCommand}"/> for what it is and what it is unsuited to.</param>
    Task<TReply> Request<TCommand, TReply>(
        TCommand command, TimeSpan? timeout = null, CancellationToken cancellationToken = default,
        string? idempotencyKey = null, IReadOnlyDictionary<string, string>? metadata = null)
        where TCommand : notnull;

    /// <summary>
    /// Sends a read-only <c>[WorkflowQuery]</c> and waits for its reply. The third verb alongside
    /// <see cref="Send{TCommand}"/> (mutate, no reply) and <see cref="Request{TCommand, TReply}"/>
    /// (mutate and reply): <see cref="Query{TQuery, TReply}"/> observes and changes nothing.
    ///
    /// Because a query cannot write, it takes a different route to the workflow than a command: it
    /// is delivered directly, skipping whatever at-least-once machinery a runtime uses for commands
    /// (guaranteed delivery of a read buys nothing), and it dispatches immediately even while a step
    /// is executing — which is the point, for anything watching a long-running workflow's progress
    /// live.
    ///
    /// No <c>idempotencyKey</c>: replaying a read has no side effect to deduplicate.
    /// </summary>
    /// <param name="query">The query to deliver.</param>
    /// <param name="timeout">How long this caller waits for the reply. Independent of the workflow's
    /// own <see cref="Sagant.Settings.WorkflowSettings.DefaultQueryTimeout"/>, which is what actually
    /// bounds the handler — this timeout only ends the wait on this side.</param>
    /// <param name="cancellationToken">Cancels waiting for the reply.</param>
    Task<TReply> Query<TQuery, TReply>(TQuery query, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        where TQuery : notnull;

    Task<Done> Suspend(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default);
    Task<Done> Resume(TimeSpan? timeout = null, CancellationToken cancellationToken = default);
    Task<Done> Terminate(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asks the workflow to stop, letting it unwind first — see <see cref="Sagant.Protocol.Cancel"/>.
    /// Returns once the decision is durable, not once compensation has finished; wait for the run's
    /// completion if you need the latter.
    ///
    /// Prefer this to <see cref="Terminate"/> for anything a workflow should get to clean up after.
    /// </summary>
    Task<Done> Cancel(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Force-stops the workflow if still active, then physically purges everything persisted for it
    /// — see <see cref="Sagant.Protocol.Delete"/> for the full contract (works at any
    /// status, cascades to owned children, leaves the id fully reusable).
    /// </summary>
    Task<Done> Delete(string? reason = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    Task<WorkflowStatus> GetStatus(TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <paramref name="command"/>, then waits for the run to finish and returns how it ended
    /// along with its final state — going beyond a mere acknowledgement the command was accepted.
    ///
    /// A failed run comes back as a <see cref="WorkflowResult{TState}"/> carrying
    /// <see cref="Sagant.Protocol.WorkflowOutcome.Failed"/>, not as a thrown exception: a workflow
    /// that failed is a business result to decide about, not an exceptional condition in the caller's
    /// own control flow.
    ///
    /// The explicit <typeparamref name="TState"/> here is the one place it can't be erased — it must
    /// match the actual state type of <typeparamref name="TWorkflow"/>, which is not checked at
    /// compile time by this interface alone; the implementation checks it at runtime and throws
    /// immediately on a mismatch right at the call site, well before it could otherwise surface as an
    /// obscure cast exception three layers down.
    /// </summary>
    /// <param name="command">The command to deliver.</param>
    /// <param name="timeout">How long to wait for the workflow to reach a terminal status.</param>
    /// <param name="idempotencyKey">
    /// Optional, caller-supplied. Entirely opt-in — omit it and every call is treated as distinct.
    /// If supplied and this workflow instance has already handled a command with the same key, the
    /// outcome from the first call is returned without invoking the handler again.
    /// Meant for a caller retrying after an ambiguous failure (e.g. a timeout where it's unclear
    /// whether the command was actually delivered) to safely resend without risking a duplicate side
    /// effect. Backed by a capacity-bounded ledger per workflow instance — only the most recent
    /// <see cref="Sagant.Settings.WorkflowSettings.IdempotencyLedgerCapacity"/> keys are remembered,
    /// oldest evicted first, so a key resent long after many other commands have gone through is no
    /// longer deduplicated.
    /// </param>
    /// <param name="cancellationToken">Cancels waiting for the workflow to complete.</param>
    Task<WorkflowResult<TState>> RunAndAwaitResult<TState>(object command, TimeSpan timeout, string? idempotencyKey = null, CancellationToken cancellationToken = default);
}
