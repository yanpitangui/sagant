namespace Sagant;

/// <summary>
/// Everything a step handler is given for one execution attempt: the workflow state it runs
/// against, which attempt this is, and the token the runtime cancels when it stops waiting on this
/// step.
///
/// <see cref="State"/> is a value carried by this one invocation. That is what keeps two
/// concurrently-executing handlers from observing each other: a step runs off the runtime's own
/// thread and can be suspended at any <c>await</c>, so state reached through anything shared by the
/// workflow instance could be replaced underneath it between two reads inside a single handler body.
/// Passing it per invocation makes that unrepresentable.
///
/// <see cref="CancellationToken"/> is cancelled when the runtime stops waiting on this step — a
/// timeout, a suspend, a terminate, or a graceful-handoff window expiring. Cooperative, as
/// everywhere else in .NET: a step built on <c>HttpClient</c>/EF that honours it unwinds promptly; a
/// step that ignores it runs to completion with its result discarded.
/// </summary>
/// <param name="State">The workflow state this attempt runs against.</param>
/// <param name="Attempt">1-based attempt number — <c>1</c> on the first execution, incremented per
/// retry. The same numbering <c>TransitionCause.StepSucceeded</c>/<c>StepFailed</c> report and the
/// <c>sagant.step.duration</c> metric tags.</param>
/// <param name="CancellationToken">Cancelled when the runtime stops waiting on this step.</param>
public readonly record struct StepContext<TState>(
    TState State,
    int Attempt,
    CancellationToken CancellationToken);

/// <summary>
/// Everything a command handler is given: the workflow state to decide against.
///
/// No cancellation token and no attempt number, deliberately — a command handler is synchronous and
/// is never retried by the runtime. It is a decision function over <c>(state, command)</c>: inspect
/// the state, return an effect. Work that needs I/O belongs in a step, which the returned effect can
/// transition to.
///
/// <see cref="State"/> is a value carried by this one invocation, for the same reason as
/// <see cref="StepContext{TState}.State"/>.
/// </summary>
/// <param name="State">The workflow state this command decides against.</param>
public readonly record struct CommandContext<TState>(TState State);

/// <summary>
/// Everything a query handler is given: the workflow state to read, and a cancellation token.
///
/// <see cref="State"/> is the snapshot taken when this query was dispatched. A query may run
/// concurrently with a step, so the state it holds can be superseded while it is still executing —
/// it reads a consistent point in time, which is what a read wants, and it cannot write, so nothing
/// downstream depends on that point still being current.
///
/// <see cref="CancellationToken"/> is cancelled when the runtime gives up on this query: its own
/// server-side timeout, or the workflow instance stopping. A caller's own request timeout never
/// reaches the entity, so this token is the only bound that exists.
/// </summary>
/// <param name="State">Snapshot of the workflow state at dispatch time.</param>
/// <param name="CancellationToken">Cancelled on query timeout or workflow stop.</param>
public readonly record struct QueryContext<TState>(
    TState State,
    CancellationToken CancellationToken);
