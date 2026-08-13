using Sagant.Descriptors;
namespace Sagant.Settings;

/// <summary>
/// Describes how a step (or the workflow as a whole) recovers from failure: how many times to
/// retry, which step to fail over to once the retry budget is exhausted, and (optionally) how long
/// to wait before each retry.
/// </summary>
/// <param name="BackoffForAttempt">Computes the delay before a retry, given the 1-based attempt
/// number about to run (same numbering <c>TransitionCause.StepFailed</c> reports) — e.g. the
/// value passed is <c>2</c> for the delay before the second attempt. <c>null</c> (the default)
/// means no delay: a retry starts immediately, exactly like every version of this engine before
/// this field existed. See <see cref="RetryBackoff"/> for ready-made fixed/exponential
/// implementations, or supply any <c>Func&lt;int, TimeSpan&gt;</c> of your own — there's no
/// interface to implement.</param>
/// <param name="FailoverStepName">The step to run once the budget is spent, or <c>null</c> when the
/// strategy ends the run or parks it — see <paramref name="ParkOnExhaustion"/>.</param>
/// <param name="ParkOnExhaustion">Holds the instance at its failed step, keeping the run alive so a
/// problem outside the workflow can be fixed and the step retried through
/// <c>IWorkflowHandle.Resume</c>. Meaningful only where no failover step is set, since a failover is
/// itself the recovery path. Zeebe raises an incident here and waits; this is that behaviour, chosen
/// per step.</param>
public sealed record RecoverStrategy(
    int MaxRetries,
    string? FailoverStepName,
    object? FailoverStepInput,
    Func<int, TimeSpan>? BackoffForAttempt = null,
    bool ParkOnExhaustion = false)
{
    public static RecoverStrategyBuilder WithMaxRetries(int maxRetries) => new(maxRetries);

    public RecoverStrategy WithBackoff(Func<int, TimeSpan> backoff) => this with { BackoffForAttempt = backoff };
}

/// <summary>
/// Fluent continuation from <see cref="RecoverStrategy.MaxRetries"/> — names what happens once that
/// budget is spent, which every strategy has to say.
/// </summary>
public readonly struct RecoverStrategyBuilder
{
    private readonly int _maxRetries;

    internal RecoverStrategyBuilder(int maxRetries) => _maxRetries = maxRetries;

    /// <summary>Run <paramref name="step"/> — a compensating path the workflow models itself.</summary>
    public RecoverStrategy FailoverTo<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, TInput input) =>
        new(_maxRetries, step.Name, input);

    /// <inheritdoc cref="FailoverTo{TWorkflow, TInput}"/>
    public RecoverStrategy FailoverTo<TWorkflow>(StepRef<TWorkflow, NoInput> step) =>
        new(_maxRetries, step.Name, null);

    /// <summary>
    /// Hold the instance at the step that failed, for someone to look at.
    ///
    /// The run stays alive and keeps its state, its step, and that step's input, so
    /// <c>IWorkflowHandle.Resume</c> re-runs exactly the attempt that failed with a fresh budget.
    /// Reach for this where the cause is likely outside the workflow — a gateway that is down, a
    /// credential that expired — and the right response is to fix that and try again.
    ///
    /// <para><b>A parked run is alive, and what waits on one has to allow for that.</b> A caller in
    /// <c>RunAndAwaitResult</c> is released with a <see cref="Protocol.WorkflowResult{TState}.Parked"/>
    /// carrying the failure, since waiting longer achieves nothing until someone acts on it. A parked
    /// <em>child</em> reports nothing to its parent, so the group awaiting it waits with it — for a
    /// child, a strategy that fails or fails over keeps the parent's group moving.</para>
    /// </summary>
    public RecoverStrategy ThenPark() => new(_maxRetries, null, null, ParkOnExhaustion: true);

    /// <summary>End the run as failed once the budget is spent.</summary>
    public RecoverStrategy ThenFail() => new(_maxRetries, null, null);
}
