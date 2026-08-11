using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Settings;

namespace Sagant.Execution;

/// <summary>
/// What a driver should do about a step attempt that failed. Exactly one of the two cases, decided by
/// <see cref="WorkflowTransitionPlanner.PlanStepFailure{TState}"/>.
/// </summary>
public abstract record StepFailurePlan<TState>
{
    private StepFailurePlan()
    {
    }

    /// <summary>
    /// The retry budget has room. <paramref name="Events"/> records the attempt;
    /// <paramref name="RetryDelayUntil"/> is <c>null</c> to start immediately, otherwise the absolute
    /// instant to wait until.
    /// </summary>
    /// <param name="Events">Facts to record before the next attempt.</param>
    /// <param name="RetryDelayUntil">Absolute instant the next attempt may begin, or <c>null</c> for
    /// immediately. An absolute instant, so a crash mid-wait resumes the remaining delay
    /// (guarantee D2).</param>
    /// <param name="Attempt">1-based number of the attempt about to run — what
    /// <see cref="RecoverStrategy.BackoffForAttempt"/> was asked about.</param>
    public sealed record Retry(
        IReadOnlyList<WorkflowEvent> Events,
        DateTimeOffset? RetryDelayUntil,
        int Attempt) : StepFailurePlan<TState>;

    /// <summary>
    /// The budget is exhausted, or there was no strategy at all. <paramref name="Transition"/> is the
    /// failover step when one is configured, otherwise an end carrying the failure reason.
    /// </summary>
    public sealed record Conclude(Transition Transition) : StepFailurePlan<TState>;
}
