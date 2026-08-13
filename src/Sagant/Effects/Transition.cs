using Sagant.Protocol;
using Sagant.Settings;
namespace Sagant.Effects;

/// <summary>
/// What a workflow does next after a command or step handler runs. Closed hierarchy (all cases
/// are nested sealed records) so consumers pattern-match exhaustively.
/// </summary>
public abstract record Transition
{
    private Transition()
    {
    }

    public sealed record StepTransition(string StepName, object? Input) : Transition;

    public sealed record PauseTransition(string? Reason, PauseSettings? Settings) : Transition;

    /// <summary>
    /// The run is over. <paramref name="Outcome"/> says how — the single carrier for every terminal
    /// outcome, so adding one later is a change to <see cref="WorkflowOutcome"/> alone.
    ///
    /// A runtime enriches the outcome before persisting it: a <see cref="WorkflowOutcome.Failed"/>
    /// raised from a handler knows its message but not which step it was in or how many attempts had
    /// run, and the runtime fills those from the envelope.
    /// </summary>
    public sealed record TerminalTransition(WorkflowOutcome Outcome) : Transition;

    public sealed record DeleteTransition(string? Reason) : Transition;

    /// <summary>
    /// Hold the instance where it stands. The run stays alive with its step and input intact, so
    /// <c>IWorkflowHandle.Resume</c> re-runs that attempt with a fresh budget.
    ///
    /// Two routes reach it, both of them the engine's own: a step exhausting its retry budget under
    /// <see cref="Settings.RecoverStrategy.ParkOnExhaustion"/>, and an instance standing on a step
    /// the running deployment has no code for (guarantee E5). A handler that wants to wait has
    /// <c>ThenPause</c>, which is the workflow's own decision to wait for something; this case is the
    /// engine reporting that it ran out of ways to make progress.
    /// </summary>
    public sealed record ParkTransition(WorkflowFailure Failure) : Transition;

    /// <summary>
    /// Begin a fresh cycle under the same id: continue at <paramref name="StepName"/> and reclaim the
    /// history recorded so far.
    ///
    /// This is what bounds a workflow that runs indefinitely. Recorded events accumulate for as long
    /// as an instance lives, and a run with no natural end accumulates without limit; a restart gives
    /// such a workflow a point where its past becomes reclaimable while the run itself carries on.
    ///
    /// Carrying state forward is the handler's own decision, through the same
    /// <c>UpdateState</c> it would use on any other transition — a cycle counter survives, a
    /// per-cycle accumulation is dropped by writing a fresh value.
    ///
    /// What the instance keeps: its id, its state, and its deduplication ledgers, since a producer
    /// keeps counting sequence numbers across a restart. What it loses: its recorded history, its
    /// retry count, its workflow deadline (the next cycle establishes its own), and any children it
    /// owns, which are closed under <c>ParentClosePolicy</c> exactly as a terminal transition closes
    /// them.
    /// </summary>
    public sealed record RestartTransition(string StepName, object? Input, string? Reason) : Transition;

    /// <summary>
    /// Start <see cref="Children"/> and durably wait for their lifecycle outcomes — never a live
    /// <c>await</c>; this is persisted data, applied by the runtime driver the same way every other
    /// <c>Transition</c> case is. <paramref name="GroupId"/> is <c>null</c> for the common case
    /// (the runtime driver generates a durable id at persist time), non-null only when a workflow
    /// author explicitly named the group.
    /// </summary>
    public sealed record AwaitChildrenTransition(
        string? GroupId,
        IReadOnlyList<ChildStart> Children,
        CompletionPolicy CompletionPolicy,
        FailurePolicy FailurePolicy,
        RemainingChildrenPolicy RemainingChildrenPolicy,
        string ResumeStepName) : Transition;

    public sealed record NoTransition : Transition
    {
        public static readonly NoTransition Instance = new();
    }
}
