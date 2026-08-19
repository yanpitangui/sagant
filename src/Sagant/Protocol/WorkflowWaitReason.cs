namespace Sagant.Protocol;

/// <summary>
/// Why a run reported by <c>WorkflowResult{TState}.Waiting</c> is holding — the structured
/// counterpart to a bare string, same reasoning as <see cref="WorkflowFailure"/>: a reader gets the
/// parts that matter as data, not a message to parse.
/// </summary>
/// <param name="Reason">The free text a business pause (<c>ThenPause</c>) or an operator
/// <c>Suspend</c> was given, if any.</param>
/// <param name="CurrentStepName">The step the run is holding at. Populated for an operator hold
/// (<c>Suspended</c> keeps it, so <c>Resume</c> knows what to re-execute); <c>null</c> for a business
/// pause (<c>Paused</c> clears it — there is no step to resume, only a command to wait for).</param>
/// <param name="Deadline">The absolute instant this hold stops waiting on its own, if it has one —
/// <c>WorkflowRuntimeState.PauseDeadline</c> or <c>WorkflowRuntimeState.HoldDeadline</c> depending on
/// which status this is. <c>null</c> for a hold that waits for a command alone, however long that
/// takes.</param>
/// <param name="TimeoutStepName">The step run when <paramref name="Deadline"/> passes. <c>null</c>
/// whenever <paramref name="Deadline"/> is.</param>
public sealed record WorkflowWaitReason(
    string? Reason,
    string? CurrentStepName,
    DateTimeOffset? Deadline,
    string? TimeoutStepName);
