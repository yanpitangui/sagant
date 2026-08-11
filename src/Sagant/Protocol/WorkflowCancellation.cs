namespace Sagant.Protocol;

/// <summary>
/// Passed as the input to a workflow's cancellation step (see
/// <see cref="Settings.WorkflowSettings.CancellationStepName"/>), so that step can tell a
/// cancellation apart from any other way it might be reached and can see why one was asked for.
/// </summary>
/// <param name="Reason">Why cancellation was requested, as supplied by the caller.</param>
public sealed record WorkflowCancellation(string? Reason);
