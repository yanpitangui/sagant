using Sagant.Execution;

namespace Sagant.Protocol;

/// <summary>
/// Engine-level activation nudge, sent by infrastructure holding a deadline on an instance's behalf
/// (see <see cref="IWorkflowDeadlineScheduler"/>) when that deadline comes due.
///
/// Carries no instruction, and the handler writes nothing: activation is the whole effect. An
/// instance re-arms every pending deadline from its persisted absolute instant as it recovers, and
/// one already past fires straight away — which is what bounds the lateness guarantee <c>D8</c>
/// describes to the moment a wake lands.
///
/// It follows the plain lifecycle path alongside <see cref="GetStatus"/>, so it reaches an instance
/// while a step is running and answers immediately.
/// </summary>
/// <param name="Kind">Which deadline prompted the wake, recorded for tracing. Recovery re-arms every
/// deadline the instance holds, so the answer is the same for any value here.</param>
public sealed record Wake(WorkflowTimerKind Kind);
