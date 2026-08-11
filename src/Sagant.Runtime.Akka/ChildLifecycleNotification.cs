namespace Sagant.Runtime.Akka;

using Sagant.Protocol;

/// <summary>
/// A child workflow's report of its own terminal outcome to whichever workflow is waiting on it —
/// internal to this assembly, so an external caller has no code path to fabricate one (the same
/// non-impersonation guarantee <c>StepCompleted</c>/<c>StepFailed</c> already have as
/// actor-internal-only messages). Delivered through the same <c>Akka.Delivery</c> producer/consumer
/// path as any other command — durable, at-least-once, deduplicated by the existing
/// <c>HighestAppliedSeqNr</c> mechanism at the transport layer; <see cref="RelationshipId"/>/
/// <see cref="Generation"/> is the separate semantic-staleness identity a receiving parent checks on
/// top of that. <see cref="ResultTraceParent"/> carries the reporting child's own trace context at
/// report time — the source for backward-linking a group's <c>ResumeAt</c> step activity to each
/// member's final trace.
/// </summary>
internal sealed record ChildLifecycleNotification(
    string RelationshipId, string ChildWorkflowId, int Generation, ChildStatus Status, object? Result, WorkflowFailure? Failure,
    string? ResultTraceParent = null);
