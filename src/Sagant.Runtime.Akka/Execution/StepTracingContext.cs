using System.Diagnostics;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Execution;

/// <summary>
/// Owns <see cref="WorkflowEntityActor{TWorkflow, TState}"/>'s distributed-tracing bookkeeping for
/// the current incarnation: which span the next command/step should chain off of, the cross-restart
/// link consumed exactly once after recovery, and the cross-child-group result links consumed once a
/// resume step starts. Pure state holder with no Akka dependency of its own — every ordering
/// guarantee this backs (siblings-not-chains on retry, link-not-continue across a crash, ...) lives
/// in the actor's own call sites and doc comments; this collaborator only relocates where the state
/// itself lives.
/// </summary>
internal sealed class StepTracingContext
{
    private ActivityContext? _recoveredLinkContext;
    private IReadOnlyList<ActivityLink>? _pendingResumeLinks;

    /// <summary>
    /// The parent context the next span (command or step) should chain off of — purely live, scoped
    /// to this incarnation only: this value once anything has run in it, <c>default</c> (no parent)
    /// if nothing has yet, including right after a fresh recovery. Deliberately distinct from
    /// <c>WorkflowRuntimeState.LastTraceParent</c> (the persisted, cross-restart value) — see
    /// <see cref="ResolveParentContext"/> and <see cref="ConsumeParentLink"/> for why the two must
    /// never be conflated.
    /// </summary>
    public string? LastActivityTraceParent { get; set; }

    public Activity? CurrentStepActivity { get; set; }

    public DateTimeOffset CurrentStepStartedAt { get; set; }

    /// <summary>
    /// <see cref="LastActivityTraceParent"/> once anything has run in this incarnation, <c>default</c>
    /// otherwise. Using <c>WorkflowRuntimeState.LastTraceParent</c> as a fallback here would silently
    /// continue the pre-crash trace under the same TraceId, misrepresenting a restart as a continuous
    /// trace — that persisted value has exactly one job, backing a cross-restart *link* (a different
    /// relationship), consumed via <see cref="ConsumeRecoveredLink"/> instead. See
    /// <c>WorkflowTracingTests.TraceContext_SurvivesActorRestart...</c>, which guards this: a
    /// recovered span must link to a new trace, never pick up the old TraceId directly.
    /// </summary>
    public ActivityContext ResolveParentContext() =>
        LastActivityTraceParent is { } traceParent && ActivityContext.TryParse(traceParent, null, out var parsed)
            ? parsed
            : default;

    /// <summary>
    /// Called once, from <c>OnRecoveryCompleted</c>, with the just-recovered envelope's persisted
    /// <c>LastTraceParent</c> — captures the cross-restart link this incarnation's first span should
    /// carry, consumed exactly once via <see cref="ConsumeRecoveredLink"/>.
    /// </summary>
    public void RecordRecoveredLink(string? persistedLastTraceParent)
    {
        if (persistedLastTraceParent is { } traceParent && ActivityContext.TryParse(traceParent, null, out var parsed))
        {
            _recoveredLinkContext = parsed;
        }
    }

    public IEnumerable<ActivityLink>? ConsumeRecoveredLink()
    {
        if (_recoveredLinkContext is not { } link)
        {
            return null;
        }

        _recoveredLinkContext = null;
        return new[] { new ActivityLink(link) };
    }

    /// <summary>
    /// The forward half of parent/child trace linking: a fresh child's very first activity links back
    /// to whichever span was active on the parent when it started this child (captured into
    /// <see cref="ChildWorkflowRelationship.TraceParent"/> at <c>AwaitChildren</c> time, carried to
    /// the child on its own <c>WorkflowEnvelope.ParentRelationship</c> — <paramref name="parentRelationship"/>
    /// here). <paramref name="persistedLastTraceParent"/> is read straight off the just-delivered
    /// envelope's own <c>LastTraceParent</c>, since this runs before the relationship is merged onto
    /// the actor's persisted envelope — on the very first delivery, that envelope doesn't have it yet.
    /// Gated on it being <c>null</c> — true only before
    /// this entity's very first activity has ever completed — so this fires exactly once in the
    /// entity's lifetime with no extra mutable flag needed, and never re-links a later command once
    /// that first activity's outcome has persisted. A fresh child never recovers on its first
    /// message, so this and <see cref="ConsumeRecoveredLink"/> never both apply to the same activity.
    /// </summary>
    public IEnumerable<ActivityLink>? ConsumeParentLink(string? persistedLastTraceParent, ChildWorkflowRelationship? parentRelationship)
    {
        if (persistedLastTraceParent is not null)
        {
            return null;
        }

        if (parentRelationship?.TraceParent is { } traceParent && ActivityContext.TryParse(traceParent, null, out var parsed))
        {
            return new[] { new ActivityLink(parsed) };
        }

        return null;
    }

    /// <summary>
    /// Set once an <c>AwaitChildren</c> group finalizes (see <c>ApplyChildLifecycleNotification</c>),
    /// consumed exactly once when the resume step starts — an in-memory-only, single-incarnation
    /// value: if this entity crashes between the group finalizing and the resume step completing, a
    /// recovery-triggered retry of that same step attempt loses these links (keeping only whatever
    /// <see cref="ConsumeRecoveredLink"/> provides), the same "best effort across a crash mid-attempt"
    /// tradeoff this codebase already accepts for other per-attempt-only data.
    /// </summary>
    public void SetPendingResumeLinks(IReadOnlyList<ActivityLink> links) => _pendingResumeLinks = links;

    public IEnumerable<ActivityLink>? ConsumeResumeLinks()
    {
        if (_pendingResumeLinks is not { } links)
        {
            return null;
        }

        _pendingResumeLinks = null;
        return links;
    }

    /// <summary>
    /// Forces the in-flight step's span closed early, ahead of that step's own <c>Task</c> eventually
    /// resolving (if ever) and <c>StepDescriptor{TState}.Invoke</c>'s own <c>using</c> naturally
    /// disposing it. Only needed for Suspend/Terminate: cancellation is cooperative-only, so a step
    /// that doesn't observe its <see cref="CancellationToken"/> keeps running orphaned — its span
    /// would otherwise stay open indefinitely in any exporter watching it, misrepresenting a
    /// suspended/terminated workflow as a still-hung operation.
    /// </summary>
    public void ForceCloseCurrentStepActivity(string description)
    {
        if (CurrentStepActivity is null)
        {
            return;
        }

        CurrentStepActivity.SetStatus(ActivityStatusCode.Error, description);
        CurrentStepActivity.Dispose();
        CurrentStepActivity = null;
    }

    public static IEnumerable<ActivityLink>? CombineLinks(IEnumerable<ActivityLink>? first, IEnumerable<ActivityLink>? second) =>
        (first, second) switch
        {
            (null, null) => null,
            (not null, null) => first,
            (null, not null) => second,
            _ => first!.Concat(second!),
        };

    public static IReadOnlyList<ActivityLink> BuildResultLinks(IEnumerable<ChildWorkflowRelationship> members)
    {
        var links = new List<ActivityLink>();
        foreach (var member in members)
        {
            if (member.ResultTraceParent is { } traceParent && ActivityContext.TryParse(traceParent, null, out var parsed))
            {
                links.Add(new ActivityLink(parsed));
            }
        }

        return links;
    }
}
