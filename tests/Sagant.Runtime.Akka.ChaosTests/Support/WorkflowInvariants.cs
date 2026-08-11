using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.ChaosTests.Support;

/// <summary>
/// What must hold for a workflow instance once the dust settles, checked against the events it
/// actually recorded.
///
/// Asserting after the fact rather than during is what makes a chaos test worth trusting. A test
/// that checks state while faults are still landing is asserting on timing, and will be flaky for
/// reasons that have nothing to do with the engine. Recorded events are durable facts: they can be
/// read once the cluster is quiet, and they say what really happened rather than what a probe
/// happened to observe.
///
/// Harness-independent by construction — it needs only an <see cref="IWorkflowEventFeed"/>, so the
/// same assertions run against an in-process cluster, separate node processes, or a deployment.
/// </summary>
public static class WorkflowInvariants
{
    /// <summary>Every invariant below, for one instance.</summary>
    public static async Task AssertAll(IWorkflowEventFeed feed, string entityId)
    {
        var events = await Read(feed, entityId);

        AssertAtMostOneTerminalEvent(entityId, events);
        AssertTerminalIsLast(entityId, events);
        AssertEveryStepAttemptIsAccountedFor(entityId, events);
        AssertDeadlinesOnlyEverResume(entityId, events);
    }

    private static async Task<List<WorkflowEvent>> Read(IWorkflowEventFeed feed, string entityId)
    {
        var events = new List<WorkflowEvent>();
        await foreach (var item in feed.ReadEntity(entityId))
        {
            events.Add(item.Event);
        }

        return events;
    }

    /// <summary>
    /// A run ends once. Two terminal events mean a relocated or recovered instance re-ran a
    /// transition it had already committed — the failure at-least-once delivery and the
    /// single-writer guarantee (C4) exist to rule out.
    /// </summary>
    private static void AssertAtMostOneTerminalEvent(string entityId, List<WorkflowEvent> events)
    {
        var terminal = events.Count(e => e is WorkflowEvent.RunFinished or WorkflowEvent.RunDeleted);
        Assert.True(
            terminal <= 1,
            $"{entityId}: {terminal} terminal events; a run ends once (D1, C4). Sequence: {Describe(events)}");
    }

    /// <summary>
    /// Nothing happens after a run ends. An event recorded past its terminal one means a late
    /// message reopened a finished instance, which H2 and the terminal guards exist to prevent.
    /// </summary>
    private static void AssertTerminalIsLast(string entityId, List<WorkflowEvent> events)
    {
        var terminalIndex = events.FindIndex(e => e is WorkflowEvent.RunFinished or WorkflowEvent.RunDeleted);
        if (terminalIndex < 0)
        {
            return;
        }

        var after = events.Skip(terminalIndex + 1).ToList();
        Assert.True(
            after.Count == 0,
            $"{entityId}: {after.Count} events recorded after the run ended: {Describe(after)}");
    }

    /// <summary>
    /// A step attempt reports one outcome. The same step and attempt number appearing as both
    /// succeeded and failed means a stale result from an abandoned attempt was applied — what C3
    /// promises is discarded.
    /// </summary>
    private static void AssertEveryStepAttemptIsAccountedFor(string entityId, List<WorkflowEvent> events)
    {
        var outcomes = events
            .OfType<WorkflowEvent.CausedEvent>()
            .Select(e => e.Cause)
            .Where(c => c is TransitionCause.StepSucceeded or TransitionCause.StepFailed)
            .Select(c => c switch
            {
                TransitionCause.StepSucceeded s => (Step: s.StepName, s.Attempt, Succeeded: true),
                TransitionCause.StepFailed f => (Step: f.StepName, f.Attempt, Succeeded: false),
                _ => throw new InvalidOperationException("unreachable"),
            })
            .ToList();

        var contradictory = outcomes
            .GroupBy(o => (o.Step, o.Attempt))
            .Where(g => g.Select(o => o.Succeeded).Distinct().Count() > 1)
            .Select(g => $"{g.Key.Step}#{g.Key.Attempt}")
            .ToList();

        Assert.True(
            contradictory.Count == 0,
            $"{entityId}: step attempts reported both success and failure: {string.Join(", ", contradictory)} (C3)");
    }

    /// <summary>
    /// Guarantee D2: a deadline survives a crash as an absolute instant, so recovery resumes the
    /// remaining wait. A step deadline moving <em>later</em> across a restart of the same attempt
    /// would mean the clock was restarted rather than resumed, which is how a workflow silently
    /// outlives the bound its author set.
    /// </summary>
    private static void AssertDeadlinesOnlyEverResume(string entityId, List<WorkflowEvent> events)
    {
        string? step = null;
        DateTimeOffset? deadline = null;

        foreach (var @event in events)
        {
            switch (@event)
            {
                // Entering a step, or retrying it, legitimately sets a fresh deadline.
                case WorkflowEvent.StepStarted started:
                    step = started.StepName;
                    deadline = started.StepDeadline;
                    break;

                case WorkflowEvent.StepRetryScheduled retry:
                    deadline = retry.StepDeadline;
                    break;

                // Resuming re-arms the same attempt, so its deadline may not drift outward.
                case WorkflowEvent.RunResumed resumed when deadline is { } previous && resumed.StepDeadline is { } now:
                    Assert.True(
                        now <= previous,
                        $"{entityId}: step '{step}' resumed with a later deadline ({now:O} > {previous:O}); "
                        + "a resumed wait is the remaining one, never a fresh clock (D2)");
                    deadline = now;
                    break;
            }
        }
    }

    private static string Describe(IEnumerable<WorkflowEvent> events) =>
        string.Join(" -> ", events.Select(e => e.GetType().Name));
}
