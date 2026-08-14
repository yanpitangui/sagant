using Sagant.Clients;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Settings;

namespace Sagant.Scheduling;

/// <summary>
/// A schedule's own state. Immutable, like any workflow's.
/// </summary>
/// <param name="Spec">When it fires.</param>
/// <param name="TargetWorkflowType">Which workflow each occurrence starts.</param>
/// <param name="TargetCommand">The command each occurrence sends — carried as <c>object</c> for the
/// same reason <see cref="ChildStart.Command"/> is: a schedule addresses any workflow, so the type is
/// settled at the call site that created it rather than by this one.</param>
/// <param name="NextFireUtc">The occurrence this schedule is currently waiting for.</param>
/// <param name="FireCount">How many occurrences have started.</param>
/// <param name="SkippedCount">How many were passed over, by overlap policy or the catch-up window.
/// </param>
public sealed record ScheduleState(
    IScheduleSpec? Spec = null,
    string? TargetWorkflowType = null,
    object? TargetCommand = null,
    OverlapPolicy Overlap = OverlapPolicy.Skip,
    TimeSpan? CatchUpWindow = null,
    int? EndsAfter = null,
    DateTimeOffset? NextFireUtc = null,
    int FireCount = 0,
    int SkippedCount = 0,
    bool Paused = false,
    string? LastStartedEntityId = null);

/// <summary>
/// Runs a workflow on a schedule, by being a workflow itself.
///
/// Everything a schedule needs is something the engine already does. Waiting until an instant is a
/// pause with a deadline, so it inherits the deadline machinery and the wake that brings a passivated
/// instance back. Durability, retries, visibility and the command surface come from being an ordinary
/// workflow. What is left is arithmetic: which instant comes next.
///
/// <para>Recurrence stays here rather than being handed to whatever stores the deadline. A scheduler
/// that computed its own occurrences would hold a copy of the truth in a place this instance's journal
/// cannot see, and cron dialects differ enough between products that the copy would eventually
/// disagree.</para>
///
/// <para>An occurrence starts its target through <see cref="IWorkflowClient"/> rather than as a child.
/// A run started that way is independent: it outlives the cycle that began it, so a schedule rolling
/// its own history cannot take a still-running occurrence down with it.</para>
/// </summary>
public partial class ScheduleWorkflow : Workflow<ScheduleState>
{
    private readonly IWorkflowClient _client;
    private readonly TimeProvider _time;

    public ScheduleWorkflow(IWorkflowClient client, TimeProvider time)
    {
        _client = client;
        _time = time;
    }

    public override ScheduleState EmptyState() => new();

    public override WorkflowSettings Settings() => WorkflowSettings.Default;

    [WorkflowCommandHandler]
    public CommandEffect<ScheduleState> Start(StartSchedule cmd, CommandContext<ScheduleState> ctx)
    {
        var now = _time.GetUtcNow();
        var next = cmd.Spec.NextAfter(now);

        var state = ctx.State with
        {
            Spec = cmd.Spec,
            TargetWorkflowType = cmd.TargetWorkflowType,
            TargetCommand = cmd.TargetCommand,
            Overlap = cmd.Overlap,
            CatchUpWindow = cmd.CatchUpWindow,
            EndsAfter = cmd.EndsAfter,
            NextFireUtc = next,
            Paused = false,
        };

        return next is null
            ? Effects.UpdateState(state).Complete()
            : Effects.UpdateState(state).TransitionTo(Steps.WaitStep).ThenReply("scheduled");
    }

    /// <summary>
    /// Waits until the next occurrence. A pause with a deadline, so the instance holds nothing while
    /// it waits and the deadline machinery brings it back — a schedule sleeping until next month
    /// costs a row in a bucket and nothing resident.
    /// </summary>
    [WorkflowStep]
    public StepEffect<ScheduleState> WaitStep(StepContext<ScheduleState> ctx)
    {
        if (ctx.State.NextFireUtc is not { } next)
        {
            return StepEffects.ThenComplete();
        }

        var wait = next - _time.GetUtcNow();
        return StepEffects.ThenPause(
            PauseSettings
                .WithTimeout(wait > TimeSpan.Zero ? wait : TimeSpan.Zero)
                .WithReason($"next occurrence at {next:O}")
                .TimeoutHandler(Steps.FireStep));
    }

    /// <summary>
    /// Starts one occurrence and works out the next.
    ///
    /// The occurrence's id is derived from the instant it was scheduled for, so a fire that runs twice
    /// — a duplicated wake, a retry — addresses the same instance both times and the second is the
    /// engine's ordinary duplicate-command case rather than a second run of the work.
    /// </summary>
    [WorkflowStep]
    public async Task<StepEffect<ScheduleState>> FireStep(StepContext<ScheduleState> ctx)
    {
        var state = ctx.State;
        if (state.Spec is null || state.TargetWorkflowType is null || state.TargetCommand is null)
        {
            return StepEffects.ThenComplete();
        }

        var now = _time.GetUtcNow();
        var occurrence = state.NextFireUtc ?? now;
        var skip = await ShouldSkipAsync(state, occurrence, now, ctx.CancellationToken);

        var started = state.LastStartedEntityId;
        var fireCount = state.FireCount;
        var skipped = state.SkippedCount;

        if (skip is null)
        {
            started = OccurrenceIdFor(occurrence);
            await _client.For(state.TargetWorkflowType, started)
                .Send(state.TargetCommand, ctx.CancellationToken);
            fireCount++;
        }
        else
        {
            skipped++;
        }

        // Computed from the instant this occurrence was scheduled for, then advanced past anything
        // already in the past — so a fire that overran its own interval catches up to the present in
        // one move and the sequence keeps its original phase.
        var next = NextAfterNow(state.Spec, occurrence, now);

        var ended = state.EndsAfter is { } limit && fireCount >= limit;
        var updated = state with
        {
            NextFireUtc = ended ? null : next,
            FireCount = fireCount,
            SkippedCount = skipped,
            LastStartedEntityId = started,
        };

        if (ended || next is null)
        {
            return StepEffects.UpdateState(updated).ThenComplete();
        }

        // A fresh cycle per occurrence, so the history of the ones before it becomes reclaimable and
        // a schedule running for years keeps a journal the size of one cycle.
        return StepEffects.UpdateState(updated).ThenRestartAt(Steps.WaitStep, $"occurrence {fireCount}");
    }

    [WorkflowCommandHandler]
    public CommandEffect<ScheduleState> Pause(PauseSchedule cmd, CommandContext<ScheduleState> ctx) =>
        Effects.UpdateState(ctx.State with { Paused = true }).Pause("schedule held").ThenReply("paused");

    [WorkflowCommandHandler]
    public CommandEffect<ScheduleState> Resume(ResumeSchedule cmd, CommandContext<ScheduleState> ctx)
    {
        if (ctx.State.Spec is not { } spec)
        {
            return Effects.Reply("schedule was never configured");
        }

        // Measured from now, so a schedule held across several occurrences resumes at the next one
        // rather than firing everything it slept through.
        return Effects
            .UpdateState(ctx.State with { Paused = false, NextFireUtc = spec.NextAfter(_time.GetUtcNow()) })
            .TransitionTo(Steps.WaitStep)
            .ThenReply("resumed");
    }

    [WorkflowCommandHandler]
    public CommandEffect<ScheduleState> Trigger(TriggerSchedule cmd, CommandContext<ScheduleState> ctx) =>
        Effects.TransitionTo(Steps.FireStep).ThenReply("triggered");

    [WorkflowCommandHandler]
    public CommandEffect<ScheduleState> Cancel(CancelSchedule cmd, CommandContext<ScheduleState> ctx) =>
        Effects.UpdateState(ctx.State with { NextFireUtc = null }).Complete();

    [WorkflowQuery]
    public QueryEffect Status(GetScheduleStatus query, QueryContext<ScheduleState> ctx) =>
        QueryEffects.Reply(new ScheduleStatus(
            ctx.State.Paused,
            ctx.State.NextFireUtc,
            ctx.State.FireCount,
            ctx.State.LastStartedEntityId,
            ctx.State.SkippedCount));

    /// <summary>Why this occurrence is being passed over, or <c>null</c> to run it.</summary>
    private async Task<string?> ShouldSkipAsync(
        ScheduleState state, DateTimeOffset occurrence, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (state.CatchUpWindow is { } window && now - occurrence > window)
        {
            return "outside the catch-up window";
        }

        if (state.Overlap != OverlapPolicy.Skip || state.LastStartedEntityId is not { } previous)
        {
            return null;
        }

        var status = await _client.For(state.TargetWorkflowType!, previous)
            .GetStatus(cancellationToken: cancellationToken);

        return status is Protocol.WorkflowStatus.Finished or Protocol.WorkflowStatus.Deleted
            ? null
            : "the previous occurrence is still running";
    }

    /// <summary>
    /// The first occurrence strictly after <paramref name="now"/>, walking forward from
    /// <paramref name="from"/>. Walking rather than computing against <paramref name="now"/> directly
    /// is what keeps a schedule's phase: "every hour on the hour" stays on the hour after an outage.
    /// </summary>
    private static DateTimeOffset? NextAfterNow(IScheduleSpec spec, DateTimeOffset from, DateTimeOffset now)
    {
        var cursor = from;
        for (var i = 0; i < MaxCatchUpSteps; i++)
        {
            if (spec.NextAfter(cursor) is not { } candidate)
            {
                return null;
            }

            cursor = candidate;
            if (cursor > now)
            {
                return cursor;
            }
        }

        // A gap wider than this many occurrences: land on the first one after now and carry on,
        // giving up the original phase rather than the schedule.
        return spec.NextAfter(now);
    }

    /// <summary>
    /// How far the walk above goes before it stops chasing the phase. Reached only by a schedule that
    /// was away for a very long time relative to its interval — a per-minute schedule down for a week.
    /// </summary>
    private const int MaxCatchUpSteps = 10_000;

    private static string OccurrenceIdFor(DateTimeOffset occurrence) =>
        occurrence.UtcDateTime.ToString("yyyyMMddHHmmss");
}
