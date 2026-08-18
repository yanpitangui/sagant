using Sagant.Descriptors;

namespace Sagant.Scheduling;

/// <summary>What a schedule does when its previous run is still going as the next one comes due.</summary>
public enum OverlapPolicy
{
    /// <summary>Start it anyway. Right for work that is independent run to run.</summary>
    Allow,

    /// <summary>
    /// Leave this occurrence out. Right for work that would conflict with itself — a reconciliation
    /// pass reading the same rows, for one.
    ///
    /// <para><b>It waits for the previous occurrence to finish, however long that takes.</b> This
    /// reads as "skip while they overlap", and it is: an occurrence that never finishes overlaps
    /// every one after it, so a single stuck run leaves the schedule waking on time, deciding to
    /// skip, and never firing again. The schedule looks healthy from the outside — what says
    /// otherwise is <see cref="ScheduleStatus.SkippedCount"/> climbing while
    /// <see cref="ScheduleStatus.FireCount"/> stands still.</para>
    ///
    /// <para>A schedule using this wants its target bounded — a workflow timeout, or a deadline on
    /// whatever the target waits for — so that a run which stops making progress eventually ends on
    /// its own. <see cref="Allow"/> trades that guard back for a schedule that fires regardless.
    /// </para>
    /// </summary>
    Skip,
}

/// <summary>Starts a schedule, or replaces the spec of one already running.</summary>
/// <param name="Spec">When it fires.</param>
/// <param name="TargetWorkflowType">The registered type name of the workflow each occurrence starts.
/// </param>
/// <param name="TargetCommand">The command each occurrence sends. Stored on the schedule's own state
/// and therefore written to its journal, so it carries the same serialization requirement it already
/// has as a command.</param>
/// <param name="Overlap">See <see cref="OverlapPolicy"/>.</param>
/// <param name="CatchUpWindow">How late an occurrence may be and still run. One missed by more than
/// this is skipped, so a schedule coming back after a long outage runs once, catching up in a single
/// fire, without replaying every occurrence it slept through. <c>null</c> runs a late occurrence
/// however late it is.</param>
/// <param name="EndsAfter">How many occurrences to run before the schedule finishes. <c>null</c>
/// runs until deleted.</param>
public sealed record StartSchedule(
    IScheduleSpec Spec,
    string TargetWorkflowType,
    object TargetCommand,
    OverlapPolicy Overlap = OverlapPolicy.Skip,
    TimeSpan? CatchUpWindow = null,
    int? EndsAfter = null)
{
    /// <summary>
    /// Names the target workflow at compile time, so a mistyped type fails the build, at the call
    /// site, before it ever gets near the first fire. The command is checked there too, against
    /// whatever that workflow handles.
    /// </summary>
    public static StartSchedule For<TWorkflow>(
        IScheduleSpec spec,
        object command,
        OverlapPolicy overlap = OverlapPolicy.Skip,
        TimeSpan? catchUpWindow = null,
        int? endsAfter = null)
        where TWorkflow : IWorkflowTypeInfo =>
        new(spec, TWorkflow.WorkflowTypeName, command, overlap, catchUpWindow, endsAfter);
}

/// <summary>Holds a running schedule. It keeps its place, so resuming carries on from there.</summary>
public sealed record PauseSchedule;

/// <summary>Puts a held schedule back to work, computing its next occurrence from now.</summary>
public sealed record ResumeSchedule;

/// <summary>Runs an occurrence immediately, leaving the scheduled sequence alone.</summary>
public sealed record TriggerSchedule;

/// <summary>Ends the schedule. Occurrences already started carry on — they are runs of their own.
/// </summary>
public sealed record CancelSchedule;

/// <summary>Reports what a schedule is doing.</summary>
public sealed record GetScheduleStatus;

/// <summary>What <see cref="GetScheduleStatus"/> answers.</summary>
public sealed record ScheduleStatus(
    bool Paused,
    DateTimeOffset? NextFireUtc,
    int FireCount,
    string? LastStartedEntityId,
    int SkippedCount);
