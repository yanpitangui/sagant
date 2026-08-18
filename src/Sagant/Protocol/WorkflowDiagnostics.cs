using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Sagant.Protocol;

/// <summary>
/// The <see cref="System.Diagnostics.ActivitySource"/> every <see cref="WorkflowEntityActor{TWorkflow, TState}"/>
/// emits spans on — one per command handled, one per step execution (retries produce one span
/// per attempt, so a flaky step's retry history is directly visible in a trace waterfall, standing
/// on its own next to whatever logs exist). No OpenTelemetry package dependency: this is the
/// runtime-native primitive the OTel .NET SDK itself is built on. Wire it into your own OTel setup via
/// <c>.AddSource("Sagant")</c>.
///
/// Also the single owner of every metric instrument the framework emits (<see cref="Meter"/>) —
/// wire that side in via <c>.AddMeter("Sagant")</c>. Instruments are recorded from two different
/// kinds of call site, both deliberately kept thin (one line, no instrument/tag knowledge of
/// their own):
/// <list type="bullet">
/// <item><see cref="StepDescriptor{TState}.Invoke"/>/<see cref="CommandDescriptor{TState}.Invoke"/>
/// (core) call <see cref="RecordStepDuration"/>/<see cref="RecordCommandHandled"/> directly —
/// automatic for every runtime driver, including <see cref="Testing.WorkflowTestHarness{TWorkflow, TState}"/>,
/// since both drivers execute steps/commands through these same two methods.</item>
/// <item>A runtime driver calls <see cref="RecordStatusChange"/> itself, once, at every place it
/// actually assigns a new <see cref="WorkflowRuntimeState{TState}.Status"/> — whether that change
/// came from a business-authored <c>Transition</c> (<c>PauseTransition</c>/<c>EndTransition</c>/
/// <c>DeleteTransition</c>) or an operator control command (<c>Suspend</c>/<c>Resume</c>/
/// <c>Terminate</c>). Those two kinds of status change have no common data shape to dispatch on
/// automatically — a <c>Transition</c> is data a handler returns, a control command is an
/// operator override with no handler invocation behind it at all — so the call itself stays
/// driver-owned, but what it does with the status is still centralized here.</item>
/// </list>
/// </summary>
public static class WorkflowDiagnostics
{
    /// <summary>
    /// Prefer this over <c>ActivitySource.Name</c>/<c>Meter.Name</c> when subscribing a
    /// listener/exporter — referencing the lazily-constructed <see cref="ActivitySource"/>/
    /// <see cref="Meter"/> from inside a <c>ShouldListenTo</c> predicate is what first triggers
    /// its own construction, which re-enters the same predicate before the field assignment
    /// completes.
    /// </summary>
    public const string SourceName = "Sagant";

    public static readonly ActivitySource ActivitySource = new(SourceName, "1.0.0");

    public static readonly Meter Meter = new(SourceName, "1.0.0");

    private static readonly Histogram<double> StepDurationSeconds = Meter.CreateHistogram<double>(
        "sagant.step.duration", unit: "s", description: "Duration of a single step execution attempt.");

    private static readonly Counter<long> CommandCount = Meter.CreateCounter<long>(
        "sagant.command.count", description: "Commands successfully applied to a workflow instance.");

    private static readonly Histogram<double> QueryDurationSeconds = Meter.CreateHistogram<double>(
        "sagant.query.duration", unit: "s", description: "Duration of a single query handler execution.");

    private static readonly Counter<long> StepRetryCount = Meter.CreateCounter<long>(
        "sagant.step.retry", description: "Step failures that were followed by a scheduled retry.");

    private static readonly Counter<long> WorkflowPausedCount = Meter.CreateCounter<long>(
        "sagant.workflow.paused", description: "Workflow instances entering the Paused status.");

    private static readonly Counter<long> WorkflowSuspendedCount = Meter.CreateCounter<long>(
        "sagant.workflow.suspended", description: "Workflow instances entering the Suspended status via an operator Suspend.");

    private static readonly Counter<long> WorkflowResumedCount = Meter.CreateCounter<long>(
        "sagant.workflow.resumed", description: "Workflow instances entering the Running status from Paused or Suspended.");

    private static readonly Counter<long> WorkflowFinishedCount = Meter.CreateCounter<long>(
        "sagant.workflow.finished", description: "Workflow runs finishing, tagged by outcome.");

    private static readonly Counter<long> WorkflowDeletedCount = Meter.CreateCounter<long>(
        "sagant.workflow.deleted", description: "Workflow instances whose persisted data was purged.");

    /// <summary>Called from <see cref="StepDescriptor{TState}.Invoke"/> once a step attempt's
    /// <c>Task</c> settles, success or failure alike, both recorded on this one histogram —
    /// <paramref name="outcome"/> is the dimension that separates them.</summary>
    public static void RecordStepDuration(string workflowType, string stepName, int attempt, TimeSpan duration, string outcome) =>
        StepDurationSeconds.Record(
            duration.TotalSeconds,
            new KeyValuePair<string, object?>("workflow.type", workflowType),
            new KeyValuePair<string, object?>("step.name", stepName),
            new KeyValuePair<string, object?>("attempt", attempt),
            new KeyValuePair<string, object?>("outcome", outcome));

    /// <summary>Called from <see cref="CommandDescriptor{TState}.Invoke"/> for a command handler
    /// that returns successfully. A handler that throws surfaces its failure on the command's own
    /// <see cref="Activity"/>, same as a step's exception path.</summary>
    public static void RecordCommandHandled(string workflowType, string commandType) =>
        CommandCount.Add(1,
            new KeyValuePair<string, object?>("workflow.type", workflowType),
            new KeyValuePair<string, object?>("command.type", commandType));

    /// <summary>Called from <see cref="QueryDescriptor{TState}.Invoke"/> once a query handler's
    /// <c>Task</c> settles. <paramref name="outcome"/> separates <c>ok</c> from <c>error</c> and
    /// from <c>cancelled</c> — a query that hit its own server-side timeout is a distinct signal
    /// from one that threw, and both matter for a read path a dashboard depends on.</summary>
    public static void RecordQueryDuration(string workflowType, string queryType, TimeSpan duration, string outcome) =>
        QueryDurationSeconds.Record(
            duration.TotalSeconds,
            new KeyValuePair<string, object?>("workflow.type", workflowType),
            new KeyValuePair<string, object?>("query.type", queryType),
            new KeyValuePair<string, object?>("outcome", outcome));

    /// <summary>Called by a runtime driver once it has decided a failed step attempt falls within
    /// its retry budget and will retry — the driver's own <c>RecoverStrategy</c>/attempt-count
    /// check; this decision has no shared data shape to dispatch on automatically.</summary>
    public static void RecordStepRetryScheduled(string workflowType, string stepName) =>
        StepRetryCount.Add(1,
            new KeyValuePair<string, object?>("workflow.type", workflowType),
            new KeyValuePair<string, object?>("step.name", stepName));

    /// <summary>
    /// Called by a runtime driver when a run finishes. Tagged by outcome, so "how many workflows
    /// failed" is answerable as a metric query.
    /// </summary>
    public static void RecordOutcome(string workflowType, WorkflowOutcome outcome) =>
        WorkflowFinishedCount.Add(1,
            new KeyValuePair<string, object?>("workflow.type", workflowType),
            new KeyValuePair<string, object?>("outcome", outcome switch
            {
                WorkflowOutcome.Completed => "completed",
                WorkflowOutcome.Failed => "failed",
                WorkflowOutcome.TimedOut => "timed_out",
                WorkflowOutcome.Terminated => "terminated",
                _ => "unknown",
            }));

    /// <summary>Called by a runtime driver at every place it actually assigns a new
    /// <see cref="WorkflowRuntimeState{TState}.Status"/> to a workflow instance — the driver's own
    /// job is only to notice the change and report which status it landed on; this decides which
    /// counter that maps to. Reserve calls to genuine status changes (skip it, for example, for a
    /// <c>StepTransition</c> that lands back on <c>Running</c> while already <c>Running</c>).</summary>
    public static void RecordStatusChange(string workflowType, WorkflowStatus newStatus)
    {
        // Finished is reported through RecordOutcome instead, which carries the far more useful
        // dimension: not that a run ended, but how.
        switch (newStatus)
        {
            case WorkflowStatus.Paused:
                WorkflowPausedCount.Add(1, new KeyValuePair<string, object?>("workflow.type", workflowType));
                break;
            case WorkflowStatus.Suspended:
                WorkflowSuspendedCount.Add(1, new KeyValuePair<string, object?>("workflow.type", workflowType));
                break;
            case WorkflowStatus.Running:
                WorkflowResumedCount.Add(1, new KeyValuePair<string, object?>("workflow.type", workflowType));
                break;
            case WorkflowStatus.Deleted:
                WorkflowDeletedCount.Add(1, new KeyValuePair<string, object?>("workflow.type", workflowType));
                break;
        }
    }
}
