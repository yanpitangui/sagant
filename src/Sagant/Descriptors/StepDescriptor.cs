using System.Diagnostics;
using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Descriptors;

/// <summary>
/// Zero-reflection binding of a step's durable name to its input type and a compiled invoker.
/// Emitted by the source generator, one per <c>[WorkflowStep]</c> method.
///
/// Everything the step body receives arrives as one <see cref="StepContext{TState}"/>: the state
/// this attempt runs against, the 1-based attempt number, and the cancellation token. The state is a
/// value belonging to this invocation alone, which is what keeps a step and a concurrently
/// dispatched handler from observing each other across an <c>await</c>.
///
/// The context's <see cref="StepContext{TState}.CancellationToken"/> is cancelled when the runtime
/// stops waiting on this step's <c>Task</c> — a timeout, <c>Suspend</c>, <c>Terminate</c>, or a
/// graceful-handoff grace window expiring (see <c>GracefulShutdown</c>). Cancelling doesn't forcibly
/// stop anything by itself; it's cooperative, same as everywhere else in .NET — a step built on
/// <c>HttpClient</c>/EF/etc. that honors the token unwinds promptly, avoiding running to completion
/// orphaned (its eventual result would just be discarded anyway, via the epoch check, but a step
/// already past the point where it caused an irreversible real-world side effect can't un-cause it —
/// cancellation only helps for the part of the step that hasn't happened yet).
///
/// Returns a <c>Task</c> even for a step declared synchronously — the generator wraps that case, so
/// a runtime driver has one shape to drive.
/// </summary>
public readonly struct StepDescriptor<TState>
{
    private readonly Func<Workflow<TState>, StepContext<TState>, object?, Task<StepEffect<TState>>> _invoke;

    public StepDescriptor(string name, Type inputType, Func<Workflow<TState>, StepContext<TState>, object?, Task<StepEffect<TState>>> invoke)
    {
        Name = name;
        InputType = inputType;
        _invoke = invoke;
    }

    public string Name { get; }

    public Type InputType { get; }

    /// <summary>
    /// Builds this attempt's <see cref="StepContext{TState}"/> from <paramref name="state"/>,
    /// <paramref name="attempt"/> and <paramref name="cancellationToken"/>, then invokes the step
    /// inside an <see cref="Activity"/> span on
    /// <see cref="WorkflowDiagnostics.ActivitySource"/> — the one sanctioned way any runtime drives
    /// a step, and now the one place tracing for it lives, so every runtime gets it for free —
    /// span lifecycle management lives here once, shared by every driver.
    ///
    /// The span's lifetime is tied to the returned <c>Task</c>, closed by a <c>using</c> wrapping
    /// the <c>await</c> inside <see cref="Invoke"/> — a deliberate choice: a runtime that
    /// fires-and-forgets this Task across a message-passing boundary (resumed on an arbitrary
    /// different thread, outside the original async call stack) needs a lifetime mechanism that
    /// survives the hop, and ambient <see cref="Activity.Current"/>, being <c>AsyncLocal</c>-scoped,
    /// does not survive it; the <c>using</c> around the <c>await</c> closes correctly regardless of
    /// which thread resumes the Task.
    ///
    /// <paramref name="parentContext"/>/<paramref name="links"/> and <paramref name="configureActivity"/>
    /// are how a runtime supplies what only it knows: where this span fits in a wider trace, and
    /// runtime-specific tags (e.g. a persistence id) or cross-restart trace-link setup. Both stay
    /// optional — a caller with nothing to add (like <see cref="Testing.WorkflowTestHarness{TWorkflow, TState}"/>)
    /// just gets an unparented span if anything's listening, or no span at all if nothing is.
    ///
    /// <paramref name="attempt"/> also drives the <c>sagant.step.duration</c> metric recorded here
    /// (see <see cref="WorkflowDiagnostics.RecordStepDuration"/>) — every runtime driver gets this
    /// metric for free just by calling <see cref="Invoke"/>, same as the span above.
    /// </summary>
    public async Task<StepEffect<TState>> Invoke(
        Workflow<TState> workflow,
        TState state,
        object? input,
        int attempt,
        CancellationToken cancellationToken,
        ActivityContext parentContext = default,
        IEnumerable<ActivityLink>? links = null,
        Action<Activity>? configureActivity = null)
    {
        using var activity = WorkflowDiagnostics.ActivitySource.StartActivity(
            $"{workflow.WorkflowTypeName}.Step.{Name}", ActivityKind.Internal, parentContext, tags: null, links: links);
        if (activity is not null)
        {
            configureActivity?.Invoke(activity);
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = await _invoke(workflow, new StepContext<TState>(state, attempt, cancellationToken), input).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
            WorkflowDiagnostics.RecordStepDuration(workflow.WorkflowTypeName, Name, attempt, stopwatch.Elapsed, "ok");
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddTag("exception.type", ex.GetType().FullName);
            activity?.AddTag("exception.message", ex.Message);
            WorkflowDiagnostics.RecordStepDuration(workflow.WorkflowTypeName, Name, attempt, stopwatch.Elapsed, "error");
            throw;
        }
    }
}

/// <summary>
/// Implemented (by the source generator) on every workflow class that declares
/// <c>[WorkflowStep]</c> methods. Lets the runtime dispatch a persisted step name to the
/// corresponding step method without reflection — required for NativeAOT/trimming.
/// </summary>
public interface IWorkflowStepDispatcher<TState>
{
    bool TryGetStep(string stepName, out StepDescriptor<TState> descriptor);

    System.Collections.Generic.IReadOnlyCollection<string> StepNames { get; }
}
