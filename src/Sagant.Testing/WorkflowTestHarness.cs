using Sagant.Execution;
using Microsoft.Extensions.Time.Testing;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Settings;

namespace Sagant.Testing;

internal interface IWorkflowTestHarnessChild
{
    WorkflowStatus Status { get; }
    WorkflowOutcome? Outcome { get; }
    object StateObject { get; }
}

/// <summary>
/// Pure in-memory test harness for a workflow's command/step/query handlers — no
/// <c>ActorSystem</c>, no persistence, no <c>ClusterSharding</c>.
///
/// It is a driver, not an imitation of one. It holds the same
/// <see cref="WorkflowRuntimeState{TState}"/> a durable driver persists, plans every transition
/// through the same <see cref="Execution.WorkflowTransitionPlanner"/>, and dispatches through the
/// same generated <see cref="IWorkflowStepDispatcher{TState}"/>/
/// <see cref="IWorkflowCommandDispatcher{TState}"/>/<see cref="IWorkflowQueryDispatcher{TState}"/>
/// tables production traffic uses. Deadlines, retry budget, pause rules and child-group policy are
/// therefore the same code.
///
/// What differs is only what a durable driver's environment provides: this one keeps the envelope in
/// memory, drives a step chain by returning through <see cref="RunUntilStop{TCommand}"/>, and
/// compares deadlines against its <see cref="TimeProvider"/> on demand. It has no control plane
/// (<c>Suspend</c>/<c>Resume</c>/<c>Terminate</c>), no children to actually start, and nothing to
/// purge.
///
/// A step that throws is retried according to its resolved <see cref="RecoverStrategy"/>
/// (step-specific override, else <see cref="WorkflowSettings.DefaultStepRecoverStrategy"/>), then
/// fails over once the budget is exhausted. Retries run back-to-back with no simulated wait —
/// <see cref="RecoverStrategy.BackoffForAttempt"/> is a pure <c>Func&lt;int, TimeSpan&gt;</c>,
/// directly unit-testable on its own (see <c>RetryBackoffTests</c>) — so this harness exercises the
/// *decision*, not the delay. A step with no <see cref="RecoverStrategy"/> configured lets the
/// exception propagate straight to the caller.
///
/// Takes a <see cref="TimeProvider"/> (defaulting to <see cref="TimeProvider.System"/>) as its clock
/// abstraction — pass a <see cref="FakeTimeProvider"/> to control time in a test. Use
/// <see cref="AdvanceTime"/> to move it forward: every deadline (a pause timeout, the workflow-wide
/// <see cref="WorkflowSettings.WorkflowTimeout"/>) is just a <see cref="DateTimeOffset"/> compared
/// against <see cref="TimeProvider.GetUtcNow"/> on demand — nothing runs in the background — so
/// advancing and firing whatever became due is one call, with no separate follow-up to remember.
/// </summary>
public sealed class WorkflowTestHarness<TWorkflow, TState>
    : IWorkflowTestHarnessChild
    where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>
{
    private readonly TimeProvider _timeProvider;
    private readonly ResolvedWorkflowSettings _settings;
    private readonly WorkflowInstanceIdentity _identity;
    private readonly List<WorkflowEvent> _events = new();
    private readonly Dictionary<string, IWorkflowTestHarnessChild> _childHarnesses = new();

    /// <summary>
    /// The same <see cref="WorkflowRuntimeState{TState}"/> a durable driver persists. The harness
    /// holds it in memory, which is the one difference between the two — every
    /// deadline, status, retry count and child relationship lives in the same shape here as in
    /// production, so a decision made against one is a decision made against the other.
    /// </summary>
    private WorkflowRuntimeState<TState> _envelope;

    public TWorkflow Workflow { get; }

    /// <summary>The harness's current lifecycle status. This exposes the same state its own
    /// transition tracker already maintains so a parent harness can deliver a registered child's
    /// terminal lifecycle result without depending on an Akka runtime.</summary>
    public WorkflowStatus Status => _envelope.Status;

    /// <summary>How this run finished, or <c>null</c> while it is still going.</summary>
    public WorkflowOutcome? Outcome => _envelope.Outcome;

    object IWorkflowTestHarnessChild.StateObject => State!;

    /// <summary>The workflow's tracked state between calls — advances automatically after any
    /// effect whose <see cref="PersistenceEffect{TState}"/> is <see cref="PersistenceEffect{TState}.UpdateState"/>.
    /// Settable directly: e.g. to jump straight into testing a specific step (a compensation
    /// cascade, say) without first replaying everything that would normally produce that state.</summary>
    public TState State
    {
        get => _envelope.UserState;
        set => _envelope = _envelope with { UserState = value };
    }

    /// <summary>The full runtime envelope, for a test asserting on runtime-owned bookkeeping —
    /// deadlines, retry count, child relationships.</summary>
    public WorkflowRuntimeState<TState> Envelope => _envelope;

    /// <summary>Every event this run recorded, in order. A durable driver writes these and announces
    /// them; the harness keeps them so a test can assert on the lifecycle a subscriber observes —
    /// including each event's <c>Cause</c>, which names what drove it.</summary>
    public IReadOnlyList<WorkflowEvent> Events => _events;

    public WorkflowTestHarness(
        TWorkflow workflow, TState? initialState = default, TimeProvider? timeProvider = null, string? instanceId = null)
    {
        Workflow = workflow;
        _identity = new WorkflowInstanceIdentity(
            instanceId ?? workflow.WorkflowTypeName, instanceId ?? workflow.WorkflowTypeName, workflow.WorkflowTypeName);
        _settings = ResolvedWorkflowSettings.From(workflow.Settings());
        _envelope = new WorkflowRuntimeState<TState>(
            initialState is null ? workflow.EmptyState() : initialState,
            CurrentStepName: null, CurrentStepInput: null, RetryCount: 0, Status: WorkflowStatus.Running);
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Registers a child harness under the durable workflow id used by an
    /// <see cref="StepEffectsBuilder{TState}.AwaitChildren"/> effect.</summary>
    public WorkflowTestHarness<TWorkflow, TState> WithChild<TChildWorkflow, TChildState>(
        string workflowId, WorkflowTestHarness<TChildWorkflow, TChildState> childHarness)
        where TChildWorkflow : Workflow<TChildState>, IWorkflowStepDispatcher<TChildState>, IWorkflowCommandDispatcher<TChildState>, IWorkflowQueryDispatcher<TChildState>, IWorkflowChildResultDispatcher<TChildState>, IWorkflowTypeInfo
    {
        _childHarnesses[workflowId] = childHarness;
        return this;
    }

    /// <summary>Delivers the registered child's terminal state to its active child group, causing
    /// that group's resume step to run once its policy is satisfied. A child id may occur in only
    /// one active group; use the group-explicit overload when a test intentionally models more
    /// than one active relationship for the same child.</summary>
    public Task DeliverChildLifecycle(string childWorkflowId, CancellationToken cancellationToken = default) =>
        DeliverChildLifecycleCore(FindActiveChildGroup(childWorkflowId), childWorkflowId, cancellationToken);

    /// <summary>Delivers the registered child's terminal state to the named child group. Prefer
    /// <see cref="DeliverChildLifecycle(string,CancellationToken)"/> when the child belongs to one
    /// active group, which is the usual workflow test.</summary>
    public Task DeliverChildLifecycle(string groupId, string childWorkflowId, CancellationToken cancellationToken = default) =>
        DeliverChildLifecycleCore(groupId, childWorkflowId, cancellationToken);

    /// <summary>Replays a child lifecycle delivery to its active child group. Once that group has
    /// finalized this is a no-op, mirroring the actor runtime's generation/finalization guard.</summary>
    public Task RedeliverChildLifecycle(string childWorkflowId, CancellationToken cancellationToken = default) =>
        DeliverChildLifecycleCore(FindActiveChildGroup(childWorkflowId, includeFinalized: true), childWorkflowId, cancellationToken);

    /// <summary>Replays a child lifecycle delivery to the named child group. Prefer
    /// <see cref="RedeliverChildLifecycle(string,CancellationToken)"/> when the child belongs to
    /// one active group.</summary>
    public Task RedeliverChildLifecycle(string groupId, string childWorkflowId, CancellationToken cancellationToken = default) =>
        DeliverChildLifecycleCore(groupId, childWorkflowId, cancellationToken);

    /// <summary>Dispatches <paramref name="command"/> to its <c>[WorkflowCommandHandler]</c>.</summary>
    public CommandEffect<TState> RunCommand<TCommand>(TCommand command)
        where TCommand : notnull
    {
        if (!((IWorkflowCommandDispatcher<TState>)Workflow).TryGetHandler(typeof(TCommand), out var descriptor))
        {
            throw new InvalidOperationException(
                $"No [WorkflowCommandHandler] registered for {typeof(TCommand)} on {typeof(TWorkflow).Name}.");
        }

        var effect = descriptor.Invoke(Workflow, State, command);
        Apply(effect.Persistence);
        TrackTransition(effect.Transition, new TransitionCause.Command(typeof(TCommand).Name));
        return effect;
    }

    /// <summary>
    /// Dispatches <paramref name="query"/> to its <c>[WorkflowQuery]</c> and returns the reply,
    /// against <see cref="State"/> as it stands right now. A query cannot persist or transition (see
    /// <see cref="QueryEffect"/>), so nothing here advances the harness's own state — which is
    /// exactly the property that lets <see cref="RunStepInterleaved{TInput}"/> dispatch one into a
    /// step that is still running.
    /// </summary>
    /// <exception cref="WorkflowCommandException">The handler returned an error reply.</exception>
    public async Task<TReply> RunQuery<TQuery, TReply>(TQuery query, CancellationToken cancellationToken = default)
        where TQuery : notnull
    {
        if (!((IWorkflowQueryDispatcher<TState>)Workflow).TryGetQuery(typeof(TQuery), out var descriptor))
        {
            throw new InvalidOperationException(
                $"No [WorkflowQuery] registered for {typeof(TQuery)} on {typeof(TWorkflow).Name}.");
        }

        var effect = await descriptor.Invoke(Workflow, State, query, cancellationToken);
        return effect.Reply switch
        {
            Reply.ReplyValue rv => (TReply)rv.Value!,
            Reply.ErrorValue ev => throw new WorkflowCommandException(ev.Message),
            _ => throw new InvalidOperationException(
                $"Query {typeof(TQuery).Name} returned no reply; a query handler must reply."),
        };
    }

    /// <summary>
    /// Dispatches a step and, while its <c>Task</c> is still in flight, runs
    /// <paramref name="whileStepRuns"/> — the harness's window into the one concurrency the runtime
    /// actually permits: a query running alongside an executing step.
    ///
    /// The step body has to cooperate by suspending at an await the caller controls (a
    /// <see cref="TaskCompletionSource"/> the callback completes), otherwise it simply runs to
    /// completion first and nothing interleaves. Dispatch a query from the callback; a command
    /// handler dispatched here would model something the runtime driver deliberately prevents by
    /// deferring commands until an in-flight step settles.
    /// </summary>
    public async Task<StepEffect<TState>> RunStepInterleaved<TInput>(
        StepRef<TWorkflow, TInput> step, TInput input, Func<Task> whileStepRuns, CancellationToken cancellationToken = default)
    {
        var stepTask = RunStepCore(step.Name, input, cancellationToken);
        await whileStepRuns();
        return await stepTask;
    }

    /// <summary>No-input form of <see cref="RunStepInterleaved{TInput}"/>.</summary>
    public Task<StepEffect<TState>> RunStepInterleaved(
        StepRef<TWorkflow, NoInput> step, Func<Task> whileStepRuns, CancellationToken cancellationToken = default) =>
        RunStepInterleaved<object?>(new StepRef<TWorkflow, object?>(step.Name), null, whileStepRuns, cancellationToken);

    /// <summary>Dispatches a no-input <c>[WorkflowStep]</c> by its generated <c>Steps.X</c> ref.</summary>
    public Task<StepEffect<TState>> RunStep(StepRef<TWorkflow, NoInput> step, CancellationToken cancellationToken = default) =>
        RunStepCore(step.Name, null, cancellationToken);

    /// <summary>Dispatches a <c>[WorkflowStep]</c> that declares an input parameter, by its
    /// generated <c>Steps.X</c> ref.</summary>
    public Task<StepEffect<TState>> RunStep<TInput>(StepRef<TWorkflow, TInput> step, TInput input, CancellationToken cancellationToken = default) =>
        RunStepCore(step.Name, input, cancellationToken);

    /// <summary>Dispatches <paramref name="command"/>, then follows <see cref="Transition.StepTransition"/>
    /// automatically — the same chain <c>WorkflowEntityActor</c> would drive in production — until
    /// it reaches <see cref="Transition.PauseTransition"/>, <see cref="Transition.TerminalTransition"/>,
    /// <see cref="Transition.DeleteTransition"/>, or <see cref="Transition.NoTransition"/>, and
    /// returns whichever step effect stopped the chain. Use this for "does the whole path work"
    /// tests; use <see cref="RunStep(StepRef{TWorkflow,NoInput},CancellationToken)"/> alone when you
    /// want to assert on one specific step.</summary>
    public async Task<StepEffect<TState>> RunUntilStop<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        where TCommand : notnull
    {
        var commandEffect = RunCommand(command);
        if (commandEffect.Transition is not Transition.StepTransition st)
        {
            throw new InvalidOperationException(
                $"{typeof(TCommand).Name}'s effect was a {commandEffect.Transition.GetType().Name}, not a step " +
                "transition — there's no step chain to run. Use RunCommand directly to assert on a command " +
                "that doesn't enter the step chain.");
        }

        return await RunUntilStopCore(st.StepName, st.Input, cancellationToken);
    }

    /// <summary>Resumes the <see cref="Transition.StepTransition"/> chain from
    /// <paramref name="fromStep"/> — for jumping straight into a specific branch (e.g. the
    /// compensation cascade) after hand-seeding <see cref="State"/>, without replaying whatever
    /// would normally lead there. See <see cref="RunUntilStop{TCommand}"/> for the stopping
    /// condition.</summary>
    public Task<StepEffect<TState>> RunUntilStop(StepRef<TWorkflow, NoInput> fromStep, CancellationToken cancellationToken = default) =>
        RunUntilStopCore(fromStep.Name, null, cancellationToken);

    /// <summary>Checks whether the workflow is currently paused and its <see cref="PauseSettings.Timeout"/>
    /// deadline (measured against the harness's <see cref="TimeProvider"/>) has passed. If so, fires
    /// it exactly like <c>WorkflowEntityActor</c> would: transitions into
    /// <see cref="PauseSettings.TimeoutHandlerStepName"/> and follows the step chain from there —
    /// same stopping condition as <see cref="RunUntilStop{TCommand}"/>. Advance a
    /// <c>FakeTimeProvider</c> passed to the constructor, then call this. Returns <c>null</c> if the
    /// workflow isn't paused, its pause has no <see cref="PauseSettings"/>, or the deadline hasn't
    /// passed yet.</summary>
    public async Task<StepEffect<TState>?> RunPauseTimeoutIfDue(CancellationToken cancellationToken = default)
    {
        if (_envelope.Status != WorkflowStatus.Paused
            || _envelope.PauseDeadline is not { } deadline
            || _envelope.PauseTimeoutStepName is not { } handlerStepName
            || _timeProvider.GetUtcNow() < deadline)
        {
            return null;
        }

        // The transition into the handler step clears PauseDeadline/PauseTimeoutStepName, so this
        // cannot fire twice for one pause — same as the actor, where the persisted envelope does it.
        return await RunUntilStopCore(handlerStepName, null, cancellationToken);
    }

    /// <summary>Checks whether the workflow's overall <see cref="WorkflowSettings.WorkflowTimeout"/>
    /// deadline (measured against the harness's <see cref="TimeProvider"/>) has passed while the
    /// workflow is actively <see cref="WorkflowStatus.Running"/>, and fires it exactly like
    /// <c>WorkflowEntityActor.HandleWorkflowTimedOut</c> would: follows
    /// <see cref="WorkflowSettings.WorkflowRecoverStrategy"/> into its failover step, or — with no
    /// strategy configured — ends the workflow with reason <c>"workflow timeout"</c>. Deliberately a
    /// no-op while <see cref="WorkflowStatus.Paused"/>: the workflow-level timeout is a ceiling on
    /// active processing time, not on time spent waiting for human input; a paused workflow's own
    /// <see cref="PauseSettings.Timeout"/> — see <see cref="RunPauseTimeoutIfDue"/> — is what governs
    /// a stuck approval. Advance a <c>FakeTimeProvider</c> passed to the constructor, then call this.
    /// Returns <c>null</c> if the deadline hasn't passed, no <see cref="WorkflowSettings.WorkflowTimeout"/>
    /// is configured, or the workflow isn't currently running.</summary>
    public async Task<StepEffect<TState>?> RunWorkflowTimeoutIfDue(CancellationToken cancellationToken = default)
    {
        if (_envelope.Status is not WorkflowStatus.Running
            || _envelope.WorkflowDeadline is not { } deadline
            || _timeProvider.GetUtcNow() < deadline)
        {
            return null;
        }

        // What a fired workflow timeout means is the planner's to decide, so this harness and the
        // durable driver reach the same conclusion from the same function.
        if (WorkflowTransitionPlanner.PlanWorkflowTimeout(_envelope, _settings) is not { } transition)
        {
            return null;
        }

        // A recovery step runs like any other; everything else settles the run where it stands.
        if (transition is Transition.StepTransition failover)
        {
            return await RunUntilStopCore(failover.StepName, failover.Input, cancellationToken);
        }

        var settled = new StepEffect<TState>(PersistenceEffect<TState>.NoPersistence.Instance, transition);
        TrackTransition(settled.Transition, new TransitionCause.Control("WorkflowTimedOut"));
        return settled;
    }

    /// <summary>Advances the harness's <see cref="TimeProvider"/> by <paramref name="delta"/>, then
    /// immediately checks for a due pause timeout (<see cref="RunPauseTimeoutIfDue"/>) followed by a
    /// due workflow timeout (<see cref="RunWorkflowTimeoutIfDue"/>), firing whichever is due first.
    /// Prefer this over calling <c>FakeTimeProvider.Advance</c> directly and calling those two
    /// yourself: it's easy to advance time and forget the follow-up call, which produces a test that
    /// silently never exercises the timeout it meant to prove — nothing else surfaces that miss.
    /// Folding the check into the only way time moves removes the whole failure mode. Requires a
    /// <see cref="FakeTimeProvider"/> to have been passed to the constructor; throws
    /// <see cref="InvalidOperationException"/> otherwise (e.g. against the default
    /// <see cref="TimeProvider.System"/>).</summary>
    public async Task<StepEffect<TState>?> AdvanceTime(TimeSpan delta, CancellationToken cancellationToken = default)
    {
        if (_timeProvider is not FakeTimeProvider fake)
        {
            throw new InvalidOperationException(
                $"{nameof(AdvanceTime)} requires a {nameof(FakeTimeProvider)} passed to the harness " +
                $"constructor — got {_timeProvider.GetType().Name}.");
        }

        fake.Advance(delta);
        return await RunPauseTimeoutIfDue(cancellationToken) ?? await RunWorkflowTimeoutIfDue(cancellationToken);
    }

    /// <summary>Tracks pause/workflow-timeout state off every transition — same bookkeeping
    /// <c>WorkflowEntityActor.PersistEnvelopeThen</c> folds into the persisted envelope on each
    /// step/pause/end/delete transition. <see cref="Transition.NoTransition"/> is deliberately a
    /// no-op: production never even reaches <c>PersistEnvelopeThen</c> for a bare
    /// no-persistence-no-transition command effect (see <c>ApplyCommandEffect</c>'s short-circuit),
    /// so nothing here — status, pause deadline, or workflow deadline — should move either.</summary>
    /// <summary>
    /// Holds the workflow where it stands, exactly as an operator <c>Suspend</c> would. Throws
    /// <see cref="WorkflowCommandException"/> when the workflow isn't in a status it can be
    /// suspended from — the same rejection a caller would see.
    /// </summary>
    public void Suspend() => ApplyControl(WorkflowTransitionPlanner.PlanSuspend(_envelope, new TransitionCause.Control("Suspend")));

    /// <summary>
    /// Puts a suspended workflow back to work and re-runs its current step from the beginning
    /// (guarantee E4). Returns the effect the resumed step chain settled on, or <c>null</c> when
    /// there was no step to resume.
    /// </summary>
    public async Task<StepEffect<TState>?> Resume(CancellationToken cancellationToken = default)
    {
        var plan = WorkflowTransitionPlanner.PlanResume(
            _envelope, _timeProvider.GetUtcNow(), _settings, new TransitionCause.Control("Resume"));
        ApplyControl(plan);

        return _envelope.CurrentStepName is { } stepName
            ? await RunUntilStopCore(stepName, _envelope.CurrentStepInput, cancellationToken)
            : null;
    }

    /// <summary>Stops the workflow where it stands, without unwinding — see <see cref="Cancel"/> for
    /// the graceful counterpart.</summary>
    public void Terminate(string? reason = null) =>
        ApplyControl(WorkflowTransitionPlanner.PlanTerminate(_envelope, reason, new TransitionCause.Control("Terminate")));

    /// <summary>
    /// Asks the workflow to stop and unwind through its configured cancellation step, running that
    /// step chain to its own conclusion. Returns the effect it settled on, or <c>null</c> when no
    /// cancellation step is configured and the run simply finished as cancelled.
    /// </summary>
    public async Task<StepEffect<TState>?> Cancel(string? reason = null, CancellationToken cancellationToken = default)
    {
        if (WorkflowTransitionPlanner.PlanCancel(_envelope, reason, _settings) is not { } transition)
        {
            return null;
        }

        if (transition is Transition.StepTransition step)
        {
            return await RunUntilStopCore(step.StepName, step.Input, cancellationToken);
        }

        TrackTransition(transition, new TransitionCause.Control("PauseTimedOut"));
        return null;
    }

    /// <summary>Applies a control plan's envelope and the decisions this harness can carry out, and
    /// surfaces a rejection as the exception a caller would receive.</summary>
    private void ApplyControl(ControlPlan<TState> plan)
    {
        if (plan is ControlPlan<TState>.Reject reject)
        {
            throw new WorkflowCommandException(reject.Reason);
        }

        var apply = (ControlPlan<TState>.Apply)plan;
        _envelope = WorkflowEventFold.ApplyAll(_envelope, apply.Events);
        _events.AddRange(apply.Events);
        foreach (var decision in apply.AfterPersist)
        {
            switch (decision)
            {
                case WorkflowDecision.RecordStatusChange rsc:
                    WorkflowDiagnostics.RecordStatusChange(Workflow.WorkflowTypeName, rsc.Status);
                    break;
                case WorkflowDecision.RecordOutcome ro:
                    WorkflowDiagnostics.RecordOutcome(Workflow.WorkflowTypeName, ro.Outcome);
                    break;
            }
        }
    }

    /// <summary>
    /// Applies a transition through the same <see cref="WorkflowTransitionPlanner"/> the durable
    /// driver uses, so every deadline, status and child-group rule is decided in one place.
    ///
    /// The harness carries out the decisions it can: recording a status change and capturing the
    /// events a run produced. It ignores the rest by design — it has no children to start, nothing to
    /// purge, no history to reclaim on a restart (it holds its envelope in memory, so a restart is
    /// simply the fold resetting it), and no live timers, because its deadlines are compared against
    /// <see cref="TimeProvider"/> on demand. It also ignores
    /// <see cref="WorkflowDecision.StartStep"/>: the harness drives a step chain by returning through
    /// <see cref="RunUntilStopCore"/>, so acting on that decision as well would run each step twice.
    /// </summary>
    private void TrackTransition(Transition transition, TransitionCause cause)
    {
        if (transition is Transition.NoTransition)
        {
            return;
        }

        var plan = WorkflowTransitionPlanner.Plan(
            _envelope, transition, PersistenceEffect<TState>.NoPersistence.Instance,
            _timeProvider.GetUtcNow(), _settings, _identity, cause);

        // The same fold a durable driver applies as it writes, and the same one recovery applies —
        // so what this harness believes about a workflow is what production would believe.
        _envelope = WorkflowEventFold.ApplyAll(_envelope, plan.Events);
        _events.AddRange(plan.Events);

        foreach (var decision in plan.AfterPersist)
        {
            switch (decision)
            {
                case WorkflowDecision.RecordStatusChange rsc:
                    WorkflowDiagnostics.RecordStatusChange(Workflow.WorkflowTypeName, rsc.Status);
                    break;
                case WorkflowDecision.RecordOutcome ro:
                    WorkflowDiagnostics.RecordOutcome(Workflow.WorkflowTypeName, ro.Outcome);
                    break;
            }
        }
    }

    private async Task<StepEffect<TState>> RunUntilStopCore(string stepName, object? input, CancellationToken cancellationToken)
    {
        while (true)
        {
            var effect = await RunStepCore(stepName, input, cancellationToken);
            if (effect.Transition is not Transition.StepTransition next)
            {
                return effect;
            }

            stepName = next.StepName;
            input = next.Input;
        }
    }

    private async Task<StepEffect<TState>> RunStepCore(string stepName, object? input, CancellationToken cancellationToken)
    {
        if (!((IWorkflowStepDispatcher<TState>)Workflow).TryGetStep(stepName, out var descriptor))
        {
            throw new InvalidOperationException(
                $"No [WorkflowStep] named '{stepName}' registered on {typeof(TWorkflow).Name}.");
        }

        while (true)
        {
            try
            {
                var attempt = _envelope.RetryCount + 1;
                var startedAt = _timeProvider.GetUtcNow();
                var effect = await descriptor.Invoke(Workflow, State, input, attempt, cancellationToken);
                Apply(effect.Persistence);
                TrackTransition(
                    effect.Transition,
                    new TransitionCause.StepSucceeded(stepName, attempt, _timeProvider.GetUtcNow() - startedAt));
                return effect;
            }
            catch (Exception ex)
            {
                // Same decision the durable driver makes, from the same function: retry within
                // budget, else fail over, else end (guarantee E1).
                var failedAttempt = _envelope.RetryCount + 1;
                var plan = WorkflowTransitionPlanner.PlanStepFailure(
                    _envelope, stepName, ex.Message, _timeProvider.GetUtcNow(), _settings);

                if (plan is StepFailurePlan<TState>.Retry retry)
                {
                    // Backoff is a pure Func<int, TimeSpan> and is unit-testable on its own, so the
                    // harness exercises the retry *decision* without simulating the wait.
                    _envelope = WorkflowEventFold.ApplyAll(_envelope, retry.Events);
                    _events.AddRange(retry.Events);
                    continue;
                }

                var conclusion = ((StepFailurePlan<TState>.Conclude)plan).Transition;
                if (conclusion is Transition.TerminalTransition && _settings.RecoverStrategyFor(stepName) is null)
                {
                    // No strategy at all: the exception is the caller's to see, unchanged.
                    throw;
                }

                var failoverEffect = new StepEffect<TState>(
                    PersistenceEffect<TState>.NoPersistence.Instance, conclusion);
                TrackTransition(
                    failoverEffect.Transition,
                    new TransitionCause.StepFailed(stepName, failedAttempt, ex.Message, TimeSpan.Zero, WillRetry: false));
                return failoverEffect;
            }
        }
    }

    private void Apply(PersistenceEffect<TState> persistence)
    {
        if (persistence is PersistenceEffect<TState>.UpdateState u)
        {
            State = u.NewState;
        }
    }


    private string FindActiveChildGroup(string childWorkflowId, bool includeFinalized = false)
    {
        var groups = _envelope.ChildGroups ?? new Dictionary<string, ChildGroupState>();
        var groupIds = (_envelope.Children ?? Array.Empty<ChildWorkflowRelationship>())
            .Where(child => child.ChildWorkflowId == childWorkflowId
                && (includeFinalized || !groups[child.GroupId].Finalized))
            .Select(child => child.GroupId)
            .Distinct()
            .ToList();

        return groupIds.Count switch
        {
            1 => groupIds[0],
            0 => throw new InvalidOperationException($"Child '{childWorkflowId}' is not a member of an active child group."),
            _ => throw new InvalidOperationException(
                $"Child '{childWorkflowId}' belongs to multiple active child groups; use the overload that specifies a group id."),
        };
    }

    private async Task DeliverChildLifecycleCore(string groupId, string childWorkflowId, CancellationToken cancellationToken)
    {
        if (!_childHarnesses.TryGetValue(childWorkflowId, out var childHarness))
        {
            throw new InvalidOperationException($"No child harness registered for workflow id '{childWorkflowId}'.");
        }
        var trackedGroups = _envelope.ChildGroups ?? new Dictionary<string, ChildGroupState>();
        if (!trackedGroups.TryGetValue(groupId, out var group))
        {
            throw new InvalidOperationException($"No child group '{groupId}' is being tracked.");
        }
        if (group.Finalized)
        {
            return;
        }

        var children = (_envelope.Children ?? Array.Empty<ChildWorkflowRelationship>()).ToList();
        var index = children.FindIndex(c => c.GroupId == groupId && c.ChildWorkflowId == childWorkflowId);
        if (index < 0)
        {
            throw new InvalidOperationException($"Child '{childWorkflowId}' is not a member of group '{groupId}'.");
        }

        // The child's own outcome decides how the parent sees it — the same mapping the durable
        // driver applies, and what makes CompletionPolicy.AllSuccessful mean what its name says.
        var childStatus = childHarness.Outcome switch
        {
            WorkflowOutcome.Completed => ChildStatus.Completed,
            WorkflowOutcome.Failed or WorkflowOutcome.TimedOut => ChildStatus.Failed,
            WorkflowOutcome.Terminated => ChildStatus.Terminated,
            null when childHarness.Status == WorkflowStatus.Deleted => ChildStatus.Cancelled,
            _ => throw new InvalidOperationException(
                $"Child '{childWorkflowId}' is {childHarness.Status}, not finished; run it to completion before delivering its lifecycle."),
        };
        children[index] = children[index] with
        {
            Status = childStatus,
            Result = childStatus == ChildStatus.Completed ? childHarness.StateObject : null,
            Failure = childHarness.Outcome is WorkflowOutcome.Failed f ? f.Cause : null,
        };

        var members = children.Where(c => c.GroupId == groupId).ToList();
        var outcome = ChildGroupPolicy.EvaluateGroupOutcome(group, members);
        if (outcome is null)
        {
            _envelope = _envelope with { Children = children };
            return;
        }

        if (group.RemainingChildrenPolicy == RemainingChildrenPolicy.Terminate)
        {
            var pendingIds = members.Where(c => c.Status is ChildStatus.Pending or ChildStatus.TerminationRequested)
                .Select(c => c.RelationshipId).ToHashSet();
            for (var i = 0; i < children.Count; i++)
            {
                if (pendingIds.Contains(children[i].RelationshipId))
                {
                    children[i] = children[i] with { Status = ChildStatus.TerminationRequested };
                }
            }
        }

        _envelope = _envelope with
        {
            Children = _settings.PruneFinalizedChildren
                ? ChildGroupPolicy.PruneFinalizedGroupMembers(children, groupId)
                : children,
            ChildGroups = new Dictionary<string, ChildGroupState>(trackedGroups)
            {
                [groupId] = group with { Generation = group.Generation + 1, Finalized = true },
            },
        };
        await RunUntilStopCore(group.ResumeStepName, new ChildGroupResult(outcome.Value, members), cancellationToken);
    }
}
