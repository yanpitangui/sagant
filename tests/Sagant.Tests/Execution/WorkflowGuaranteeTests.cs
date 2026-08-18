using System.Collections.Immutable;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Settings;

namespace Sagant.Tests.Execution;

/// <summary>
/// The guarantees in <c>docs/guarantees.md</c>, asserted directly against
/// <see cref="WorkflowTransitionPlanner"/>. Each test names the guarantee it holds.
///
/// These run against the pure planner directly, with no driver in between, which is the point: a
/// guarantee that only held because of how one driver happens to be wired would be no guarantee at
/// all. Every driver
/// reaches these decisions through this one function, so proving it here proves it for all of them —
/// in milliseconds, with no <c>ActorSystem</c> and no persistence.
/// </summary>
public class WorkflowGuaranteeTests
{
    private sealed record OrderState(string Value);

    private static readonly DateTimeOffset Now = new(2026, 8, 12, 12, 0, 0, TimeSpan.Zero);

    private static readonly WorkflowInstanceIdentity Identity =
        new("OrderWorkflow-order-1", "order-1", "OrderWorkflow");

    private static WorkflowRuntimeState<OrderState> FreshEnvelope() =>
        new(new OrderState("initial"), CurrentStepName: null, CurrentStepInput: null,
            RetryCount: 0, Status: WorkflowStatus.Running);

    private static ResolvedWorkflowSettings Settings(Action<WorkflowSettingsBuilder>? configure = null)
    {
        var builder = WorkflowSettings.Create();
        configure?.Invoke(builder);
        return ResolvedWorkflowSettings.From(builder.Build());
    }

    /// <summary>The state a retry plan's events produce.</summary>
    private static WorkflowRuntimeState<OrderState> Folded(StepFailurePlan<OrderState>.Retry retry) =>
        WorkflowEventFold.ApplyAll(FreshEnvelope(), retry.Events);

    /// <summary>The state a plan's events produce — folded exactly as a driver and recovery fold
    /// them, so these assertions are about what a real instance would hold. Fold onto the envelope
    /// the plan was made from: an event carries only what changed, so the rest of the instance comes
    /// from underneath it.</summary>
    private static WorkflowRuntimeState<OrderState> Folded(
        WorkflowRuntimeState<OrderState> envelope, TransitionPlan<OrderState> plan) =>
        WorkflowEventFold.ApplyAll(envelope, plan.Events);

    /// <summary>Stands in wherever a test cares about the transition itself. Every batch names what
    /// drove it, so the planner requires a cause.</summary>
    private static readonly TransitionCause TestCause = new TransitionCause.Control("Test");

    /// <summary>The state an instance reaches by taking <paramref name="transition"/>.</summary>
    private static WorkflowRuntimeState<OrderState> Next(
        WorkflowRuntimeState<OrderState> envelope, Transition transition,
        ResolvedWorkflowSettings? settings = null, DateTimeOffset? now = null) =>
        Folded(envelope, Plan(envelope, transition, settings, now));

    private static TransitionPlan<OrderState> Plan(
        WorkflowRuntimeState<OrderState> envelope, Transition transition,
        ResolvedWorkflowSettings? settings = null, DateTimeOffset? now = null,
        TransitionCause? cause = null) =>
        WorkflowTransitionPlanner.Plan(
            envelope, transition, PersistenceEffect<OrderState>.NoPersistence.Instance, now ?? Now, settings ?? Settings(), Identity,
            cause ?? TestCause);

    // ── D3 ───────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void D3_WorkflowDeadline_IsEstablishedOnTheFirstTransition()
    {
        var settings = Settings(b => b.Timeout(TimeSpan.FromMinutes(30)));

        var next = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null), settings);

        Assert.Equal(Now + TimeSpan.FromMinutes(30), next.WorkflowDeadline);
    }

    [Fact]
    public void D3_WorkflowDeadline_IsNeverRecomputed()
    {
        var settings = Settings(b => b.Timeout(TimeSpan.FromMinutes(30)));
        var afterFirst = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null), settings);

        var muchLater = Now + TimeSpan.FromMinutes(20);
        var afterSecond = Next(afterFirst, new Transition.StepTransition("Ship", null), settings, muchLater);

        Assert.Equal(afterFirst.WorkflowDeadline, afterSecond.WorkflowDeadline);
    }

    /// <summary>Including across a pause, which is where a naive "recompute on each transition" would
    /// silently extend the deadline every time a workflow resumed.</summary>
    [Fact]
    public void D3_WorkflowDeadline_SurvivesAPause()
    {
        var settings = Settings(b => b.Timeout(TimeSpan.FromMinutes(30)));
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null), settings);
        var paused = Next(running, new Transition.PauseTransition("approval", null), settings, Now + TimeSpan.FromMinutes(5));

        var resumed = Next(paused, new Transition.StepTransition("Charge", null), settings, Now + TimeSpan.FromMinutes(25));

        Assert.Equal(running.WorkflowDeadline, resumed.WorkflowDeadline);
    }

    // ── D6 ───────────────────────────────────────────────────────────────────────────────────────

    /// <summary>The terminal envelope records the termination *and* the plan asks for the sends, so a
    /// crash between the two recovers from durable relationship state.</summary>
    [Fact]
    public void D6_TerminalTransition_MarksPendingChildrenAndAsksForTheirTermination()
    {
        var withChild = FreshEnvelope() with
        {
            Children = ChildrenOf(PendingChild("item-1", ParentClosePolicy.Terminate)),
        };

        var plan = Plan(withChild, new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance));

        Assert.Equal(ChildStatus.TerminationRequested, Folded(withChild, plan).Children!.Values.Single().Status);
        Assert.Single(plan.AfterPersist.OfType<WorkflowDecision.TerminateChild>());
    }

    [Fact]
    public void D6_TerminalTransition_LeavesAbandonPolicyChildrenAlone()
    {
        var withChild = FreshEnvelope() with
        {
            Children = ChildrenOf(PendingChild("item-1", ParentClosePolicy.Abandon)),
        };

        var plan = Plan(withChild, new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance));

        Assert.Equal(ChildStatus.Pending, Folded(withChild, plan).Children!.Values.Single().Status);
        Assert.Empty(plan.AfterPersist.OfType<WorkflowDecision.TerminateChild>());
    }

    // ── D7 ───────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void D7_AwaitChildren_RecordsEveryRelationshipBeforeAskingForAnySend()
    {
        var plan = Plan(FreshEnvelope(), AwaitTwoChildren());

        Assert.Equal(2, Folded(FreshEnvelope(), plan).Children!.Count);
        Assert.All(Folded(FreshEnvelope(), plan).Children!.Values, c => Assert.Equal(ChildStatus.Pending, c.Status));
        Assert.Equal(2, plan.AfterPersist.OfType<WorkflowDecision.StartChild>().Count());
    }

    // ── C2 ───────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void C2_StepTransition_ChainsWithoutReleasingDeferredCommands()
    {
        var plan = Plan(FreshEnvelope(), new Transition.StepTransition("Charge", null));

        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.StartStep);
        Assert.DoesNotContain(plan.AfterPersist, d => d is WorkflowDecision.ReleaseDeferredCommands);
    }

    [Theory]
    [InlineData("pause")]
    [InlineData("end")]
    [InlineData("children")]
    public void C2_SettledTransition_ReleasesDeferredCommands(string kind)
    {
        Transition transition = kind switch
        {
            "pause" => new Transition.PauseTransition("waiting", null),
            "end" => new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance),
            _ => AwaitTwoChildren(),
        };

        var plan = Plan(FreshEnvelope(), transition);

        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.ReleaseDeferredCommands);
        Assert.DoesNotContain(plan.AfterPersist, d => d is WorkflowDecision.StartStep);
    }

    // ── E3 ───────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void E3_PauseTransition_ArmsThePauseDeadlineAndNotTheStepDeadline()
    {
        var pauseSettings = PauseSettings.WithTimeout(TimeSpan.FromHours(24)).TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AutoCancel"));

        var plan = Plan(FreshEnvelope(), new Transition.PauseTransition(pauseSettings.Reason, pauseSettings));

        var paused = Folded(FreshEnvelope(), plan);
        Assert.Equal(Now + TimeSpan.FromHours(24), paused.PauseDeadline);
        Assert.Equal("AutoCancel", paused.PauseTimeoutStepName);
        Assert.Null(paused.StepDeadline);
        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.ArmTimer { Kind: WorkflowTimerKind.Pause });
    }

    /// <summary>Leaving a pause must clear its deadline, or the pause timeout could fire against a
    /// workflow that has already moved on.</summary>
    [Fact]
    public void E3_LeavingAPause_ClearsThePauseDeadlineAndCancelsItsTimer()
    {
        var pauseSettings = PauseSettings.WithTimeout(TimeSpan.FromHours(24)).TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AutoCancel"));
        var paused = Next(FreshEnvelope(), new Transition.PauseTransition(null, pauseSettings));

        var plan = Plan(paused, new Transition.StepTransition("Charge", null));

        var resumed = Folded(paused, plan);
        Assert.Null(resumed.PauseDeadline);
        Assert.Null(resumed.PauseTimeoutStepName);
        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.CancelTimer { Kind: WorkflowTimerKind.Pause });
    }

    // ── E5 ───────────────────────────────────────────────────────────────────────────────────────

    /// <summary>An instance standing on a step the deployed code has stopped registering is held at
    /// that step. A deploy that drops a step stalls the runs sitting on it, and each one keeps its state,
    /// its step and that step's input for whoever puts the step back.</summary>
    [Fact]
    public void E5_AnUnknownStepName_ParksTheRunAtThatStep()
    {
        var standing = Next(FreshEnvelope(), new Transition.StepTransition("Charge", 42));

        var parked = Next(standing, WorkflowTransitionPlanner.PlanUnknownStep("Charge"));

        Assert.Equal(WorkflowStatus.Suspended, parked.Status);
        Assert.Equal("Charge", parked.CurrentStepName);
        Assert.Equal(42, parked.CurrentStepInput);
        Assert.Equal("Charge", parked.ParkedFailure!.StepName);
        Assert.Null(parked.Outcome);
    }

    /// <summary>A caller waiting on the run is released, since the run makes no further progress
    /// until someone deploys the step and resumes it.</summary>
    [Fact]
    public void E5_ParkingOnAnUnknownStep_ReleasesWhoeverIsWaitingOnTheRun()
    {
        var standing = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null));

        var plan = Plan(standing, WorkflowTransitionPlanner.PlanUnknownStep("Charge"));

        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.NotifyCompletionWatchers);
    }

    /// <summary>Deploying the missing step back and resuming continues the run from where it stood,
    /// which is what makes a step removal recoverable.</summary>
    [Fact]
    public void E5_AParkedUnknownStep_ResumesAtTheSameStep()
    {
        var standing = Next(FreshEnvelope(), new Transition.StepTransition("Charge", 42));
        var parked = Next(standing, WorkflowTransitionPlanner.PlanUnknownStep("Charge"));

        var resume = Assert.IsType<ControlPlan<OrderState>.Apply>(
            WorkflowTransitionPlanner.PlanResume(parked, Now, Settings(), TestCause));
        var resumed = WorkflowEventFold.ApplyAll(parked, resume.Events);

        Assert.Equal(WorkflowStatus.Running, resumed.Status);
        Assert.Equal("Charge", resumed.CurrentStepName);
        Assert.Equal(42, resumed.CurrentStepInput);
        Assert.Null(resumed.ParkedFailure);
        Assert.Contains(resume.AfterPersist, d => d is WorkflowDecision.StartStep);
    }

    // ── G3 / park on exhausted retries ───────────────────────────────────────────────────────────

    /// <summary>A step that exhausts its budget under a parking strategy holds the instance in place,
    /// keeping the run alive, so a transient problem outside the workflow stays fixable. The step
    /// name and
    /// input survive, which is what lets Resume re-run exactly the attempt that failed.</summary>
    [Fact]
    public void ExhaustedRetriesUnderParking_SuspendsTheRunAtItsFailedStep()
    {
        var settings = ParkSettings();
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Charge", 42), settings);
        var exhausted = running with { RetryCount = 2 };

        var plan = WorkflowTransitionPlanner.PlanStepFailure(exhausted, "Charge", "gateway down", Now, settings);

        var conclude = Assert.IsType<StepFailurePlan<OrderState>.Conclude>(plan);
        var parked = Folded(exhausted, Plan(exhausted, conclude.Transition, settings));
        Assert.Equal(WorkflowStatus.Suspended, parked.Status);
        Assert.Equal("Charge", parked.CurrentStepName);
        Assert.Equal(42, parked.CurrentStepInput);
        Assert.Null(parked.Outcome);
    }

    /// <summary>The failure is readable while parked, so an operator can see what to fix before
    /// deciding to resume. Outcome stays null — the run has not ended.</summary>
    [Fact]
    public void ParkedRun_CarriesTheFailureThatParkedIt()
    {
        var settings = ParkSettings();
        var exhausted = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null), settings) with { RetryCount = 2 };

        var plan = WorkflowTransitionPlanner.PlanStepFailure(exhausted, "Charge", "gateway down", Now, settings);
        var parked = Folded(exhausted, Plan(exhausted, ((StepFailurePlan<OrderState>.Conclude)plan).Transition, settings));

        Assert.Equal("gateway down", parked.ParkedFailure!.Message);
        Assert.Equal("Charge", parked.ParkedFailure.StepName);
        Assert.Equal(3, parked.ParkedFailure.Attempts);
    }

    /// <summary>Resuming a parked run re-runs the failed step with a fresh budget — the existing
    /// Resume path (guarantee E4), so parking needs no control command of its own. The failure is
    /// cleared along with it, since the run has left the parked state that failure explained.</summary>
    [Fact]
    public void ResumingAParkedRun_RetriesTheFailedStepAndClearsTheFailure()
    {
        var settings = ParkSettings();
        var exhausted = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null), settings) with { RetryCount = 2 };
        var failurePlan = WorkflowTransitionPlanner.PlanStepFailure(exhausted, "Charge", "gateway down", Now, settings);
        var parked = Folded(exhausted, Plan(exhausted, ((StepFailurePlan<OrderState>.Conclude)failurePlan).Transition, settings));

        var resume = Assert.IsType<ControlPlan<OrderState>.Apply>(
            WorkflowTransitionPlanner.PlanResume(parked, Now, settings, TestCause));
        var resumed = WorkflowEventFold.ApplyAll(parked, resume.Events);

        Assert.Equal(WorkflowStatus.Running, resumed.Status);
        Assert.Equal("Charge", resumed.CurrentStepName);
        Assert.Equal(0, resumed.RetryCount);
        Assert.Null(resumed.ParkedFailure);
    }

    /// <summary>A step with a retry budget and no recovery path ends the run once the budget is
    /// spent — expressible on its own now, where a strategy previously always meant a failover.</summary>
    [Fact]
    public void ExhaustedRetriesUnderFailing_EndsTheRun()
    {
        var settings = Settings(b => b.StepRecovery(
            Ref.Step<DocWorkflowFor<string>, NoInput>("Charge"),
            RecoverStrategy.WithMaxRetries(2).ThenFail()));
        var exhausted = FreshEnvelope() with { RetryCount = 2 };

        var plan = WorkflowTransitionPlanner.PlanStepFailure(exhausted, "Charge", "gateway down", Now, settings);

        var conclude = Assert.IsType<StepFailurePlan<OrderState>.Conclude>(plan);
        var terminal = Assert.IsType<Transition.TerminalTransition>(conclude.Transition);
        Assert.IsType<WorkflowOutcome.Failed>(terminal.Outcome);
    }

    private static ResolvedWorkflowSettings ParkSettings() =>
        Settings(b => b.StepRecovery(
            Ref.Step<DocWorkflowFor<string>, NoInput>("Charge"),
            RecoverStrategy.WithMaxRetries(2).ThenPark()));

    // ── G5 / restart ─────────────────────────────────────────────────────────────────────────────

    /// <summary>A restart keeps the instance alive and continues at a named step, so the run goes on
    /// under the same id with its history reclaimable.</summary>
    [Fact]
    public void Restart_ContinuesAtItsStep_AndAsksForHistoryToBeReclaimed()
    {
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Loop", null));

        var plan = Plan(running, new Transition.RestartTransition("Loop", 7, "next cycle"));

        var restarted = Folded(running, plan);
        Assert.Equal(WorkflowStatus.Running, restarted.Status);
        Assert.Equal("Loop", restarted.CurrentStepName);
        Assert.Equal(7, restarted.CurrentStepInput);
        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.ReclaimHistory);
        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.StartStep);
    }

    /// <summary>The workflow deadline is reset, so a workflow that restarts indefinitely is bounded
    /// per cycle. D3 writes it once per instance, which would otherwise expire mid-loop.</summary>
    [Fact]
    public void Restart_ClearsTheWorkflowDeadline_SoTheNextCycleGetsItsOwn()
    {
        var settings = Settings(b => b.Timeout(TimeSpan.FromMinutes(30)));
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Loop", null), settings);
        Assert.NotNull(running.WorkflowDeadline);

        var restarted = Folded(running, Plan(running, new Transition.RestartTransition("Loop", null, null), settings));

        Assert.Null(restarted.WorkflowDeadline);
    }

    /// <summary>Delivery bookkeeping survives a restart: the producer keeps counting sequence
    /// numbers across it, so a redelivery arriving afterwards is still recognised.</summary>
    [Fact]
    public void Restart_KeepsTheDeduplicationLedgers()
    {
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Loop", null)) with
        {
            HighestAppliedSeqNr = SeqNrLedger.Empty(8).Record("producer-1", 4),
        };

        var restarted = Folded(running, Plan(running, new Transition.RestartTransition("Loop", null, null)));

        Assert.True(restarted.HighestAppliedSeqNr!.TryGetHighest("producer-1", out var highest));
        Assert.Equal(4, highest);
    }

    /// <summary>A command that drove a transition is recorded on the event that transition produced,
    /// so "who did what, and when" is answerable from the event stream.</summary>
    [Fact]
    public void CommandCause_OpensTheBatchWithTheCauseItRecords()
    {
        var plan = Plan(
            FreshEnvelope(),
            new Transition.StepTransition("Charge", null),
            cause: new TransitionCause.Command("ApproveOrder"));

        var caused = Assert.IsType<WorkflowEvent.StepStarted>(plan.Events[0]);
        var command = Assert.IsType<TransitionCause.Command>(caused.Cause);
        Assert.Equal("ApproveOrder", command.CommandType);
    }

    /// <summary>Caller-supplied metadata is what turns "an approval happened" into "who approved it".
    /// It travels on the cause, so one batch carries it once.</summary>
    [Fact]
    public void CommandCause_CarriesCallerMetadata()
    {
        var metadata = new Dictionary<string, string> { ["user"] = "operator-7", ["correlation"] = "abc-123" };

        var plan = Plan(
            FreshEnvelope(),
            new Transition.StepTransition("Charge", null),
            cause: new TransitionCause.Command("ApproveOrder") { Metadata = metadata });

        var caused = Assert.IsType<WorkflowEvent.StepStarted>(plan.Events[0]);
        Assert.Equal("operator-7", caused.Cause.Metadata!["user"]);
        Assert.Equal("abc-123", caused.Cause.Metadata["correlation"]);
    }

    /// <summary>A step outcome names itself the same way a command does, so one event type answers
    /// "what happened here" whatever drove it.</summary>
    [Fact]
    public void StepOutcomeCause_RecordsTheMeasuredAttempt()
    {
        var plan = Plan(
            FreshEnvelope(),
            new Transition.StepTransition("Ship", null),
            cause: new TransitionCause.StepSucceeded("Charge", 2, TimeSpan.FromMilliseconds(120)));

        var caused = Assert.IsType<WorkflowEvent.StepStarted>(plan.Events[0]);
        var succeeded = Assert.IsType<TransitionCause.StepSucceeded>(caused.Cause);
        Assert.Equal("Charge", succeeded.StepName);
        Assert.Equal(2, succeeded.Attempt);
        Assert.Equal(TimeSpan.FromMilliseconds(120), succeeded.Duration);
    }

    /// <summary>A pause reason travels on the event, so anything reading the event stream alone can
    /// say why an instance is waiting.</summary>
    [Fact]
    public void PauseTransition_RecordsItsReasonOnTheEvent()
    {
        var plan = Plan(FreshEnvelope(), new Transition.PauseTransition("awaiting approval", null));

        var paused = Assert.Single(plan.Events.OfType<WorkflowEvent.RunPaused>());
        Assert.Equal("awaiting approval", paused.Reason);
    }

    [Fact]
    public void E3_LeavingAPause_ReportsAResume()
    {
        var paused = Next(FreshEnvelope(), new Transition.PauseTransition("approval", null));

        var plan = Plan(paused, new Transition.StepTransition("Charge", null));

        Assert.Equal(WorkflowStatus.Running, Folded(paused, plan).Status);
        Assert.Contains(plan.AfterPersist,
            d => d is WorkflowDecision.RecordStatusChange { Status: WorkflowStatus.Running });
    }

    // ── E1 ───────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void E1_StepTransition_ResetsTheRetryCount()
    {
        var retrying = FreshEnvelope() with { RetryCount = 4 };

        var plan = Plan(retrying, new Transition.StepTransition("Charge", null));

        Assert.Equal(0, Folded(retrying, plan).RetryCount);
    }

    // ── E1 / E2, via PlanStepFailure ─────────────────────────────────────────────────────────────

    private static ResolvedWorkflowSettings RetrySettings(Func<int, TimeSpan>? backoff = null, TimeSpan? stepTimeout = null)
    {
        var strategy = RecoverStrategy.WithMaxRetries(2).FailoverTo(Ref.Step<DocWorkflowFor<string>>("Compensate"));
        if (backoff is not null)
        {
            strategy = strategy.WithBackoff(backoff);
        }

        var builder = WorkflowSettings.Create().StepRecovery(Ref.Step<DocWorkflowFor<string>, NoInput>("Charge"), strategy);
        if (stepTimeout is { } timeout)
        {
            builder = builder.DefaultStepTimeout(timeout);
        }

        return ResolvedWorkflowSettings.From(builder.Build());
    }

    [Fact]
    public void E1_FailureWithinBudget_Retries()
    {
        var plan = WorkflowTransitionPlanner.PlanStepFailure(
            FreshEnvelope(), "Charge", "boom", Now, RetrySettings());

        var retry = Assert.IsType<StepFailurePlan<OrderState>.Retry>(plan);
        Assert.Equal(1, Folded(retry).RetryCount);
        Assert.Equal(2, retry.Attempt);
    }

    /// <summary>A failed attempt records the step, its number, the error and how long it ran, so a
    /// consumer reading the event stream can report a retried-then-succeeded failure. Only the
    /// terminal failure reaches <see cref="WorkflowOutcome.Failed"/>, so an attempt that retries is
    /// visible here alone.</summary>
    [Fact]
    public void FailedAttempt_RecordsTheAttemptOnItsOwnEvent()
    {
        var plan = WorkflowTransitionPlanner.PlanStepFailure(
            FreshEnvelope(), "Charge", "boom", Now, RetrySettings(), duration: TimeSpan.FromMilliseconds(340));

        var retry = Assert.IsType<StepFailurePlan<OrderState>.Retry>(plan);
        var caused = Assert.Single(retry.Events.OfType<WorkflowEvent.StepRetryScheduled>());
        var failed = Assert.IsType<TransitionCause.StepFailed>(caused.Cause);
        Assert.Equal("Charge", failed.StepName);
        Assert.Equal(1, failed.Attempt);
        Assert.Equal("boom", failed.Error);
        Assert.Equal(TimeSpan.FromMilliseconds(340), failed.Duration);
        Assert.True(failed.WillRetry);
    }

    [Fact]
    public void E1_FailureWithBudgetExhausted_FailsOver()
    {
        var exhausted = FreshEnvelope() with { RetryCount = 2 };

        var plan = WorkflowTransitionPlanner.PlanStepFailure(exhausted, "Charge", "boom", Now, RetrySettings());

        var conclude = Assert.IsType<StepFailurePlan<OrderState>.Conclude>(plan);
        Assert.Equal("Compensate", Assert.IsType<Transition.StepTransition>(conclude.Transition).StepName);
    }

    [Fact]
    public void E1_FailureWithNoStrategy_EndsCarryingTheReason()
    {
        var plan = WorkflowTransitionPlanner.PlanStepFailure(
            FreshEnvelope(), "Charge", "connection reset", Now, Settings());

        var conclude = Assert.IsType<StepFailurePlan<OrderState>.Conclude>(plan);
        var failed = Assert.IsType<WorkflowOutcome.Failed>(
            Assert.IsType<Transition.TerminalTransition>(conclude.Transition).Outcome);
        Assert.Contains("connection reset", failed.Cause.Message);
        Assert.Equal("Charge", failed.Cause.StepName);
    }

    /// <summary>A failed attempt must not carry its state forward — E1's "state unchanged" half.</summary>
    [Fact]
    public void E1_RetryEnvelope_KeepsTheStateTheAttemptStartedFrom()
    {
        var envelope = FreshEnvelope() with { UserState = new OrderState("before") };

        var plan = WorkflowTransitionPlanner.PlanStepFailure(envelope, "Charge", "boom", Now, RetrySettings());

        // A retry records the attempt, never the state — so nothing the failed attempt did can leak
        // into what the next one starts from (guarantee E1).
        var retry = Assert.IsType<StepFailurePlan<OrderState>.Retry>(plan);
        Assert.DoesNotContain(retry.Events, e => e is WorkflowEvent.UserStateChanged<OrderState>);
    }

    /// <summary>
    /// The whole point of E2: with a 30s backoff and a 5s step timeout, a deadline measured from
    /// "now" would already be 25s expired by the time the attempt began. It must be measured from
    /// when the attempt actually starts.
    /// </summary>
    [Fact]
    public void E2_RetryDeadline_IsMeasuredFromWhenTheAttemptStarts()
    {
        var settings = RetrySettings(backoff: _ => TimeSpan.FromSeconds(30), stepTimeout: TimeSpan.FromSeconds(5));

        var plan = WorkflowTransitionPlanner.PlanStepFailure(FreshEnvelope(), "Charge", "boom", Now, settings);

        var retry = Assert.IsType<StepFailurePlan<OrderState>.Retry>(plan);
        Assert.Equal(Now + TimeSpan.FromSeconds(30), retry.RetryDelayUntil);
        Assert.Equal(Now + TimeSpan.FromSeconds(35), Folded(retry).StepDeadline);
        Assert.True(Folded(retry).StepDeadline > retry.RetryDelayUntil);
    }

    [Fact]
    public void E2_NoBackoff_StartsImmediately()
    {
        var plan = WorkflowTransitionPlanner.PlanStepFailure(
            FreshEnvelope(), "Charge", "boom", Now, RetrySettings(stepTimeout: TimeSpan.FromSeconds(5)));

        var retry = Assert.IsType<StepFailurePlan<OrderState>.Retry>(plan);
        Assert.Null(retry.RetryDelayUntil);
        Assert.Equal(Now + TimeSpan.FromSeconds(5), Folded(retry).StepDeadline);
    }

    /// <summary>A backoff function returning a negative delay must not pull the deadline backwards.</summary>
    [Fact]
    public void E2_NegativeBackoff_IsTreatedAsNoDelay()
    {
        var settings = RetrySettings(backoff: _ => TimeSpan.FromSeconds(-10), stepTimeout: TimeSpan.FromSeconds(5));

        var plan = WorkflowTransitionPlanner.PlanStepFailure(FreshEnvelope(), "Charge", "boom", Now, settings);

        var retry = Assert.IsType<StepFailurePlan<OrderState>.Retry>(plan);
        Assert.Null(retry.RetryDelayUntil);
        Assert.Equal(Now + TimeSpan.FromSeconds(5), Folded(retry).StepDeadline);
    }

    // ── E3, via PlanWorkflowTimeout ──────────────────────────────────────────────────────────────

    [Fact]
    public void E3_WorkflowTimeout_DoesNothingWhilePaused()
    {
        var paused = FreshEnvelope() with { Status = WorkflowStatus.Paused };

        Assert.Null(WorkflowTransitionPlanner.PlanWorkflowTimeout(paused, Settings()));
    }

    [Theory]
    [InlineData(WorkflowStatus.Finished)]
    [InlineData(WorkflowStatus.Suspended)]
    [InlineData(WorkflowStatus.Deleted)]
    public void E3_WorkflowTimeout_DoesNothingOnceTheInstanceIsNotRunning(WorkflowStatus status)
    {
        var envelope = FreshEnvelope() with { Status = status };

        Assert.Null(WorkflowTransitionPlanner.PlanWorkflowTimeout(envelope, Settings()));
    }

    [Fact]
    public void E3_WorkflowTimeout_WhileRunning_FailsOverWhenConfigured()
    {
        var settings = Settings(b => b.Timeout(TimeSpan.FromMinutes(5), Ref.Step<DocWorkflowFor<string>>("Refund")));

        var transition = WorkflowTransitionPlanner.PlanWorkflowTimeout(FreshEnvelope(), settings);

        Assert.Equal("Refund", Assert.IsType<Transition.StepTransition>(transition).StepName);
    }

    [Fact]
    public void E3_WorkflowTimeout_WhileRunning_EndsWhenNoFailoverConfigured()
    {
        var transition = WorkflowTransitionPlanner.PlanWorkflowTimeout(FreshEnvelope(), Settings());

        Assert.IsType<WorkflowOutcome.TimedOut>(Assert.IsType<Transition.TerminalTransition>(transition).Outcome);
    }

    // ── Pause timeout ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void PauseTimeout_FiresIntoTheConfiguredHandler()
    {
        var pauseSettings = PauseSettings.WithTimeout(TimeSpan.FromHours(1)).TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AutoCancel"));
        var paused = Next(FreshEnvelope(), new Transition.PauseTransition(null, pauseSettings));

        var transition = WorkflowTransitionPlanner.PlanPauseTimeout(paused);

        Assert.Equal("AutoCancel", Assert.IsType<Transition.StepTransition>(transition).StepName);
    }

    /// <summary>A pause timer that outlived its pause is a stale no-op — it produces no transition
    /// at all.</summary>
    [Fact]
    public void PauseTimeout_DoesNothingOnceTheInstanceHasResumed()
    {
        var pauseSettings = PauseSettings.WithTimeout(TimeSpan.FromHours(1)).TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AutoCancel"));
        var paused = Next(FreshEnvelope(), new Transition.PauseTransition(null, pauseSettings));
        var resumed = Next(paused, new Transition.StepTransition("Charge", null));

        Assert.Null(WorkflowTransitionPlanner.PlanPauseTimeout(resumed));
    }

    // ── H3 ───────────────────────────────────────────────────────────────────────────────────────

    /// <summary>The counter advances only in the envelope the id was derived from, so a step retried
    /// before that envelope was persisted derives the same id again.</summary>
    [Fact]
    public void H3_GroupId_IsStableAcrossARetriedStep()
    {
        var envelope = FreshEnvelope();

        var first = Next(envelope, AwaitTwoChildren());
        var retried = Next(envelope, AwaitTwoChildren());

        Assert.Equal(first.ChildGroups!.Keys.Single(), retried.ChildGroups!.Keys.Single());
    }

    [Fact]
    public void H3_GroupSequence_AdvancesOnlyForGeneratedIds()
    {
        var afterGenerated = Next(FreshEnvelope(), AwaitTwoChildren());
        Assert.Equal(1, afterGenerated.ChildGroupSequence);

        var afterExplicit = Next(afterGenerated, AwaitTwoChildren("explicit-group"));
        Assert.Equal(1, afterExplicit.ChildGroupSequence);
    }

    // ── Terminal bookkeeping ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void TerminalTransition_CancelsTheWorkflowTimerAndReleasesCompletionWatchers()
    {
        var settings = Settings(b => b.Timeout(TimeSpan.FromMinutes(30)));
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null), settings);

        var plan = Plan(running, new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance), settings);

        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.CancelTimer { Kind: WorkflowTimerKind.Workflow });
        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.NotifyCompletionWatchers);
        Assert.Null(Folded(running, plan).CurrentStepName);
    }

    [Fact]
    public void DeleteTransition_PurgesAndReportsDeletedRatherThanEnded()
    {
        var plan = Plan(FreshEnvelope(), new Transition.DeleteTransition("gdpr"));

        Assert.Contains(plan.AfterPersist, d => d is WorkflowDecision.PurgeAndStop);
        // Deletion is not an outcome: a run purged before finishing never reports one.
        Assert.Empty(plan.AfterPersist.OfType<WorkflowDecision.RecordOutcome>());
        Assert.Single(plan.Events.OfType<WorkflowEvent.RunDeleted>());
        Assert.Null(Folded(FreshEnvelope(), plan).Outcome);
    }

    [Fact]
    public void ChildInstance_ReportsItsTerminalStatusToItsParent()
    {
        var asChild = FreshEnvelope() with
        {
            ParentRelationship = PendingChild("order-1", ParentClosePolicy.Abandon),
        };

        var plan = Plan(asChild, new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance));

        var notify = Assert.Single(plan.AfterPersist.OfType<WorkflowDecision.NotifyParent>());
        Assert.IsType<WorkflowOutcome.Completed>(notify.Outcome);
    }

    [Fact]
    public void StatusChange_IsReportedOnlyWhenTheStatusActuallyMoves()
    {
        var running = Next(FreshEnvelope(), new Transition.StepTransition("Charge", null));

        var stillRunning = Plan(running, new Transition.StepTransition("Ship", null));

        Assert.Empty(stillRunning.AfterPersist.OfType<WorkflowDecision.RecordStatusChange>());
    }

    // ── E9, via PlanCancel ───────────────────────────────────────────────────────────────────────

    [Fact]
    public void E9_Cancel_WithACancellationStep_UnwindsThroughIt()
    {
        var settings = Settings(b => b.CancelVia(Ref.Step<DocWorkflowFor<string>>("Compensate")));

        var transition = WorkflowTransitionPlanner.PlanCancel(FreshEnvelope(), "customer changed mind", settings);

        var step = Assert.IsType<Transition.StepTransition>(transition);
        Assert.Equal("Compensate", step.StepName);
        Assert.Equal("customer changed mind", Assert.IsType<WorkflowCancellation>(step.Input).Reason);
    }

    /// <summary>Nothing to unwind still reports Cancelled, distinct from Terminated: what was asked
    /// for is worth recording even where the effect ends up the same.</summary>
    [Fact]
    public void E9_Cancel_WithNoCancellationStep_FinishesAsCancelled()
    {
        var transition = WorkflowTransitionPlanner.PlanCancel(FreshEnvelope(), "no longer needed", Settings());

        var cancelled = Assert.IsType<WorkflowOutcome.Cancelled>(
            Assert.IsType<Transition.TerminalTransition>(transition).Outcome);
        Assert.Equal("no longer needed", cancelled.Reason);
    }

    [Fact]
    public void E9_Cancel_OnAFinishedRun_DoesNothing()
    {
        var finished = Next(FreshEnvelope(), new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance));

        Assert.Null(WorkflowTransitionPlanner.PlanCancel(finished, "too late", Settings(b => b.CancelVia(Ref.Step<DocWorkflowFor<string>>("Compensate")))));
    }

    /// <summary>The compensation step decides the run's final outcome, so a compensation that failed
    /// reports that failure — the runtime never overrides it to insist the cancel was clean.</summary>
    [Fact]
    public void E9_CancellationStep_MayFinishAsFailedInsteadOfCancelled()
    {
        var settings = Settings(b => b.CancelVia(Ref.Step<DocWorkflowFor<string>>("Compensate")));
        var unwinding = Next(FreshEnvelope(), WorkflowTransitionPlanner.PlanCancel(FreshEnvelope(), null, settings)!, settings);

        var plan = Plan(
            unwinding,
            new Transition.TerminalTransition(new WorkflowOutcome.Failed(new WorkflowFailure("refund declined"))),
            settings);

        var failed = Assert.IsType<WorkflowOutcome.Failed>(Folded(unwinding, plan).Outcome);
        Assert.Equal("refund declined", failed.Cause.Message);
    }

    [Fact]
    public void E9_CancelledParent_CancelsItsChildrenRatherThanTerminatingThem()
    {
        var withChild = FreshEnvelope() with
        {
            Children = ChildrenOf(PendingChild("item-1", ParentClosePolicy.Terminate)),
        };

        var plan = Plan(withChild, new Transition.TerminalTransition(new WorkflowOutcome.Cancelled("parent cancelled")));

        Assert.Single(plan.AfterPersist.OfType<WorkflowDecision.CancelChild>());
        Assert.Empty(plan.AfterPersist.OfType<WorkflowDecision.TerminateChild>());
    }

    [Fact]
    public void E9_NonCancelledTerminalParent_StillTerminatesItsChildren()
    {
        var withChild = FreshEnvelope() with
        {
            Children = ChildrenOf(PendingChild("item-1", ParentClosePolicy.Terminate)),
        };

        var plan = Plan(withChild, new Transition.TerminalTransition(WorkflowOutcome.Completed.Instance));

        Assert.Single(plan.AfterPersist.OfType<WorkflowDecision.TerminateChild>());
        Assert.Empty(plan.AfterPersist.OfType<WorkflowDecision.CancelChild>());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────

    private static IImmutableDictionary<string, ChildWorkflowRelationship> ChildrenOf(params ChildWorkflowRelationship[] children) =>
        children.ToImmutableDictionary(c => c.RelationshipId);

    private static ChildWorkflowRelationship PendingChild(string childId, ParentClosePolicy policy) =>
        new(
            RelationshipId: $"parent:group-1:{childId}",
            ParentWorkflowType: "OrderWorkflow",
            ParentWorkflowId: "order-1",
            ChildWorkflowType: "ItemWorkflow",
            ChildWorkflowId: childId,
            GroupId: "group-1",
            Generation: 0,
            Status: ChildStatus.Pending,
            Result: null,
            Failure: null,
            TraceParent: null,
            ParentClosePolicy: policy,
            Command: new object());

    private static Transition.AwaitChildrenTransition AwaitTwoChildren(string? groupId = null) =>
        new(
            groupId,
            new[]
            {
                new ChildStart("ItemWorkflow", "item-1", new object(), ParentClosePolicy.Terminate),
                new ChildStart("ItemWorkflow", "item-2", new object(), ParentClosePolicy.Terminate),
            },
            CompletionPolicy.AllSuccessful,
            FailurePolicy.FailFast,
            RemainingChildrenPolicy.Terminate,
            "OnItemsDone");
}
