using Microsoft.Extensions.Time.Testing;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Settings;
using Sagant.Protocol;
using Sagant.Testing;

namespace Sagant.Tests;

/// <summary>
/// Proves <see cref="WorkflowTestHarness{TWorkflow,TState}"/> genuinely needs nothing from
/// Akka.NET: this class doesn't derive from <c>Akka.TestKit.Xunit2.TestKit</c>, never touches
/// <c>ActorSystem</c>/<c>IActorRef</c>/persistence, and every test runs synchronously fast.
/// <see cref="CounterWorkflow"/>'s dispatcher tables are hand-written (same stand-in pattern as
/// <see cref="WorkflowStepDispatcherTests"/>), which is irrelevant to the
/// harness, which only depends on <see cref="IWorkflowStepDispatcher{TState}"/>/
/// <see cref="IWorkflowCommandDispatcher{TState}"/>, exactly what the generator emits for a real
/// <c>[WorkflowStep]</c>/<c>[WorkflowCommandHandler]</c>-attributed workflow.
/// </summary>
public class WorkflowTestHarnessTests
{
    private sealed record CounterState(int Value, bool Doubled = false, bool AutoCancelled = false);

    private sealed record Begin(int StartAt);

    private sealed record Ping;

    private sealed record Peek;

    private sealed class CounterWorkflow : Workflow<CounterState>, IWorkflowStepDispatcher<CounterState>, IWorkflowCommandDispatcher<CounterState>, IWorkflowQueryDispatcher<CounterState>, IWorkflowChildResultDispatcher<CounterState>
    {
        public static class Steps
        {
            public static readonly StepRef<CounterWorkflow, NoInput> Increment = new("Increment");
            public static readonly StepRef<CounterWorkflow, NoInput> Double = new("Double");
            public static readonly StepRef<CounterWorkflow, int> AddAmount = new("AddAmount");
            public static readonly StepRef<CounterWorkflow, NoInput> Boom = new("Boom");
            public static readonly StepRef<CounterWorkflow, NoInput> Flaky = new("Flaky");
            public static readonly StepRef<CounterWorkflow, NoInput> WaitForApproval = new("WaitForApproval");
            public static readonly StepRef<CounterWorkflow, NoInput> AutoCancel = new("AutoCancel");
            public static readonly StepRef<CounterWorkflow, NoInput> Slow = new("Slow");
        }

        private static readonly Dictionary<string, StepDescriptor<CounterState>> StepDescriptors = new()
        {
            ["Increment"] = new("Increment", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).IncrementStep(ctx)),
            ["Double"] = new("Double", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).DoubleStep(ctx)),
            ["AddAmount"] = new("AddAmount", typeof(int), static (w, ctx, input) => ((CounterWorkflow)w).AddAmountStep((int)input!, ctx)),
            ["Boom"] = new("Boom", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).BoomStep(ctx)),
            ["Flaky"] = new("Flaky", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).FlakyStep(ctx)),
            ["WaitForApproval"] = new("WaitForApproval", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).WaitForApprovalStep(ctx)),
            ["AutoCancel"] = new("AutoCancel", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).AutoCancelStep(ctx)),
            ["Slow"] = new("Slow", typeof(NoInput), static (w, ctx, _) => ((CounterWorkflow)w).SlowStep(ctx)),
        };

        private static readonly Dictionary<Type, QueryDescriptor<CounterState>> QueryDescriptors = new()
        {
            [typeof(Peek)] = new(typeof(Peek), nameof(Peek),
                static (w, ctx, q) => Task.FromResult(((CounterWorkflow)w).PeekHandler((Peek)q, ctx))),
        };

        private static readonly Dictionary<Type, CommandDescriptor<CounterState>> CommandDescriptors = new()
        {
            [typeof(Begin)] = new(typeof(Begin), nameof(Begin), static (w, ctx, cmd) => ((CounterWorkflow)w).Start((Begin)cmd, ctx)),
            [typeof(Ping)] = new(typeof(Ping), nameof(Ping), static (w, ctx, cmd) => ((CounterWorkflow)w).PingHandler((Ping)cmd, ctx)),
        };

        private readonly WorkflowSettings _settings;
        private int _flakyAttempts;

        public CounterWorkflow(WorkflowSettings? settings = null)
        {
            _settings = settings ?? WorkflowSettings.Default;
        }

        public override CounterState EmptyState() => new(0);

        public override WorkflowSettings Settings() => _settings;

        bool IWorkflowStepDispatcher<CounterState>.TryGetStep(string stepName, out StepDescriptor<CounterState> descriptor) =>
            StepDescriptors.TryGetValue(stepName, out descriptor);

        IReadOnlyCollection<string> IWorkflowStepDispatcher<CounterState>.StepNames => StepDescriptors.Keys;

        bool IWorkflowQueryDispatcher<CounterState>.TryGetQuery(Type queryType, out QueryDescriptor<CounterState> descriptor) =>
            QueryDescriptors.TryGetValue(queryType, out descriptor);

        bool IWorkflowChildResultDispatcher<CounterState>.TryGetChildResultHandler(out ChildResultDescriptor<CounterState> descriptor) { descriptor = default; return false; }

        bool IWorkflowCommandDispatcher<CounterState>.TryGetHandler(Type commandType, out CommandDescriptor<CounterState> descriptor) =>
            CommandDescriptors.TryGetValue(commandType, out descriptor);

        public CommandEffect<CounterState> Start(Begin cmd, CommandContext<CounterState> ctx) =>
            Effects.UpdateState(new CounterState(cmd.StartAt)).TransitionTo(Steps.Increment);

        public CommandEffect<CounterState> PingHandler(Ping cmd, CommandContext<CounterState> ctx) => Effects.Reply("pong");

        public Task<StepEffect<CounterState>> IncrementStep(StepContext<CounterState> ctx) =>
            Task.FromResult(StepEffects.UpdateState(ctx.State with { Value = ctx.State.Value + 1 }).ThenTransitionTo(Steps.Double));

        public Task<StepEffect<CounterState>> DoubleStep(StepContext<CounterState> ctx) =>
            Task.FromResult(StepEffects.UpdateState(ctx.State with { Value = ctx.State.Value * 2, Doubled = true }).ThenComplete());

        public Task<StepEffect<CounterState>> AddAmountStep(int amount, StepContext<CounterState> ctx) =>
            Task.FromResult(StepEffects.UpdateState(ctx.State with { Value = ctx.State.Value + amount }).ThenComplete());

        public Task<StepEffect<CounterState>> BoomStep(StepContext<CounterState> ctx) =>
            throw new InvalidOperationException("boom");

        /// <summary>Throws on its first two invocations, succeeds on the third — for proving a
        /// harness-driven retry genuinely re-invokes the step — a counter alone could not prove
        /// that.</summary>
        public Task<StepEffect<CounterState>> FlakyStep(StepContext<CounterState> ctx)
        {
            _flakyAttempts++;
            if (_flakyAttempts < 3)
            {
                throw new InvalidOperationException($"flaky attempt {_flakyAttempts}");
            }

            return Task.FromResult(StepEffects.UpdateState(ctx.State with { Value = ctx.State.Value + 1 }).ThenComplete());
        }

        public Task<StepEffect<CounterState>> WaitForApprovalStep(StepContext<CounterState> ctx) =>
            Task.FromResult(StepEffects.ThenPause(
                PauseSettings.WithTimeout(TimeSpan.FromMinutes(10)).TimeoutHandler(Steps.AutoCancel)));

        public Task<StepEffect<CounterState>> AutoCancelStep(StepContext<CounterState> ctx) =>
            Task.FromResult(StepEffects.UpdateState(ctx.State with { AutoCancelled = true }).ThenComplete());

        public QueryEffect PeekHandler(Peek query, QueryContext<CounterState> ctx) =>
            QueryEffects.Reply(ctx.State.Value);

        /// <summary>Parks at <see cref="Gate"/> so a test can dispatch a query into the window where
        /// this step is running, then reads its state again on the far side of that await.</summary>
        public async Task<StepEffect<CounterState>> SlowStep(StepContext<CounterState> ctx)
        {
            SeenBeforeAwait = ctx.State.Value;
            await Gate.Task;
            SeenAfterAwait = ctx.State.Value;
            return StepEffects.UpdateState(ctx.State with { Value = ctx.State.Value + 100 }).ThenComplete();
        }

        public readonly TaskCompletionSource Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int? SeenBeforeAwait;

        public int? SeenAfterAwait;
    }

    [Fact]
    public void RunCommand_DispatchesToHandler_AndAdvancesState()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        var effect = harness.RunCommand(new Begin(5));

        Assert.Equal(5, harness.State.Value);
        Assert.IsType<Transition.StepTransition>(effect.Transition);
    }

    [Fact]
    public async Task RunStep_DispatchesSingleStep_AndAdvancesState()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow())
        {
            State = new CounterState(5),
        };

        var effect = await harness.RunStep(CounterWorkflow.Steps.Increment);

        Assert.Equal(6, harness.State.Value);
        Assert.Equal(new Transition.StepTransition("Double", null), effect.Transition);
    }

    [Fact]
    public async Task RunStep_WithInput_PassesInputThrough()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow())
        {
            State = new CounterState(10),
        };

        await harness.RunStep(CounterWorkflow.Steps.AddAmount, 7);

        Assert.Equal(17, harness.State.Value);
    }

    [Fact]
    public async Task RunUntilStop_FromCommand_FollowsChainToEnd()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        // 3 -> Increment -> 4 -> Double -> 8, End
        var final = await harness.RunUntilStop(new Begin(3));

        Assert.Equal(8, harness.State.Value);
        Assert.True(harness.State.Doubled);
        Assert.IsType<Transition.TerminalTransition>(final.Transition);
    }

    [Fact]
    public async Task RunUntilStop_FromStep_ResumesMidChainAfterHandSeededState()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow())
        {
            // Jump straight to Double, skipping Increment entirely — the point of State being
            // writable from outside a running step.
            State = new CounterState(100),
        };

        var final = await harness.RunUntilStop(CounterWorkflow.Steps.Double);

        Assert.Equal(200, harness.State.Value);
        Assert.IsType<Transition.TerminalTransition>(final.Transition);
    }

    [Fact]
    public async Task RunUntilStop_CommandWithoutStepTransition_Throws()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunUntilStop(new Ping()));
    }

    [Fact]
    public async Task RunStep_UnregisteredStepName_Throws()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.RunStep(new StepRef<CounterWorkflow, NoInput>("DoesNotExist")));
    }

    [Fact]
    public void RunCommand_UnregisteredCommandType_Throws()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        Assert.Throws<InvalidOperationException>(() => harness.RunCommand("not a registered command"));
    }

    [Fact]
    public async Task RunStep_StepThrows_PropagatesException()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunStep(CounterWorkflow.Steps.Boom));
    }

    [Fact]
    public async Task RunStep_StepThrows_WithRecoverStrategy_RetriesUntilSuccess()
    {
        var settings = WorkflowSettings.Create()
            .StepRecovery(CounterWorkflow.Steps.Flaky, RecoverStrategy.WithMaxRetries(3).FailoverTo(CounterWorkflow.Steps.Boom))
            .Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings))
        {
            State = new CounterState(0),
        };

        var effect = await harness.RunStep(CounterWorkflow.Steps.Flaky);

        // Two failed attempts consumed from the retry budget, third attempt succeeded.
        Assert.Equal(1, harness.State.Value);
        Assert.IsType<Transition.TerminalTransition>(effect.Transition);
    }

    [Fact]
    public async Task RunStep_StepThrows_RetriesExhausted_FailsOverWithStateUnchanged()
    {
        var settings = WorkflowSettings.Create()
            .StepRecovery(CounterWorkflow.Steps.Boom, RecoverStrategy.WithMaxRetries(1).FailoverTo(CounterWorkflow.Steps.Increment))
            .Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings))
        {
            State = new CounterState(9),
        };

        var effect = await harness.RunStep(CounterWorkflow.Steps.Boom);

        Assert.Equal(new Transition.StepTransition("Increment", null), effect.Transition);
        Assert.Equal(9, harness.State.Value);
    }

    [Fact]
    public async Task RunPauseTimeoutIfDue_DeadlineNotYetReached_ReturnsNull()
    {
        var timeProvider = new FakeTimeProvider();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), timeProvider: timeProvider);

        await harness.RunStep(CounterWorkflow.Steps.WaitForApproval);
        timeProvider.Advance(TimeSpan.FromMinutes(5)); // PauseSettings.Timeout is 10 minutes

        var fired = await harness.RunPauseTimeoutIfDue();

        Assert.Null(fired);
        Assert.False(harness.State.AutoCancelled);
    }

    [Fact]
    public async Task RunPauseTimeoutIfDue_DeadlineReached_AutoTransitionsIntoTimeoutHandler()
    {
        var timeProvider = new FakeTimeProvider();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), timeProvider: timeProvider);

        await harness.RunStep(CounterWorkflow.Steps.WaitForApproval);
        timeProvider.Advance(TimeSpan.FromMinutes(10));

        var fired = await harness.RunPauseTimeoutIfDue();

        Assert.NotNull(fired);
        Assert.True(harness.State.AutoCancelled);
        Assert.IsType<Transition.TerminalTransition>(fired!.Transition);
    }

    [Fact]
    public async Task RunPauseTimeoutIfDue_NotPaused_ReturnsNull()
    {
        var timeProvider = new FakeTimeProvider();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), timeProvider: timeProvider);

        timeProvider.Advance(TimeSpan.FromDays(1));

        Assert.Null(await harness.RunPauseTimeoutIfDue());
    }

    [Fact]
    public async Task RunUntilStop_StepThrows_RetriesExhausted_FollowsChainIntoFailoverStep()
    {
        var settings = WorkflowSettings.Create()
            .StepRecovery(CounterWorkflow.Steps.Boom, RecoverStrategy.WithMaxRetries(0).FailoverTo(CounterWorkflow.Steps.Increment))
            .Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings))
        {
            State = new CounterState(9),
        };

        // Boom fails over straight to Increment, and RunUntilStop keeps following the chain from there.
        var final = await harness.RunUntilStop(CounterWorkflow.Steps.Boom);

        Assert.Equal(20, harness.State.Value);
        Assert.IsType<Transition.TerminalTransition>(final.Transition);
    }

    [Fact]
    public async Task RunWorkflowTimeoutIfDue_DeadlineNotYetReached_ReturnsNull()
    {
        var timeProvider = new FakeTimeProvider();
        var settings = WorkflowSettings.Create().Timeout(TimeSpan.FromMinutes(30)).Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings), timeProvider: timeProvider);

        harness.RunCommand(new Begin(1)); // sticky WorkflowDeadline arms here, same as production
        timeProvider.Advance(TimeSpan.FromMinutes(10));

        Assert.Null(await harness.RunWorkflowTimeoutIfDue());
    }

    [Fact]
    public async Task RunWorkflowTimeoutIfDue_DeadlineReached_NoStrategy_EndsWorkflow()
    {
        var timeProvider = new FakeTimeProvider();
        var settings = WorkflowSettings.Create().Timeout(TimeSpan.FromMinutes(30)).Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings), timeProvider: timeProvider);

        harness.RunCommand(new Begin(1));
        timeProvider.Advance(TimeSpan.FromMinutes(30));

        var fired = await harness.RunWorkflowTimeoutIfDue();

        Assert.NotNull(fired);
        Assert.IsType<WorkflowOutcome.TimedOut>(
            Assert.IsType<Transition.TerminalTransition>(fired!.Transition).Outcome);
    }

    [Fact]
    public async Task RunWorkflowTimeoutIfDue_DeadlineReached_WithStrategy_FollowsFailoverChain()
    {
        var timeProvider = new FakeTimeProvider();
        var settings = WorkflowSettings.Create()
            .Timeout(TimeSpan.FromMinutes(30), CounterWorkflow.Steps.AutoCancel)
            .Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings), timeProvider: timeProvider);

        harness.RunCommand(new Begin(1));
        timeProvider.Advance(TimeSpan.FromMinutes(30));

        var fired = await harness.RunWorkflowTimeoutIfDue();

        Assert.NotNull(fired);
        Assert.True(harness.State.AutoCancelled);
        Assert.IsType<Transition.TerminalTransition>(fired!.Transition);
    }

    [Fact]
    public async Task RunWorkflowTimeoutIfDue_DoesNotFireWhilePaused()
    {
        var timeProvider = new FakeTimeProvider();
        var settings = WorkflowSettings.Create().Timeout(TimeSpan.FromMinutes(30)).Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings), timeProvider: timeProvider);

        // WaitForApproval's own transition is what arms the sticky WorkflowDeadline here — same
        // deadline math applies to a Paused workflow, it's only the *firing* that's suppressed.
        await harness.RunStep(CounterWorkflow.Steps.WaitForApproval);
        timeProvider.Advance(TimeSpan.FromMinutes(30));

        Assert.Null(await harness.RunWorkflowTimeoutIfDue());
    }

    [Fact]
    public async Task RunWorkflowTimeoutIfDue_NoWorkflowTimeoutConfigured_ReturnsNull()
    {
        var timeProvider = new FakeTimeProvider();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), timeProvider: timeProvider);

        harness.RunCommand(new Begin(1));
        timeProvider.Advance(TimeSpan.FromDays(1));

        Assert.Null(await harness.RunWorkflowTimeoutIfDue());
    }

    [Fact]
    public async Task AdvanceTime_FiresDueTimeoutAutomatically()
    {
        var timeProvider = new FakeTimeProvider();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), timeProvider: timeProvider);

        await harness.RunStep(CounterWorkflow.Steps.WaitForApproval);
        var fired = await harness.AdvanceTime(TimeSpan.FromMinutes(10)); // PauseSettings.Timeout is 10 minutes

        Assert.NotNull(fired);
        Assert.True(harness.State.AutoCancelled);
    }

    [Fact]
    public async Task AdvanceTime_WithoutAdvanceableTimeProvider_Throws()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.AdvanceTime(TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task RunQuery_DispatchesToQueryHandler_AndReturnsReply()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), new CounterState(42));

        var value = await harness.RunQuery<Peek, int>(new Peek());

        Assert.Equal(42, value);
    }

    [Fact]
    public async Task RunQuery_DoesNotAdvanceState()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), new CounterState(42));

        await harness.RunQuery<Peek, int>(new Peek());

        Assert.Equal(42, harness.State.Value);
        Assert.Equal(WorkflowStatus.Running, harness.Status);
    }

    [Fact]
    public async Task RunQuery_UnregisteredQueryType_Throws()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunQuery<Ping, string>(new Ping()));
    }

    /// <summary>
    /// The defect this whole handler-context change exists to close: a step reading its state on
    /// both sides of an await, with a handler dispatched into the gap between them, must see one
    /// state. State reaches the step as a value on <c>StepContext</c>, so there is nothing shared for
    /// the interleaved query to overwrite.
    /// </summary>
    [Fact]
    public async Task RunStepInterleaved_QueryDispatchedMidStep_LeavesTheStepsStateUntouched()
    {
        var workflow = new CounterWorkflow();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(workflow, new CounterState(7));

        var queryReply = 0;
        await harness.RunStepInterleaved(CounterWorkflow.Steps.Slow, async () =>
        {
            queryReply = await harness.RunQuery<Peek, int>(new Peek());
            workflow.Gate.SetResult();
        });

        Assert.Equal(7, workflow.SeenBeforeAwait);
        Assert.Equal(7, workflow.SeenAfterAwait);
        Assert.Equal(7, queryReply);
        Assert.Equal(107, harness.State.Value);
    }

    /// <summary>A query dispatched while a step runs reads the state as it stands at dispatch time,
    /// and is not retroactively affected by the step's own effect landing afterwards.</summary>
    [Fact]
    public async Task RunStepInterleaved_QuerySeesPreStepState_StepEffectAppliesAfterwards()
    {
        var workflow = new CounterWorkflow();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(workflow, new CounterState(1));

        var duringStep = 0;
        await harness.RunStepInterleaved(CounterWorkflow.Steps.Slow, async () =>
        {
            duringStep = await harness.RunQuery<Peek, int>(new Peek());
            workflow.Gate.SetResult();
        });

        var afterStep = await harness.RunQuery<Peek, int>(new Peek());

        Assert.Equal(1, duringStep);
        Assert.Equal(101, afterStep);
    }

    // ── control plane, with no ActorSystem ───────────────────────────────────────────────────────

    [Fact]
    public void Suspend_FromRunning_HoldsTheWorkflow()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());
        harness.RunCommand(new Begin(5));

        harness.Suspend();

        Assert.Equal(WorkflowStatus.Suspended, harness.Status);
    }

    [Fact]
    public void Suspend_FromANonRunningStatus_IsRejected()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());
        harness.RunCommand(new Begin(5));
        harness.Suspend();

        Assert.Throws<WorkflowCommandException>(() => harness.Suspend());
    }

    /// <summary>Guarantee E4: resume restarts the step from the beginning.</summary>
    [Fact]
    public async Task Resume_RestartsTheHeldStepFresh()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(), new CounterState(1));
        harness.RunCommand(new Begin(5));
        harness.Suspend();

        var effect = await harness.Resume();

        Assert.Equal(WorkflowStatus.Finished, harness.Status);
        Assert.NotNull(effect);
        Assert.Equal(0, harness.Envelope.RetryCount);
    }

    [Fact]
    public async Task Resume_WithoutHavingBeenSuspended_IsRejected()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());

        await Assert.ThrowsAsync<WorkflowCommandException>(() => harness.Resume());
    }

    [Fact]
    public void Terminate_FinishesAsTerminated()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());
        harness.RunCommand(new Begin(5));

        harness.Terminate("operator stopped it");

        Assert.Equal(WorkflowStatus.Finished, harness.Status);
        var terminated = Assert.IsType<WorkflowOutcome.Terminated>(harness.Outcome);
        Assert.Equal("operator stopped it", terminated.Reason);
    }

    /// <summary>With no cancellation step configured there is nothing to unwind, so the run finishes
    /// straight away — still as cancelled, never as terminated.</summary>
    [Fact]
    public async Task Cancel_WithNoCancellationStep_FinishesAsCancelled()
    {
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow());
        harness.RunCommand(new Begin(5));

        await harness.Cancel("customer changed their mind");

        var cancelled = Assert.IsType<WorkflowOutcome.Cancelled>(harness.Outcome);
        Assert.Equal("customer changed their mind", cancelled.Reason);
    }

    /// <summary>With one configured, cancellation unwinds through it — and that step decides the
    /// final outcome.</summary>
    [Fact]
    public async Task Cancel_WithACancellationStep_UnwindsThroughIt()
    {
        var settings = WorkflowSettings.Create().CancelVia(Ref.Step<DocWorkflowFor<string>>("AutoCancel")).Build();
        var harness = new WorkflowTestHarness<CounterWorkflow, CounterState>(new CounterWorkflow(settings));
        harness.RunCommand(new Begin(5));

        await harness.Cancel("no longer needed");

        Assert.True(harness.State.AutoCancelled);
        Assert.Equal(WorkflowStatus.Finished, harness.Status);
    }
}
