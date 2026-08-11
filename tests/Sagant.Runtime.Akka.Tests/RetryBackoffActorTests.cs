using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Akka.Actor;
using Akka.TestKit;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The engine-level wiring for <see cref="RecoverStrategy.BackoffForAttempt"/> — does the actor
/// actually wait, and does a crash/rebalance mid-wait resume the *remaining* delay rather than
/// losing it or retrying immediately. This is the part <see cref="RetryBackoffTests"/> can't cover
/// (that file is pure functions, no Akka) — same split as engine timeouts already have between
/// this style of test and plain unit tests. Uses <see cref="TestScheduler"/> so waiting is
/// deterministic, never a real delay.
/// </summary>
public class RetryBackoffActorTests : WorkflowActorTestKit
{
    public RetryBackoffActorTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.scheduler.implementation = "Akka.TestKit.TestScheduler, Akka.TestKit"
        akka.loglevel = OFF
        """;

    private TestScheduler Scheduler => (TestScheduler)Sys.Scheduler;

    /// <summary>Sent by the "FlakyStep" delegate in every test below, every time it runs — the
    /// observability channel for "did the step get invoked (again)", instead of a shared counter
    /// the test would otherwise have to read across threads. Delivered through <c>TestActor</c>'s
    /// own mailbox, so <c>ExpectMsg</c>/<c>ExpectNoMsg</c> below get proper cross-thread visibility
    /// for free via Akka's own message-passing guarantees — no <c>Interlocked</c>/<c>Volatile</c>
    /// needed anywhere in this file.</summary>
    private sealed record StepAttempt(int Number);

    /// <summary>
    /// Blocks until the actor has actually *scheduled* the backoff retry (persisted
    /// <c>RetryDelayUntil</c>), not just until the step has run. <c>HandleStepFailed</c> — which
    /// reads "now" to compute the backoff deadline — runs on a *later*, separate actor mailbox turn
    /// than the step itself (the failure reaches the actor via <c>PipeTo</c>, not inline). Calling
    /// <see cref="TestScheduler.Advance"/> before that later turn has actually run races it:
    /// <c>HandleStepFailed</c> would read an already-advanced "now", silently inflating the deadline
    /// by however much virtual time the test advanced in the gap — reproduced and confirmed via
    /// reflection into <see cref="TestScheduler"/>'s internal queue (the scheduled fire time was
    /// consistently later than the persisted <c>RetryDelayUntil</c> by exactly however much the test
    /// had advanced in that window). Only manifests under scheduling jitter (parallel test load
    /// widens the gap enough to lose the race); the fix is closing the gap, not giving it more
    /// timeout budget to hide in.
    /// </summary>
    private Diagnostics<TestState> AwaitRetryScheduled(IActorRef actor)
    {
        Diagnostics<TestState> diagnostics = null!;
        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.NotNull(diagnostics.Envelope.RetryDelayUntil);
        }, TimeSpan.FromSeconds(5));
        return diagnostics;
    }

    [Fact]
    public void Backoff_DelaysRetry_DoesNotRetryBeforeTheDelayElapses()
    {
        // Plain int, not Interlocked: mutated only inside the step delegate, which the actor model
        // guarantees runs one turn at a time on its own thread — never concurrently with itself.
        // The test never reads this directly; it observes StepAttempt messages instead (see above).
        var attempts = 0;
        var script = Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempts++;
                TestActor.Tell(new StepAttempt(attempts));
                return attempts == 1
                    ? throw new InvalidOperationException("first attempt fails")
                    : Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "recovered" }).ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("FlakyStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("Compensate")).WithBackoff(RetryBackoff.Fixed(TimeSpan.FromMinutes(10))))
            .Build();

        var actor = CreateActor(nameof(Backoff_DelaysRetry_DoesNotRetryBeforeTheDelayElapses), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);

        // The first StepAttempt arrives before the command's own "accepted" reply — StartStep runs
        // as a side effect of applying the transition, which happens before the reply is sent (see
        // PersistEnvelopeThen: ApplyTransitionSideEffects before afterPersist).
        Assert.Equal(1, ExpectMsg<StepAttempt>(TimeSpan.FromSeconds(5)).Number);
        ExpectMsg<string>();
        AwaitRetryScheduled(actor);

        // Well short of the 10-minute backoff — the retry must not have happened yet.
        Scheduler.Advance(TimeSpan.FromMinutes(2));
        ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Crosses the 10-minute mark — now it retries and succeeds.
        Scheduler.Advance(TimeSpan.FromMinutes(9));
        Assert.Equal(2, ExpectMsg<StepAttempt>(TimeSpan.FromSeconds(5)).Number);

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("recovered", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Backoff_ActorRestartsMidWait_ResumesRemainingDelay_DoesNotRetryImmediately()
    {
        // See the StepAttempt/attempts comment on the first test in this file.
        var attempts = 0;
        var script = Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempts++;
                TestActor.Tell(new StepAttempt(attempts));
                return attempts == 1
                    ? throw new InvalidOperationException("first attempt fails")
                    : Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "recovered" }).ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("FlakyStep")).ThenReply("accepted"));
        var settings = WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("Compensate")).WithBackoff(RetryBackoff.Fixed(TimeSpan.FromMinutes(10))))
            .Build();

        var persistenceId = nameof(Backoff_ActorRestartsMidWait_ResumesRemainingDelay_DoesNotRetryImmediately);
        var actor = CreateActor(persistenceId, script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        Assert.Equal(1, ExpectMsg<StepAttempt>(TimeSpan.FromSeconds(5)).Number);
        ExpectMsg<string>();

        // Wait for RetryDelayUntil to actually be persisted — see AwaitRetryScheduled's doc comment.
        // Stopping the actor before that persist completes would race it the same way advancing the
        // scheduler would: the in-flight HandleStepFailed could still be holding a stale "now".
        AwaitRetryScheduled(actor);

        // Kill the actor mid-backoff-wait — same as a crash or ClusterSharding rebalance. Nothing
        // durable was consumed yet (the retry hasn't happened), only RetryDelayUntil was persisted.
        Watch(actor);
        Sys.Stop(actor);
        ExpectTerminated(actor);

        // A fresh instance, same persistenceId — replays from the in-mem journal/snapshot.
        var recovered = CreateActor(persistenceId, script, settings);

        // If this resumed instance retried immediately instead of resuming the remaining ~10-minute
        // wait, a second StepAttempt would already be sitting in the mailbox here. Give recovery a
        // moment to actually complete first — reaching Status.Running is safe to treat as
        // "OnRecoveryCompleted already ran and re-armed the live timer" (Akka.Persistence delivers
        // RecoveryCompleted before any other queued command, so a reply to GetDiagnostics can't
        // arrive before it has).
        AwaitAssert(() =>
        {
            recovered.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Running, diagnostics.Envelope.Status);
        }, TimeSpan.FromSeconds(5));
        ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Short of the remaining delay — still must not have retried.
        Scheduler.Advance(TimeSpan.FromMinutes(2));
        ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Crosses the (resumed) remaining delay — retries now, on the recovered instance.
        Scheduler.Advance(TimeSpan.FromMinutes(9));
        Assert.Equal(2, ExpectMsg<StepAttempt>(TimeSpan.FromSeconds(5)).Number);

        AwaitAssert(() =>
        {
            recovered.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("recovered", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void NoBackoffConfigured_RetriesImmediately_UnchangedFromBeforeThisFeature()
    {
        // See the StepAttempt/attempts comment on the first test in this file.
        var attempts = 0;
        var script = Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempts++;
                TestActor.Tell(new StepAttempt(attempts));
                return attempts == 1
                    ? throw new InvalidOperationException("first attempt fails")
                    : Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(new TestState { Value = "recovered" }).ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("FlakyStep")).ThenReply("accepted"));
        // No .WithBackoff(...) — BackoffForAttempt is null, so retry behavior stays whatever the
        // no-backoff path resolves to.
        var settings = WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("Compensate")))
            .Build();

        var actor = CreateActor(nameof(NoBackoffConfigured_RetriesImmediately_UnchangedFromBeforeThisFeature), script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);

        // No Scheduler.Advance at all — an immediate retry must already have happened. The first
        // StepAttempt arrives before the command's own "accepted" reply — StartStep runs as a side
        // effect of applying the transition, which happens before the reply is sent (see
        // PersistEnvelopeThen: ApplyTransitionSideEffects before afterPersist).
        Assert.Equal(1, ExpectMsg<StepAttempt>(TimeSpan.FromSeconds(5)).Number);
        ExpectMsg<string>();
        Assert.Equal(2, ExpectMsg<StepAttempt>(TimeSpan.FromSeconds(5)).Number);

        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Finished, diagnostics.Envelope.Status);
            Assert.Equal("recovered", diagnostics.Envelope.UserState.Value);
        }, TimeSpan.FromSeconds(5));
    }
}
