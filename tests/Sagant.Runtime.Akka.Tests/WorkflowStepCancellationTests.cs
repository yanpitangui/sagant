using Sagant.Protocol;
using Sagant.Descriptors;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Runtime.Akka;
using Akka.Actor;
using Akka.TestKit;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Verifies the <see cref="CancellationToken"/> <see cref="StepDescriptor{TState}.Invoke"/> passes
/// to a step actually gets cancelled at the points that stop waiting on the step: a step timeout,
/// <see cref="Suspend"/>, <see cref="Terminate"/>, and a <see cref="GracefulShutdown"/> grace
/// window expiring. A step that never declares a <see cref="CancellationToken"/> parameter (the
/// common case, covered by every other test in this suite) never observes any of this — these
/// specifically exercise the opt-in path via <see cref="WorkflowActorTestKit.CreateActor"/>'s
/// <c>ctSteps</c> parameter.
/// </summary>
public class WorkflowStepCancellationTests : WorkflowActorTestKit
{
    public WorkflowStepCancellationTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.scheduler.implementation = "Akka.TestKit.TestScheduler, Akka.TestKit"
        akka.loglevel = OFF
        """;

    private TestScheduler Scheduler => (TestScheduler)Sys.Scheduler;

    private static (Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>> Handler, TaskCompletionSource<bool> Cancelled)
        HangingCtAwareStep()
    {
        var cancelled = new TaskCompletionSource<bool>();
        Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>> handler = (_, _, ct) =>
            {
                // Never completes on its own, but — unlike a step that ignores the token — actually
                // resolves once cancelled, same as a real HttpClient/EF call would. Leaving this
                // task permanently unresolved (as an earlier version of this helper did) orphans it
                // for the rest of the test process, which was observed to interfere with unrelated
                // later tests sharing the thread pool.
                var stepTask = new TaskCompletionSource<StepEffect<TestState>>();
                ct.Register(() =>
                {
                    cancelled.TrySetResult(true);
                    stepTask.TrySetCanceled(ct);
                });
                return stepTask.Task;
            };
        return (handler, cancelled);
    }

    [Fact]
    public async Task StepTimeout_CancelsTheStepsToken()
    {
        var (handler, cancelled) = HangingCtAwareStep();
        var settings = WorkflowSettings.Create().DefaultStepTimeout(TimeSpan.FromSeconds(5)).Build();

        var actor = CreateActor(nameof(StepTimeout_CancelsTheStepsToken), Script()
            .CancellableStep("CtStep", handler)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("CtStep")).ThenReply("accepted")), settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        Scheduler.Advance(TimeSpan.FromSeconds(6));

        var completed = await Task.WhenAny(cancelled.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(cancelled.Task, completed);
    }

    [Fact]
    public async Task Suspend_CancelsTheStepsToken()
    {
        var (handler, cancelled) = HangingCtAwareStep();

        var actor = CreateActor(nameof(Suspend_CancelsTheStepsToken), Script()
            .CancellableStep("CtStep", handler)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("CtStep")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        var completed = await Task.WhenAny(cancelled.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(cancelled.Task, completed);
    }

    [Fact]
    public async Task Terminate_CancelsTheStepsToken()
    {
        var (handler, cancelled) = HangingCtAwareStep();

        var actor = CreateActor(nameof(Terminate_CancelsTheStepsToken), Script()
            .CancellableStep("CtStep", handler)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("CtStep")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Terminate(), TestActor);
        ExpectMsg<Sagant.Protocol.Done>();

        var completed = await Task.WhenAny(cancelled.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(cancelled.Task, completed);
    }

    [Fact]
    public async Task GracefulShutdown_GraceExpiry_CancelsTheStepsTokenAndStops()
    {
        var (handler, cancelled) = HangingCtAwareStep();
        var actor = CreateActor(nameof(GracefulShutdown_GraceExpiry_CancelsTheStepsTokenAndStops), Script()
            .CancellableStep("CtStep", handler)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("CtStep")).ThenReply("accepted")),
            gracefulShutdownGrace: TimeSpan.FromSeconds(5));
        Watch(actor);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new GracefulShutdown(), TestActor);
        // Force a mailbox round-trip before advancing virtual time: Tell is fire-and-forget, so
        // without this, Advance can race ahead of the actor actually processing GracefulShutdown
        // and arming the grace timer — the timer would then get scheduled for a point in virtual
        // time already in the past relative to the (already-advanced) clock, and never fire.
        actor.Tell(new GetStatus(), TestActor);
        ExpectMsg<WorkflowStatusReply>();

        Scheduler.Advance(TimeSpan.FromSeconds(6));

        ExpectTerminated(actor);
        var completed = await Task.WhenAny(cancelled.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(cancelled.Task, completed);
    }

    /// <summary>The grace window is derived from <c>akka.cluster.sharding.handoff-timeout</c>
    /// (falls back to Akka's own 60s default when Sharding isn't loaded, as here) and can only be
    /// shortened by a caller-supplied value, never lengthened past that ceiling — see
    /// <see cref="WorkflowEntityActor{TWorkflow, TState}"/>'s constructor doc comment. An
    /// oversized request (999s) should still self-stop around the ~50s ceiling (60s default minus
    /// 10s headroom), not wait anywhere near 999s.</summary>
    [Fact]
    public void GracefulShutdown_RequestedGraceLargerThanHandoffCeiling_IsClampedToTheCeiling()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var actor = CreateActor(nameof(GracefulShutdown_RequestedGraceLargerThanHandoffCeiling_IsClampedToTheCeiling), Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted")),
            gracefulShutdownGrace: TimeSpan.FromSeconds(999));
        Watch(actor);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new GracefulShutdown(), TestActor);
        // Force a mailbox round-trip before advancing virtual time — see the comment on the same
        // pattern in GracefulShutdown_GraceExpiry_CancelsTheStepsTokenAndStops above.
        actor.Tell(new GetStatus(), TestActor);
        ExpectMsg<WorkflowStatusReply>();

        // Past the ~50s ceiling (60s default handoff-timeout minus 10s headroom) — must have
        // stopped, nowhere close to the requested 999s.
        Scheduler.Advance(TimeSpan.FromSeconds(55));
        ExpectTerminated(actor);
    }
}
