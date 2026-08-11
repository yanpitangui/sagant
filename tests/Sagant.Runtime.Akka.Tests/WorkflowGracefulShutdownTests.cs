using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Effects;
using Sagant.Runtime.Akka;
using Akka.Actor;
using Akka.TestKit;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Verifies <see cref="GracefulShutdown"/> — ClusterSharding's default hand-off-stop message for
/// workflow entities (see <see cref="WorkflowClusterShardingExtensionsTests"/> for the wiring
/// itself; these tests drive the actor directly).
/// </summary>
public class WorkflowGracefulShutdownTests : WorkflowActorTestKit
{
    public WorkflowGracefulShutdownTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.scheduler.implementation = "Akka.TestKit.TestScheduler, Akka.TestKit"
        akka.loglevel = OFF
        """;

    private TestScheduler Scheduler => (TestScheduler)Sys.Scheduler;

    [Fact]
    public void NoStepInFlight_StopsImmediately()
    {
        var actor = CreateActor(nameof(NoStepInFlight_StopsImmediately), Script());
        Watch(actor);

        actor.Tell(new GracefulShutdown(), TestActor);

        ExpectTerminated(actor);
    }

    [Fact]
    public void StepInFlight_LetsItFinishAndPersist_ThenStopsWithoutStartingNextStep()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var firstStepResult = new TaskCompletionSource<StepEffect<TestState>>();
        var actor = CreateActor(nameof(StepInFlight_LetsItFinishAndPersist_ThenStopsWithoutStartingNextStep), Script()
            .Step("First", (_, _) => firstStepResult.Task)
            .Step("Second", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("First")).ThenReply("accepted")));
        Watch(actor);
        actor.Tell(new StartWorkflow(1), TestActor);
        // One event covers both halves: "First" started, because the StartWorkflow command said so.
        FishForMessage<WorkflowFeedItem>(item =>
            item.Event is WorkflowEvent.StepStarted { StepName: "First", Cause: TransitionCause.Command });
        ExpectMsg<string>();

        actor.Tell(new GracefulShutdown(), TestActor);

        // "First" finishes only after the shutdown request arrived — its own completion still
        // gets to persist normally (real-world side effects it already caused aren't silently
        // orphaned), it just never starts "Second".
        firstStepResult.SetResult(new StepEffectsBuilder<TestState>().ThenTransitionTo(Step("Second")));

        FishForMessage<WorkflowFeedItem>(item =>
            item.Event is WorkflowEvent.CausedEvent { Cause: TransitionCause.StepSucceeded { StepName: "First" } });
        ExpectTerminated(actor);

        // "Second" never starts on this node — no further StepStarted notification arrives before
        // the actor terminates (ExpectTerminated above already proves no more messages were needed
        // to reach that point, but assert explicitly there's nothing about "Second" queued either).
        ExpectNoMsg(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void StepInFlight_GraceExpires_ForcesStopAnyway()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var actor = CreateActor(nameof(StepInFlight_GraceExpires_ForcesStopAnyway), Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted")),
            gracefulShutdownGrace: TimeSpan.FromSeconds(5));
        Watch(actor);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new GracefulShutdown(), TestActor);
        // Force a mailbox round-trip before advancing virtual time: Tell is fire-and-forget, so
        // without this, Advance can race ahead of the actor actually processing GracefulShutdown
        // and arming the grace timer.
        actor.Tell(new GetStatus(), TestActor);
        ExpectMsg<WorkflowStatus>();

        Scheduler.Advance(TimeSpan.FromSeconds(6));

        ExpectTerminated(actor);
    }
}
