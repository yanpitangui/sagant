using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Verifies the <see cref="WorkflowFeedItem"/>s a subscriber (a live dashboard, a log line per step,
/// whatever) can use to watch a workflow run without touching its persistence.
///
/// Each item carries one durably-written <see cref="WorkflowEvent"/>, and each event names what drove
/// it through <see cref="TransitionCause"/>. So a subscriber reads "OnlyStep started, because the
/// StartWorkflow command said so" from a single message.
/// </summary>
public class WorkflowEventPublishingTests : WorkflowActorTestKit
{
    public WorkflowEventPublishingTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    /// <summary>The next published event of type <typeparamref name="TEvent"/>, skipping command
    /// replies and any other traffic sharing the test actor's mailbox.</summary>
    private WorkflowFeedItem NextEvent<TEvent>(Predicate<TEvent>? where = null) where TEvent : WorkflowEvent =>
        FishForMessage<WorkflowFeedItem>(item =>
            item.Event is TEvent e && (where is null || where(e)));

    [Fact]
    public void ExternalCommand_PublishesTheEventItCaused_NamingTheCommand()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var script = Script()
            .Step("OnlyStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("OnlyStep")).ThenReply("accepted"));

        var persistenceId = nameof(ExternalCommand_PublishesTheEventItCaused_NamingTheCommand);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);

        // One event, carrying both halves: the step that started, and the command that started it.
        // A subscriber needs no separate signal to tell an engine-driven transition from a
        // caller-driven one.
        var item = NextEvent<WorkflowEvent.StepStarted>();
        Assert.Equal(persistenceId, item.EntityId);
        var started = Assert.IsType<WorkflowEvent.StepStarted>(item.Event);
        Assert.Equal("OnlyStep", started.StepName);
        var command = Assert.IsType<TransitionCause.Command>(started.Cause);
        Assert.Equal(nameof(StartWorkflow), command.CommandType);
    }

    [Fact]
    public void HappyPathStep_PublishesItsStart_ThenTheRunFinishingOnItsSuccess()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var script = Script()
            .Step("OnlyStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("OnlyStep")).ThenReply("accepted"));

        var persistenceId = nameof(HappyPathStep_PublishesItsStart_ThenTheRunFinishingOnItsSuccess);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);

        NextEvent<WorkflowEvent.StepStarted>(e => e.StepName == "OnlyStep");

        // The step's success shows up as the cause of whatever it transitioned into — here, the run
        // ending. Duration rides that cause, measured inside StepDescriptor.Invoke.
        var item = NextEvent<WorkflowEvent.RunFinished>();
        var finished = Assert.IsType<WorkflowEvent.RunFinished>(item.Event);
        Assert.IsType<WorkflowOutcome.Completed>(finished.Outcome);
        var succeeded = Assert.IsType<TransitionCause.StepSucceeded>(finished.Cause);
        Assert.Equal("OnlyStep", succeeded.StepName);
        Assert.Equal(1, succeeded.Attempt);
    }

    [Fact]
    public void FailingStep_PublishesTheFailureWithItsRetryFlag_ThenSucceedsOnRetry()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var attempt = 0;
        var script = Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempt++;
                return attempt == 1
                    ? Task.FromException<StepEffect<TestState>>(new InvalidOperationException("boom"))
                    : Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("FlakyStep")).ThenReply("accepted"));

        var settings = WorkflowSettings.Create()
            .StepRecovery(Step("FlakyStep"), RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("FlakyStep")))
            .Build();

        var persistenceId = nameof(FailingStep_PublishesTheFailureWithItsRetryFlag_ThenSucceedsOnRetry);
        var actor = CreateActor(persistenceId, script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);

        NextEvent<WorkflowEvent.StepStarted>(e => e.StepName == "FlakyStep");

        // The retry that follows names the attempt that failed. This is the only place a
        // retried-then-succeeded error survives — WorkflowOutcome.Failed carries the terminal
        // failure alone, and this run goes on to succeed.
        var item = NextEvent<WorkflowEvent.StepRetryScheduled>();
        var failed = Assert.IsType<TransitionCause.StepFailed>(
            Assert.IsType<WorkflowEvent.StepRetryScheduled>(item.Event).Cause);
        Assert.Equal("FlakyStep", failed.StepName);
        Assert.Equal(1, failed.Attempt);
        Assert.True(failed.WillRetry);
        Assert.Contains("boom", failed.Error);

        var finished = NextEvent<WorkflowEvent.RunFinished>();
        var succeeded = Assert.IsType<TransitionCause.StepSucceeded>(
            Assert.IsType<WorkflowEvent.RunFinished>(finished.Event).Cause);
        Assert.Equal(2, succeeded.Attempt);
    }

    [Fact]
    public void Pause_PublishesRunPausedCarryingItsReason()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var script = Script()
            .Step("PausingStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause(
                PauseSettings.WithTimeout(TimeSpan.FromMinutes(30)).WithReason("needs approval").TimeoutHandler(Step("PausingStep")))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("PausingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Pause_PublishesRunPausedCarryingItsReason), script);
        actor.Tell(new StartWorkflow(1), TestActor);

        var item = NextEvent<WorkflowEvent.RunPaused>();
        var paused = Assert.IsType<WorkflowEvent.RunPaused>(item.Event);
        Assert.Equal("needs approval", paused.Reason);
        // Reaching a pause is the pausing step succeeding, so the cause names that step.
        Assert.IsType<TransitionCause.StepSucceeded>(paused.Cause);
    }

    private sealed record ApproveCommand;

    /// <summary>The common real-world resume path — an ordinary <c>[WorkflowCommandHandler]</c> (an
    /// approval, here) transitioning a paused workflow straight to its next step. The event that
    /// leaves the pause is the next step starting, and its cause names the approval, so one message
    /// says both that the workflow moved on and who moved it.</summary>
    [Fact]
    public void ResumingFromPauseViaBusinessCommand_PublishesTheNextStepNamingTheApproval()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var script = Script()
            .Step("PausingStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause(
                PauseSettings.WithTimeout(TimeSpan.FromMinutes(30)).WithReason("needs approval").TimeoutHandler(Step("PausingStep")))))
            .Step("NextStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("PausingStep")).ThenReply("accepted"))
            .Command<ApproveCommand>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("NextStep")).ThenReply("approved"));

        var actor = CreateActor(nameof(ResumingFromPauseViaBusinessCommand_PublishesTheNextStepNamingTheApproval), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        NextEvent<WorkflowEvent.RunPaused>();

        actor.Tell(new ApproveCommand(), TestActor);

        var item = NextEvent<WorkflowEvent.StepStarted>(e => e.StepName == "NextStep");
        var command = Assert.IsType<TransitionCause.Command>(
            Assert.IsType<WorkflowEvent.StepStarted>(item.Event).Cause);
        Assert.Equal(nameof(ApproveCommand), command.CommandType);

        NextEvent<WorkflowEvent.RunFinished>();
    }

    /// <summary>Engine-level Suspend/Resume (an operator holding an instance), distinct from the
    /// business-level pause above. It goes through <c>HandleResume</c>, so its cause is a
    /// <see cref="TransitionCause.Control"/> naming the operator action.</summary>
    [Fact]
    public void Suspend_ThenResume_PublishesRunResumedNamingTheControlAction()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(WorkflowFeedItem));

        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("HangingStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("HangingStep")).ThenReply("accepted"));

        var actor = CreateActor(nameof(Suspend_ThenResume_PublishesRunResumedNamingTheControlAction), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        NextEvent<WorkflowEvent.StepStarted>(e => e.StepName == "HangingStep");

        actor.Tell(new Suspend(), TestActor);
        var suspended = NextEvent<WorkflowEvent.RunSuspended>();
        Assert.Equal(
            "Suspend",
            Assert.IsType<TransitionCause.Control>(
                Assert.IsType<WorkflowEvent.RunSuspended>(suspended.Event).Cause).Kind);

        actor.Tell(new Resume(), TestActor);
        var resumed = NextEvent<WorkflowEvent.RunResumed>();
        Assert.Equal(
            "Resume",
            Assert.IsType<TransitionCause.Control>(
                Assert.IsType<WorkflowEvent.RunResumed>(resumed.Event).Cause).Kind);
    }
}
