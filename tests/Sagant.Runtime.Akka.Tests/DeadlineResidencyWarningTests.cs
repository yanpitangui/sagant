using Akka.Event;
using Sagant.Effects;
using Sagant.Runtime.Akka.Tests.Support;
using Sagant.Settings;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The warning an instance gives when it is about to wait longer than it will stay resident and
/// nothing is watching its deadline for it.
///
/// Both conditions have to hold for lateness to be possible, and both are settled by the moment of
/// arming, which is why the check sits there — that also sidesteps the order a host's builder calls
/// happen in. What it costs to get wrong is silence: a deployment accepting lateness would have
/// nothing telling it so.
/// </summary>
public class DeadlineResidencyWarningTests : WorkflowActorTestKit
{
    public DeadlineResidencyWarningTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = WARNING
        """;

    /// <summary>Pauses for longer than any residency a test configures.</summary>
    private WorkflowScript PausingScript(TimeSpan pauseFor) =>
        Script()
            .Step("Wait", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().ThenPause(
                    PauseSettings.WithTimeout(pauseFor).TimeoutHandler(Step("OnTimeout")))))
            .Step("OnTimeout", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("Wait")).ThenReply("accepted"));

    [Fact]
    public void AnInstanceWaitingPastItsResidency_SaysSoWhenNothingWatchesItsDeadline()
    {
        // The keep-alive interval is half the passivation window, so this stands for a two-second one.
        var actor = CreateActor(
            nameof(AnInstanceWaitingPastItsResidency_SaysSoWhenNothingWatchesItsDeadline),
            PausingScript(TimeSpan.FromHours(4)),
            keepAliveInterval: TimeSpan.FromSeconds(1));

        EventFilter.Warning(contains: "deadline scheduler").ExpectOne(() =>
        {
            actor.Tell(new StartWorkflow(1), TestActor);
            ExpectMsg<string>();
        });
    }

    /// <summary>
    /// A deadline that lands inside the window is fired by the instance itself, so there is nothing to
    /// warn about — the common case, and the one that has to stay quiet.
    /// </summary>
    [Fact]
    public void AnInstanceWaitingInsideItsResidency_SaysNothing()
    {
        var actor = CreateActor(
            nameof(AnInstanceWaitingInsideItsResidency_SaysNothing),
            PausingScript(TimeSpan.FromMilliseconds(200)),
            keepAliveInterval: TimeSpan.FromMinutes(30));

        EventFilter.Warning(contains: "deadline scheduler").Expect(0, () =>
        {
            actor.Tell(new StartWorkflow(1), TestActor);
            ExpectMsg<string>();
            Thread.Sleep(300);
        });
    }

    /// <summary>
    /// Passivation off means the instance holds its own timer however long the wait, so there is
    /// nothing to warn about there either.
    /// </summary>
    [Fact]
    public void WithPassivationOff_AnInstanceSaysNothingHoweverLongItWaits()
    {
        var actor = CreateActor(
            nameof(WithPassivationOff_AnInstanceSaysNothingHoweverLongItWaits),
            PausingScript(TimeSpan.FromDays(30)));

        EventFilter.Warning(contains: "deadline scheduler").Expect(0, () =>
        {
            actor.Tell(new StartWorkflow(1), TestActor);
            ExpectMsg<string>();
            Thread.Sleep(300);
        });
    }
}
