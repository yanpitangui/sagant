using Sagant.Execution;
using Sagant.Runtime.Akka.ChaosTests.Support;

namespace Sagant.Runtime.Akka.ChaosTests;

/// <summary>
/// A node leaving politely while one of its entities is mid-step.
///
/// This is the ordinary case: a rolling deploy drains nodes, so this path runs on every release,
/// where a crash is occasional. The claim is that <c>GracefulShutdown</c> lets an in-flight step
/// finish and persist before the entity hands off — a step is at-least-once (<c>R1</c>), so
/// abandoning one is survivable, and paying for it on every deploy is a different matter.
///
/// The step counts its own runs, which turns "did it finish before handoff" into something the
/// recorded state answers directly.
/// </summary>
[Collection(PostgresJournalCollection.Name)]
public class GracefulHandoffTests
{
    private readonly PostgresJournalFixture _postgres;

    public GracefulHandoffTests(PostgresJournalFixture postgres) => _postgres = postgres;

    /// <summary>
    /// The step is still running when its node is asked to leave. Once a replacement node picks the
    /// instance up, the step's own effect is on record, so it completed where it started.
    /// </summary>
    [Fact]
    public async Task ANodeLeavingWhileAStepRuns_LetsThatStepFinish()
    {
        const string entityId = "graceful-handoff-1";

        var node = await ChaosNode.StartAsync(
            "graceful-handoff", _postgres.ConnectionString, cycles: 1,
            slowStepDuration: TimeSpan.FromSeconds(3));

        var accepted = await node.Client.For<SlowStepWorkflow>(entityId)
            .Request<BeginSlowWork, string>(new BeginSlowWork(3000), TimeSpan.FromSeconds(30));
        Assert.Equal("accepted", accepted);

        // Well inside the step's own duration, so the shutdown lands while it is genuinely running.
        await Task.Delay(TimeSpan.FromSeconds(1));

        // The polite path: CoordinatedShutdown, cluster leave, shard handoff — what a rolling deploy
        // does, as opposed to the abrupt termination the other chaos tests use.
        await node.StopAsync();

        await using var replacement = await ChaosNode.StartAsync(
            "graceful-handoff", _postgres.ConnectionString, cycles: 1,
            slowStepDuration: TimeSpan.FromSeconds(3));

        var events = await Eventually(
            () => ReadEvents(replacement, entityId),
            read => read.Any(e => e is WorkflowEvent.RunPaused),
            TimeSpan.FromSeconds(60));

        // The step's effect is a pause, so seeing one means the step ran to completion and its
        // transition was persisted before the node went away.
        Assert.Contains(events, e => e is WorkflowEvent.RunPaused);

        // And it ran once: a step abandoned at handoff would be re-run on the node that took over,
        // leaving two successes on record for the same attempt.
        var successes = events
            .OfType<WorkflowEvent.CausedEvent>()
            .Count(e => e.Cause is TransitionCause.StepSucceeded { StepName: nameof(SlowStepWorkflow.SlowStep) });

        Assert.True(
            successes == 1,
            $"the step succeeded {successes} times; a graceful handoff should let the in-flight attempt "
            + "finish where it started");
    }

    private static async Task<List<WorkflowEvent>> ReadEvents(ChaosNode node, string entityId)
    {
        var events = new List<WorkflowEvent>();
        await foreach (var item in node.Feed.ReadEntity(entityId))
        {
            events.Add(item.Event);
        }

        return events;
    }

    private static async Task<T> Eventually<T>(Func<Task<T>> read, Func<T, bool> until, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        var last = await read();
        while (DateTimeOffset.UtcNow < deadline && !until(last))
        {
            await Task.Delay(500);
            last = await read();
        }

        return last;
    }
}
