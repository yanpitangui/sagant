using Sagant.Runtime.Akka.ChaosTests.Support;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.ChaosTests;

/// <summary>
/// A workflow that restarts, interrupted by a node dying.
///
/// This is the durability path with the least margin. A restart makes an instance's history
/// reclaimable while the instance keeps running, so a process disappearing partway through leaves
/// disk in a state nothing else produces: a fresh cycle recorded, its snapshot possibly written,
/// the history behind it possibly still there. Guarantee <c>E11</c> claims that costs disk and
/// nothing else — a crash replays the old events plus the restart and folds to the same envelope.
///
/// Asserted once everything settles, so what is checked is the durable record the cluster actually
/// kept, standing apart from whatever a probe caught mid-flight.
/// </summary>
[Collection(PostgresJournalCollection.Name)]
public class RestartUnderCrashTests
{
    private readonly PostgresJournalFixture _postgres;

    public RestartUnderCrashTests(PostgresJournalFixture postgres) => _postgres = postgres;

    /// <summary>
    /// A run cycles, its node is killed abruptly, and a replacement node picks the instance up from
    /// the shared journal. The cycle count is what makes recovery checkable: the instance either
    /// knows how far it got or it does not.
    /// </summary>
    [Fact]
    public async Task ANodeDyingMidCycle_LeavesTheInstanceRecoverableOnAnother()
    {
        const string entityId = "restart-crash-1";
        const int cycles = 12;

        var first = await ChaosNode.StartAsync("chaos-restart-a", _postgres.ConnectionString, cycles);
        int cycleBeforeCrash;
        try
        {
            // Request, whose reply confirms the command reached its handler, so a workflow that never
            // started is distinguishable from one whose history cannot be read.
            var accepted = await first.Client.For<RestartingWorkflow>(entityId)
                .Request<BeginCycling, string>(new BeginCycling(cycles), TimeSpan.FromSeconds(30));
            Assert.Equal("accepted", accepted);

            // Let it get somewhere into the loop. Where exactly is deliberately unpinned — the claim
            // under test holds at every point, so the test should not depend on reaching one.
            await Task.Delay(TimeSpan.FromSeconds(2));

            var before = await ReadState(first, entityId);
            cycleBeforeCrash = before.Cycle;
            Assert.True(cycleBeforeCrash > 0, "the workflow never started cycling, so nothing was interrupted");
        }
        finally
        {
            await first.CrashAsync();
        }

        var second = await ChaosNode.StartAsync("chaos-restart-b", _postgres.ConnectionString, cycles);
        try
        {
            // Recovery reads the shared journal: whatever the dead node had reclaimed is already
            // reclaimed, and whatever it had not is replayed.
            var recovered = await Eventually(
                () => ReadState(second, entityId),
                state => state.Cycle >= cycleBeforeCrash,
                TimeSpan.FromSeconds(30));

            Assert.True(
                recovered.Cycle >= cycleBeforeCrash,
                $"recovered at cycle {recovered.Cycle}, behind the {cycleBeforeCrash} reached before the crash — "
                + "a restart lost a cycle it had already recorded");

            await WorkflowInvariants.AssertAll(second.Feed, entityId);
        }
        finally
        {
            await second.StopAsync();
        }
    }

    /// <summary>
    /// The point of <c>E11</c>: an instance that has cycled many times holds roughly one cycle's
    /// worth of events, so a perpetual workflow does not grow without bound. Checked after a crash
    /// specifically, because reclamation is the step a dying process is most likely to skip.
    /// </summary>
    [Fact]
    public async Task AfterCyclingAndCrashing_TheJournalHoldsFarLessThanEveryCycle()
    {
        const string entityId = "restart-crash-2";
        const int cycles = 20;

        var first = await ChaosNode.StartAsync("chaos-bounded-a", _postgres.ConnectionString, cycles);
        try
        {
            await first.Client.For<RestartingWorkflow>(entityId).Send(new BeginCycling(cycles));
            await Task.Delay(TimeSpan.FromSeconds(3));
        }
        finally
        {
            await first.CrashAsync();
        }

        var second = await ChaosNode.StartAsync("chaos-bounded-b", _postgres.ConnectionString, cycles);
        try
        {
            var state = await Eventually(
                () => ReadState(second, entityId),
                s => s.Settled || s.Cycle > 0,
                TimeSpan.FromSeconds(30));

            var recorded = new List<WorkflowFeedItem>();
            await foreach (var item in second.Feed.ReadEntity(entityId))
            {
                recorded.Add(item);
            }

            // Every cycle writes at least a state change and a restart. Holding fewer events than
            // that product means history behind the current cycle really was released — an
            // accumulating workflow that never ends would keep growing past this bound.
            Assert.True(
                recorded.Count < state.Cycle * 2,
                $"{recorded.Count} events retained across {state.Cycle} cycles; a restart is meant to "
                + "release what came before it (E11)");

            await WorkflowInvariants.AssertAll(second.Feed, entityId);
        }
        finally
        {
            await second.StopAsync();
        }
    }

    /// <summary>
    /// The instance's state, asked of the instance itself.
    ///
    /// Deliberately not folded from the journal: a restart reclaims the events behind it, including
    /// the state change of the cycle it closed, so a restarting workflow's state lives in its
    /// snapshot. Reading it from a live entity after a crash is exactly what proves recovery
    /// rebuilt it — the entity answering at all means it recovered from the shared store.
    /// </summary>
    private static Task<RestartingState> ReadState(ChaosNode node, string entityId) =>
        node.Client.For<RestartingWorkflow>(entityId)
            .Query<GetCycle, RestartingState>(new GetCycle(), TimeSpan.FromSeconds(15));

    private static async Task<T> Eventually<T>(
        Func<Task<T>> read, Func<T, bool> until, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        T last = await read();
        while (DateTimeOffset.UtcNow < deadline)
        {
            last = await read();
            if (until(last))
            {
                return last;
            }

            await Task.Delay(250);
        }

        return last;
    }
}
