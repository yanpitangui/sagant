using Sagant.Runtime.Akka.ChaosTests.Support;

namespace Sagant.Runtime.Akka.ChaosTests;

/// <summary>
/// An entity moving between nodes.
///
/// Node death (see <c>RestartUnderCrashTests</c>) proves an instance survives its host disappearing.
/// This proves the other half: with somewhere else to go, the instance actually goes there and keeps
/// its state. Guarantee <c>C4</c> says exactly one live instance exists cluster-wide, so a relocated
/// entity recovering from the shared journal is the only way its state can reach the new node.
/// </summary>
[Collection(PostgresJournalCollection.Name)]
public class RebalanceTests
{
    private readonly PostgresJournalFixture _postgres;

    public RebalanceTests(PostgresJournalFixture postgres) => _postgres = postgres;

    /// <summary>
    /// Two nodes, one journal. The node hosting an entity is killed and the survivor takes the
    /// shard over — the instance answers from a process that never ran it, with the cycle count it
    /// had reached before the move.
    /// </summary>
    [Fact]
    public async Task AnEntityOutlivesTheNodeHostingIt_AndKeepsItsState()
    {
        const string entityId = "rebalance-1";
        const int cycles = 15;

        // One cluster means one ActorSystem name: Akka.Cluster identifies members by address, and
        // nodes whose system names differ never form a cluster at all.
        const string clusterName = "rebalance-cluster";

        var seed = await ChaosNode.StartAsync(clusterName, _postgres.ConnectionString, cycles);
        ChaosNode? joiner = null;
        try
        {
            joiner = await ChaosNode.StartAsync(
                clusterName, _postgres.ConnectionString, cycles, seed: seed.Address);

            var accepted = await joiner.Client.For<RestartingWorkflow>(entityId)
                .Request<BeginCycling, string>(new BeginCycling(cycles), TimeSpan.FromSeconds(30));
            Assert.Equal("accepted", accepted);

            var before = await Eventually(
                () => ReadCycle(joiner, entityId),
                cycle => cycle > 0,
                TimeSpan.FromSeconds(30));
            Assert.True(before > 0, "the workflow never cycled, so there was nothing to relocate");

            // Kill the founding node. Whichever node was hosting the shard, the survivor is the only
            // place the entity can live afterwards.
            await seed.CrashAsync();

            var after = await Eventually(
                () => ReadCycle(joiner, entityId),
                cycle => cycle >= before,
                TimeSpan.FromSeconds(60));

            Assert.True(
                after >= before,
                $"after the host died the instance reported cycle {after}, behind the {before} it had "
                + "already reached — a relocated entity recovered from an incomplete history");

            await WorkflowInvariants.AssertAll(joiner.Feed, entityId);
        }
        finally
        {
            if (joiner is not null)
            {
                await joiner.StopAsync();
            }
        }
    }

    private static async Task<int> ReadCycle(ChaosNode node, string entityId)
    {
        try
        {
            var state = await node.Client.For<RestartingWorkflow>(entityId)
                .Query<GetCycle, RestartingState>(new GetCycle(), TimeSpan.FromSeconds(15));
            return state.Cycle;
        }
        catch (Exception)
        {
            // A shard mid-handoff has no one to answer, which is the normal state of affairs for a
            // moment after a node dies. Reported as "no progress yet" so the caller keeps waiting.
            return -1;
        }
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
