using Sagant.Runtime.Akka.ChaosTests.Support;

namespace Sagant.Runtime.Akka.ChaosTests;

/// <summary>
/// Many workflows running at once while nodes die under them.
///
/// The single-instance tests answer "can this survive a crash". This one answers the question that
/// actually matters for a durability claim: does it survive <em>every</em> time, across instances at
/// different points in their lives when the fault lands. One workflow crashing at one moment
/// exercises one interleaving; a fleet exercises whichever ones the scheduler happens to produce,
/// and asserts the same invariants over all of them.
///
/// Every assertion is on recorded events, read once the cluster is quiet. Nothing here waits on a
/// timing window, so a slow machine makes the test slower rather than flakier.
/// </summary>
[Collection(PostgresJournalCollection.Name)]
public class FleetUnderFaultTests
{
    private readonly PostgresJournalFixture _postgres;

    public FleetUnderFaultTests(PostgresJournalFixture postgres) => _postgres = postgres;

    /// <summary>
    /// Twenty instances cycling, their host killed part-way through, and the survivor picking up
    /// every one of them. Each is checked against <see cref="WorkflowInvariants"/>: one terminal
    /// event at most, nothing recorded after it, no step attempt claiming both success and failure,
    /// and deadlines that only ever resume.
    /// </summary>
    [Fact]
    public async Task AFleetCyclingThroughANodeDeath_HoldsEveryInvariant()
    {
        const string cluster = "fleet-fault-cluster";
        const int instances = 20;
        const int cycles = 8;

        var ids = Enumerable.Range(1, instances).Select(i => $"fleet-{i}").ToList();

        var seed = await ChaosNode.StartAsync(cluster, _postgres.ConnectionString, cycles);
        ChaosNode? survivor = null;
        try
        {
            survivor = await ChaosNode.StartAsync(
                cluster, _postgres.ConnectionString, cycles, seed: seed.Address);

            // Started through the seed so the fleet spreads across both nodes' shards, which is what
            // puts instances at different points in their lives when the fault lands.
            await Task.WhenAll(ids.Select(id =>
                seed.Client.For<RestartingWorkflow>(id)
                    .Request<BeginCycling, string>(new BeginCycling(cycles), TimeSpan.FromSeconds(45))));

            // Long enough for the fleet to be genuinely mid-flight — some cycling, some settled —
            // without waiting for all of it, which would leave nothing to interrupt.
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
        finally
        {
            await seed.CrashAsync();
        }

        try
        {
            // Quiescence, not a fixed sleep: the fleet is done when every instance stops moving.
            await AwaitQuiescence(survivor!, ids, TimeSpan.FromSeconds(90));

            foreach (var id in ids)
            {
                await WorkflowInvariants.AssertAll(survivor!.Feed, id);
            }

            // Every instance is still addressable, so none was lost with the node that hosted it.
            foreach (var id in ids)
            {
                var events = 0;
                await foreach (var _ in survivor!.Feed.ReadEntity(id))
                {
                    events++;
                }

                Assert.True(events > 0, $"{id} left no recorded events at all — the instance vanished with its host");
            }

            // Guards against the whole thing passing vacuously: a fleet that died on its first step
            // would still have recorded events and still hold every invariant, while proving
            // nothing. Reaching a later cycle is what shows instances genuinely ran through the
            // fault and kept going.
            var cycled = 0;
            foreach (var id in ids)
            {
                if (await ReadCycle(survivor!, id) > 1)
                {
                    cycled++;
                }
            }

            Assert.True(
                cycled >= instances / 2,
                $"only {cycled} of {instances} instances got past their first cycle — the fleet was not "
                + "meaningfully running when its host died, so the invariants held over nothing");
        }
        finally
        {
            await survivor!.StopAsync();
        }
    }

    /// <summary>How far an instance got, asked of the instance — a cycling workflow's state lives in
    /// its snapshot, since each restart reclaims the cycle it closed.</summary>
    private static async Task<int> ReadCycle(ChaosNode node, string entityId)
    {
        try
        {
            var state = await node.Client.For<RestartingWorkflow>(entityId)
                .Query<GetCycle, RestartingState>(new GetCycle(), TimeSpan.FromSeconds(20));
            return state.Cycle;
        }
        catch (Exception)
        {
            return -1;
        }
    }

    /// <summary>
    /// Waits until the fleet's recorded event counts stop changing, so assertions run against a
    /// settled cluster. Two consecutive identical readings is the signal — an instance still
    /// recovering, cycling, or relocating is still writing.
    /// </summary>
    private static async Task AwaitQuiescence(ChaosNode node, IReadOnlyList<string> ids, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        var previous = -1;

        while (DateTimeOffset.UtcNow < deadline)
        {
            var total = 0;
            foreach (var id in ids)
            {
                await foreach (var _ in node.Feed.ReadEntity(id))
                {
                    total++;
                }
            }

            if (total == previous && total > 0)
            {
                return;
            }

            previous = total;
            await Task.Delay(1500);
        }
    }
}
