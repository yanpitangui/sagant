using Sagant.Runtime.Akka.ChaosTests.Support;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.ChaosTests;

/// <summary>
/// How the feed is addressed.
///
/// A caller holds the id it passed to <c>IWorkflowClient.For</c>; the journal is keyed by a
/// persistence id that prefixes it with the workflow's type name. The feed bridges the two.
///
/// This belongs in the chaos suite because only real <c>ClusterSharding</c> puts the two ids apart:
/// a bare actor is constructed with its persistence id and entity id set to the same string, so a
/// feed that confused them would still pass every in-memory test.
/// </summary>
[Collection(PostgresJournalCollection.Name)]
public class FeedAddressingTests
{
    private readonly PostgresJournalFixture _postgres;

    public FeedAddressingTests(PostgresJournalFixture postgres) => _postgres = postgres;

    /// <summary>
    /// Reads by the id a caller actually has. Against a sharded workflow the journal holds
    /// <c>RestartingWorkflow-feed-addressing-1</c>, so a feed that queried the bare entity id would
    /// return nothing at all — silently, since an empty stream is a legitimate answer for an
    /// instance that has not run.
    /// </summary>
    [Fact]
    public async Task ReadEntity_TakesTheRoutableId_NotTheJournalsPrefixedOne()
    {
        const string entityId = "feed-addressing-1";

        await using var node = await ChaosNode.StartAsync(
            "feed-addressing", _postgres.ConnectionString, cycles: 1);

        var accepted = await node.Client.For<RestartingWorkflow>(entityId)
            .Request<BeginCycling, string>(new BeginCycling(1), TimeSpan.FromSeconds(30));
        Assert.Equal("accepted", accepted);

        var items = await Eventually(
            () => ReadAll(node, entityId),
            read => read.Count > 0,
            TimeSpan.FromSeconds(30));

        Assert.NotEmpty(items);

        // Every item reports the routable id back, so a consumer can feed it straight into
        // IWorkflowClient.For without knowing the journal's key format.
        Assert.All(items, item => Assert.Equal(entityId, item.EntityId));
        Assert.All(items, item => Assert.Equal(nameof(RestartingWorkflow), item.WorkflowType));
    }

    /// <summary>
    /// The prefix is the workflow type, so two workflow types could share an entity id. Splitting a
    /// persistence id on the first separator is what keeps a GUID entity id — which contains several
    /// more — from being truncated.
    /// </summary>
    [Fact]
    public void PersistenceId_SplitsOnTheFirstSeparatorOnly()
    {
        const string guidEntityId = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        var persistenceId = $"{nameof(RestartingWorkflow)}-{guidEntityId}";

        Assert.Equal(nameof(RestartingWorkflow), WorkflowPersistenceId.WorkflowTypeOf(persistenceId));
        Assert.Equal(guidEntityId, WorkflowPersistenceId.EntityIdOf(persistenceId));
    }

    private static async Task<List<WorkflowFeedItem>> ReadAll(ChaosNode node, string entityId)
    {
        var items = new List<WorkflowFeedItem>();
        await foreach (var item in node.Feed.ReadEntity(entityId))
        {
            items.Add(item);
        }

        return items;
    }

    private static async Task<T> Eventually<T>(Func<Task<T>> read, Func<T, bool> until, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        var last = await read();
        while (DateTimeOffset.UtcNow < deadline && !until(last))
        {
            await Task.Delay(250);
            last = await read();
        }

        return last;
    }
}
