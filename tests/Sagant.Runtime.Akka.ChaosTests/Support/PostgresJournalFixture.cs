using Testcontainers.PostgreSql;

namespace Sagant.Runtime.Akka.ChaosTests.Support;

/// <summary>
/// A real Postgres instance, shared by every node in a chaos run.
///
/// A shared store is what makes multi-node testing mean anything: <c>ClusterSharding</c> relocating
/// an entity after a node dies only proves something if the node it lands on recovers that entity's
/// own history. An in-memory journal is per-<c>ActorSystem</c>, so every node would hold a private,
/// empty view and a relocated entity would come back as a fresh instance — the exact bug these tests
/// exist to catch, rendered untestable.
///
/// Postgres specifically, through <c>Akka.Persistence.Sql</c>, because that is what
/// <c>samples/OrderFulfillment</c> runs on: the same plugin, the same SQL dialect, the same
/// <c>autoInitialize</c> schema. A guarantee proven against a different store is a guarantee about
/// that store.
/// </summary>
public sealed class PostgresJournalFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("sagant_chaos")
        .WithUsername("sagant")
        .WithPassword("sagant")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

/// <summary>
/// Shares one Postgres container across every chaos test class. Starting a container per class
/// would dominate the runtime of a suite whose point is to run many workflows through many faults;
/// each class isolates itself by persistence-id prefix instead.
/// </summary>
[CollectionDefinition(Name)]
public sealed class PostgresJournalCollection : ICollectionFixture<PostgresJournalFixture>
{
    public const string Name = "postgres-journal";
}
