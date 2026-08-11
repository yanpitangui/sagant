using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence.Query;
using Akka.Persistence.Sql.Hosting;
using Akka.Persistence.Sql.Query;
using Akka.Streams;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sagant.Runtime.Akka.ChaosTests.Support;
using Sagant.Clients;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.ChaosTests;

/// <summary>
/// Proves the foundation every chaos test rests on: two separate <c>ActorSystem</c>s, one Postgres
/// journal, and an instance started on one of them recoverable from the other.
///
/// Worth its own test because everything above it is meaningless without it. If each node held a
/// private journal, a relocated entity would come back empty and every invariant would pass while
/// proving nothing.
/// </summary>
[Collection(PostgresJournalCollection.Name)]
public class SharedJournalSmokeTests
{
    private readonly PostgresJournalFixture _postgres;

    public SharedJournalSmokeTests(PostgresJournalFixture postgres) => _postgres = postgres;

    private async Task<IHost> StartNode(string systemName)
    {
        var builder = Host.CreateApplicationBuilder();

        // The options overload rather than the connection-string one: SqlJournalOptions owns
        // QueryPluginId and builds the read journal's own configuration from the same connection
        // settings. Configuring only the write journal leaves the query side without a database to
        // reach, and constructing it then fails on a timeout.
        var journalOptions = new SqlJournalOptions(isDefaultPlugin: true)
        {
            ConnectionString = _postgres.ConnectionString,
            ProviderName = ProviderName.PostgreSQL,
            AutoInitialize = true,
        };
        var snapshotOptions = new SqlSnapshotOptions(isDefaultPlugin: true)
        {
            ConnectionString = _postgres.ConnectionString,
            ProviderName = ProviderName.PostgreSQL,
            AutoInitialize = true,
        };

        builder.Services.AddAkka(systemName, akka => akka.WithSqlPersistence(journalOptions, snapshotOptions));

        var host = builder.Build();
        await host.StartAsync();

        // Constructing the read journal asks the write journal for its database configuration, and
        // a node that has never persisted anything has no write journal actor yet — the ask then
        // waits for a plugin that nothing has started. Touching it here creates it, so a query path
        // works on a node that only ever reads.
        var system = host.Services.GetRequiredService<ActorSystem>();
        _ = global::Akka.Persistence.Persistence.Instance.Apply(system).JournalFor("akka.persistence.journal.sql");

        return host;
    }

    /// <summary>
    /// The container starts, the schema initialises, and a read journal resolves against it — the
    /// three things that have to work before a fault can be injected into anything.
    /// </summary>
    [Fact]
    public async Task Postgres_IsReachable_AndItsReadJournalResolves()
    {
        var host = await StartNode("smoke-single");
        try
        {
            var system = host.Services.GetRequiredService<ActorSystem>();
            var readJournal = PersistenceQuery.Get(system)
                .ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);

            var feed = new JournalWorkflowEventFeed(readJournal, system.Materializer());

            // No instance has run yet, so this reads an empty stream rather than failing — which is
            // the point: the query path is wired, not merely constructed.
            var events = new List<WorkflowFeedItem>();
            await foreach (var item in feed.ReadEntity("nothing-here"))
            {
                events.Add(item);
            }

            Assert.Empty(events);
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    /// <summary>
    /// Two nodes, one store. A node that never wrote an instance can still read its history — the
    /// property <c>ClusterSharding</c> relies on when it relocates an entity after a node dies.
    /// </summary>
    [Fact]
    public async Task AnInstanceWrittenOnOneNode_IsReadableFromAnother()
    {
        var writer = await StartNode("smoke-writer");
        IHost? reader = null;
        try
        {
            var writerSystem = writer.Services.GetRequiredService<ActorSystem>();

            // Written through a bare persistent actor: this test is about the store being shared,
            // with no workflow machinery in the way to explain a failure.
            var probe = writerSystem.ActorOf(Props.Create(() => new JournalProbeActor("smoke-shared-1")), "probe");
            Assert.Equal("persisted", await probe.Ask<string>("first", TimeSpan.FromSeconds(15)));
            Assert.Equal("persisted", await probe.Ask<string>("second", TimeSpan.FromSeconds(15)));

            reader = await StartNode("smoke-reader");
            var readerSystem = reader.Services.GetRequiredService<ActorSystem>();
            var readJournal = PersistenceQuery.Get(readerSystem)
                .ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);

            var replayed = new List<string>();
            await foreach (var recorded in readJournal
                .CurrentEventsByPersistenceId("smoke-shared-1", 0, long.MaxValue)
                .RunAsAsyncEnumerable(readerSystem.Materializer()))
            {
                replayed.Add((string)recorded.Event);
            }

            Assert.Equal(new[] { "first", "second" }, replayed);
        }
        finally
        {
            if (reader is not null)
            {
                await reader.StopAsync();
                reader.Dispose();
            }

            await writer.StopAsync();
            writer.Dispose();
        }
    }
}

/// <summary>Writes whatever string it is told to, so a test can assert the store round-trips.</summary>
internal sealed class JournalProbeActor : global::Akka.Persistence.ReceivePersistentActor
{
    public JournalProbeActor(string persistenceId)
    {
        PersistenceId = persistenceId;
        // Replies only once the write is durable, so a test can tell a working journal from a
        // silently broken one.
        Command<string>(s =>
        {
            var replyTo = Sender;
            Persist(s, _ => replyTo.Tell("persisted"));
        });
        Recover<string>(_ => { });
    }

    public override string PersistenceId { get; }
}
