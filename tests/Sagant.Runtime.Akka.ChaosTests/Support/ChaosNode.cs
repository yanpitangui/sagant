using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Query;
using Akka.Persistence.Sql.Hosting;
using Akka.Persistence.Sql.Query;
using Akka.Remote.Hosting;
using Akka.Streams;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.ChaosTests.Support;

/// <summary>
/// One cluster node, backed by the shared Postgres journal, that a test can kill.
///
/// Killing is the point, so shutdown comes in two flavours: <see cref="StopAsync"/> leaves the way a
/// deployment would, and <see cref="CrashAsync"/> does not — see its own remarks for why the
/// difference matters.
/// </summary>
public sealed class ChaosNode : IAsyncDisposable
{
    private readonly IHost _host;

    private ChaosNode(IHost host, ActorSystem system, IWorkflowClient client, IWorkflowEventFeed feed)
    {
        _host = host;
        System = system;
        Client = client;
        Feed = feed;
    }

    public ActorSystem System { get; }

    public IWorkflowClient Client { get; }

    /// <summary>Reads what this cluster actually recorded, which is what a chaos test asserts on
    /// once everything has settled.</summary>
    public IWorkflowEventFeed Feed { get; }

    /// <summary>This node's cluster address, which a later node joins to form a real cluster.</summary>
    public global::Akka.Actor.Address Address => global::Akka.Cluster.Cluster.Get(System).SelfAddress;

    /// <param name="seed">The address to join, or <c>null</c> to found a cluster by joining itself.
    /// Two nodes sharing one journal is what makes a rebalance mean anything: the entity has
    /// somewhere else to go, and has to recover there from what the first node wrote.</param>
    /// <param name="slowStep">How long <see cref="SlowStepWorkflow"/>'s step runs — long enough that
    /// a shutdown can land while it is still going.</param>
    public static async Task<ChaosNode> StartAsync(
        string systemName, string connectionString, int cycles, int port = 0,
        global::Akka.Actor.Address? seed = null, TimeSpan? slowStepDuration = null)
    {
        var slowStep = slowStepDuration ?? TimeSpan.FromSeconds(2);

        var builder = Host.CreateApplicationBuilder();

        // Options rather than the connection-string overload: SqlJournalOptions owns QueryPluginId
        // and configures the read journal alongside the write one, which is what the feed reads
        // through.
        var journalOptions = new SqlJournalOptions(isDefaultPlugin: true)
        {
            ConnectionString = connectionString,
            ProviderName = ProviderName.PostgreSQL,
            AutoInitialize = true,
        };
        var snapshotOptions = new SqlSnapshotOptions(isDefaultPlugin: true)
        {
            ConnectionString = connectionString,
            ProviderName = ProviderName.PostgreSQL,
            AutoInitialize = true,
        };

        builder.Services.AddAkka(systemName, akka =>
        {
            akka.WithSqlPersistence(journalOptions, snapshotOptions)
                .ConfigureLoggers(loggers => loggers.LogLevel = global::Akka.Event.LogLevel.ErrorLevel)
                .WithRemoting("localhost", port)
                .WithClustering()
                .WithWorkflow<RestartingWorkflow, RestartingState>(() => new RestartingWorkflow(cycles))
                // Registered on every node so a relocated entity of either type has somewhere to
                // land, which is the whole point of the cluster these tests build.
                .WithWorkflow<SlowStepWorkflow, SlowStepState>(() => new SlowStepWorkflow(slowStep));
        }).AddWorkflowClient();

        var host = builder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<ActorSystem>();
        var cluster = global::Akka.Cluster.Cluster.Get(system);
        cluster.Join(seed ?? cluster.SelfAddress);

        using (var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(45)))
        {
            while (!cluster.State.Members.Any(m =>
                m.UniqueAddress == cluster.SelfUniqueAddress && m.Status == global::Akka.Cluster.MemberStatus.Up))
            {
                upCts.Token.ThrowIfCancellationRequested();
                await Task.Delay(100, upCts.Token);
            }
        }

        var readJournal = PersistenceQuery.Get(system).ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);
        var feed = new JournalWorkflowEventFeed(readJournal, system.Materializer());

        return new ChaosNode(host, system, host.Services.GetRequiredService<IWorkflowClient>(), feed);
    }

    /// <summary>
    /// Kills the node without letting it tidy up: no <c>CoordinatedShutdown</c>, no graceful handoff,
    /// no chance for an in-flight step to finish.
    ///
    /// A graceful stop proves the wrong thing. It lets the entity reach a clean boundary before the
    /// process ends, which is exactly the case where recovery is easy. What a durability claim rests
    /// on is the other case — the process disappearing at whatever instant it happened to be at.
    /// </summary>
    public async Task CrashAsync()
    {
        await System.Terminate();
        _host.Dispose();
    }

    public async Task StopAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    public ValueTask DisposeAsync() => new(StopAsync());
}
