using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Descriptors;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Event;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Sagant;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OrderFulfillment.Tests.Support;

/// <summary>
/// Boots a real (single-node, self-joining) cluster with a real <c>ClusterSharding</c> shard
/// region behind <see cref="WorkflowRef{TWorkflow, TState}"/> — the same path production traffic
/// takes. Extracted from <c>WorkflowClusterShardingExtensionsTests</c>'s smoke test so every
/// integration test doesn't repeat the bootstrap boilerplate.
///
/// Deliberately does NOT offer a virtual-time/<c>TestScheduler</c> option: <c>Akka.Cluster</c>'s
/// own gossip, failure-detector heartbeats, and shard-coordinator retries are all scheduled through
/// the same <c>system.Scheduler</c>. Swapping it for <see cref="Akka.TestKit.TestScheduler"/> freezes
/// those too, so the cluster never converges past Joining — real cluster and virtual time are
/// mutually exclusive, not a tuning problem. Tests that need deterministic control over a long
/// timer (e.g. a 24h pause timeout) talk to a bare <see cref="Sagant.Runtime.Akka.WorkflowEntityActor{TWorkflow, TState}"/>
/// with <c>TestScheduler</c> instead of going through this harness.
/// </summary>
public sealed class WorkflowClusterTestHarness<TWorkflow, TState> : IAsyncDisposable
    where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>, IWorkflowTypeInfo
{
    private readonly IHost _host;

    public ActorSystem System { get; }

    /// <summary>Exposed directly (not just via <see cref="Ref"/>) so a test whose
    /// <typeparamref name="TWorkflow"/> spawns a child of a different workflow type — registered via
    /// <see cref="StartAsync(string,System.Func{TWorkflow},System.Action{Akka.Hosting.AkkaConfigurationBuilder})"/>'s
    /// <c>configureExtra</c> — can resolve a handle to that child directly too.</summary>
    public IWorkflowClient Client { get; }

    private WorkflowClusterTestHarness(IHost host, ActorSystem system)
    {
        _host = host;
        System = system;
        Client = host.Services.GetRequiredService<IWorkflowClient>();
    }

    public IWorkflowHandle<TWorkflow> Ref(string entityId) => Client.For<TWorkflow>(entityId);

    public static Task<WorkflowClusterTestHarness<TWorkflow, TState>> StartAsync(
        string systemName,
        Func<TWorkflow> workflowFactory) =>
        StartAsync(systemName, workflowFactory, configureExtra: null);

    /// <summary><paramref name="configureExtra"/> runs immediately after
    /// <c>WithWorkflow&lt;TWorkflow, TState&gt;</c> — the seam a test uses to register a second
    /// workflow type (e.g. a child workflow type <typeparamref name="TWorkflow"/> itself spawns) on
    /// the same <c>ActorSystem</c>, the same way a real host's <c>Program.cs</c> would with a second
    /// <c>WithWorkflow</c> call.</summary>
    public static async Task<WorkflowClusterTestHarness<TWorkflow, TState>> StartAsync(
        string systemName,
        Func<TWorkflow> workflowFactory,
        Action<AkkaConfigurationBuilder>? configureExtra)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka(systemName, builder =>
        {
            builder
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .ConfigureLoggers(loggers => loggers.LogLevel = LogLevel.ErrorLevel)
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<TWorkflow, TState>(workflowFactory);
            configureExtra?.Invoke(builder);
        }).AddWorkflowClient();

        var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<ActorSystem>();

        var cluster = global::Akka.Cluster.Cluster.Get(system);
        cluster.Join(cluster.SelfAddress);

        using (var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
        {
            while (!cluster.State.Members.Any(m =>
                       m.UniqueAddress == cluster.SelfUniqueAddress && m.Status == global::Akka.Cluster.MemberStatus.Up))
            {
                upCts.Token.ThrowIfCancellationRequested();
                await Task.Delay(100, upCts.Token);
            }
        }

        var registry = ActorRegistry.For(system);
        using var registryCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        return new WorkflowClusterTestHarness<TWorkflow, TState>(host, system);
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }
}
