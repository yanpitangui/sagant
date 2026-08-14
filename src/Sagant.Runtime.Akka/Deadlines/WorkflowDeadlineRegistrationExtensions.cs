using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Cluster.Tools.Singleton;
using Akka.Event;
using Akka.Hosting;
using Akka.Persistence.Query;
using Akka.Streams;
using Microsoft.Extensions.DependencyInjection;
using Sagant.Clients;
using Sagant.Execution;

namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>
/// Turns on durable deadlines: a projection reading each instance's deadlines out of the journal,
/// and a scheduler that wakes an instance as its own comes up.
///
/// This is what makes idle passivation safe for an instance holding a long deadline. With it, an
/// instance stays resident while it is doing something and is brought back when its deadline
/// arrives; guarantee <c>D8</c> describes the lateness it bounds.
/// </summary>
public static class WorkflowDeadlineRegistrationExtensions
{
    internal const string TickerName = "sagant-deadline-ticker";
    internal const string ProjectionName = "sagant-deadline-projection";
    internal const string BucketShardTypeName = "sagant-deadline-bucket";

    /// <summary>
    /// Starts the deadline scheduler as a cluster singleton, so exactly one holds the index and its
    /// timer. Call it on the <c>AkkaConfigurationBuilder</c> alongside
    /// <c>WithWorkflow&lt;...&gt;</c>.
    /// </summary>
    /// <param name="builder">The Akka.Hosting builder.</param>
    /// <param name="readJournalPluginId">Which read journal the projection follows — e.g.
    /// <c>akka.persistence.query.journal.sql</c>. Its plugin must implement
    /// <see cref="IEventsByTagQuery"/>.</param>
    /// <param name="configureSettings">Adjusts the thresholds and rates, the same shape
    /// <c>WithWorkflow</c>'s <c>configureShardOptions</c> takes:
    /// <c>settings =&gt; settings.MaxWakesPerSecond = 200</c>.</param>
    public static AkkaConfigurationBuilder WithWorkflowDeadlines(
        this AkkaConfigurationBuilder builder,
        string readJournalPluginId,
        Action<WorkflowDeadlineSettings>? configureSettings = null)
    {
        var settings = new WorkflowDeadlineSettings();
        configureSettings?.Invoke(settings);
        settings.Validate();

        return builder.AddStartup(async (system, registry) =>
        {
            var scheduler = StartBuckets(system, settings);
            WorkflowDeadlineSchedulerProvider.Instance.Apply(system).Set(scheduler);

            // One reader for the cluster, as a singleton. Every node running its own would read the
            // same stream N times and each hold its own idea of how far it had got, none of which
            // would be the one to resume from.
            system.ActorOf(
                ClusterSingletonManager.Props(
                    DeadlineProjectionHostActor.Props(
                        settings,
                        () => new WorkflowDeadlineProjection(
                            PersistenceQuery.Get(system).ReadJournalFor<IReadJournal>(readJournalPluginId),
                            system.Materializer(),
                            scheduler,
                            settings,
                            TimeProvider.System,
                            Logging.GetLogger(system, typeof(WorkflowDeadlineProjection)))),
                    ClusterSingletonManagerSettings.Create(system)),
                ProjectionName);

            await Task.CompletedTask;
        });
    }

    private static IWorkflowDeadlineScheduler StartBuckets(ActorSystem system, WorkflowDeadlineSettings settings)
    {
        var client = new Clustering.WorkflowClient(
            Clustering.WorkflowHandleRegistryProvider.Instance.Apply(system));

        var region = ClusterSharding.Get(system).Start(
            typeName: BucketShardTypeName,
            entityPropsFactory: bucketId => DeadlineBucketActor.Props(bucketId, settings, client, TimeProvider.System),
            settings: ClusterShardingSettings.Create(system),
            messageExtractor: new BucketMessageExtractor());

        // One ticker for the cluster, poking each bucket as its slice arrives. It holds the last
        // bucket it reached, which is what lets a gap be walked rather than skipped.
        system.ActorOf(
            ClusterSingletonManager.Props(
                DeadlineTickerActor.Props(settings, region, TimeProvider.System),
                ClusterSingletonManagerSettings.Create(system)),
            TickerName);

        return new BucketEntityDeadlineScheduler(region, settings.WakeTimeout);
    }

    /// <summary>
    /// Registers <see cref="IWorkflowDeadlineScheduler"/> for DI resolution, so an application can
    /// read how many deadlines are armed. Call it after <c>AddAkka(...)</c>, for the same reason
    /// <c>AddWorkflowClient</c> is called there.
    /// </summary>
    public static IServiceCollection AddWorkflowDeadlines(this IServiceCollection services)
    {
        services.AddSingleton<IWorkflowDeadlineScheduler>(sp =>
        {
            var system = sp.GetRequiredService<ActorSystem>();
            return WorkflowDeadlineSchedulerProvider.Instance.Apply(system).Get();
        });

        return services;
    }
}

/// <summary>Holds the one <see cref="IWorkflowDeadlineScheduler"/> per
/// <see cref="ActorSystem"/>, reachable from the startup callback and from DI alike.</summary>
internal sealed class WorkflowDeadlineSchedulerHolder : IExtension
{
    private IWorkflowDeadlineScheduler? _scheduler;

    /// <summary>Whether a scheduler is running on this <see cref="ActorSystem"/>. Read by an entity
    /// about to arm a deadline that outlasts its own passivation window, which is the one situation
    /// where the absence of one shows up as lateness.</summary>
    public bool IsConfigured => _scheduler is not null;

    public void Set(IWorkflowDeadlineScheduler scheduler) => _scheduler = scheduler;

    public IWorkflowDeadlineScheduler Get() =>
        _scheduler ?? throw new InvalidOperationException(
            "Durable deadlines are unavailable on this ActorSystem. Call WithWorkflowDeadlines(...) " +
            "on the Akka.Hosting builder to start them.");
}

internal sealed class WorkflowDeadlineSchedulerProvider : ExtensionIdProvider<WorkflowDeadlineSchedulerHolder>
{
    public static readonly WorkflowDeadlineSchedulerProvider Instance = new();

    public override WorkflowDeadlineSchedulerHolder CreateExtension(ExtendedActorSystem system) => new();
}
