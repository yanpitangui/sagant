using Akka.Cluster.Hosting;
using Akka.Hosting;
using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Scheduling;

/// <summary>
/// Registers <see cref="ScheduleWorkflow"/> so an application can schedule work without writing the
/// registration itself.
///
/// A schedule is an ordinary workflow, so this is the ordinary <c>WithWorkflow</c> call with its
/// arguments already filled in — the client it needs comes from the same container the rest of the
/// application resolves from, and the clock from the system. Everything <c>WithWorkflow</c> exposes
/// stays available for a deployment that wants to tune the shard region behind it.
/// </summary>
public static class SchedulingRegistrationExtensions
{
    /// <summary>
    /// Registers the schedule workflow on this <c>ActorSystem</c>. Call it alongside the
    /// <c>WithWorkflow</c> calls for an application's own workflows.
    /// </summary>
    /// <param name="builder">The Akka.Hosting builder.</param>
    /// <param name="serviceProvider">Resolves <see cref="IWorkflowClient"/> when a schedule instance
    /// is created, which is after the shard regions a schedule starts work through are registered.
    /// </param>
    /// <param name="timeProvider">The clock a schedule reads to decide which occurrence is next.
    /// Supply one to drive schedules in tests.</param>
    /// <param name="configureShardOptions">Deployment-level shard tuning for the schedule region.
    /// A schedule spends most of its life paused between occurrences, so passivating it is what a
    /// deadline scheduler makes safe — see <c>WithWorkflowDeadlines</c>.</param>
    public static AkkaConfigurationBuilder WithScheduling(
        this AkkaConfigurationBuilder builder,
        IServiceProvider serviceProvider,
        TimeProvider? timeProvider = null,
        Action<ShardOptions>? configureShardOptions = null) =>
        builder.WithWorkflow<ScheduleWorkflow, ScheduleState>(
            () => new ScheduleWorkflow(
                (IWorkflowClient)serviceProvider.GetService(typeof(IWorkflowClient))!,
                timeProvider ?? TimeProvider.System),
            configureShardOptions: configureShardOptions);
}
