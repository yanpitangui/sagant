using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Persistence.Query.InMemory;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sagant.Clients;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Deadlines;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The whole point, end to end: idle passivation on, an instance holding a deadline further out than
/// the idle window, and nothing touching it. It passivates, its deadline falls due while it is gone,
/// and the projection plus scheduler bring it back on their own.
/// </summary>
public class WorkflowDeadlineSchedulerTests
{
    [Fact]
    public async Task APassivatedInstance_IsWokenForItsOwnDeadline()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("deadline-scheduler-test", builder => builder
            .AddHocon(InMemoryReadJournal.DefaultConfiguration(), HoconAddMode.Append)
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<SleepingWorkflow, SleepingState>(
                () => new SleepingWorkflow(),
                configureShardOptions: options => options.PassivateIdleEntityAfter = TimeSpan.FromSeconds(1))
            .WithWorkflowDeadlines(InMemoryReadJournal.Identifier, settings =>
            {
                // Below the workflow's 6s pause deadline, so that deadline is recorded.
                settings.ExternalArmThreshold = TimeSpan.FromSeconds(2);
                settings.RetryBackoff = TimeSpan.FromSeconds(5);
                settings.MaxRetryBackoff = TimeSpan.FromSeconds(10);
                settings.WakeTimeout = TimeSpan.FromSeconds(15);
            }))
            .AddWorkflowClient()
            .AddWorkflowDeadlines();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await ClusterSupport.JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            await client.For<SleepingWorkflow>("sleeper-1")
                .Request<StartSleeping, string>(new StartSleeping(), new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);

            var visibility = JournalWorkflowVisibilityQuery.For(system, InMemoryReadJournal.Identifier);

            // Nothing here touches the instance. It pauses, passivates a second later, and its 6s
            // pause deadline falls due while it is gone — so anything that happens after this is the
            // scheduler's doing.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            WorkflowVisibilityRecord? record;
            do
            {
                await Task.Delay(250, cts.Token);
                record = await visibility.GetAsync("sleeper-1", cts.Token);
            }
            while (record!.Status != WorkflowStatus.Finished);

            Assert.IsType<WorkflowOutcome.Completed>(record.Outcome);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    /// <summary>
    /// A deadline inside the threshold belongs to the instance's own timer, so the index stays out of
    /// it. This is what keeps step timeouts and retry backoff from reaching the scheduler at all.
    /// </summary>
    [Fact]
    public async Task ADeadlineInsideTheThreshold_IsLeftToTheInstance()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("deadline-threshold-test", builder => builder
            .AddHocon(InMemoryReadJournal.DefaultConfiguration(), HoconAddMode.Append)
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<SleepingWorkflow, SleepingState>(() => new SleepingWorkflow())
            .WithWorkflowDeadlines(
                InMemoryReadJournal.Identifier,
                // Well beyond the workflow's 6s pause deadline, so nothing it holds is recorded.
                settings => settings.ExternalArmThreshold = TimeSpan.FromHours(1)))
            .AddWorkflowClient()
            .AddWorkflowDeadlines();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await ClusterSupport.JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            await client.For<SleepingWorkflow>("sleeper-2")
                .Request<StartSleeping, string>(new StartSleeping(), new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);

            // Long enough for the pause to be written and the projection to have read it.
            await Task.Delay(TimeSpan.FromSeconds(3));

            var scheduler = (BucketEntityDeadlineScheduler)host.Services
                .GetRequiredService<IWorkflowDeadlineScheduler>();

            // The pause deadline lands inside the next couple of slices, so checking those covers
            // wherever it would have gone.
            var now = DateTimeOffset.UtcNow;
            Assert.Equal(0, await scheduler.CountInBucketAsync(now));
            Assert.Equal(0, await scheduler.CountInBucketAsync(now.AddMinutes(1)));
        }
        finally
        {
            await host.StopAsync();
        }
    }
}

public sealed record StartSleeping;

public sealed record SleepingState(string Value)
{
    public SleepingState() : this("initial") { }
}

/// <summary>Pauses for six seconds, then finishes through the timeout handler. Nothing sends it a
/// command, so the pause ends only when its deadline fires.</summary>
public partial class SleepingWorkflow : Workflow<SleepingState>
{
    public override SleepingState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<SleepingState> Start(StartSleeping cmd) =>
        Effects.TransitionTo(Steps.Sleep).ThenReply("accepted");

    [WorkflowStep]
    public StepEffect<SleepingState> Sleep() =>
        StepEffects.ThenPause(
            PauseSettings.WithTimeout(TimeSpan.FromSeconds(6)).TimeoutHandler(Steps.WakeUp));

    [WorkflowStep]
    public StepEffect<SleepingState> WakeUp() =>
        StepEffects.UpdateState(new SleepingState("awake")).ThenComplete();
}

internal static class ClusterSupport
{
    public static async Task JoinSelf(global::Akka.Actor.ActorSystem system)
    {
        var cluster = global::Akka.Cluster.Cluster.Get(system);
        cluster.Join(cluster.SelfAddress);
        using var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!cluster.State.Members.Any(m =>
                   m.UniqueAddress == cluster.SelfUniqueAddress && m.Status == global::Akka.Cluster.MemberStatus.Up))
        {
            upCts.Token.ThrowIfCancellationRequested();
            await Task.Delay(100, upCts.Token);
        }
    }
}
