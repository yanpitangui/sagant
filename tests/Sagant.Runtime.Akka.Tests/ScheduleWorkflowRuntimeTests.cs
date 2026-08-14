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
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Deadlines;
using Sagant.Scheduling;

namespace Sagant.Runtime.Akka.Tests;

public sealed record RunTask;

public sealed record TaskState(int Runs)
{
    public TaskState() : this(0) { }
}

/// <summary>The thing a schedule starts. Records that it ran, so a test can see an occurrence
/// actually reach it.</summary>
public partial class ScheduledTaskWorkflow : Workflow<TaskState>
{
    public override TaskState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<TaskState> Run(RunTask cmd, CommandContext<TaskState> ctx) =>
        Effects.UpdateState(ctx.State with { Runs = ctx.State.Runs + 1 }).Complete();
}

/// <summary>
/// A schedule driven by the real runtime rather than the harness: its own entity, its own journal,
/// and a target started through the client.
///
/// The harness settles the arithmetic — which instant is next, what is skipped. What it cannot show
/// is that an occurrence reaches its target at all, which is the part every other piece is in service
/// of.
/// </summary>
public class ScheduleWorkflowRuntimeTests
{
    private static HostApplicationBuilder BuildHost(string systemName, TimeSpan passivateAfter)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka(systemName, (builder, sp) => builder
            .AddHocon(InMemoryReadJournal.DefaultConfiguration(), HoconAddMode.Append)
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<ScheduledTaskWorkflow, TaskState>(() => new ScheduledTaskWorkflow())
            .WithScheduling(sp, configureShardOptions: o => o.PassivateIdleEntityAfter = passivateAfter)
            .WithWorkflowDeadlines(
                InMemoryReadJournal.Identifier,
                settings =>
                {
                    settings.ExternalArmThreshold = TimeSpan.FromSeconds(1);
                    settings.WakeTimeout = TimeSpan.FromSeconds(15);
                }))
            .AddWorkflowClient()
            .AddWorkflowDeadlines();

        return hostBuilder;
    }

    /// <summary>
    /// An occurrence reaches its target. The schedule's own state records the fire, and the target's
    /// records the run — both are checked, since a schedule that counted a fire it never sent would
    /// look correct from one side.
    /// </summary>
    [Fact]
    public async Task AnOccurrenceStartsItsTarget()
    {
        using var host = BuildHost("schedule-runtime-test", TimeSpan.FromMinutes(2)).Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await ClusterSupport.JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();

            await client.For<ScheduleWorkflow>("every-two-seconds")
                .Request<StartSchedule, string>(
                    StartSchedule.For<ScheduledTaskWorkflow>(
                        new EverySpec(TimeSpan.FromSeconds(2)), new RunTask()),
                    TimeSpan.FromSeconds(15));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            ScheduleStatus status;
            do
            {
                await Task.Delay(250, cts.Token);
                status = await client.For<ScheduleWorkflow>("every-two-seconds")
                    .Query<GetScheduleStatus, ScheduleStatus>(new GetScheduleStatus(), TimeSpan.FromSeconds(15));
            }
            while (status.FireCount < 1);

            Assert.NotNull(status.LastStartedEntityId);

            // The occurrence's own instance ran, which is the half a schedule's own counters cannot
            // vouch for.
            var target = await client.For<ScheduledTaskWorkflow>(status.LastStartedEntityId!)
                .GetStatus(TimeSpan.FromSeconds(15));
            Assert.Equal(WorkflowStatus.Finished, target);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    /// <summary>
    /// The whole point of a schedule being a workflow: it holds nothing between occurrences. With
    /// passivation well under the wait, the schedule is gone when its occurrence comes due, and the
    /// deadline machinery is the only thing that can bring it back.
    /// </summary>
    [Fact]
    public async Task APassivatedSchedule_IsWokenForItsOwnOccurrence()
    {
        using var host = BuildHost("schedule-passivation-test", TimeSpan.FromSeconds(1)).Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await ClusterSupport.JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();

            await client.For<ScheduleWorkflow>("every-six-seconds")
                .Request<StartSchedule, string>(
                    StartSchedule.For<ScheduledTaskWorkflow>(
                        new EverySpec(TimeSpan.FromSeconds(6)), new RunTask()),
                    TimeSpan.FromSeconds(15));

            // Read through the journal rather than the instance, so watching for the fire does not
            // itself keep the schedule resident — which would be the thing under test doing nothing.
            var visibility = JournalWorkflowVisibilityQuery.For(system, InMemoryReadJournal.Identifier);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

            while (true)
            {
                cts.Token.ThrowIfCancellationRequested();
                await Task.Delay(500, cts.Token);

                var records = new List<WorkflowVisibilityRecord>();
                await foreach (var record in visibility.ListAsync(
                    new WorkflowVisibilityFilter { WorkflowType = nameof(ScheduledTaskWorkflow) }, cts.Token))
                {
                    records.Add(record);
                }

                if (records.Count > 0)
                {
                    Assert.All(records, r => Assert.Equal(nameof(ScheduledTaskWorkflow), r.WorkflowType));
                    return;
                }
            }
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
