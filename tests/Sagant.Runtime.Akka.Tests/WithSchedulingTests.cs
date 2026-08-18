using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Scheduling;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// <c>WithScheduling</c> registers the schedule workflow so an application does not write that
/// registration itself. What it has to get right is the client: a schedule resolves one to start the
/// work it schedules, and resolving it while the <c>ActorSystem</c> is still being built would be
/// circular — so this covers a schedule actually running, going beyond only the call compiling.
/// </summary>
public class WithSchedulingTests
{
    private sealed record Noop;

    [Fact]
    public async Task ARegisteredSchedule_AcceptsAScheduleAndWaitsForIt()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("with-scheduling-test", (builder, sp) => builder
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithScheduling(sp))
            .AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await ClusterSupport.JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();

            var reply = await client.For<ScheduleWorkflow>("every-hour")
                .Request<StartSchedule, string>(
                    StartSchedule.For<ScheduleWorkflow>(
                        new EverySpec(TimeSpan.FromHours(1)), new Noop()),
                    TimeSpan.FromSeconds(15));

            Assert.Equal("scheduled", reply);

            // Waiting for its first occurrence, which is what a schedule spends its life doing.
            var status = await client.For<ScheduleWorkflow>("every-hour")
                .Query<GetScheduleStatus, ScheduleStatus>(new GetScheduleStatus(), TimeSpan.FromSeconds(15));

            Assert.False(status.Paused);
            Assert.NotNull(status.NextFireUtc);
            Assert.Equal(0, status.FireCount);
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
