using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowClientTests
{
    [Fact]
    public async Task For_ResolvesHandle_RoundTripsCommandThroughRealSharding()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("client-test-system", builder =>
        {
            builder
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<EchoWorkflow, EchoState>(() => new EchoWorkflow());
        }).AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();
        try
        {
            var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
            var cluster = global::Akka.Cluster.Cluster.Get(system);
            cluster.Join(cluster.SelfAddress);

            using var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            while (!cluster.State.Members.Any(m => m.UniqueAddress == cluster.SelfUniqueAddress && m.Status == global::Akka.Cluster.MemberStatus.Up))
            {
                upCts.Token.ThrowIfCancellationRequested();
                await Task.Delay(100, upCts.Token);
            }

            var client = host.Services.GetRequiredService<IWorkflowClient>();
            var handle = client.For<EchoWorkflow>("echo-1");

            var reply = await handle.Request<EchoPing, string>(new EchoPing("hello"), new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token);

            Assert.Equal("accepted", reply);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task For_UnregisteredWorkflowType_ThrowsImmediately()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("client-test-system-2", builder => { }) // no WithWorkflow<EchoWorkflow,...> call
            .AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();
        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            Assert.Throws<InvalidOperationException>(() => client.For<EchoWorkflow>("echo-1"));
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
