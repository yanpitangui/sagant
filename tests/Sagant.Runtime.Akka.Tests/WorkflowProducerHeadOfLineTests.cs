using System.Collections.Concurrent;
using Akka.Hosting;
using Akka.Cluster.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sagant.Descriptors;
using Sagant.Clients;
using Sagant.Effects;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.Tests;

public sealed record StallCommand(string Gate);

public sealed record PingCommand(string Text);

public sealed record StallState(string Value)
{
    public StallState() : this("initial") { }
}

/// <summary>Gates a step open from the test, so one entity can be left holding a step in flight for as
/// long as the test needs it.</summary>
public static class StallGates
{
    public static readonly ConcurrentDictionary<string, TaskCompletionSource> Gates = new();

    public static TaskCompletionSource Open(string gate) =>
        Gates.GetOrAdd(gate, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}

// Top-level partial, which is what the source generator handles.
public partial class StallingWorkflow : Workflow<StallState>
{
    public override StallState EmptyState() => new();

    [WorkflowStep]
    public async Task<StepEffect<StallState>> StallStep(string gate)
    {
        await StallGates.Open(gate).Task;
        return StepEffects.UpdateState(new StallState(gate)).ThenComplete();
    }

    [WorkflowCommandHandler]
    public CommandEffect<StallState> Handle(StallCommand cmd) =>
        Effects.TransitionTo(Steps.StallStep, cmd.Gate).ThenReply("stalling");

    [WorkflowCommandHandler]
    public CommandEffect<StallState> Handle(PingCommand cmd) =>
        Effects.UpdateState(new StallState(cmd.Text)).Reply("pong:" + cmd.Text);
}

/// <summary>
/// What one entity's in-flight step costs everything else sharing its workflow type.
///
/// A command arriving while a step runs is stashed and left unconfirmed on purpose, so a crash
/// redelivers it. Every entity of a type shares one <see cref="WorkflowProducerAdapter"/> and one
/// producer controller, which is what makes "who else is held up by that" worth pinning down: an
/// entity waiting on its own step is expected, a second entity waiting on the first one's step would
/// mean a single slow instance stops the whole type from accepting work.
/// </summary>
public class WorkflowProducerHeadOfLineTests
{
    private static async Task<(IHost Host, IWorkflowClient Client)> StartHost(string systemName)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka(systemName, builder =>
        {
            builder
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<StallingWorkflow, StallState>(() => new StallingWorkflow());
        }).AddWorkflowClient();

        var host = hostBuilder.Build();
        try
        {
            await host.StartAsync();

            var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
            var cluster = global::Akka.Cluster.Cluster.Get(system);
            cluster.Join(cluster.SelfAddress);

            using var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            while (!cluster.State.Members.Any(m =>
                       m.UniqueAddress == cluster.SelfUniqueAddress
                       && m.Status == global::Akka.Cluster.MemberStatus.Up))
            {
                upCts.Token.ThrowIfCancellationRequested();
                await Task.Delay(100, upCts.Token);
            }

            return (host, host.Services.GetRequiredService<IWorkflowClient>());
        }
        catch
        {
            await host.StopAsync();
            host.Dispose();
            throw;
        }
    }

    [Fact]
    public async Task AnEntityHoldingAStep_DoesNotStopAnotherEntityOfItsTypeAcceptingWork()
    {
        var gate = $"gate-{Guid.NewGuid():N}";
        var (host, client) = await StartHost("head-of-line");

        try
        {
            // First entity: enters a step that stays in flight until the gate opens.
            var stalled = client.For<StallingWorkflow>("stalled");
            Assert.Equal("stalling", await stalled.Request<StallCommand, string>(
                new StallCommand(gate), cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token));

            // A command arriving while that step runs is stashed and left unconfirmed, which is the
            // state everything below is measured against.
            await stalled.Send(new PingCommand("while-stalled"));

            // Second entity, same workflow type, so the same producer adapter and producer controller
            // carry it.
            var other = client.For<StallingWorkflow>("other");
            var reply = await other.Request<PingCommand, string>(
                new PingCommand("hello"), cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token);

            Assert.Equal("pong:hello", reply);
        }
        finally
        {
            StallGates.Open(gate).TrySetResult();
            await host.StopAsync();
            host.Dispose();
        }
    }
}
