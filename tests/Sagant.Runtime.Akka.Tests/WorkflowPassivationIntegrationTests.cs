using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Sagant.Clients;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sagant.Runtime.Akka.Tests;

public sealed record StartSlowWork;

public sealed record SlowWorkState(string Value)
{
    public SlowWorkState() : this("initial") { }
}

/// <summary>
/// Two steps with a slow one first, so the run spans several idle windows and still has somewhere to
/// go afterwards. An entity passivated during the first step stops once that step persists — the
/// shutdown path deliberately declines to start the next one — leaving the instance mid-chain with
/// nothing scheduled to bring it back.
/// </summary>
public partial class SlowStepWorkflow : Workflow<SlowWorkState>
{
    public override SlowWorkState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<SlowWorkState> Start(StartSlowWork cmd) =>
        Effects.TransitionTo(Steps.SlowStep).ThenReply("accepted");

    [WorkflowStep]
    public async Task<StepEffect<SlowWorkState>> SlowStep(StepContext<SlowWorkState> ctx)
    {
        await Task.Delay(TimeSpan.FromSeconds(6), ctx.CancellationToken);
        return StepEffects.UpdateState(new SlowWorkState("slow-done")).ThenTransitionTo(Steps.FinishStep);
    }

    [WorkflowStep]
    public StepEffect<SlowWorkState> FinishStep() =>
        StepEffects.UpdateState(new SlowWorkState("finished")).ThenComplete();
}

/// <summary>
/// Real single-node cluster with idle passivation turned on and a step that outlasts the idle window
/// several times over. Covers what the keep-alive is for: cluster sharding measures idleness by the
/// messages it routes, and a step running off-actor-thread sends none.
/// </summary>
public class WorkflowPassivationIntegrationTests
{
    [Fact]
    public async Task AStepOutlastingTheIdleWindow_RunsToCompletionAnyway()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("passivation-keepalive-test", builder => builder
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<SlowStepWorkflow, SlowWorkState>(
                () => new SlowStepWorkflow(),
                configureShardOptions: options => options.PassivateIdleEntityAfter = TimeSpan.FromSeconds(2)))
            .AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        var cluster = global::Akka.Cluster.Cluster.Get(system);
        cluster.Join(cluster.SelfAddress);
        using var upCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!cluster.State.Members.Any(m => m.UniqueAddress == cluster.SelfUniqueAddress && m.Status == global::Akka.Cluster.MemberStatus.Up))
        {
            upCts.Token.ThrowIfCancellationRequested();
            await Task.Delay(100, upCts.Token);
        }

        try
        {
            var handle = host.Services.GetRequiredService<IWorkflowClient>().For<SlowStepWorkflow>("slow-1");

            var result = await handle.RunAndAwaitResult<SlowWorkState>(
                new StartSlowWork(), TimeSpan.FromSeconds(30));

            var finished = Assert.IsType<WorkflowResult<SlowWorkState>.Finished>(result);
            Assert.IsType<WorkflowOutcome.Completed>(finished.Outcome);
            Assert.Equal("finished", finished.State.Value);
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
