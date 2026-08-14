using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Persistence.Query.InMemory;
using Akka.Remote.Hosting;
using Sagant.Clients;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Settings;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sagant.Runtime.Akka.Tests;

public sealed record StartAndWait;

public sealed record WaitingState(string Value)
{
    public WaitingState() : this("initial") { }
}

/// <summary>
/// Pauses with a short timeout, so an instance left alone crosses its pause deadline while it holds
/// nothing else to do — the shape cluster sharding passivates.
/// </summary>
public partial class PausingWorkflow : Workflow<WaitingState>
{
    public override WaitingState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<WaitingState> Start(StartAndWait cmd) =>
        Effects.TransitionTo(Steps.WaitStep).ThenReply("accepted");

    [WorkflowStep]
    public StepEffect<WaitingState> WaitStep() =>
        StepEffects.ThenPause(
            PauseSettings.WithTimeout(TimeSpan.FromSeconds(3)).TimeoutHandler(Steps.OnTimeout));

    [WorkflowStep]
    public StepEffect<WaitingState> OnTimeout() =>
        StepEffects.UpdateState(new WaitingState("timed-out")).ThenComplete();
}

/// <summary>
/// Real single-node cluster with idle passivation on, covering what <see cref="Wake"/> is for: a
/// paused instance whose deadline falls due while it is passivated, brought back by the one message
/// that carries no instruction.
/// </summary>
public class WorkflowWakeTests
{
    [Fact]
    public async Task APauseDeadlineElapsedWhilePassivated_FiresWhenWoken()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("wake-test", builder => builder
            // The read journal's own reference.conf reaches an ActorSystem built this way once it is
            // added explicitly, which is what lets this test observe an instance through the journal.
            .AddHocon(InMemoryReadJournal.DefaultConfiguration(), HoconAddMode.Append)
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<PausingWorkflow, WaitingState>(
                () => new PausingWorkflow(),
                configureShardOptions: options => options.PassivateIdleEntityAfter = TimeSpan.FromSeconds(1)))
            .AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            var reply = await client.For<PausingWorkflow>("wake-1")
                .Request<StartAndWait, string>(new StartAndWait(), TimeSpan.FromSeconds(10));
            Assert.Equal("accepted", reply);

            // Long enough for the instance to pause, sit idle past the 1s passivation window, and
            // then cross its own 3s pause deadline while it is gone.
            await Task.Delay(TimeSpan.FromSeconds(6));

            // Read status straight off the journal. Asking the instance would activate it, which is
            // the very thing under test, so this is the one route that observes the gap without
            // closing it.
            var visibility = JournalWorkflowVisibilityQuery.For(system, InMemoryReadJournal.Identifier);
            var beforeWake = await visibility.GetAsync("wake-1");
            Assert.NotNull(beforeWake);
            Assert.Equal(WorkflowStatus.Paused, beforeWake!.Status);

            // The whole wake protocol: no payload, resolved by type name, and the reply says only
            // that the instance is up.
            var done = await client.For(nameof(PausingWorkflow), "wake-1")
                .Wake(WorkflowTimerKind.Pause, TimeSpan.FromSeconds(10));
            Assert.Same(Done.Instance, done);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            WorkflowVisibilityRecord? afterWake;
            do
            {
                cts.Token.ThrowIfCancellationRequested();
                await Task.Delay(200, cts.Token);
                afterWake = await visibility.GetAsync("wake-1", cts.Token);
            }
            while (afterWake!.Status != WorkflowStatus.Finished);

            Assert.IsType<WorkflowOutcome.Completed>(afterWake.Outcome);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task WakingARunningInstance_ChangesNothing()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("wake-noop-test", builder => builder
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<PausingWorkflow, WaitingState>(() => new PausingWorkflow()))
            .AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            await client.For<PausingWorkflow>("wake-2")
                .Request<StartAndWait, string>(new StartAndWait(), TimeSpan.FromSeconds(10));

            var handle = client.For(nameof(PausingWorkflow), "wake-2");

            // Three wakes well inside the pause deadline. Each one activates an instance that is
            // already up, so each is answered and leaves the instance where it was.
            for (var i = 0; i < 3; i++)
            {
                Assert.Same(Done.Instance, await handle.Wake(WorkflowTimerKind.Pause, TimeSpan.FromSeconds(10)));
            }

            Assert.Equal(WorkflowStatus.Paused, await handle.GetStatus(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task ResolvingAnUnregisteredTypeName_Throws()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("wake-unregistered-test", builder => builder
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithRemoting("localhost", 0)
            .WithClustering()
            .WithWorkflow<PausingWorkflow, WaitingState>(() => new PausingWorkflow()))
            .AddWorkflowClient();

        using var host = hostBuilder.Build();
        await host.StartAsync();

        var system = host.Services.GetRequiredService<global::Akka.Actor.ActorSystem>();
        await JoinSelf(system);

        try
        {
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            var ex = Assert.Throws<InvalidOperationException>(() => client.For("NoSuchWorkflow", "x"));
            Assert.Contains("NoSuchWorkflow", ex.Message);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static async Task JoinSelf(global::Akka.Actor.ActorSystem system)
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
