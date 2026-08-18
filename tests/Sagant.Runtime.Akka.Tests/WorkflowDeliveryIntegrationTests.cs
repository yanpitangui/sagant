using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Sagant.Clients;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Execution;
using Sagant.Runtime.Akka.Clustering;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sagant.Runtime.Akka.Tests;

public sealed record DeliveryEchoPing(string Text);

public sealed record DeliveryEchoState(string Value)
{
    public DeliveryEchoState() : this("initial") { }
}

// Top-level: the source generator only handles top-level partial classes today — same reason
// EchoWorkflow.cs itself is top-level. Deliberately a separate fixture from EchoWorkflow
// (shared by WorkflowClientTests): its reply must genuinely depend on the command payload, staying
// clear of the literal constant "accepted", otherwise a reply1 == reply2 assertion across two
// different payloads under the same idempotency key can't distinguish "replayed the cached reply"
// from "handler genuinely re-ran on different input and coincidentally produced the same constant
// back" — see WorkflowDeliveryIntegrationTests's idempotency-replay tests.
public partial class DeliveryEchoWorkflow : Workflow<DeliveryEchoState>
{
    public override DeliveryEchoState EmptyState() => new();

    [WorkflowStep]
    public Task<StepEffect<DeliveryEchoState>> EchoStep(string text) =>
        Task.FromResult(StepEffects.UpdateState(new DeliveryEchoState(text)).ThenComplete());

    [WorkflowCommandHandler]
    public CommandEffect<DeliveryEchoState> Handle(DeliveryEchoPing ping) =>
        Effects.TransitionTo(Steps.EchoStep, ping.Text).ThenReply($"accepted:{ping.Text}");
}

/// <summary>
/// Real single-node cluster (in-memory journal/snapshot store, self-join) exercising reliable
/// command delivery (Akka.Delivery) + idempotency end to end — mirrors <see cref="WorkflowClientTests"/>'s
/// pattern. A plain business-command round trip is already covered by
/// <see cref="WorkflowClientTests.For_ResolvesHandle_RoundTripsCommandThroughRealSharding"/> and isn't
/// duplicated here; transport-level seqNr dedup is already covered at the unit level (no real cluster
/// needed) by <c>WorkflowDeliveryHandlingTests.Delivery_DuplicateSeqNr_SkipsHandlerButStillConfirms</c>.
/// </summary>
public class WorkflowDeliveryIntegrationTests
{
    private static async Task<(IHost Host, IWorkflowClient Client)> StartHost(
        string systemName, bool recordWrites = false)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka(systemName, builder =>
        {
            if (recordWrites)
            {
                builder.AddHocon(Support.RecordingJournal.Config, HoconAddMode.Prepend);
            }
            else
            {
                builder.WithInMemoryJournal();
            }

            builder
                .WithInMemorySnapshotStore()
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<DeliveryEchoWorkflow, DeliveryEchoState>(() => new DeliveryEchoWorkflow());
        }).AddWorkflowClient();

        // `using` here alone isn't enough for the failure-during-startup case this guards against —
        // a `using var host = hostBuilder.Build();` local would still leak if StartAsync or the
        // cluster-up wait below throws before this method returns, because the `using` block's
        // scope ends at the method boundary, well after the return statement's own caller. Building the
        // host, then immediately entering try/finally around everything that can throw, is what
        // actually guarantees StopAsync/Dispose run on any failure path.
        var host = hostBuilder.Build();
        try
        {
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

            return (host, host.Services.GetRequiredService<IWorkflowClient>());
        }
        catch
        {
            await host.StopAsync();
            host.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Caller metadata reaches the persisted event, which is what makes "who approved this order,
    /// and under what correlation id" answerable from the event stream alone. It travels on the
    /// Akka.Delivery envelope, so this exercises the real client path a business command takes.
    /// </summary>
    [Fact]
    public async Task Send_WithMetadata_RecordsItOnTheEventTheCommandCauses()
    {
        var (host, client) = await StartHost("delivery-metadata-test", recordWrites: true);
        try
        {
            var handle = client.For<DeliveryEchoWorkflow>("echo-meta-1");

            await handle.Request<DeliveryEchoPing, string>(
                new DeliveryEchoPing("hello"),
                TimeSpan.FromSeconds(15),
                metadata: new Dictionary<string, string> { ["user"] = "operator-7", ["correlation"] = "abc-123" });

            var caused = Support.RecordingJournal.EventsFor("DeliveryEchoWorkflow-echo-meta-1")
                .OfType<WorkflowEvent.CausedEvent>()
                .Select(e => e.Cause)
                .OfType<TransitionCause.Command>()
                .Single(c => c.CommandType == nameof(DeliveryEchoPing));

            Assert.Equal("operator-7", caused.Metadata!["user"]);
            Assert.Equal("abc-123", caused.Metadata["correlation"]);
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task Request_WithRepeatIdempotencyKey_ReplaysCachedReply()
    {
        var (host, client) = await StartHost("delivery-idem-test");
        try
        {
            var handle = client.For<DeliveryEchoWorkflow>("echo-idem-1");

            var reply1 = await handle.Request<DeliveryEchoPing, string>(new DeliveryEchoPing("hello"), TimeSpan.FromSeconds(15), idempotencyKey: "key-1");
            var reply2 = await handle.Request<DeliveryEchoPing, string>(new DeliveryEchoPing("hello-again"), TimeSpan.FromSeconds(15), idempotencyKey: "key-1");

            Assert.Equal("accepted:hello", reply1);
            Assert.Equal(reply1, reply2); // replayed the cached "accepted:hello" reply, never re-invoking the handler for "accepted:hello-again"

            var status = await handle.GetStatus(TimeSpan.FromSeconds(15));
            Assert.Equal(WorkflowStatus.Finished, status); // the EchoStep transition from the first Request has landed; also proves the entity answers control commands as well as business ones
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task Request_AfterEntityPassivatesAndReactivates_StillReplaysCachedReplyFromRecoveredLedger()
    {
        // A short PassivateIdleEntityAfter forces ClusterSharding to stop the entity actor once it's
        // been idle past that deadline (checked on a repeating tick at half that interval — see
        // Akka.Cluster.Sharding.Shard.PassivateIdleEntities/the constructor's ScheduleTellRepeatedlyCancelable).
        // The *next* message to the same entity id then reactivates a brand-new actor instance, which
        // must recover WorkflowRuntimeState (including IdempotencyLedger/HighestAppliedSeqNr) from the
        // InMemory journal/snapshot store before it can answer — this is the same recovery path a real
        // crash-and-restart or ClusterSharding rebalance goes through, just triggered deterministically
        // via idle passivation, with the process itself left running.
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("delivery-restart-test", builder =>
        {
            builder
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<DeliveryEchoWorkflow, DeliveryEchoState>(
                    () => new DeliveryEchoWorkflow(),
                    configureShardOptions: options => options.PassivateIdleEntityAfter = TimeSpan.FromSeconds(2));
        }).AddWorkflowClient();

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
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            var handle = client.For<DeliveryEchoWorkflow>("echo-restart-1");

            var reply1 = await handle.Request<DeliveryEchoPing, string>(new DeliveryEchoPing("hello"), TimeSpan.FromSeconds(15), idempotencyKey: "restart-key-1");
            Assert.Equal("accepted:hello", reply1);

            // Sit idle well past the 2s PassivateIdleEntityAfter (plus the up-to-1s tick granularity)
            // so the entity actor is actually stopped before the next message.
            await Task.Delay(TimeSpan.FromSeconds(6));

            var reply2 = await handle.Request<DeliveryEchoPing, string>(new DeliveryEchoPing("hello-again"), TimeSpan.FromSeconds(15), idempotencyKey: "restart-key-1");

            Assert.Equal(reply1, reply2); // recovered ledger still replays "accepted:hello" after a real actor stop/restart, with the handler left uninvoked
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Send_WhenProducerBufferFull_FaultsTheCallersTask()
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Services.AddAkka("delivery-backpressure-test", builder =>
        {
            builder
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .WithRemoting("localhost", 0)
                .WithClustering()
                .WithWorkflow<EchoWorkflow, EchoState>(() => new EchoWorkflow(), producerBufferCapacity: 1);
        }).AddWorkflowClient();

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
            var client = host.Services.GetRequiredService<IWorkflowClient>();
            var handle = client.For<EchoWorkflow>("echo-backpressure-1");

            // Fire enough concurrent sends to exceed a 1-deep buffer before any of them can drain —
            // at least one must fault with WorkflowProducerAdapter.ProducerBufferFullException.
            var sends = Enumerable.Range(0, 20)
                .Select(i => handle.Send(new EchoPing($"msg-{i}")).AsTask())
                .ToArray();

            var aggregate = await Task.WhenAny(Task.WhenAll(sends), Task.Delay(TimeSpan.FromSeconds(10)))
                .ContinueWith(_ => Task.WhenAll(sends));

            await Assert.ThrowsAsync<WorkflowProducerAdapter.ProducerBufferFullException>(async () =>
            {
                try { await aggregate; }
                catch (AggregateException ex) { throw ex.InnerExceptions.First(); }
            });
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
