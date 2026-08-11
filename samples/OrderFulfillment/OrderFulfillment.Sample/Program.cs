using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Protocol;
using Aaron.Akka.Aspire;
using Aaron.Akka.Discovery.Redis;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Persistence.Sql.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using LinqToDB;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using OrderFulfillment.Sample;
using System.Runtime.CompilerServices;

// Demo host: 3 replicas of this same process (see OrderFulfillment.AppHost's Program.cs), each a
// full node running ClusterSharding for both OrderFulfillmentWorkflow and ItemFulfillmentWorkflow
// plus the Razor Pages UI. Pages render from OrderReadModelRepository's Postgres tables and push
// live updates over Server-Sent Events (/orders/stream). Every replica's WorkflowEventLoggerActor
// subscribes to the cluster-wide WorkflowEventPubSubBridge topic and writes into the same
// Postgres tables, so any replica's UI shows any order regardless of which replica placed it or
// hosts its entity. Both the engine journal/snapshot store (Akka.Persistence.Sql) and the read
// model (OrderReadModelRepository, LinqToDB) point at the same Postgres instance.
//
// AddOtlpExporter() reads OTEL_EXPORTER_OTLP_ENDPOINT, injected by OrderFulfillment.AppHost.
var builder = WebApplication.CreateBuilder(args);

// Injected by OrderFulfillment.AppHost via .WithReference(ordersDb) as a standard ASP.NET Core
// connection-string configuration key.
var ordersDbConnectionString = builder.Configuration.GetConnectionString("orders-db")
    ?? throw new InvalidOperationException(
        "Connection string 'orders-db' not found — run this via OrderFulfillment.AppHost, not standalone.");

// Registered on every replica: whichever one ends up hosting a given order's (or item's) entity
// resolves these through the (AkkaConfigurationBuilder, IServiceProvider) overload of AddAkka below.
builder.Services.AddSingleton<IInventoryService, SimulatedInventoryService>();
builder.Services.AddSingleton<IPaymentService, SimulatedPaymentService>();
builder.Services.AddSingleton<IShippingService, SimulatedShippingService>();
builder.Services.AddSingleton<INotificationService, SimulatedNotificationService>();
builder.Services.AddSingleton(new OrderReadModelRepository(ordersDbConnectionString));
builder.Services.AddSingleton<OrderChangeSignal>();
builder.Services.AddSingleton<FaultInjectionRegistry>();
builder.Services.AddSingleton<OrderPlacementService>();

builder.Services.AddRazorPages();

// akka-cluster-membership and the actor-system liveness check are registered into this same
// IHealthChecksBuilder by WithAspireClusterBootstrap below (see Aaron.Akka.Aspire) — this call just
// wires the ASP.NET Core health-check middleware up to receive them.
builder.Services.AddHealthChecks();

builder.Services.AddAkka("order-fulfillment-demo", (akkaBuilder, sp) =>
{
    akkaBuilder
        .WithSqlPersistence(
            connectionString: ordersDbConnectionString,
            providerName: ProviderName.PostgreSQL,
            autoInitialize: true)
        .ConfigureLoggers(loggers =>
        {
            loggers.LogLevel = Akka.Event.LogLevel.InfoLevel;
            loggers.AddLoggerFactory(); // routes Akka's own logging into Microsoft.Extensions.Logging, and from there into the OTLP log exporter above
        })
        // Reads the Akka:Cluster:* env vars OrderFulfillment.AppHost's AddAkka(...).WithClustering(redis)
        // injects into each replica (remote/management ports, service name, contact points), wires
        // Akka.Management's ClusterBootstrap against the akka-discovery Redis instance for peer
        // discovery, and registers the akka-cluster-membership/liveness health checks
        // builder.Services.AddHealthChecks() above surfaces.
        .WithAspireClusterBootstrap(sp,
            configureDiscovery: (b, config) =>
            {
                var redisConn = config.GetConnectionString("akka-discovery");
                if (!string.IsNullOrEmpty(redisConn))
                {
                    b.WithRedisDiscovery(redisConn, config["Akka:Cluster:ServiceName"]);
                }
            },
            clusterConfigure: c => c.Roles = ["sample"])
        .WithWorkflow<OrderFulfillmentWorkflow, OrderState>(() => new OrderFulfillmentWorkflow(
            sp.GetRequiredService<IPaymentService>(),
            sp.GetRequiredService<INotificationService>(),
            sp.GetRequiredService<FaultInjectionRegistry>()))
        // A second WithWorkflow call for a different workflow type on the same ActorSystem — see
        // WorkflowEventPubSubBridge's own doc comment confirming exactly one bridge instance
        // still ends up registered regardless of how many workflow types share it.
        .WithWorkflow<ItemFulfillmentWorkflow, ItemState>(() => new ItemFulfillmentWorkflow(
            sp.GetRequiredService<IInventoryService>(),
            sp.GetRequiredService<IShippingService>()));
}).AddWorkflowClient();

builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(WorkflowDiagnostics.SourceName).AddOtlpExporter())
    .WithMetrics(m => m.AddMeter(WorkflowDiagnostics.SourceName).AddOtlpExporter())
    .WithLogging(l => l.AddOtlpExporter());

var app = builder.Build();

app.UseStaticFiles();
app.UseAntiforgery();
app.MapRazorPages();

// Server-Sent Events push for live updates — the client's EventSource connects here on page load and
// refetches the order list/detail fragments (see wwwroot/app.js) on every message. Each connection
// gets its own Akka.Streams queue fed by a local OrderChangeSignal.Changed subscription. DropHead
// coalesces bursts into "something changed, go refetch" — the client re-reads current state from
// Postgres on whichever signal it receives.
app.MapGet("/orders/stream", (OrderChangeSignal changeSignal, ActorSystem sys) =>
    TypedResults.ServerSentEvents(StreamOrderChanges(changeSignal, sys, app.Lifetime.ApplicationStopping)));

async IAsyncEnumerable<string> StreamOrderChanges(OrderChangeSignal changeSignal, ActorSystem system, [EnumeratorCancellation] CancellationToken ct)
{
    var (queue, source) = Source.Queue<string>(bufferSize: 16, OverflowStrategy.DropHead).PreMaterialize(system);
    void OnChanged() => queue.OfferAsync("changed");
    changeSignal.Changed += OnChanged;
    try
    {
        await foreach (var item in source.RunAsAsyncEnumerable(system).WithCancellation(ct))
        {
            yield return item;
        }
    }
    finally
    {
        changeSignal.Changed -= OnChanged;
        queue.Complete();
    }
}

app.MapHealthChecks("/healthz");
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = c => c.Tags.Contains("liveness") });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("readiness") });

await app.StartAsync();

var system = app.Services.GetRequiredService<ActorSystem>();

// PlaceOrder/Approve/GetOrderState ride the Akka.Delivery producer (see WithWorkflow), which queues
// a send until the target shard region is routable.
var eventLogger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("WorkflowEvents");
var readModel = app.Services.GetRequiredService<OrderReadModelRepository>();
var changeSignal = app.Services.GetRequiredService<OrderChangeSignal>();
var eventLoggerActor = system.ActorOf(Props.Create(() =>
    new WorkflowEventLoggerActor(eventLogger, readModel, changeSignal)));

// Subscribes to WorkflowEventPubSubBridge's cluster-wide topic — this replica's UI renders
// every order live regardless of which replica placed it or hosts its entity. Immediate and
// best-effort: a replica that was down for part of a run never sees what it missed.
DistributedPubSub.Get(system).Mediator.Tell(
    new Subscribe(WorkflowEventPubSubBridge.PubSubTopic, eventLoggerActor));

// The durable half of the same feed closes that gap. Reading the recorded events replays every
// workflow this cluster has run into this replica's read model, so a replica joining late — or
// restarting — arrives with a complete view rather than only what happened while it was watching.
//
// Replaying what the journal still holds is what makes this a demonstration of recorded events
// outliving the process that wrote them: the read model is derived data, rebuildable from them.
// Every write below is an upsert keyed by its natural key, so replaying costs duplicate work and
// changes no result.
//
// How far back that reaches is bounded by each instance's most recent restart, since a restart
// reclaims the cycle it closed (guarantee V5). These orders run to a conclusion and never restart,
// so here it reaches their first event; a workflow that cycles would replay its current cycle only.
//
// A deployment that would rather not re-read everything on each start stores the
// WorkflowFeedPosition each item carries, in the same transaction as its own write, and resumes
// from it — see docs/guarantees.md V4.
_ = Task.Run(async () =>
{
    try
    {
        var feed = JournalWorkflowEventFeed.For(system, SqlReadJournal.Identifier);
        await foreach (var recorded in feed.Read())
        {
            eventLoggerActor.Tell(recorded);
        }

        eventLogger.LogInformation("read model caught up from the recorded event feed");
    }
    catch (Exception ex)
    {
        // Catch-up is a convenience for this replica's own view; the live subscription above keeps
        // working regardless, so a failure here leaves the UI showing what it observes from now on.
        eventLogger.LogWarning(ex, "catching up from the recorded event feed failed");
    }
});

await app.WaitForShutdownAsync();
