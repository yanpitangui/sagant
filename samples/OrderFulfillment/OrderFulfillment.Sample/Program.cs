using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Deadlines;
using Sagant.Scheduling;
using Sagant.Protocol;
using Akka.Aspire;
using Akka.Discovery.Redis;
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
// IHealthChecksBuilder by WithAspireClusterBootstrap below (see Akka.Aspire) — this call just
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
        .WithWorkflow<OrderFulfillmentWorkflow, OrderState>(
            () => new OrderFulfillmentWorkflow(
                sp.GetRequiredService<IPaymentService>(),
                sp.GetRequiredService<INotificationService>(),
                sp.GetRequiredService<FaultInjectionRegistry>()),
            // Ten seconds, well under the 120-second default, so an order awaiting approval
            // passivates halfway through its 20-second approval window. The deadline scheduler
            // registered below is then what brings it back to auto-cancel — the demo shows a
            // deadline firing for an instance that has already left memory by the time it lands.
            configureShardOptions: options => options.PassivateIdleEntityAfter = TimeSpan.FromSeconds(10))
        // A second WithWorkflow call for a different workflow type on the same ActorSystem — see
        // WorkflowEventPubSubBridge's own doc comment confirming exactly one bridge instance
        // still ends up registered regardless of how many workflow types share it.
        .WithWorkflow<ItemFulfillmentWorkflow, ItemState>(() => new ItemFulfillmentWorkflow(
            sp.GetRequiredService<IInventoryService>(),
            sp.GetRequiredService<IShippingService>()))
        // Registers the schedule workflow. Ten seconds idle, well under the two minutes between
        // occurrences, so the schedule really is gone while it waits and the deadline scheduler
        // below is what brings it back — which is the thing worth watching.
        .WithScheduling(sp, configureShardOptions: o => o.PassivateIdleEntityAfter = TimeSpan.FromSeconds(10))
        // Reads every instance's deadlines out of the same journal the workflows write to, and wakes
        // one as its own comes due. This is what makes the passivation window above safe: an order
        // waiting for approval releases its memory and still auto-cancels on time.
        .WithWorkflowDeadlines(
            SqlReadJournal.Identifier,
            // Below the 10-second passivation window, so a deadline landing inside that window is
            // left to the instance's own timer and everything past it is recorded here.
            settings => settings.ExternalArmThreshold = TimeSpan.FromSeconds(5));
}).AddWorkflowClient().AddWorkflowDeadlines();

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
// restarting — arrives with a complete view, going all the way back, well beyond only what
// happened while it was watching.
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

// A standing order placed every fifteen seconds, by a schedule that is itself a workflow.
//
// Worth watching for what it costs: between occurrences the schedule holds a pause with a deadline
// fifteen seconds out, past this deployment's ten-second passivation window, so it releases its
// memory and the deadline scheduler brings it back. Nothing is resident while it waits.
//
// The same command every time, so each occurrence's own entity id is what separates the runs — and
// that id comes from the instant the occurrence was scheduled for, so a fire that happens twice
// still lands on that same one order, never placing a second.
//
// Idempotent on a restart: the schedule has a fixed id, so a replica coming up sends StartSchedule
// to the instance that already exists and replaces its spec there, with no second one started.
_ = Task.Run(async () =>
{
    try
    {
        var client = app.Services.GetRequiredService<IWorkflowClient>();

        // Asked, so a schedule that never reaches its entity says so here — a plain sent gets no
        // such answer. A
        // fire-and-forget send would sit buffered behind a shard region still finding its
        // coordinator and report nothing, which reads as a schedule that simply never fires.
        var accepted = await client.For<ScheduleWorkflow>("standing-order").Request<StartSchedule, string>(
            StartSchedule.For<OrderFulfillmentWorkflow>(
                spec: new EverySpec(TimeSpan.FromSeconds(15)),
                command: new PlaceOrder(
                    CustomerId: "standing-order-customer",
                    // An array: targeting IReadOnlyList<T> with a collection expression instead
                    // compiles to a compiler-generated list type the JSON serializer cannot
                    // construct on the way back. A schedule stores its command and replays it from
                    // the journal, so what it holds has to survive a round trip.
                    Items: new[] { new OrderLineItem("SKU-STANDING", 1) },
                    ShippingAddress: "1 Recurring Way"),
                // Fifteen seconds is close enough to how long an order takes that this matters: an
                // occurrence arriving while the previous one is still running is passed over,
                // never run alongside it. A skipped occurrence is counted, so the schedule's status
                // says so.
                overlap: OverlapPolicy.Skip,
                // A replica down for a while places one order on the way back — the catch-up window
                // caps it there, well short of the dozens it slept through.
                catchUpWindow: TimeSpan.FromSeconds(30)),
            timeout: TimeSpan.FromSeconds(90));

        // Read back — genuinely, never assumed — this says when the first occurrence is actually
        // due, which is the difference between a schedule that is waiting and one that was never
        // started.
        var status = await client.For<ScheduleWorkflow>("standing-order")
            .Query<GetScheduleStatus, ScheduleStatus>(new GetScheduleStatus(), TimeSpan.FromSeconds(30));

        eventLogger.LogInformation(
            "standing order schedule {Accepted}; next occurrence at {NextFire}, fired {FireCount} so far",
            accepted, status.NextFireUtc, status.FireCount);
    }
    catch (Exception ex)
    {
        eventLogger.LogWarning(ex, "registering the standing order schedule failed");
    }
});

// The schedule's own account of itself, on a loop. Which of the three numbers moves says where a
// schedule that looks idle actually is: FireCount climbing means it is placing orders and the
// question is downstream, SkippedCount climbing means it is passing occurrences over, and neither
// moving with NextFireUtc in the past means nothing is waking it.
_ = Task.Run(async () =>
{
    var client = app.Services.GetRequiredService<IWorkflowClient>();

    var stopping = app.Lifetime.ApplicationStopping;

    while (!stopping.IsCancellationRequested)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(15), stopping);

            var status = await client.For<ScheduleWorkflow>("standing-order")
                .Query<GetScheduleStatus, ScheduleStatus>(new GetScheduleStatus(), TimeSpan.FromSeconds(10));

            eventLogger.LogInformation(
                "standing order schedule: fired {FireCount}, skipped {SkippedCount}, next at {NextFire} (now {Now:HH:mm:ss})",
                status.FireCount, status.SkippedCount, status.NextFireUtc, DateTimeOffset.UtcNow);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            eventLogger.LogWarning(ex, "reading the standing order schedule's status failed");
        }
    }
});

await app.WaitForShutdownAsync();
