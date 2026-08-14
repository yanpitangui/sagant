using Aaron.Akka.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

// A real Postgres instance backs the demo host's journal/snapshot-store (see OrderFulfillment.Sample's
// Program.cs) — Sagant.Runtime.Akka has no opinion on the persistence backend, so this is pure host
// wiring, nothing engine-side. A fixed password (not Aspire's own per-run random default) keeps a
// reused data volume's baked-in SCRAM auth in sync with what every run actually connects with; no
// WithLifetime(ContainerLifetime.Persistent) here either, so the volume itself is also session-scoped
// rather than surviving across separate `dotnet run`s.
// init-scripts/001-orders-schema.sql creates "orders-db" itself and the sample's whole read-model
// schema (orders/order_items/workflow_views/step_runs/event_log — see OrderReadModelRepository) in
// one docker-entrypoint-initdb.d pass, ahead of AddDatabase's own (idempotent) CREATE DATABASE below
// — see that script's own doc comment for why the ordering has to work that way.
var postgresPassword = builder.AddParameter("postgres-password", "sagant-demo-postgres", secret: true);
var postgres = builder.AddPostgres("postgres", password: postgresPassword)
    .WithDataVolume()
    .WithArgs("-c", "max_connections=500")
    .WithInitFiles("./init-scripts");
var ordersDb = postgres.AddDatabase("orders-db");

// Redis backs Akka.Management's ClusterBootstrap peer discovery for the 3 replicas below (see
// Aaron.Akka.Aspire.Hosting/Aaron.Akka.Discovery.Redis in OrderFulfillment.Sample's Program.cs).
// Default (non-persistent) container lifetime: each replica's entry here is only valid for that
// specific run's dynamically-assigned ports. Aaron.Akka.Discovery.Redis deregisters a replica's own
// entry on graceful shutdown only, so a fresh container every run is what keeps a prior run's dead
// entries from ever piling up — ClusterBootstrap waits on every discovered entry, live or not.
var akkaDiscovery = builder.AddRedis("akka-discovery");

var akka = builder.AddAkka("order-fulfillment-cluster")
    .WithClustering(akkaDiscovery);

// 3 symmetric replicas, each a full node: Razor Pages UI, ClusterSharding worker, everything. Pages
// render statelessly per request straight from OrderStore.Snapshot() and push live updates over
// Server-Sent Events, so any replica can answer any request — Aspire's ordinary proxied+replicated
// endpoint (round-robin per request) works fine. See this sample's README for the full architecture.
builder.AddProject<Projects.OrderFulfillment_Sample>("order-fulfillment-sample")
    .WithReference(ordersDb)
    .WaitFor(ordersDb)
    .WithReference(akka)
    .WithHttpEndpoint(name: "http")
    .WithReplicas(3);

builder.Build().Run();
