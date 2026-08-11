# Integration guide

How to wire Sagant's Akka.NET runtime into a real host — package references, DI registration,
single-node and multi-node clustering, and observability. For how the runtime works internally, see
[akka-runtime.md](akka-runtime.md).

## Packages

Reference `Sagant` (workflow authoring) and `Sagant.Runtime.Akka` (the runtime). Both bring in
`Sagant.SourceGenerators` transitively — a consumer only ever adds these two `PackageReference`s,
never a separate analyzer package.

## Registering a workflow

`WithWorkflow<TWorkflow, TState>` registers one workflow type on `ClusterSharding`, via
`Akka.Hosting`'s fluent `AkkaConfigurationBuilder`. Use the `(builder, IServiceProvider)` overload
of `AddAkka`, not the plain `Action<AkkaConfigurationBuilder>` one, whenever the workflow factory
needs to resolve its own dependencies from DI:

```csharp scaffold=statements
services.AddSingleton<IPaymentService, RealPaymentService>();

services.AddAkka("my-system", (builder, sp) => builder
    .WithRemoting("localhost", 0)
    .WithClustering()
    .WithWorkflow<OrderFulfillmentWorkflow, OrderState>(() =>
        new OrderFulfillmentWorkflow(sp.GetRequiredService<IPaymentService>())))
    .AddWorkflowClient();
```

`AddWorkflowClient()` must be called on the `IServiceCollection` `AddAkka(...)` returns — i.e.
chained directly after it, as above. `AddAkka` doesn't invoke the configuration callback
synchronously; it registers a lazy `ActorSystem` factory and only runs the callback when something
first resolves `ActorSystem` from DI, in practice during `host.StartAsync()` — by which point
`IServiceCollection` is already read-only. Calling `AddWorkflowClient` right after `AddAkka` (not
from inside the callback) avoids that entirely; `IWorkflowClient`'s actual construction stays fully
lazy regardless.

## Joining the cluster

Join `Akka.Cluster` however the deployment already does — self-join for a single node, seed-nodes
or a discovery mechanism (e.g. `Akka.Discovery`/Aspire service discovery) for multi-node. The one
Sagant-specific requirement: wait for `MemberStatus.Up` before resolving `IWorkflowClient` and
sending traffic. `samples/OrderFulfillment`'s `AppHost`/`Program.cs` shows a full production-shaped
3-node setup — `Akka.Persistence.Sql` against Postgres, Redis-backed discovery for seed-node lookup,
and `WithAspireClusterBootstrap` registering cluster-membership/actor-system liveness checks into
`IHealthChecksBuilder`.

For local development or tests, `WithInMemoryJournal()`/`WithInMemorySnapshotStore()` avoid standing
up a real persistence backend:

```csharp scaffold=statements
builder
    .WithInMemoryJournal()
    .WithInMemorySnapshotStore()
    .WithRemoting("localhost", 0)
    .WithClustering()
    .WithWorkflow<EchoWorkflow, EchoState>(() => new EchoWorkflow());
```

An in-memory journal loses all persisted state on process exit — fine for a test or a demo, not for
anything meant to survive a restart.

## Driving a workflow

`IWorkflowClient`/`IWorkflowHandle<TWorkflow>` are the entire public surface application code should
ever touch — no `IActorRef`, `ActorRegistry`, or `ClusterSharding` type leaks past them:

```csharp scaffold=file
public sealed class OrderPlacementService(IWorkflowClient client)
{
    public Task<string> PlaceAsync(string orderId, int amount) =>
        client.For<OrderFulfillmentWorkflow>(orderId)
            .Request<PlaceOrder, string>(new PlaceOrder(amount), TimeSpan.FromSeconds(10));
}
```

`IWorkflowHandle<TWorkflow>` also exposes `Send` (fire-and-forget) and `RunAndAwaitResult` (wait for
the workflow to reach a terminal status and return its final state, typed). Pass an `idempotencyKey`
to `Send`/`Request`/`RunAndAwaitResult` for a caller-driven retry after an ambiguous outcome to
replay the cached reply instead of re-invoking the handler — see
[akka-runtime.md](akka-runtime.md#akkadelivery-and-idempotency).

## Deployment-level tuning

Cluster/infra decisions, passed to `WithWorkflow` itself, separate from `WorkflowSettings`
(business-level configuration a workflow author sets — see
[workflow-model.md](workflow-model.md#settings-retries-and-pause)):

```csharp scaffold=statements
builder.WithWorkflow<OrderFulfillmentWorkflow, OrderState>(
    () => new OrderFulfillmentWorkflow(sp.GetRequiredService<IPaymentService>()),
    numberOfShards: 200,
    gracefulShutdownGrace: TimeSpan.FromSeconds(20),
    configureShardOptions: options => options.PassivateIdleEntityAfter = TimeSpan.FromMinutes(10));
```

`WithWorkflow` disables idle passivation by default, because a workflow legitimately sits idle while
holding a deadline and a live timer belongs to a live entity — re-enabling it, as above, trades
bounded timer lateness for memory. See
[akka-runtime.md](akka-runtime.md#deployment-level-configuration).

See [akka-runtime.md](akka-runtime.md#deployment-level-configuration) for the full parameter list.

## Observability

Sagant emits standard `System.Diagnostics` `Activity` spans and `Meter` metrics under
`WorkflowDiagnostics.SourceName` — point OpenTelemetry at it and every command/step execution shows
up as a span, correlated into one trace per workflow run, with no extra instrumentation code in the
workflow itself:

```csharp scaffold=statements
services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(WorkflowDiagnostics.SourceName).AddOtlpExporter())
    .WithMetrics(m => m.AddMeter(WorkflowDiagnostics.SourceName).AddOtlpExporter());
```

`samples/OrderFulfillment` wires exactly this against an OTLP exporter — run its `AppHost` and watch
every order's traces live in the Aspire dashboard.

## The worked example

`samples/OrderFulfillment` is the reference for how a real multi-step workflow with compensation
cascades and pause-for-approval should be built:

- `OrderFulfillment.AppHost` — Aspire host, 3 symmetric replicas, Postgres, OTLP collector.
- `OrderFulfillment.Sample` — Razor Pages + Server-Sent Events UI on every replica (stateless
  per-request rendering, no per-connection server state, so Aspire's ordinary round-robin proxy
  works with no session affinity needed).
- `OrderFulfillment.Tests` — integration tests against the real `IWorkflowClient`/`ClusterSharding`
  path, not `WorkflowTestHarness`.

Mirror it rather than reinventing patterns when building a new workflow-backed feature.
