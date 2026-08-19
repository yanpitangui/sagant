# Sagant

Durable, step-orchestrated workflow engine for .NET. Extend one base class, mark methods as steps
or command handlers, and get retries with compensation, pause/resume, step/workflow timeouts, and
crash-safe recovery — without hand-rolling a state machine or a persistence model.

Sagant is split into a runtime-agnostic core and a pluggable runtime:

| Package | What it is |
|---|---|
| `Sagant` | Workflow authoring: `Workflow<TState>`, effects, settings, `[WorkflowStep]`/`[WorkflowCommandHandler]`. No dependency on any execution engine. See [`docs/workflow-model.md`](docs/workflow-model.md). |
| `Sagant.Runtime.Akka` | Runs a workflow via a persistent Akka.NET `ClusterSharding` entity actor. Implements `Sagant`'s `IWorkflowClient`/`IWorkflowHandle` contract. See [`docs/akka-runtime.md`](docs/akka-runtime.md). |
| `Sagant.SourceGenerators` | Roslyn generator behind `[WorkflowStep]`/`[WorkflowCommandHandler]` — zero-reflection dispatch tables, AOT-friendly. See [`docs/workflow-model.md`](docs/workflow-model.md#the-source-generator). |
| `Sagant.Testing` | `WorkflowTestHarness<TWorkflow, TState>` for testing a workflow's own logic with zero infrastructure — no `ActorSystem`, no persistence. See [`docs/testing.md`](docs/testing.md). |

A different runtime just needs to drive `Workflow<TState>` through the generated dispatch tables
and implement `IWorkflowClient`/`IWorkflowHandle`.

**Docs:** this README is a quickstart — for deeper coverage of the workflow model, design
guidelines, child workflows, the Akka runtime's internals, integration, and testing, see
[`docs/`](docs/).

## Quickstart

Reference `Sagant` and `Sagant.Runtime.Akka`.

**The minimum: one required override, a command handler to start the run, a step to do
something.**

```csharp scaffold=file
public sealed record GreetingState(string Name = "", string? Greeting = null);

public sealed record Greet(string Name);

public partial class GreetingWorkflow : Workflow<GreetingState>
{
    public override GreetingState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<GreetingState> Start(Greet cmd, CommandContext<GreetingState> ctx) =>
        Effects.UpdateState(ctx.State with { Name = cmd.Name }).TransitionTo(Steps.SayHello);

    [WorkflowStep]
    public StepEffect<GreetingState> SayHello(StepContext<GreetingState> ctx) =>
        StepEffects.UpdateState(ctx.State with { Greeting = $"Hello, {ctx.State.Name}!" }).ThenComplete();
}
```

`EmptyState()` is the only member `Workflow<TState>` actually requires — `Settings()`, retries,
timeouts, pause, queries, and child workflows are all opt-in on top of this. `Steps.SayHello` is
generated for you, a typed reference to the `[WorkflowStep]` method above, so a transition never
relies on a magic string.

**Register it:**

```csharp scaffold=statements
services.AddAkka("my-system", builder => builder
    .WithClustering()
    .WithWorkflow<GreetingWorkflow, GreetingState>(() => new GreetingWorkflow()))
    .AddWorkflowClient();
```

**Drive it — the only thing application code ever touches:**

```csharp scaffold=file
public sealed class GreetingService(IWorkflowClient client)
{
    public async Task<string> GreetAsync(string id, string name)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = await client.For<GreetingWorkflow>(id)
            .RunAndAwaitResult<GreetingState>(new Greet(name), cancellationToken: cts.Token);

        return result.State.Greeting!;
    }
}
```

`RunAndAwaitResult` sends the command, waits for the run to reach a terminal status, and returns
its final state — the caller gets the actual greeting back. That's a complete, runnable workflow —
define, register, drive. `IWorkflowClient`/
`IWorkflowHandle<TWorkflow>` are the full public surface a caller ever touches: no `IActorRef`,
`ActorRegistry`, or `ClusterSharding` type leaks into application code.

## A fuller example

Real workflows add retries with failover, dependencies resolved from DI, and reads that don't wait
on a running step. Here's one that does, built from the same three handler kinds above:

```csharp scaffold=file
public partial class OrderFulfillmentWorkflow : Workflow<OrderState>
{
    public override OrderState EmptyState() => OrderState.Empty();

    public override WorkflowSettings Settings() => WorkflowSettings.Create()
        .DefaultStepTimeout(TimeSpan.FromSeconds(5))
        .StepRecovery(Steps.ChargePaymentStep, RecoverStrategy.WithMaxRetries(2).FailoverTo(Steps.RefundPaymentStep))
        .Build();

    [WorkflowCommandHandler]
    public CommandEffect<OrderState> Start(PlaceOrder cmd, CommandContext<OrderState> ctx) =>
        Effects.UpdateState(ctx.State with { Amount = cmd.Amount })
            .TransitionTo(Steps.ChargePaymentStep)
            .ThenReply("accepted");

    [WorkflowStep]
    public async Task<StepEffect<OrderState>> ChargePaymentStep(StepContext<OrderState> ctx)
    {
        var paymentId = await _payment.Charge(ctx.State.CustomerId, ctx.State.Amount, ctx.CancellationToken);
        return StepEffects.UpdateState(ctx.State with { PaymentId = paymentId }).ThenComplete();
    }

    [WorkflowStep]
    public async Task<StepEffect<OrderState>> RefundPaymentStep(StepContext<OrderState> ctx)
    {
        await _payment.Refund(ctx.State.PaymentId!, ctx.CancellationToken);
        return StepEffects.ThenFail("payment was refunded");
    }

    [WorkflowQuery]
    public QueryEffect Progress(GetProgress query, QueryContext<OrderState> ctx) =>
        QueryEffects.Reply(ctx.State.Status);
}
```

Registration is the same shape, with the factory now resolving `IPaymentService` from DI — the
`(builder, IServiceProvider)` overload of `AddAkka` exists for exactly this, where the minimal
example above had no dependency to resolve and used the plain `Action<AkkaConfigurationBuilder>`
overload instead:

```csharp scaffold=statements
services.AddSingleton<IPaymentService, RealPaymentService>();

services.AddAkka("my-system", (builder, sp) => builder
    .WithClustering()
    .WithWorkflow<OrderFulfillmentWorkflow, OrderState>(() =>
        new OrderFulfillmentWorkflow(sp.GetRequiredService<IPaymentService>())))
    .AddWorkflowClient();
```

Driving it looks the same too, just with a typed reply this time:

```csharp scaffold=file
public sealed class OrderPlacementService(IWorkflowClient client)
{
    public async Task<string> PlaceAsync(string orderId, int amount)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        return await client.For<OrderFulfillmentWorkflow>(orderId)
            .Request<PlaceOrder, string>(new PlaceOrder(amount), cts.Token);
    }
}
```

See [`docs/workflow-model.md`](docs/workflow-model.md#the-source-generator) for how the generator
works, and [`docs/workflow-model.md#effects-applied-by-the-driver`](docs/workflow-model.md#effects-applied-by-the-driver)
for the full set of transitions a handler can produce (pause, delete, await-children, and so on —
not just the transition-to-next-step and end shown above).

## Three handler kinds

| Attribute | Signature | Runs |
|---|---|---|
| `[WorkflowCommandHandler]` | `(TCommand, CommandContext<TState>) -> CommandEffect<TState>` | Synchronously, on the workflow's own thread. Decides; never does I/O. |
| `[WorkflowStep]` | `(TInput?, StepContext<TState>) -> StepEffect<TState>` | Off-thread, with retries, timeouts and cancellation. Where I/O lives. |
| `[WorkflowQuery]` | `(TQuery, QueryContext<TState>) -> QueryEffect` | Off-thread, read-only, concurrently with a running step. |

Steps and queries may be declared synchronously or as `Task<...>`; the generator adapts either.
Command handlers are synchronous by design — a command that needs external data validates in the
caller, or accepts and transitions into a step that does the work.

State reaches a handler as a value on its context, never as shared instance state, so a step
suspended at an `await` and a handler dispatched while it waits cannot observe each other.

`QueryEffect` carries a reply and nothing else — no persistence, no transition. That's a compile-time
property, and it's what lets a query dispatch immediately, with no running step to queue behind:
there's no write for it to race with. Reach for a query for anything a caller reads (a live progress
view, a dashboard poll); reach for a command when the workflow should move.

The engine never checks the workflow's current *state* itself before routing a command to its
handler by type: guarding which states a command is valid from is the handler's own job — inspect
`ctx.State` and return a `NoPersistence`/no-transition effect (e.g. a rejection reply) when
called somewhere it shouldn't be. Skip that guard and the handler will happily run — and
transition/update state — even after the workflow has already reached a terminal state.

The reference Akka.NET runtime additionally guarantees a command handler never runs against a state a
step still in flight is about to supersede — see
[`docs/akka-runtime.md#command-dispatch-waits-for-an-in-flight-step-to-settle`](docs/akka-runtime.md#command-dispatch-waits-for-an-in-flight-step-to-settle).

Retries, backoff, per-step timeout overrides, and pause-with-timeout are all configured through
`Settings()` — see [`docs/workflow-model.md#settings-retries-and-pause`](docs/workflow-model.md#settings-retries-and-pause).

Three verbs on a handle, between the two you've already seen: `Send` mutates without waiting (the
`GreetingService` example), `Request` mutates and returns a reply (`OrderPlacementService`), and
`Query` observes:

```csharp scaffold=statements
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
var status = await client.For<OrderFulfillmentWorkflow>(orderId)
    .Query<GetProgress, OrderStatus>(new GetProgress(), cts.Token);
```

A query takes a different route from a command — delivered directly, bypassing the at-least-once
machinery entirely (guaranteed delivery of a read buys nothing), and dispatched with no wait
for a running step. It also carries no idempotency key: replaying a read has no side effect to
deduplicate. Its handler is bounded by the workflow's own `DefaultQueryTimeout`, because a caller's
timeout ends the caller's wait and never reaches the workflow.

`IWorkflowClient`/`IWorkflowHandle<TWorkflow>` are the full public surface. No `IActorRef`,
`ActorRegistry`, or ClusterSharding types in application code. See
[`docs/integration-guide.md`](docs/integration-guide.md) for single-node vs. multi-node clustering,
joining the cluster, and deployment-level tuning (shard count, idle-entity passivation, etc.), and
[`docs/akka-runtime.md`](docs/akka-runtime.md) for how `WorkflowEntityActor` actually drives all of
this — persistence, timeouts, retries, `Akka.Delivery`, and graceful shutdown.

## Child workflows

A step can start other workflow instances as children and durably wait for their outcomes:

```csharp scaffold=workflow-member
[WorkflowStep]
public StepEffect<OrderState> StartLineItemWorkflows(StepContext<OrderState> ctx)
{
    var children = ctx.State.LineItems.Select(item =>
        StepEffects.Child<LineItemWorkflow>(item.Sku, new ProcessLineItem(item.Sku, item.Quantity)));

    return StepEffects.AwaitChildren(children, Steps.OnLineItemsDone);
}

[WorkflowStep]
public StepEffect<OrderState> OnLineItemsDone(ChildGroupResult result, StepContext<OrderState> ctx) =>
    result.Outcome == GroupOutcome.Succeeded
        ? StepEffects.UpdateState(ctx.State with { LineItems = result.GetAll<LineItemWorkflow, LineItemState>().Values.ToList() }).ThenComplete()
        : StepEffects.ThenFail("line item failure");
```

By default, every child must succeed (`CompletionPolicy.AllSuccessful`), the group fails fast on the
first failure (`FailurePolicy.FailFast`), and remaining children are terminated
(`RemainingChildrenPolicy.Terminate`) — all three are independently configurable via the
`Action<ChildGroupOptions>` overload of `AwaitChildren`. See [`docs/child-workflows.md`](docs/child-workflows.md)
for the full model, including heterogeneous groups, `ParentClosePolicy`, and durability guarantees.

## Testing a workflow

`WorkflowTestHarness<TWorkflow, TState>` (in `Sagant.Testing`) drives a workflow's own step/command
logic directly — no `ActorSystem`, no persistence, no ClusterSharding:

```csharp scaffold=statements
var harness = new WorkflowTestHarness<OrderFulfillmentWorkflow, OrderState>(
    new OrderFulfillmentWorkflow(fakePaymentService));

var effect = await harness.RunUntilStop(new PlaceOrder(500));

Assert.IsType<Transition.TerminalTransition>(effect.Transition);

var status = await harness.RunQuery<GetProgress, OrderStatus>(new GetProgress());
```

A step that throws is retried against its configured `RecoverStrategy`, then fails over once the
retry budget is exhausted — retry/backoff/failover policy is testable directly here, in
milliseconds, with no `ActorSystem`.

The harness takes a `TimeProvider`, so a paused workflow's timeout is testable too: advance a
`FakeTimeProvider` past `PauseSettings.Timeout` and call `RunPauseTimeoutIfDue()` to assert it
auto-transitions into the configured handler step. See [`docs/testing.md`](docs/testing.md) for
retries, workflow-level timeouts, and testing child workflows.

## Observability

Sagant fully supports OpenTelemetry: every command and step execution is a span, correlated into one
trace per workflow run. Point an OTLP exporter at it and you get full step-by-step tracing with no
extra instrumentation code. `samples/OrderFulfillment` does exactly this — run the AppHost and watch
every order's traces live in the Aspire dashboard. See
[`docs/integration-guide.md#observability`](docs/integration-guide.md#observability) for the DI
wiring, and [`docs/akka-runtime.md#tracing`](docs/akka-runtime.md#tracing) for how spans stay
correlated across retries, crashes, and cluster relocation.

## The worked example

`samples/OrderFulfillment` is a full saga: multi-step orchestration across four simulated
services, retries with compensation cascades, pause-for-approval with a timeout handler, step and
workflow-level timeouts — all exercised through the real `IWorkflowClient`/`ClusterSharding` path
in `OrderFulfillment.Tests`, and rendered live in `OrderFulfillment.Sample` (a 3-node cluster with
a Razor Pages + Server-Sent Events UI on every replica: place an order, watch steps
execute/retry/compensate in real time, approve a paused order). See
[`docs/integration-guide.md#the-worked-example`](docs/integration-guide.md#the-worked-example) for
a walkthrough of its host wiring.

## License

MIT
