# OrderFulfillment sample

A multi-step saga built on Sagant, driven through `IWorkflowClient` into a real
`WorkflowEntityActor`/`ClusterSharding` deployment. See the root
[`README.md`](../../README.md) for how this sample fits into the rest of the repo.

## What this demonstrates

- `OrderFulfillmentWorkflow`: multi-service orchestration (payment, notification), retries,
  step and workflow timeouts, pause-for-approval, and a compensation cascade.
- **Child workflows**: an order splits into one `ItemFulfillmentWorkflow` per line item —
  each reserves inventory and arranges its own shipment as its own durable entity, parented
  under the order via `AwaitChildren`/`ParentClosePolicy`. One item failing fails the whole
  order (refund, cascade-close the rest) — see `OrderFulfillmentWorkflow.FulfillItemsStep`'s
  own doc comments for exactly how the parent reads success/failure back out of the group.
- **Durable deadlines against passivation**: entities passivate after 10 seconds idle, while an
  order awaiting approval waits 20 for its auto-cancel. So the order releases its memory
  halfway through that wait, and `WithWorkflowDeadlines(SqlReadJournal.Identifier)` is what
  brings it back on time — a deadline firing for an instance that is no longer in memory.
  Place an order over the approval threshold, leave it alone, and watch it auto-cancel at 20
  seconds despite nothing having touched it since.
- **A schedule that is itself a workflow** (`Sagant.Scheduling`): a standing order placed every
  fifteen seconds. Between occurrences it holds a pause with a deadline past the ten-second
  passivation window, so it releases its memory and the deadline scheduler brings it back — the
  same mechanism the approval auto-cancel above uses, applied to something that recurs forever.
  Each occurrence's entity id comes from the instant it was scheduled for, so a fire that happens
  twice lands on the same order rather than placing two. Its overlap policy is `Skip`, so an
  occurrence arriving while the previous order is still running is counted as skipped rather than
  run alongside it — visible in the schedule's own status.
- A real 3-node Akka.NET cluster, not a single self-joining process. Each replica is a
  full, symmetric node: UI and `ClusterSharding` worker together, for both workflow types.
  An order's (or an item's) entity can be hosted on any of the 3, and `ClusterSharding`
  moves it if that node goes away — the "chaos exercise" below makes this visible.
- Cluster-wide live observability over **both halves of the visibility seam**: every
  `WorkflowFeedItem` (a recorded `WorkflowEvent` plus the `TransitionCause` naming what
  drove it) reaches every replica's UI immediately, and on startup each replica replays
  the recorded events through `IWorkflowEventFeed` so one joining late or restarting
  arrives with a complete view. Business status comes off `UserStateChanged<OrderState>`
  in the feed, so the read model needs no round trip to the entity at all. Live delivery
  reaches every replica regardless
  of which replica is hosting the entity it's about, via a `DistributedPubSub` bridge
  (`Sagant.Runtime.Akka.Clustering.WorkflowEventPubSubBridge`) — an order placed
  through one replica's UI is watchable from any other replica too.
- **A durable, Postgres-backed read model** (`OrderReadModelRepository`) — no per-replica
  in-memory cache, no cluster-singleton registry to backfill from. Every replica reads and
  writes the same shared tables, so a fresh replica's very first read already sees every
  order any other replica has ever placed.
- One correlated distributed trace per workflow run, spanning every step/retry/command,
  tagged with which node actually executed each span (`workflow.node`) — visible in the
  Aspire dashboard's Traces view.
- Stateless server-rendered UI, deliberately: Razor Pages render straight from
  `OrderReadModelRepository` on every request, and a plain Server-Sent Events endpoint
  (`/orders/stream`) pushes "something changed, go refetch" signals to a small
  vanilla-JS client. No per-connection server-side state (no SignalR circuit, no
  WebSocket) — see the Architecture section for why that choice, not Blazor Server,
  is what makes symmetric replicas behind Aspire's ordinary load-balancing proxy work
  at all.
- **Delete**: a terminal order (succeeded/failed/cancelled) can be purged —
  `IWorkflowHandle.Delete()` cascades to any still-running item child
  (`ParentClosePolicy.Terminate`), and the read model tombstones the row rather than
  querying the now-purged entity again (see `OrderReadModelRepository.SoftDeleteAsync`'s
  own doc comment for why that matters).

## Architecture

```
                         ┌─────────────────────────┐
                         │   OrderFulfillment.AppHost │  (Aspire)
                         └───────────┬─────────────┘
                 ┌───────────────────┼───────────────────┐
                 ▼                   ▼                   ▼
           ┌──────────┐       ┌──────────┐        ┌─────────────┐
           │ postgres │       │  akka-   │        │  order-fulfillment- │
           │(orders-db)│      │discovery │        │  sample × 3 replicas │
           └────┬─────┘       │ (Redis)  │        └──────────┬──────────┘
                │             └────┬─────┘                   │
                │                  │                          │
                └──────────────────┴──────────────────────────┘
        journal/snapshots AND      cluster peer discovery   1 Akka.Cluster,
        the read model (shared)    (Akka.Management          3 members
                                     ClusterBootstrap)
```

- **`postgres`** backs both the journal/snapshot-store (`Akka.Persistence.Sql`) and the
  sample's own read model (`OrderReadModelRepository`, via LinqToDB) — one shared instance,
  not per-replica. `WithLifetime(ContainerLifetime.Persistent)` keeps it (and its data)
  across restarts. The read-model schema (`orders`/`order_items`/`workflow_views`/
  `step_runs`/`event_log`) is created by `init-scripts/001-orders-schema.sql`, mounted via
  `WithInitFiles` — see that script's own doc comment for exactly when it runs and why (it
  also creates the `orders-db` database itself, ahead of Aspire's own `AddDatabase` call).
- **`akka-discovery`** (Redis) is purely cluster peer-discovery plumbing — stateless,
  disposable. Aspire assigns each of the 3 replicas a dynamic port with no fixed "node 0"
  address to hardcode as a seed node, so peer discovery goes through
  [`akkadotnet/Akka.Management`](https://github.com/akkadotnet/Akka.Management)'s
  Redis-backed `Akka.Management.Cluster.Bootstrap` integration instead of manual
  seed-nodes HOCON.
- **`order-fulfillment-sample`** — 3 identical replicas (`WithReplicas(3)` in the
  AppHost), sitting behind Aspire's ordinary proxied endpoint. Each is a full node:
  Razor Pages UI, `ClusterSharding` worker for both `OrderFulfillmentWorkflow` and
  `ItemFulfillmentWorkflow`, and a `WorkflowEventPubSubBridge` subscriber. All 3
  join the same `Akka.Cluster`.
- **Why Razor Pages + SSE, not Blazor Server**: this sample used Blazor Server first.
  Blazor Server's SignalR circuit lives in-process on whichever replica served the
  page — a browser's later WebSocket upgrade has to reach that exact same process, and
  Aspire's replica proxy has no session affinity, so it would round-robin the upgrade
  onto a different replica that had never heard of that circuit (`WebSocket failed to
  connect` / `No Connection with that ID`). A single non-replicated frontend sidesteps
  that, but then only one replica ever runs any UI at all. Razor Pages renders
  statelessly per request — any replica can answer any request — and SSE is a plain,
  stateless HTTP push with the same property, so symmetric replicas behind an ordinary
  proxy just work, no session affinity needed anywhere.
- **Why a Postgres read model, not a per-replica cache**: every replica used to keep its
  own in-memory `OrderStore`, backfilled at startup from a `ClusterSingletonManager`-hosted
  registry actor (to cover the gap between a replica's own `DistributedPubSub` subscription
  finishing and orders placed before that). Moving the read model into the same shared
  Postgres instance the journal already uses removes that gap (and the registry/backfill
  machinery) entirely — every replica already shares one source of truth from the moment an
  order is placed. `WorkflowEventLoggerActor` still subscribes to the same
  cluster-wide pub-sub topic (a separate concern from the storage backend — that's
  cross-node event delivery), and writes into Postgres, with
  natural-key upserts (`step_runs`) / idempotent inserts (`event_log`) absorbing the
  N-way duplicate delivery every replica's subscriber sees for the same notification.

## Running it

```bash
dotnet run --project samples/OrderFulfillment/OrderFulfillment.AppHost
```

Open the Aspire dashboard URL printed at startup. You'll see `postgres`,
`akka-discovery`, and 3 `order-fulfillment-sample` instances in the resource list. Click
any `order-fulfillment-sample` replica's endpoint to open the UI — placing a multi-item
order, approving a paused one, and watching live step progress (including each item
child's own inline-nested pipeline) works identically from any of the 3.

## What to watch for

- **3 separate replicas**, not 1 — each with its own endpoint, its own Structured Logs
  stream, its own process.
- **Cross-replica visibility**: place an order from replica A's UI, open replica B's UI
  in another tab — the same order shows up there too, live, as its steps execute.
- **Item children render inline**, indented under the "Fulfill items" step — click into
  a multi-item order to watch each item's own reserve/ship pipeline run independently.
- **One trace per workflow run** in the Traces view, even though steps/retries can
  execute on different nodes — the engine's `LastTraceParent` design links them as one
  trace, not fragments.
- **Duplicate log lines are expected, not a bug**: every replica's `WorkflowFeedItem`
  handler writes to the same shared Postgres tables from the same cluster-wide broadcast —
  step-run/log writes are natural-key upserts/idempotent inserts, so N-way duplicate
  delivery converges to one row, not N. What you *will* see duplicated is the structured
  log line in each replica's own OTLP export (one per replica, by design — that's local
  observability, not the read model).

## Chaos mini-exercise: watch an entity rebalance

1. Place an order.
2. In the Aspire dashboard's **Traces** view, find a span for that order and check its
   `workflow.node` tag — that's the replica currently hosting the entity.
3. Stop that replica from the Aspire dashboard while the order is mid-flight.
4. Watch the order keep progressing (from another replica's UI) — `ClusterSharding`
   reactivates the entity on a surviving node, resuming from the last persisted state
   in `postgres` — crash-safe recovery. Same for any item child currently reserving/
   shipping on that node.
5. Check the `workflow.node` tag on the next span for that order — it's now a different
   node.

## Troubleshooting

- **Nothing loads on first run**: `postgres` and `akka-discovery` take a few seconds to
  become healthy; `WaitFor(ordersDb)` blocks the sample's startup on Postgres already,
  but the very first `dotnet run` after pulling images can still take a minute.
- **Port conflicts**: if the Aspire dashboard's default port is already bound by
  something else, Aspire will fail to start — free the port or configure a different one
  via `ASPNETCORE_URLS`/`DOTNET_ASPIRE_DASHBOARD_URL`.
- **Schema changes not showing up**: `init-scripts/001-orders-schema.sql` only runs the
  first time the Postgres data volume is created — a change to it after that needs the
  volume wiped (`docker volume rm`, or delete it from Docker Desktop) to take effect.
- **Live updates stop working**: check the browser console for `/orders/stream` errors.
  Each replica serves its own SSE stream, keyed off the same Postgres-backed read model;
  a browser reconnects automatically (built into `EventSource`) if the replica it was
  talking to restarts.
