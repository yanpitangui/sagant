# Durable deadlines and recurring schedules

Two extensions live near each other in `Sagant.Runtime.Akka`/`Sagant.Scheduling.Akka`, are easy to
reach for at the same time, and solve unrelated problems:

- **`WithWorkflowDeadlines`** — engine infrastructure. Makes *any* registered workflow's deadline
  (step timeout, workflow timeout, pause timeout, hold timeout) fire close to its instant even after
  the instance has passivated and released its memory. You write no business logic for this; it's a
  deployment-level knob, the same tier as `numberOfShards`.
- **`WithScheduling`** — a business feature. Registers `ScheduleWorkflow`, a workflow built into
  `Sagant.Scheduling` whose whole job is starting *other* workflows on a recurring cadence — "place a
  standing order every 15 seconds," "run a reconciliation pass daily at 02:00." You configure what it
  starts and when.

## Not the same thing

| | `WithWorkflowDeadlines` | `WithScheduling` |
|---|---|---|
| What it registers | A journal projection + one `ClusterSingleton` wake service | `ScheduleWorkflow` on `ClusterSharding`, via an ordinary `WithWorkflow` call underneath |
| Applies to | Every workflow type registered on the `ActorSystem` | Whichever workflow type(s) a schedule targets |
| The problem it solves | A passivated instance has no live timer — guarantee `D8` | There's no built-in way to run a workflow repeatedly on a timer at all |
| What you configure | Read-journal plugin id, wake-rate/threshold tuning | Which workflow to start, on what spec, with what command |
| Skip it and | Deadlines still fire, but late — whenever something next reactivates the instance | Nothing — there's no schedule without it |

The connection between them: `ScheduleWorkflow` spends most of its life paused, waiting for the next
occurrence (`WaitStep`, below) — it is itself an ordinary workflow holding an ordinary pause
deadline, so it benefits from `WithWorkflowDeadlines` exactly the way any workflow you write would.
Register both together so a schedule keeps firing on time after passivating between occurrences,
rather than only when something else happens to touch it:

```csharp scaffold=statements
services.AddAkka("my-system", (b, provider) =>
{
    b.WithClustering()
        .WithWorkflow<OrderFulfillmentWorkflow, OrderState>(() => new OrderFulfillmentWorkflow(provider.GetRequiredService<IPaymentService>()))
        .WithScheduling(provider)
        .WithWorkflowDeadlines("akka.persistence.query.journal.sql");
})
.AddWorkflowClient()
.AddWorkflowDeadlines();
```

## Durable deadlines — `WithWorkflowDeadlines`

Without it, an instance that passivates while holding a deadline further out than
`PassivateIdleEntityAfter` (see
[akka-runtime.md#idle-passivation](akka-runtime.md#idle-passivation)) only fires that deadline when
something else next activates the instance — guarantee `D8`. `WithWorkflowDeadlines` closes that
gap: a projection reads each instance's own persisted deadline out of the journal into an index, and
a `ClusterSingleton` wakes an instance as its own comes due, via a normal message send — guarantee
`D8b`. The wake carries no instruction and writes nothing: it just activates the instance, which
re-arms its own deadline from its persisted envelope and fires it if already past. A wake that
arrives twice costs a spare activation; one that's dropped is retried.

```csharp scaffold=statements
builder.WithWorkflowDeadlines(
    "akka.persistence.query.journal.sql",
    configureSettings: settings => settings.MaxWakesPerSecond = 200);
```

| Parameter | Default | What it does |
|---|---|---|
| `readJournalPluginId` | — (required) | Which read journal the projection follows, e.g. `akka.persistence.query.journal.sql` (`SqlReadJournal.Identifier`). Its plugin must implement `IEventsByTagQuery`. |
| `configureSettings` | leaves every `WorkflowDeadlineSettings` default in place | Callback over `WorkflowDeadlineSettings` — tune wake rate, thresholds, and retry behavior. |

`WorkflowDeadlineSettings` fields, all optional to touch:

| Field | Default | What it does |
|---|---|---|
| `ExternalArmThreshold` | `1 minute` | How far out a deadline has to be before it's worth recording in the index at all — a nearer one is served by the instance's own live timer, which is exact and free. **Keep this below `PassivateIdleEntityAfter`**: the default pairs a one-minute threshold with the 120-second passivation window, leaving margin for the write-to-projection delay. |
| `MaxWakesPerSecond` | `50` | Ceiling on wakes handed to the cluster per second — the wake itself is cheap, but each one starts whatever work the deadline triggers, so this bounds that downstream load. |
| `WakeBurst` | `10` | How far `MaxWakesPerSecond` may be exceeded momentarily. |
| `MaxWakesInFlight` | `16` | How many wakes may be outstanding at once. Each waits for an instance to activate and replay its journal, so this self-throttles against a cluster that's currently slow to do that. |
| `WakeTimeout` | `20 seconds` | How long one wake waits before the scheduler moves on, leaving the entry armed for a later attempt. |
| `RetryBackoff` | `30 seconds` | Gap before a still-live entry that already fired comes up again — firing repeats until the instance's own events retire it. |
| `MaxRetryBackoff` | `10 minutes` | Ceiling on `RetryBackoff`, which doubles per attempt. |
| `ProjectionLanes` | `16` | How many parallel lanes the projection spreads recorded deadlines across, hashed by instance id — read-time only, safe to change on any restart. |
| `ProjectionCheckpointEvery` | `100` events | How often the projection's read position is checkpointed, bounding how much a restart replays. |
| `MaxWakesPerTick` | `500` | How many due entries one scheduler tick processes, bounding the work a single tick starts. |
| `MaxWakeAttempts` | `5` | How many unanswered retries before a bucket lets an entry go — exhausting them leaves the instance back on `D8`'s terms. |
| `MaxBucketCatchUp` | `240` buckets | How many past time-buckets the ticker walks in one pass after being down for a while. |

`AddWorkflowDeadlines()` (on `IServiceCollection`, called after `AddAkka(...)` the same way
`AddWorkflowClient()` is) registers `IWorkflowDeadlineScheduler` for DI resolution, if an application
wants to read how many deadlines are currently armed.

**A deployment that starts no deadline scheduler** doesn't break — it just accepts `D8`'s lateness
on any deadline that outlasts the passivation window. Whether to run it is a real choice: a fleet
whose deadlines all land inside `PassivateIdleEntityAfter` (a short pause, a tight step timeout)
needs none of this.

## Recurring schedules — `WithScheduling` / `ScheduleWorkflow`

`ScheduleWorkflow` is an ordinary workflow (`Sagant.Scheduling`) that computes its next occurrence,
pauses until that instant, and on waking starts the target workflow through `IWorkflowClient` as an
independent run — no parent/child relationship, so the schedule's own history rolling forward
(`ThenRestartAt` per occurrence, keeping its journal the size of one cycle) never touches a
still-running occurrence. `WithScheduling` is `WithWorkflow<ScheduleWorkflow, ScheduleState>` with
its arguments already filled in — the client it needs comes from the same container the rest of the
application resolves from:

```csharp scaffold=statements
builder.WithScheduling(sp, configureShardOptions: o => o.PassivateIdleEntityAfter = TimeSpan.FromSeconds(10));
```

| Parameter | Default | What it does |
|---|---|---|
| `serviceProvider` | — (required) | Resolves `IWorkflowClient` when a schedule instance activates — after the shard regions a schedule starts work through are already registered. |
| `timeProvider` | `TimeProvider.System` | The clock a schedule reads to decide which occurrence is next. Supply a `FakeTimeProvider` to drive schedules deterministically in a real-runtime test. |
| `configureShardOptions` | `WithWorkflow`'s own defaults | Deployment-level shard tuning for the schedule region specifically — everything `WithWorkflow` exposes stays available, since this *is* a `WithWorkflow` call underneath. |

### Starting one — `StartSchedule`

```csharp scaffold=statements
using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
var accepted = await client.For<ScheduleWorkflow>("standing-order").Request<StartSchedule, string>(
    StartSchedule.For<OrderFulfillmentWorkflow>(
        spec: new EverySpec(TimeSpan.FromSeconds(15)),
        command: new PlaceOrder(CustomerId: "standing-order-customer", Amount: 42),
        overlap: OverlapPolicy.Skip,
        catchUpWindow: TimeSpan.FromSeconds(30)),
    startCts.Token);
```

`StartSchedule.For<TWorkflow>(...)` names the target workflow at compile time, so a mistyped type
fails the build rather than the first fire. Sending it again to the same schedule id replaces the
spec of one already running.

| Field | Required? | What it does |
|---|---|---|
| `Spec` | yes | An `IScheduleSpec` — see below for the four shipped implementations. |
| `TargetWorkflowType`/`TWorkflow` | yes | The workflow type each occurrence starts (typed via `For<TWorkflow>`, or a raw type name string on the non-generic constructor). |
| `TargetCommand` | yes | The command each occurrence sends. Stored on the schedule's own state and written to its journal, so it carries the same round-trip serialization requirement any persisted command does — see [`docs/adr/0003-serialization.md`](adr/0003-serialization.md)'s `SerializationRoundTripAssertions.AssertRoundTrips`. |
| `Overlap` | no, default `OverlapPolicy.Skip` | What happens when the previous occurrence is still running as the next comes due — see below. |
| `CatchUpWindow` | no, default `null` (unbounded) | How late an occurrence may be and still run. One missed by more than this is skipped outright, so a schedule coming back after a long outage catches up in a single fire rather than replaying everything it slept through. |
| `EndsAfter` | no, default `null` (runs until deleted) | How many occurrences to run before the schedule finishes on its own. |

### Spec types (`IScheduleSpec`)

| Type | Fires | Notes |
|---|---|---|
| `EverySpec(interval)` | Every `interval`, counted from the previous occurrence's own scheduled time | A slow fire never pushes the schedule later. |
| `DailyAtSpec(at, zone)` | Once a day at a wall-clock `TimeOnly` in a named `TimeZoneInfo` | `02:00 Europe/Lisbon` is a different UTC instant in summer than winter — resolved correctly across the boundary. |
| `CronSpec(expression, zone)` | A standard 5-field or seconds-leading 6-field cron expression (via Cronos), in a named zone | Handles spring-forward/fall-back the same way a person reading "02:30 daily" would expect on the two days a year it's ambiguous. |
| `OnceAtSpec(at)` | Once, at a fixed instant | A delayed start with no recurrence — the schedule finishes after firing. |

Write your own by implementing `IScheduleSpec.NextAfter(previous)` — it must be a pure function of
`previous` (no clock of its own) and strictly monotonic, so a replay years later computes the same
answer a live run did.

### Overlap policy

`OverlapPolicy.Skip` (the default) leaves an occurrence out when the previous one hasn't reached a
terminal/deleted/never-started status yet — right for work that would conflict with itself, like a
reconciliation pass reading the same rows. It waits for the previous occurrence indefinitely by
default: a genuinely stuck run reads as `ScheduleStatus.SkippedCount` climbing while `FireCount`
stands still, from the outside looking otherwise healthy. Bound the target's own runtime (a workflow
timeout, a deadline on whatever it waits for) so a stalled run eventually ends on its own —
`ScheduleWorkflow` additionally gives up waiting after `MaxConsecutiveOverlapSkips` (4) consecutive
skips and fires anyway, so one truly wedged occurrence can't silently freeze the schedule forever.
`OverlapPolicy.Allow` starts every occurrence regardless — right for work that's independent run to
run.

### Controlling a schedule

All handled by `ScheduleWorkflow`'s own command handlers, sent the same way any workflow command is:

| Command | Effect |
|---|---|
| `PauseSchedule` | Holds the schedule — it keeps its place, so `ResumeSchedule` continues from there. |
| `ResumeSchedule` | Puts a held schedule back to work, computing its next occurrence from *now* — occurrences slept through while paused are left behind, not caught up on. |
| `TriggerSchedule` | Runs an occurrence immediately, leaving the regular sequence untouched. |
| `CancelSchedule` | Ends the schedule. Occurrences already started keep running — they're independent runs, not children. |
| `GetScheduleStatus` (query) | Returns `ScheduleStatus(Paused, NextFireUtc, FireCount, LastStartedEntityId, SkippedCount)`. |

```csharp scaffold=statements
using var statusCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
var status = await client.For<ScheduleWorkflow>("standing-order")
    .Query<GetScheduleStatus, ScheduleStatus>(new GetScheduleStatus(), statusCts.Token);
```

## Where to go next

- [akka-runtime.md#idle-passivation](akka-runtime.md#idle-passivation) — the passivation default
  `WithWorkflowDeadlines` bounds the lateness of.
- `docs/guarantees.md` `D8`/`D8b` — the guarantees these mechanisms hold.
- `samples/OrderFulfillment/OrderFulfillment.Sample/Program.cs` — both extensions wired together
  against a real Postgres-backed cluster, including a standing-order schedule running end to end.
