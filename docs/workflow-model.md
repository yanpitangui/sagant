# The workflow model

This describes `Sagant`, the runtime-agnostic core. Nothing here depends on Akka.NET, an
`ActorSystem`, or any specific execution engine — a workflow author only ever sees these types, and
a runtime driver (`WorkflowEntityActor`, `WorkflowTestHarness`, or one you write yourself) is what
turns them into running state.

## `Workflow<TState>`

Every workflow extends `Workflow<TState>`, where `TState` is a plain, typically immutable (`record`)
type holding whatever data the workflow needs to remember between steps.

```csharp scaffold=file
public partial class OrderFulfillmentWorkflow : Workflow<OrderState>
{
    public override OrderState EmptyState() => OrderState.Empty();
}
```

- `EmptyState()` is what a brand-new, never-persisted instance starts with.
- State reaches a handler as `ctx.State`, a value on the handler's own context
  (`CommandContext<TState>` / `StepContext<TState>` / `QueryContext<TState>`). A workflow instance
  holds no state of its own, so a step suspended at an `await` and a handler dispatched while it
  waits cannot observe each other.
- `Settings()` (override to configure step/workflow timeouts and retries) returns a
  `WorkflowSettings` — see [Settings, retries, and pause](#settings-retries-and-pause) below.

The class must be `partial` — the source generator (below) adds a nested `Steps` class and the
dispatcher interface implementations to it.

## Command handlers and steps

A workflow has three kinds of handler method, all dispatched by the runtime, never called directly
by application code:

- **Command handlers** (`[WorkflowCommandHandler]`) — no side effects; a read to decide the
  transition is fine. They react to an external message (from `IWorkflowHandle.Send`/`Request`) and
  decide what happens next: update state, reply, and/or transition. The engine never checks the
  workflow's current *state* itself before routing a command to its handler by type — guarding
  which states a command is valid from is the handler's own job: inspect `ctx.State` and return
  a no-transition effect (e.g. an `Error` reply) when called somewhere it shouldn't be. A runtime
  driver may separately guarantee something about *when*, relative to other in-flight work, a
  handler actually runs — see
  [akka-runtime.md#command-dispatch-waits-for-an-in-flight-step-to-settle](akka-runtime.md#command-dispatch-waits-for-an-in-flight-step-to-settle)
  for what the reference Akka runtime guarantees there; this core layer makes no promise about it
  either way.

```csharp scaffold=workflow-member
  [WorkflowCommandHandler]
  public CommandEffect<OrderState> Start(PlaceOrder cmd, CommandContext<OrderState> ctx) =>
      Effects.UpdateState(ctx.State with { Amount = cmd.Amount })
          .TransitionTo(Steps.ChargePaymentStep)
          .ThenReply("accepted");
  ```

- **Steps** (`[WorkflowStep]`) — where I/O happens. A step is where a workflow calls out to a
  payment gateway, sends an email, writes to another system — anything that can fail, and anything
  that benefits from the engine's retry/failover/timeout machinery.

```csharp scaffold=workflow-member
  [WorkflowStep]
  public async Task<StepEffect<OrderState>> ChargePaymentStep(StepContext<OrderState> ctx)
  {
      var paymentId = await _payment.Charge(ctx.State.CustomerId, ctx.State.Amount);
      return StepEffects.UpdateState(ctx.State with { PaymentId = paymentId }).ThenComplete();
  }
  ```

  See [design-guidelines.md#1-step-vs-command-handler](design-guidelines.md#1-step-vs-command-handler)
  for when a piece of logic belongs in a step versus a command handler.

  A step method can optionally declare a `CancellationToken` parameter — the generator forwards it
  only to methods that ask for it. It's cancelled when the runtime stops waiting on the step's
  `Task`: a timeout, `Suspend`, `Terminate`, or a graceful-handoff grace window expiring.
  Cancellation is cooperative, like everywhere else in .NET — a step built on `HttpClient`/EF/etc.
  that honors the token unwinds promptly; one that doesn't just runs to completion with its result
  discarded.

## Effects, applied by the driver

Handlers never mutate state or drive a transition directly — they return an **effect**, a plain
data value describing what should happen, and the runtime driver applies it. This is what makes
`WorkflowTestHarness` possible at all: it applies the exact same effects `WorkflowEntityActor`
would, with no `ActorSystem` underneath.

An effect is a `PersistenceEffect<TState>` (update state, or don't) paired with a `Transition`:

| Transition | Produced by | Meaning |
|---|---|---|
| `StepTransition` | `.TransitionTo(...)` / `.ThenTransitionTo(...)` | Run the named step next. |
| `PauseTransition` | `.Pause(...)` / `.ThenPause(...)` | Stop and wait for an external command (e.g. human approval), optionally with a timeout — see [Pause](#pause). |
| `TerminalTransition` | `.Complete()` / `.Fail(...)` / `.Cancel(...)` (and `Then`-prefixed forms on steps) | Terminal: the run finished, carrying a `WorkflowOutcome` saying how. |
| `DeleteTransition` | `.Delete(...)` / `.ThenDelete(...)` | Terminal: the workflow completed and its persisted state should be dropped. |
| `AwaitChildrenTransition` | `.AwaitChildren(...)` | Start one or more child workflows and durably wait for their outcomes — see [child-workflows.md](child-workflows.md). |
| `NoTransition` | `.Reply(...)` / `.Error(...)` with no transition call | Stay on the current step; only used by command handlers. |

Build these through `Effects` (command handlers) or `StepEffects` (steps) — two separate fluent
builders on `Workflow<TState>`, because the two handler kinds can produce different shapes:
`StepEffectsBuilder` has no `Reply`/`Error` (steps are internal orchestration, nothing to reply to),
and only `EffectsBuilder` produces a `CommandEffect<TState>` (which carries an optional
`Reply`/`Error` payload for the caller). `StepEffectsBuilder` produces a `StepEffect<TState>`
instead.

A `StepTransition` chains automatically at the runtime level: returning `.ThenTransitionTo(nextStep)`
from a step causes the runtime to dispatch `nextStep` next, with no further external input, until a
step returns a pause/end/delete/await-children transition instead.

## The source generator

`Sagant.SourceGenerators` (Roslyn incremental generator) scans every `partial` class deriving from
`Workflow<TState>` for `[WorkflowStep]`/`[WorkflowCommandHandler]` methods and emits, in a matching
`partial` file:

- A nested `Steps` class: one typed `StepRef<TWorkflow, TInput>` per `[WorkflowStep]` method (e.g.
  `Steps.ChargePaymentStep`). Passing `Steps.ChargePaymentStep` to `.TransitionTo(...)` means a
  transition target is always a compile-checked reference, never a bare string that a rename could
  silently break.
- `IWorkflowStepDispatcher<TState>`/`IWorkflowCommandDispatcher<TState>` implementations: explicit
  dictionaries from step/command name or type to a descriptor that invokes the method directly —
  no reflection, so the generated code is NativeAOT/trimming-friendly.
- `IWorkflowTypeInfo.WorkflowTypeName` as a compile-time string literal, read by
  `Workflow<TState>.WorkflowTypeName` and used to identify the workflow type in traces, metrics, and
  (for child workflows) the runtime's type registry.

If the workflow class is nested inside one or more containing classes, the generator walks that
chain and re-opens every level as `partial` in the generated file. A class in the chain that isn't
declared `partial` is reported as diagnostic `SAG001`.

## Settings, retries, and pause

`Workflow<TState>.Settings()` returns a `WorkflowSettings` — the business-level configuration a
workflow author sets in code: overall workflow timeout, default step timeout/retry policy, and
per-step overrides. (Deployment-level knobs like cluster shard count or entity idle-passivation
live on the runtime driver's own registration API instead — see
[integration-guide.md](integration-guide.md).)

```csharp scaffold=workflow-member
public override WorkflowSettings Settings() => WorkflowSettings.Create()
    .DefaultStepTimeout(TimeSpan.FromSeconds(5))
    .StepRecovery(Steps.ChargePaymentStep, RecoverStrategy.WithMaxRetries(2).FailoverTo(Steps.RefundPaymentStep))
    .Timeout(TimeSpan.FromMinutes(30), Steps.EscalateStep)
    .Build();
```

`RecoverStrategy` describes how a step (or the workflow as a whole) recovers from failure: a retry
budget (`MaxRetries`), a step to fail over to once that budget is exhausted, and optionally a
`BackoffForAttempt` delay function (`Func<int, TimeSpan>` — see `RetryBackoff` for ready-made
fixed/exponential implementations). With no backoff configured, a retry starts immediately.
`RecoverStrategy.WithMaxRetries(n).ThenPark()` holds the run at the exhausted step instead of failing
over — see guarantee `E12` in `docs/guarantees.md`.

Every `WorkflowSettingsBuilder` method, in the rough order a workflow author reaches for them:

| Method | Default if unset | What it configures |
|---|---|---|
| `DefaultStepTimeout(timeout)` | no timeout | Fallback timeout for any step without its own `StepTimeout` override. |
| `DefaultStepRecovery(strategy)` | fails immediately on the first error, no retry | Fallback `RecoverStrategy` for any step without its own `StepRecovery` override. |
| `StepTimeout(step, timeout)` | inherits `DefaultStepTimeout` | Per-step timeout override, keyed by the generated `Steps` ref. |
| `StepRecovery(step, strategy)` | inherits `DefaultStepRecovery` | Per-step `RecoverStrategy` override. Calling both this and `StepTimeout` for the same step merges into one entry — order doesn't matter. |
| `Timeout(timeout)` | no workflow-wide timeout | The workflow-wide deadline, measured against active processing time only (see [Pause](#pause) below for why a paused workflow doesn't count against it). With no failover step, expiry ends the run `TimedOut` (guarantee `E7`). |
| `Timeout(timeout, failoverStep[, input])` | — | Same deadline, but expiry transitions into `failoverStep` instead of ending the run directly — a chance to compensate before finishing. |
| `IdempotencyLedgerCapacity(n)` | `50` | How many caller-supplied idempotency keys (the `idempotencyKey` param on `Send`/`Request`/`RunAndAwaitResult`) this instance remembers at once, oldest evicted first — see [akka-runtime.md](akka-runtime.md#akkadelivery-and-idempotency). |
| `SeqNrDedupCapacity(n)` | `16` | How many transport-level producer incarnations this instance remembers a highest-applied-sequence-number for, oldest evicted first — the internal half of at-least-once delivery dedup, distinct from the caller-facing idempotency ledger above. |
| `PruneFinalizedChildren(prune = true)` | `false` | Whether a finalized `AwaitChildren` group's terminal-status members are dropped from the runtime's per-instance child history once that group finalizes. Trades away historical detail (queries/diagnostics lose the pruned child's record) for a smaller footprint on a workflow that fans out repeatedly over its lifetime. |
| `DefaultQueryTimeout(timeout)` | `WorkflowSettings.BuiltInQueryTimeout` (30s) | Fallback bound on how long a `[WorkflowQuery]` handler is allowed to run before the runtime stops waiting, replies with `WorkflowQueryTimeoutException`, and cancels its token. |
| `QueryTimeout<TQuery>(timeout)` | inherits `DefaultQueryTimeout` | Per-query-type override, keyed by `typeof(TQuery).Name` (the same literal the generator bakes into the query descriptor). |
| `CancelVia(step)` | no cancellation step — `Cancel` finishes the run immediately, still reporting `Cancelled` | Names the step a `Cancel` command routes to, so the workflow gets a chance to unwind (refund, notify, release a hold) before it finishes — see guarantee `E9`. Two overloads: one taking a `StepRef<TWorkflow, WorkflowCancellation>` (reads the caller's cancellation reason), one taking a plain `StepRef<TWorkflow, NoInput>`. |
| `HoldTimeout(timeout, step)` | `null` — an operator `Suspend` or a parked step waits for a person indefinitely | How long an instance stays held — by `Suspend`, or by a step that exhausted its retries under `RecoverStrategy.ThenPark()` — before `step` runs and the workflow decides for itself what becomes of a hold nobody came back for. Both the timeout and the step are required together: a deadline with nothing to run would release an instance into no particular step. |

`Timeout`/`HoldTimeout`'s failover/timeout-handler steps are typed `StepRef`s from the generated
`Steps` class, the same as every other transition target — see
[The source generator](#the-source-generator) below.

### Pause

`.Pause()`/`.ThenPause()` stops the workflow and waits for an external command — the standard shape
for "wait for a human to approve this." Passing a `PauseSettings` (`PauseSettings.WithTimeout(...)`)
adds a deadline: once it passes, the workflow auto-transitions into the configured
`TimeoutHandlerStepName`, itself a normal step, free to do I/O (e.g. call a compensating service on
auto-cancel).

A paused workflow doesn't count against `WorkflowSettings.WorkflowTimeout` — that ceiling applies
to active processing time alone. `PauseSettings.Timeout` is the knob that governs a stuck approval,
on its own separate clock.

## Observability

Every command and step execution becomes an `Activity` span (`System.Diagnostics`), and every
`[WorkflowStep]` invocation records a `sagant.step.duration` metric — both come for free from
`StepDescriptor<TState>.Invoke`/`CommandDescriptor<TState>.Invoke`, the one place any runtime driver
actually calls a handler, so no runtime has to reimplement span/metric lifecycle itself.

A runtime driver also publishes each event it records, wrapped in a `WorkflowFeedItem`, for anything
that wants to watch a workflow run without touching its persistence (a live dashboard, a log line
per step, a metrics counter). Every batch writes exactly one `CausedEvent`, carrying both what the
workflow did and the `TransitionCause` behind it — a step outcome with its duration, a failed
attempt with its error and retry decision, a command with its type and any caller metadata, or an
operator action. So one message answers "what happened, and why".

The same events read back durably through `IWorkflowEventFeed`, and `IWorkflowVisibilityQuery` lists
instances by type, status, or time without an id in hand. See
[guarantees.md](guarantees.md#visibility) for what each promises.

## Where to go next

- [child-workflows.md](child-workflows.md) — starting and awaiting child workflows from a step.
- [akka-runtime.md](akka-runtime.md) — how `Sagant.Runtime.Akka` actually drives all of this.
- [testing.md](testing.md) — exercising a workflow's own logic with `WorkflowTestHarness`.
