# The Akka.NET runtime

`Sagant.Runtime.Akka` is the reference implementation of a runtime driver: it drives
`Workflow<TState>` through the generated dispatch tables and implements `IWorkflowClient`/
`IWorkflowHandle`, backed by a persistent Akka.NET `ClusterSharding` entity actor per workflow
instance. This document covers how it works internally; see
[integration-guide.md](integration-guide.md) for how to wire it into a host.

## `WorkflowEntityActor<TWorkflow, TState>`

One `WorkflowEntityActor` instance runs per workflow id, cluster-wide, addressed through
`ClusterSharding`. It's a `ReceivePersistentActor` that:

- Dispatches external commands and executes steps.
- Executes a step by **fire-and-`PipeTo`**, never an inline `await` inside the actor — the actor
  starts the step's `Task`, keeps processing its mailbox (control commands, other in-flight work),
  and the step's completion arrives back as an ordinary message once the `Task` finishes. This is
  what keeps the actor responsive to `Suspend`/`Terminate`/`GetStatus` while a step's I/O is still
  running.
- Retries and fails over per `RecoverStrategy` — the decision itself comes from
  `WorkflowTransitionPlanner.PlanStepFailure`, shared with `WorkflowTestHarness`
  (see [testing.md](testing.md)), so the actor owns only the persisting and scheduling around it.
- Persists each transition as the **`WorkflowEvent`s** it changed, written with `PersistAll` as one
  atomic batch (guarantee D1). Each event lands, `WorkflowEventFold` applies it to the live
  `WorkflowRuntimeState<TState>`, and recovery folds the same events from scratch through the same
  function — so a live instance and a recovered one cannot disagree. See
  [Events and snapshots](#events-and-snapshots).
- Enforces step, workflow, and pause timeouts identically on a fresh start and on recovery.

## Events and snapshots

`WorkflowEvent` is a closed hierarchy in the core package, and `WorkflowEventFold` is the pure
function that applies one to a `WorkflowRuntimeState<TState>`. Both live outside this package because
they are what any runtime would need; the actor supplies only the writing and the recovery loop.

Events carry **computed facts**. `StepStarted` names the absolute instant the step's deadline falls
on, worked out at write time, so a replay happening hours later reproduces that same instant. That
rule is what keeps the fold free of a clock and of settings, and it is what guarantee D2 rests on.

A transition records only what it changed, so its cost tracks the size of that change:

| Transition | Events written |
|---|---|
| A step begins | `StepStarted` (+ `UserStateChanged` when the effect persisted) |
| A step attempt fails, another follows | `StepRetryScheduled` |
| A fan-out over *n* children begins | one `ChildrenAwaited` carrying all *n* relationships |
| **One of those children reports** | **one `ChildMemberUpdated` naming that one child** |
| The group's policy resolves | `ChildGroupFinalized` |

That fourth row is guarantee H5: the whole fan-out appends O(*n*) relationships to the journal.

Snapshots sit outside that count. Each one serializes the whole envelope, so a fan-out wide enough to
trigger several of them re-serializes its relationship list once per snapshot — see H5 for the
arithmetic and the knob.

`SnapshotPolicy` decides when to cache a fold result: always once a transition makes the instance
terminal, otherwise once `snapshotEveryNEvents` events have accumulated since the last one. The
threshold is a distance from the last snapshot, so a batch of any width crosses it — which matters
because one transition can write several events at once. `HandleSnapshotSuccess` then prunes the
journal behind the snapshot it just wrote.

## Timeouts and retries are persisted absolute deadlines

Step timeout, workflow timeout, pause timeout, and retry backoff delay are each stored as an
absolute `DateTimeOffset` on `WorkflowRuntimeState<TState>` — not a relative duration, and not only
a live in-memory timer. `OnRecoveryCompleted` re-arms a live timer against whatever's still pending.
A crash or `ClusterSharding` rebalance mid-wait resumes the *remaining* time; it never restarts the
wait from scratch or silently drops it.

The actor's `TimeProvider` defaults to `AkkaSchedulerTimeProvider` (a thin wrapper over the
`ActorSystem`'s own scheduler), specifically so deadline math lines up with
`Akka.TestKit.TestScheduler` in virtual-time tests with no separate test-only time seam. (Don't mix
`TestScheduler` with a real running cluster in the same test — `TestScheduler` freezes
`Akka.Cluster`'s own gossip/heartbeat timers too, and the test hangs forever. Keep virtual-time
tests on a bare non-sharded actor.)

## Graceful shutdown

`GracefulShutdown` (`Sagant.Runtime.Akka`) is the default `ShardOptions.HandOffStopMessage` for
workflow entities — `ClusterSharding` sends it, in place of the default `PoisonPill`, when an
entity's shard is being rebalanced or its region is shutting down. `PoisonPill` only drains the
actor's own mailbox before stopping; it has no way to know a step's `Task` is still running
off-actor-thread (the fire-and-`PipeTo` model above), so it would kill the actor out from under an
in-flight step with no chance to record whatever real-world side effect that step already caused.
`GracefulShutdown` instead lets an in-flight step run to its own completion (bounded by the
`gracefulShutdownGrace` parameter on `WithWorkflow`) and persist normally — no further step starts
here, the persisted history already reflects wherever the workflow now stands, and a respawn on
the new owning node picks up exactly there, the same as any other interruption. If nothing is in
flight, this is an immediate, ordinary stop.

The grace window is derived from the actually configured
`akka.cluster.sharding.handoff-timeout` (falling back to Akka's own 60s default when Sharding isn't
loaded at all, e.g. a unit test instantiating the actor directly), clamped safely under that ceiling
— a caller-supplied `gracefulShutdownGrace` can only shorten it, never push it past the point where
Sharding's coordinator gives up waiting and force-kills the entity anyway.

## `Akka.Delivery` and idempotency

Business-command traffic (`IWorkflowHandle.Send`/`Request`/`RunAndAwaitResult`) travels through
`Akka.Delivery`'s `ShardingProducerController`/`ShardingConsumerController` pair, giving
at-least-once delivery into the entity. This closes two separate cases:

- **Transport-level redelivery** — a producer resending because its prior acknowledgment was lost
  (e.g. the entity crashed before persisting). Deduplicated against
  `WorkflowRuntimeState.HighestAppliedSeqNr`, keyed by the sending producer's id: a redelivered
  seqNr at or below the recorded value is a genuine duplicate, skipped and re-confirmed without
  re-persisting. No caller-facing API — this is entirely internal.
- **Caller-level retry** — a caller resending after an ambiguous outcome (e.g. an `Ask` timeout
  where the command may or may not have actually landed). Closed by an opt-in, caller-supplied
  idempotency key (`WorkflowSettings.IdempotencyLedgerCapacity`, threaded through
  `IWorkflowHandle.Send`/`Request`/`RunAndAwaitResult`'s `idempotencyKey` parameter): a repeat send
  with the same key replays the cached reply from `WorkflowRuntimeState.IdempotencyLedger` instead
  of re-invoking the handler.

**Control commands** (`Suspend`/`Resume`/`Terminate`/`GetStatus`) bypass all of this and travel as
plain `Tell`/`Ask` straight to the shard region — the consumer controller's `AllowBypass = true`
setting (forced on internally) is what lets anything outside the `Akka.Delivery` envelope reach the
actor at all, with `Sender` preserved so `Ask`'s implicit reply-to still works.

### Command dispatch waits for an in-flight step to settle

A step runs fire-and-`PipeTo` off-actor-thread, so the mailbox keeps draining while one is running.
A business command arriving during that window is deferred (via the actor's inherited `Stash` —
`ReceivePersistentActor`/`Eventsourced` already implements `IWithUnboundedStash`, no separate
declaration needed): dispatching it would run it against a state the step is about to supersede,
and it is released once the step (or its whole autonomous chain) settles into something other than another
step. The deferred delivery is never `Confirmed` while it waits, so a crash mid-defer just means the
producer redelivers it once this entity — or its next incarnation, after a crash or `ClusterSharding`
rebalance — comes back; no separate durability mechanism needed for the deferral itself.

Whole-state persistence is why this deferral exists: two overlapping writers would race over the
entirety of `TState`, last writer wins, silently. A read that must not wait for a running step is a
`[WorkflowQuery]` instead — it returns a `QueryEffect`, which carries a reply and no persistence, so
it cannot join that race and is dispatched immediately. Queries also skip `Akka.Delivery` entirely
(plain `Ask` to the shard region, never stashed, never persisted, never confirmed), run concurrently
with each other, and are bounded by `WorkflowSettings.DefaultQueryTimeout` — a caller's own `Ask`
timeout completes the caller's wait and sends nothing to the entity, so the entity bounds itself.

`GetState` (`Sagant.Protocol`) is the built-in answer to the common "live progress read" case: a
"what's this workflow's state right now" query for a progress-watching UI. It's handled directly
inside `HandleDelivery`, alongside `ChildLifecycleNotification`/`Terminate`/`Delete`'s cascade
handling — framework-internal, never reaching the author's own `[WorkflowCommandHandler]` table, and
positioned above the stash guard entirely, so it always dispatches immediately. No handler to write:
send it through the existing `IWorkflowHandle.Request<TCommand, TReply>` with the workflow's own
state type as `TReply` — `handle.Request<GetState, OrderState>(new GetState())` — and the actor
replies with its currently-persisted `TState`, boxed the same way any command reply already is.

## Child workflows

Spawning and addressing a child actor is entirely internal to this runtime — a workflow author
never sees an `IActorRef` or shard region for it, only the core `ChildStart`/`ChildGroupResult`
types described in [child-workflows.md](child-workflows.md). Concretely:

- A child-start command travels the same registry-plus-producer-adapter path as any other business
  command, keyed by the relationship's deterministic id
  (`{ParentWorkflowId}:{GroupId}:{ChildWorkflowId}`) as its idempotency key — safe to redeliver any
  number of times.
- A child reports its own terminal outcome to its parent via `ChildLifecycleNotification` — an
  internal-only message type (an external caller has no code path to fabricate one, the same
  non-impersonation guarantee `StepCompleted`/`StepFailed` already have), delivered through the same
  `Akka.Delivery` path as any other command, deduplicated by the same `HighestAppliedSeqNr`
  mechanism at the transport layer. `RelationshipId`/`Generation` is the separate
  semantic-staleness check a receiving parent applies on top of that — a lifecycle notification
  raised before a group finalized and moved on is detected and dropped even if it isn't a literal
  transport duplicate.

## Tracing

Every command and step execution is a span, correlated into one trace per workflow run via a
persisted `LastTraceParent`. Retries within one workflow run are **siblings** in that trace, each its
own leaf — a crash produces a **link into a new trace** on recovery, standing apart from the trace
that came before it, and the same holds across a cross-node relocation from a `ClusterSharding`
rebalance. See
`WorkflowEntityActor.ResolveParentContext`/`OnRecoveryCompleted`/`ConsumeRecoveredLink` for the
exact mechanics — `WorkflowTracingTests` guards the recovery case specifically (a recovered span
must link to a new trace, never pick up the old `TraceId` directly).

## Clustering wiring

`WorkflowClusterShardingExtensions.WithWorkflow<TWorkflow, TState>()` registers one
`WorkflowEntityActor` type on `ClusterSharding` via `Akka.Hosting`'s fluent builder (not raw HOCON),
then, once `Akka.Hosting` resolves the shard region asynchronously at host start, populates a
per-`ActorSystem` `WorkflowHandleRegistry` that `IWorkflowClient.For<TWorkflow>(entityId)` resolves
handles out of. That registry is the entire mechanism behind `IWorkflowClient`/
`IWorkflowHandle<TWorkflow>` staying the full public surface — no `IActorRef`, `ActorRegistry`, or
`ClusterSharding` type ever leaks past it into application code.

## Deployment-level configuration

Cluster/infra decisions, passed to `WithWorkflow` as their own parameters, separate from a
workflow's own business logic in `WorkflowSettings`:

- `configureShardOptions` — pass-through tuning for `ShardOptions`. Runs after `WithWorkflow` sets
  its own defaults, so it sees and may override them:
  - `HandOffStopMessage` is a `GracefulShutdown`, so an in-flight step can finish across a rebalance.
  - `PassivateIdleEntityAfter` is **disabled**, overriding Akka Cluster Sharding's own 120-second
    default. Passivation stops an idle entity actor to free memory, leaving its persisted state
    untouched and reactivating transparently on the next message — but a live timer belongs to a live
    entity. A workflow legitimately sits idle while holding a deadline (a pause awaiting approval, a
    long workflow timeout), and under the stock default it would passivate two minutes in, losing the
    timer; the deadline then only fires when something next activates it. Re-enable it if instances
    staying resident until terminal costs more than that lateness. See `docs/guarantees.md` D8.
- `numberOfShards`, `producerBufferCapacity`, `configureProducerControllerSettings`,
  `configureConsumerControllerSettings`, `gracefulShutdownGrace`, `timeoutScheduler`, `timeProvider`
  — see the parameter docs on `WithWorkflow` itself.
