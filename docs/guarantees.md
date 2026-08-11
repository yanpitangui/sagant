# Guarantees

What Sagant promises, what it deliberately does not, and what you are responsible for. This is the
contract: implementation details may change freely, these may not.

Each guarantee has a stable id. Tests that exist to hold one of them name it.

---

## Durability

**D1 — Nothing observable happens before the write.**
A transition is durably persisted before any effect of it is visible: no reply, no delivery
confirmation, no timer armed, no child started, no notification published. A crash in that window
loses the effect, never applies it halfway.

One transition records however many facts it changed, and they are written as a single atomic batch.
Recovery therefore sees every fact of a transition or none of them; an instance is always found
mid-history at a transition boundary.

**D2 — Deadlines survive as absolute timestamps.**
Step, workflow, pause and retry-backoff deadlines persist as absolute times, not remaining
durations. A crash or cluster rebalance resumes the *remaining* wait — it never restarts the clock
and never silently drops the deadline. See D8 for what bounds lateness.

**D3 — The workflow deadline is sticky.**
Established on the first transition an instance persists, never recomputed — including across
pauses and retries.

**D4 — A delivered message is confirmed only once its effect is durable.**
Confirming earlier would let a genuinely-applied command go unrecorded after a crash, which is the
exact failure at-least-once delivery exists to rule out.

**D5 — Dedup bookkeeping is atomic with the transition it belongs to.**
Sequence numbers and idempotency-ledger entries are written in the same atomic batch as the
transition, so no crash can apply one without the other.

**D6 — Parent-close decisions are durable before they are acted on.**
`ParentClosePolicy` marks affected child relationships in the same atomic batch that makes the parent
terminal, so recovery can redeliver a termination whose send never left the process.

**D7 — A child relationship is persisted before its first send.**
Relationship state, not the delivery layer, is the source of truth for recovery: if the process died
before the send, no delivery layer could have buffered a message it never received.

**D8 — A live timer belongs to a live instance.**
A passivated instance has no running timer; its deadline is re-armed from the persisted absolute time
when the instance next activates, firing immediately if already past. A deadline is therefore never
*lost* (D2), but while an instance is passivated its lateness is bounded by whatever next activates
it rather than by the deadline.

Because a workflow legitimately sits idle while holding a deadline, `WithWorkflow` disables cluster
sharding's idle passivation by default, overriding its 120-second default. The cost is that instances
stay resident until terminal; a deployment with many long-lived workflows can trade that back through
`configureShardOptions`, accepting the lateness.

**D9 — The persisted event schema is Sagant's own compatibility burden.**
An instance's history is a sequence of engine-defined events, and every version of Sagant reads back
what an earlier one wrote: fields are only ever added as optional, never renamed or retyped, and a
case is never removed while an instance could still replay it. Upgrading Sagant does not require
draining in-flight workflows or migrating a journal. This is the engine's obligation, distinct from
`TState`, whose evolution is yours (see [Deliberate non-guarantees](#deliberate-non-guarantees)).

## Concurrency

**C1 — One step, N queries, zero in-flight commands.**
At most one step executes at a time. Queries run concurrently with a step and with each other.
Command handlers are synchronous and complete atomically, so a command is never "in flight".

**C2 — State cannot change under a running step.**
No writer can modify state while a step executes. Commands are deferred until the step chain
settles; queries cannot write at all (their effect type has no persistence member); control commands
leave state untouched.

**C3 — A stale step result is discarded.**
A result from an attempt the runtime has stopped waiting on — timed out, suspended, terminated — is
dropped rather than applied.

**C4 — Exactly one live instance per workflow id, cluster-wide.**
Enforced by the sharding coordinator: a new region cannot activate an instance until the previous
owner confirms it has stopped. Every other guarantee here assumes this single-writer property.

**C5 — Per-instance command order is preserved, including across deferral.**
Commands are delivered to an instance in send order, and a command deferred behind a running step is
still applied before commands that arrived after it.

## Execution

**E1 — Retry budget, then failover, then end.**
A failing step is retried up to its `MaxRetries`, then transitions to its failover step, then — with
no failover configured — ends. A failed attempt never updates state.

**E2 — A retry's timeout budget starts when the attempt starts.**
Backoff delay is folded into the step deadline, so a backoff longer than the step timeout cannot
expire the attempt before it begins running.

**E3 — The workflow timeout does not run while paused.**
It bounds active processing time, not time spent waiting for a human. A paused instance is governed
by its own pause timeout instead.

**E4 — Resume restarts the step fresh.**
Retry count resets and any in-flight backoff is discarded.

**E5 — An unknown step name ends the workflow.**
An instance persisted on a step name that no longer exists in code ends with a reason rather than
hanging. See [Known limits](#known-limits) — this makes step removal a deploy-safety concern.

**E6 — `Settings()` is read once per instance.**
A driver resolves a workflow's settings when it constructs the instance and never re-reads them, so
an override returning different values on different calls has only its first result observed. Step
and query overrides layer over their defaults by the same rule in every driver.

**E7 — A finished run says how it ended.**
`WorkflowStatus.Finished` always carries a `WorkflowOutcome`: `Completed`, `Failed` (with a
structured `WorkflowFailure` — message, exception type, stack trace, inner chain, step name, attempt
count), `TimedOut`, or `Terminated`. A caller switches over a closed hierarchy; the compiler points
out any case they forgot. `RunAndAwaitResult` returns that outcome alongside the final state rather
than throwing, because a failed workflow is a business result, not an exceptional condition in the
caller's control flow.

Only the workflow-level deadline produces `TimedOut`. A step timeout becomes a step failure and flows
through the retry budget like any other, surfacing as `Failed` with `ExceptionType` of
`System.TimeoutException`; a pause timeout transitions into its handler step and is not terminal.

**E8 — Deletion is not an outcome.**
Purging an instance's data says nothing about how its run ended. An instance deleted after finishing
is `Deleted` and still carries its outcome; one deleted mid-run carries none.

**E9 — Cancellation unwinds; termination does not.**
`Cancel` routes to the step named by `WorkflowSettings.CancellationStepName`, which runs like any
other — its own timeout, its own retry budget — and decides the run's final outcome. With no such
step configured there is nothing to unwind and the run finishes immediately; either way it reports
`Cancelled`, never `Terminated`, because what was asked for differs even where the effect matched.

A cancelled parent cancels the children its `ParentClosePolicy` covers, so each gets the same chance
to unwind. Every other terminal outcome terminates them instead.

`Terminate` remains abrupt by design, for when a workflow must stop whether or not it can unwind.

**E10 — Control commands are decided in one place.**
`Suspend`, `Resume`, `Terminate` and `Cancel` are decided by the same planner as every transition, so
a driver's job is to persist the result and carry out the decisions. Which statuses each applies from,
and what a rejection says, are the same everywhere.

**E11 — A restart bounds a workflow that never ends.**
`ThenRestartAt` begins a fresh cycle under the same id and makes the history behind it reclaimable,
so a run with no natural end stops accumulating events without limit. The instance keeps its id, its
state (whatever the same effect's `UpdateState` wrote), and its deduplication ledgers, since a
producer keeps counting sequence numbers across a restart. It loses its retry count, its workflow
deadline — the next cycle establishes its own, so a perpetual run is bounded per cycle rather than
overall — and any children it owns, which are closed under `ParentClosePolicy` exactly as a terminal
transition closes them.

Reclamation is a consequence, never a precondition: the fresh cycle is durable before any history is
released, so a crash in between replays the old events plus the restart and folds to the same
envelope. Losing the reclamation costs disk and changes no state.

**E12 — A step can hold its run instead of ending it.**
`RecoverStrategy.WithMaxRetries(n).ThenPark()` holds the instance at the step whose budget ran out,
in `Suspended`, with `ParkedFailure` saying what stopped it. The step and its input survive, so
`Resume` re-runs that attempt with a fresh budget (`E4`) and clears the failure. A spent budget has
three conclusions — fail over to a step, park, or end the run — and a step with no strategy ends the
run on its first failure.

The run has not ended, so nothing that waits on an ending fires: `RunAndAwaitResult` waits, and a
parked child reports nothing to its parent's group. Parking is chosen per step for that reason.

## Children

**H1 — A group's resume step runs exactly once.**
Guarded by generation and finalization, so no policy evaluation can resume a parent twice.

**H2 — Late and duplicate child reports are ignored.**
A report arriving after its group finalized, or at an already-terminal parent, is acknowledged and
dropped.

**H3 — Group ids are retry-safe.**
The group counter increments at persist time, never in a step body, so a step retried before its
increment was persisted produces the same group id.

**H4 — A child reports its own outcome.**
A child's `ChildStatus` derives from how its run finished: `Completed` from `Completed`, `Failed`
from `Failed` or `TimedOut`, `Terminated` from `Terminated`, `Cancelled` when it was deleted without
ever finishing. So `CompletionPolicy.AllSuccessful` means what its name says, and a parent reads
`ChildGroupResult.Outcome` rather than re-deriving success from each child's own state.

**H5 — A child's report writes only that child.**
Each report appends one event naming the single member it concerns, so a group of *n* children
appends O(n) relationships to the journal across the whole fan-out.

Snapshots are a separate cost and scale with how big the state is: each one serializes the parent's
whole relationship list. At a cadence of one snapshot per *k* events, a fan-out of *n* re-serializes
roughly 2n²/k relationships on top of the journal's 2n — measurable at *n* = 32 and the default
*k* = 10, where snapshots account for about twice the journal. Raise `snapshotEveryNEvents` for a
workflow that fans out widely, trading replay depth for write volume.

## Queries

**Q1 — A query observes a consistent snapshot.**
Taken at dispatch time, never a torn mid-transition state. It may be superseded while the handler
runs, which is what a read wants and cannot matter, because a query cannot write.

**Q2 — Every query is bounded by the workflow, not the caller.**
A caller's request timeout ends the caller's wait and sends nothing to the instance. Query handlers
carry a server-side timeout of their own, defaulting to
`WorkflowSettings.BuiltInQueryTimeout`.

## Observability

**O1 — One trace per run.**
Spans correlate through a persisted trace parent. Retry attempts are siblings of each other, not a
chain. A crash produces a link into a fresh trace rather than a false continuation of the old one.

**O2 — Reads do not advance the trace chain.**
A query, or a command whose effect neither persists nor transitions, never becomes the parent of the
next real step.


---

## Visibility

**V1 — Every batch names what happened and why.**
A transition writes exactly one `CausedEvent`, carrying both the change and its `TransitionCause` —
the step outcome, command, or operator action behind it, plus any caller-supplied metadata. Matching
that one base type is enough to read "why did this change" without knowing which concrete event
arrived.

**V2 — A workflow's events reach a reader in write order.**
Within one instance, order is the order they were written. Batch boundaries are invisible to a
reader, so anything it needs travels in an event's own fields.

**V3 — No ordering holds between workflows.**
A child's events and its parent's are independent streams and arrive in either order. A projection
tolerates that and encodes no cross-instance invariant.

**V4 — Recorded events are delivered at least once, and duplicates are identifiable.**
`(EntityId, SequenceNr)` identifies an event, so a consumer that sees one twice recognises it. Two
transports carry the same sequence: an in-process publish that is immediate and unresumable, and a
durable read that resumes from a `WorkflowFeedPosition`. Reconciling a live subscription against
`ReadEntity` from a per-instance high-water mark collects anything the subscription missed.

**V5 — Recorded events survive until something deliberately reclaims them.**
Snapshots bound how far recovery replays and leave the events themselves in place. Two acts reclaim
that history, both deliberate: deleting or purging the instance, and restarting it (`E11`).

**A rebuild therefore has a floor: the instance's most recent restart.** A workflow that never
restarts can be rebuilt from its first event. One that restarts daily, replayed on day 30, yields
day 30 — the earlier cycles were released as each restart closed them, and a projection that
consumed them live holds the only remaining copy. Reshaping a read model over a restarting workflow
means reshaping it from that floor forward, or keeping what the old projection recorded.

This is the price of `E11` bounding a run that never ends: history cannot both be reclaimable and
replayable.

**V6 — Instances are listable without holding an id.**
`IWorkflowVisibilityQuery` reports status, outcome, current step, attempt and timing for every
instance, filtered by type, status, start time, or id prefix. Workflow type and entity id both come
from the persistence id, so narrowing by type reads no event bodies.

**V7 — Delivery bookkeeping stays out of the feed.**
Records of how a message arrived are transport detail. They persist so deduplication survives a
crash, and no transport surfaces them.

---

## Your responsibilities

**R1 — Steps are at-least-once. Make them idempotent.**
A step's effect is persisted after the step body returns. If the process dies in between, the step
runs again on recovery — after its side effects have already happened. Charging a card, sending an
email, or calling any non-idempotent API from a step can therefore happen more than once.

This is the same contract Temporal places on activities and Step Functions on tasks. Sagant does not
and cannot make it exactly-once: the side effect is outside the transaction that records it. Use an
idempotency key at the downstream service, or make the operation naturally idempotent.

Commands have a dedup mechanism for caller retries (`idempotencyKey`); steps deliberately do not,
because the duplicate there originates from recovery, not from a caller.

**R2 — Guard commands against invalid states.**
The runtime routes a command by type and never inspects state first. Deciding which states a command
is valid from is the handler's job.

**R3 — Keep state immutable.**
State reaches a handler as a value, but nothing stops a handler mutating a mutable object in place.
Doing so bypasses the effect that would have persisted it, leaving a running instance holding data
its journal has never seen. Diagnostics `SAG002`/`SAG003` flag the shapes that allow it.

---

## Deliberate non-guarantees

**No deterministic replay, and none needed.**
Sagant persists each step's effect rather than replaying handler code. There are no determinism
constraints on handler bodies: no banned APIs, no version-gating, no replay-safe random or clock.
This is a deliberate trade against Temporal's and Durable Task Framework's model — Sagant gives up
their fine-grained recovery inside a single handler and gets ordinary, unconstrained C# in return.

**No way to resume a failed run.**
A failure is fully inspectable (E7), but there is no supported way to retry a failed workflow from
its failure point once the underlying problem is fixed.

**No `TState` schema migration.**
The configured serializer governs how a changed state type reads back. Add fields as optional, never
rename or retype in place, never remove a field a running instance might read on recovery.

**No execution history, no enumeration.**
The journal is pruned behind snapshots, notifications are fire-and-forget, and instances are
addressable only by an id you already hold.

## Known limits

- **Timer lateness when passivation is re-enabled (D8).** Off by default, so this only bites a
  deployment that turns it back on.
- **Step removal is a deploy hazard (E5).** Deploying code without a step that in-flight instances
  are persisted on ends those instances.
- **Unbounded journal growth.** A workflow that loops indefinitely keeps appending events, and
  nothing resets that history. Snapshots bound how much of it recovery has to replay; they do not
  bound how much of it accumulates.
