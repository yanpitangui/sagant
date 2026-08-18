# Sagant docs

The [root README](../README.md) is a quickstart. These documents go deeper on specific parts of the
engine:

| Doc | Covers |
|---|---|
| [guarantees.md](guarantees.md) | **The contract.** What Sagant promises (durability, concurrency, execution, children, queries, tracing), what you are responsible for, and what it deliberately does not guarantee. Start here if you're deciding whether Sagant fits. |
| [workflow-model.md](workflow-model.md) | `Workflow<TState>`, effects, transitions, the source generator, settings, retries, and pause. The runtime-agnostic core. |
| [design-guidelines.md](design-guidelines.md) | Which construct to reach for: step vs command handler, step granularity, pause vs step, child workflow vs inline, what belongs in `TState`, retry/backoff choices. |
| [child-workflows.md](child-workflows.md) | Starting child workflows from a step, `AwaitChildren` groups, completion/failure/remaining-children policies, `ParentClosePolicy`, and reading results back out. |
| [akka-runtime.md](akka-runtime.md) | How `Sagant.Runtime.Akka` actually runs a workflow: `WorkflowEntityActor`, persistence, timeouts, retries, tracing, `Akka.Delivery`, idempotency, graceful shutdown, and the full `WithWorkflow` parameter reference. |
| [deadlines-and-scheduling.md](deadlines-and-scheduling.md) | `WithWorkflowDeadlines` (bounding passivation lateness for any workflow) vs. `WithScheduling`/`ScheduleWorkflow` (recurring cron/interval schedules) — two different extensions, full parameter reference for both. |
| [integration-guide.md](integration-guide.md) | Wiring Sagant into a real host: DI registration, clustering, single-node vs. multi-node, observability. |
| [testing.md](testing.md) | `WorkflowTestHarness<TWorkflow, TState>` — driving a workflow's own logic with no `ActorSystem`. |

For terminology (what "effect," "transition," "child group," etc. mean) see [`CONTEXT.md`](../CONTEXT.md)
at the repo root. Architectural decisions live under [`docs/adr/`](adr/).
