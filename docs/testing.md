# Testing a workflow

`WorkflowTestHarness<TWorkflow, TState>`, in the `Sagant.Testing` package, drives a workflow's own
command/step/query logic directly — no `ActorSystem`, no persistence, no `ClusterSharding`.

It is not a reimplementation of the runtime. It holds the same `WorkflowRuntimeState<TState>` the
durable driver persists (exposed as `harness.Envelope`), decides transitions through the same
`WorkflowTransitionPlanner`, and dispatches through the same generated
`IWorkflowStepDispatcher<TState>`/`IWorkflowCommandDispatcher<TState>`/`IWorkflowQueryDispatcher<TState>`
tables production traffic uses. Deadlines, retry budget, pause rules and child-group policy are
therefore literally the same code, not a matching copy — see
[guarantees.md](guarantees.md) for what that fixes in place.

Prefer this for pure step/command/retry/pause/child-group logic; reach for real Akka test
infrastructure only when the thing under test is the actor/cluster-sharding driver itself (crash
recovery, real timing races, real persistence) — see
`samples/OrderFulfillment/OrderFulfillment.Tests` for that shape of test.

## Basic usage

```csharp scaffold=statements
var harness = new WorkflowTestHarness<OrderFulfillmentWorkflow, OrderState>(
    new OrderFulfillmentWorkflow(fakePaymentService));

var effect = await harness.RunUntilStop(new PlaceOrder(500));

Assert.IsType<Transition.TerminalTransition>(effect.Transition);
```

- `RunCommand(command)` dispatches one command to its `[WorkflowCommandHandler]` and returns the
  `CommandEffect<TState>` — use this to assert on a command that doesn't itself enter the step
  chain.
- `RunStep(Steps.X)` / `RunStep(Steps.X, input)` dispatches one `[WorkflowStep]` by its generated
  `Steps` ref and returns the `StepEffect<TState>` — use this to assert on one specific step in
  isolation.
- `RunUntilStop(command)` dispatches a command, then follows `StepTransition`s automatically — the
  same chain `WorkflowEntityActor` would drive in production — until it reaches a pause, end,
  delete, or no-transition, and returns whichever step effect stopped the chain. Use this for
  "does the whole path work" tests.
- `RunUntilStop(Steps.X)` (overload taking a `StepRef` instead of a command) resumes the step chain
  from a specific step — useful for jumping straight into a branch (e.g. a compensation cascade)
  after hand-seeding `harness.State`, without replaying everything that would normally lead there.
- `harness.State` is settable directly for exactly that purpose.
- `RunQuery<TQuery, TReply>(query)` dispatches a `[WorkflowQuery]` and returns its reply. A query
  cannot persist or transition, so this never advances the harness.
- `harness.Envelope` exposes the full runtime envelope for asserting on runtime-owned bookkeeping —
  deadlines, retry count, child relationships — rather than on business state.
- `harness.Notifications` collects every notification the planner decided to publish, in order, so a
  test can assert on the lifecycle a subscriber would have observed.
- The constructor takes an optional `instanceId`, used where a durable driver would use its
  persistence id — group ids and relationship ids derive from it.

## Queries running alongside a step

A query dispatches immediately rather than waiting for a running step, which is the one concurrency
the runtime permits. `RunStepInterleaved` opens that window in a test:

```csharp scaffold=statements
var reply = OrderStatus.Pending;
await harness.RunStepInterleaved(OrderFulfillmentWorkflow.Steps.SlowStep, async () =>
{
    reply = await harness.RunQuery<GetProgress, OrderStatus>(new GetProgress());
    gate.SetResult();   // release the step
});
```

The step body has to cooperate by parking on something the callback releases (a
`TaskCompletionSource`), otherwise it simply runs to completion first and nothing interleaves.
Dispatch a query from the callback — a command handler dispatched here would model something the
runtime deliberately prevents by deferring commands until an in-flight step settles.

## Retries and failover

A step that throws is retried against its resolved `RecoverStrategy` (step-specific override, else
`WorkflowSettings.DefaultStepRecoverStrategy`), then fails over to the configured step once the
retry budget is exhausted — decided by `WorkflowTransitionPlanner.PlanStepFailure`, the same call
the durable driver makes. Retries
run back-to-back with no simulated wait: `RecoverStrategy.BackoffForAttempt` is a pure
`Func<int, TimeSpan>`, directly unit-testable on its own with no harness involved at all, so the
harness only needs to exercise the retry/failover *decision*, not the delay. A step with no
`RecoverStrategy` configured just lets the exception propagate straight to the caller.

```csharp scaffold=test-member
[Fact]
public async Task ChargePayment_RetriesThenFailsOver()
{
    var harness = new WorkflowTestHarness<OrderFulfillmentWorkflow, OrderState>(
        new OrderFulfillmentWorkflow(new FlakyPaymentService(failuresBeforeSuccess: 3)));

    var effect = await harness.RunStep(OrderFulfillmentWorkflow.Steps.ChargePaymentStep);

    Assert.Equal("RefundPaymentStep", ((Transition.StepTransition)effect.Transition).StepName);
}
```

## Pause and timeouts

The harness takes a `TimeProvider` (defaulting to `TimeProvider.System`) — pass a
`Microsoft.Extensions.Time.Testing.FakeTimeProvider` to control time in a test:

```csharp scaffold=statements
var time = new FakeTimeProvider();
var harness = new WorkflowTestHarness<ApprovalWorkflow, ApprovalState>(new ApprovalWorkflow(), timeProvider: time);

await harness.RunUntilStop(new SubmitForApproval());   // pauses with PauseSettings.Timeout

var effect = await harness.AdvanceTime(TimeSpan.FromMinutes(31));

Assert.IsType<Transition.TerminalTransition>(effect!.Transition);
```

`AdvanceTime(delta)` advances the `FakeTimeProvider` and immediately checks for a due pause timeout
followed by a due workflow timeout, firing whichever is due first — prefer this over calling
`FakeTimeProvider.Advance` directly and calling `RunPauseTimeoutIfDue`/`RunWorkflowTimeoutIfDue`
yourself, since it's easy to advance time and forget the follow-up call, producing a test that
silently never exercises the timeout it meant to prove. `AdvanceTime` throws
`InvalidOperationException` if the harness wasn't constructed with a `FakeTimeProvider`.

`RunPauseTimeoutIfDue()`/`RunWorkflowTimeoutIfDue()` are available directly when you want to assert
"nothing fires yet" at an intermediate point, distinct from firing the timeout itself.

## Control plane

`Suspend`/`Resume`/`Terminate`/`Cancel` are available directly, deciding through the same planner the
durable driver uses:

```csharp scaffold=statements
harness.Suspend();                     // holds it where it stands
var effect = await harness.Resume();   // restarts the held step from the beginning
harness.Terminate("operator stopped it");
await harness.Cancel("customer changed their mind");   // unwinds through CancellationStepName
```

A command that doesn't apply from the current status throws `WorkflowCommandException` carrying the
same message a caller would see — so `Assert.Throws<WorkflowCommandException>(() => harness.Resume())`
tests the rejection path.

`Cancel` runs the configured cancellation step chain to its own conclusion and returns whichever
effect it settled on; with no cancellation step configured the run finishes as cancelled immediately
and it returns `null`.

## Testing child workflows

Register a child harness under the workflow id an `AwaitChildren` effect will use, run it to
completion, then deliver its outcome to the parent:

```csharp scaffold=statements
var childHarness = new WorkflowTestHarness<LineItemWorkflow, LineItemState>(new LineItemWorkflow(fakeInventory));
var parentHarness = new WorkflowTestHarness<OrderFulfillmentWorkflow, OrderState>(new OrderFulfillmentWorkflow())
    .WithChild("SKU-123", childHarness);

await parentHarness.RunUntilStop(new PlaceOrder(500));                        // parent starts the child, awaits the group
await childHarness.RunUntilStop(new ProcessLineItem("SKU-123", 2));           // run the child to its own terminal status

await parentHarness.DeliverChildLifecycle("SKU-123");   // runs the parent's resume step
```

`DeliverChildLifecycle(childWorkflowId)` finds the child's one active group automatically; use the
group-explicit overload (`DeliverChildLifecycle(groupId, childWorkflowId)`) when a test intentionally
models more than one active relationship for the same child id. `RedeliverChildLifecycle` replays a
delivery to a group that may have already finalized — a no-op once it has, mirroring the actor
runtime's generation/finalization guard, useful for proving redelivery-after-finalization is safe.

The child harness must have reached a terminal status (`Ended`/`Deleted`/`Terminated`) before its
lifecycle can be delivered — delivering against a still-running child throws
`InvalidOperationException`.

See [child-workflows.md](child-workflows.md) for the child-workflow model itself.
