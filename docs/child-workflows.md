# Child workflows

A step can start one or more other workflow instances as **children**, and durably wait for their
outcomes before resuming. This is how a saga composes out of smaller sagas — e.g. a fulfillment
workflow that starts one child workflow per line item, or a batch workflow that fans out to N
independent child workflows and waits for all of them.

Everything here lives in the runtime-agnostic core (`Sagant.Effects`/`Sagant.Protocol`) — a
child-workflow wait is persisted data, applied by whatever runtime driver is running the parent,
the same way every other transition is. `Sagant.Runtime.Akka` is what actually spawns and addresses
the child actors underneath (see [akka-runtime.md](akka-runtime.md#child-workflows)); a workflow
author never sees that layer.

## Starting children

`StepEffectsBuilder<TState>.Child<TWorkflow>` describes one child to start — it resolves the
child's durable type name at compile time (`IWorkflowTypeInfo.WorkflowTypeName`), no reflection, no
instance constructed yet:

```csharp scaffold=workflow-member
[WorkflowStep]
public StepEffect<OrderState> StartLineItemWorkflows(StepContext<OrderState> ctx)
{
    var children = ctx.State.LineItems.Select(item =>
        StepEffects.Child<LineItemWorkflow>(
            workflowId: item.Sku,
            command: new ProcessLineItem(item.Sku, item.Quantity)));

    return StepEffects.AwaitChildren(children, Steps.OnLineItemsDone);
}

[WorkflowStep]
public StepEffect<OrderState> OnLineItemsDone(ChildGroupResult result, StepContext<OrderState> ctx)
{
    if (result.Outcome == GroupOutcome.Failed)
    {
        return StepEffects.UpdateState(ctx.State with { Status = OrderStatus.Failed }).ThenFail("line item failure");
    }

    var results = result.GetAll<LineItemWorkflow, LineItemState>();
    return StepEffects.UpdateState(ctx.State with { LineItems = results.Values.ToList() }).ThenComplete();
}
```

- `workflowId` is the child's durable identity, and doubles as the lookup key you use later to read
  its result back out of `ChildGroupResult`.
- `command` is whatever command the child's own `[WorkflowCommandHandler]` expects to start it —
  same command-dispatch machinery as any external caller uses.
- Children in one `AwaitChildren` call can be heterogeneous — different workflow types — or use the
  `AwaitChildren<TWorkflow>(...)` homogeneous convenience overload when every child in the group is
  the same type; both produce the exact same persisted transition.

## Waiting for a group

`AwaitChildren` starts the given children and transitions the parent into a durable wait; the
parent resumes at `resumeStepName` once the group's policy is satisfied, with a `ChildGroupResult`
as that step's input. The two-argument overload (`AwaitChildren(children, resumeStepName)`) covers
the common case — all children must succeed, fail fast on the first failure, terminate the rest.
For anything else, use the `Action<ChildGroupOptions>` overload:

```csharp scaffold=skip reason="an options chain on its own, so the policy knobs read as a list"
StepEffects.AwaitChildren(children, options => options
    .AllCompleted()
    .WaitForAll()
    .ContinueRemaining()
    .ResumeAt(Steps.OnLineItemsDone));
```

Three independent policies control a group:

| Policy | Values | Answers |
|---|---|---|
| `CompletionPolicy` | `AllSuccessful` (default) — every member must reach `Completed`.<br>`AllCompleted` — every member must reach *a* terminal status; the resume step inspects individual outcomes itself. | What does "the group succeeded" mean? |
| `FailurePolicy` | `FailFast` (default) — finalize the moment a failure makes the configured `CompletionPolicy` impossible, without waiting for remaining members.<br>`WaitForAll` — finalize only once every member has reached a terminal status. | When is the group's outcome known? |
| `RemainingChildrenPolicy` | `Terminate` (default) — send `Terminate` to every still-running member once the group finalizes (fire-and-forget).<br>`Continue` — leave still-running members alone; they run to their own conclusion independently. | What happens to stragglers once the outcome is known? |

These three are independent by design: "we know the outcome" (`FailurePolicy`), "what that outcome
means" (`CompletionPolicy`), and "what to do about stragglers" (`RemainingChildrenPolicy`) are
separate decisions a workflow author might want to make differently case by case.

`ResumeAt` (or the `resumeStepName` string argument) is the only required call — every other policy
defaults to the common case above. `GroupId` is optional; omit it and the runtime driver generates
a durable one, or supply an explicit id to refer to a specific group by a human-meaningful name
later (useful when a parent starts more than one `AwaitChildren` group over its lifetime and needs
to tell them apart, e.g. in `WorkflowTestHarness.DeliverChildLifecycle`).

## Reading results

`ChildGroupResult`, passed as the resume step's input, is the one and only place to read a group's
outcome:

```csharp scaffold=workflow-member
[WorkflowStep]
public StepEffect<OrderState> OnLineItemsDone(ChildGroupResult result)
{
    var status = result.GetStatus("SKU-123");             // ChildStatus, never throws
    var failure = result.GetFailure("SKU-123");           // WorkflowFailure?, null unless Failed

    var one = result.Get<LineItemWorkflow, LineItemState>("SKU-123");       // throws if unavailable
    var ok = result.TryGet<LineItemWorkflow, LineItemState>("SKU-123", out var state);

    var all = result.GetAll<LineItemWorkflow, LineItemState>();             // homogeneous convenience

    return StepEffects.ThenComplete();
}
```

`Get`/`GetAll` throw one of three specific exceptions rather than an unqualified cast failure:

- `ChildNotInGroupException` — the `workflowId` isn't a member of this group (typically a typo).
- `ChildWorkflowTypeMismatchException` — the member exists, but its actual persisted workflow type
  doesn't match `TWorkflow`.
- `ChildResultNotAvailableException` — the member matches, but never reached `ChildStatus.Completed`
  (covers `Failed`, `Cancelled`, `Terminated`, and still-`Pending` uniformly — a group can finalize
  with a member still non-terminal under `RemainingChildrenPolicy.Continue` or a not-yet-confirmed
  `Terminate`).

`TryGet` never throws — it returns `false` for every reason `Get` would throw, the standard .NET
`TryXxx` contract.

## `ParentClosePolicy`

Set per child at `Child<TWorkflow>(...)` call time, matching Temporal's own per-child-start
`ParentClosePolicy` model:

- `Abandon` (default) — the child keeps running independently of its parent's own lifecycle.
- `Terminate` — the child is sent `Terminate` when the parent reaches any terminal status.

There's no cooperative-cancel value: this project's `Terminate` is already unconditional, bypassing
business code entirely, same as an operator-invoked `Terminate`. A cooperative cancel primitive
(one that lets the child's own business logic decide how to unwind) is a documented gap, not yet
built.

## Nesting

A child workflow can itself start and await children of its own — nothing here is scoped to one
level. `WorkflowRuntimeState.ParentRelationship` (am I a child) and `WorkflowRuntimeState.Children`/
`ChildGroups` (are these my children) are independent fields; a workflow instance can hold both at
once, simultaneously the child of one workflow and the parent of another. A child's own steps call
`StepEffects.Child<TWorkflow>`/`AwaitChildren` through the exact same `StepEffectsBuilder<TState>`
every workflow gets — there's no separate API for "a child that's also a parent."

`ParentClosePolicy.Terminate` cascades through however many levels exist: every workflow instance
runs the identical `WorkflowEntityActor<TWorkflow, TState>` type, so a `Terminate` a grandparent
sends to its child causes that child's own `HandleTerminate` to apply its own `ParentClosePolicy` to
its own children in turn, sending `Terminate` onward — the same logic runs at every level, not a
special case for depth. `WorkflowTestHarness.WithChild` composes the same way: the child harness is
just another `WorkflowTestHarness<TChildWorkflow, TChildState>`, so it can register its own children
via its own `WithChild` call.

## Durability guarantees

- A child-start command is safely redeliverable any number of times — the relationship's
  deterministic id (`{ParentWorkflowId}:{GroupId}:{ChildWorkflowId}`) doubles as the idempotency key
  for the start command itself, so a crash between sending and persisting never double-starts a
  child.
- A group can legitimately finalize with a member still `Pending` or `TerminationRequested` — the
  parent's resume step never waits for a straggler to actually confirm it stopped.
- Every parent/child relationship a workflow instance has ever held — across every group, whether
  or not that group has since finalized — is tracked in exactly one place
  (`WorkflowRuntimeState.Children`); group state (`WorkflowRuntimeState.ChildGroups`) holds policy
  and finalization only, never a duplicate of member status.
- The engine's own cascade `Terminate` (`ParentClosePolicy.Terminate`/`RemainingChildrenPolicy.Terminate`)
  rides the same reliable `Akka.Delivery` pipeline as a child-start command: at-least-once,
  automatically resent across a shard relocation, with no caller present to retry it if it went
  missing. `IWorkflowHandle.Suspend`/`Resume`/`Terminate`/`GetStatus` reach the shard region directly
  instead, with a live caller's own retry on a timed-out call as that path's redelivery mechanism.

## Testing

`WorkflowTestHarness` supports child workflows without any Akka infrastructure — register a child
harness, run it to completion, then deliver its outcome to the parent. See
[testing.md](testing.md#testing-child-workflows).
