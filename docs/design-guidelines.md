# Design guidelines

[`workflow-model.md`](workflow-model.md) documents what each construct is. This doc answers a
different question: which one do I reach for here? Seven decisions come up repeatedly when
designing a workflow — each below states the rule, then a ✅/❌ pair, then the nuance.

## 1. Step vs command handler

No *side-effecting* work in a command handler — nothing that writes, charges, sends, or mutates
state in another system. A read purely to decide the transition (e.g. look up a tier to pick the
next step) is fine.

A handler runs once with no retry/timeout coverage from the engine — a flaky read in a handler just
fails the handler outright. If a read needs retry/backoff, it belongs in a step even though it's
"just a read."

✅
```csharp scaffold=workflow-member
[WorkflowCommandHandler]
public CommandEffect<OrderState> Start(PlaceOrder cmd, CommandContext<OrderState> ctx) =>
    Effects.UpdateState(ctx.State with { Amount = cmd.Amount })
        .TransitionTo(ctx.State.CustomerId == "vip" ? Steps.FastTrackStep : Steps.ChargePaymentStep);
```

❌
```csharp scaffold=skip reason="shows a shape the engine rejects: a command handler cannot be async"
[WorkflowCommandHandler]
public async Task<CommandEffect<OrderState>> Start(PlaceOrder cmd)
{
    await _payment.Charge(cmd.Amount); // side effect: belongs in a step
    return Effects.UpdateState(...).TransitionTo(Steps.NextStep);
}
```

## 2. Step granularity

One step per independently-failing external call. Don't bundle unrelated calls into one step — it
loses independent retry/compensation per call. Don't split trivial pure logic (no I/O) into its own
step either — it adds a journal write with no retry benefit, since there's nothing in
it that can fail and need retrying.

✅ separate `ChargePaymentStep` / `SendConfirmationEmailStep`.

❌ one step doing both — an email failure re-triggers a duplicate charge attempt on retry.

## 3. Pause vs step

Pause when the next input is an external event whose timing you don't control — human approval, a
payment provider's webhook, any callback-driven signal. A step is for the workflow's own outbound
call, even one that's slow or async under the hood, because the workflow drives it and can time it
out itself.

For externals that never show up, pair the pause with `PauseSettings.WithTimeout(...)` and a
timeout-handler step.

✅
```csharp scaffold=workflow-member
[WorkflowStep]
public StepEffect<OrderState> WaitForPaymentWebhook() =>
    StepEffects.ThenPause(PauseSettings.WithTimeout(TimeSpan.FromHours(1)).TimeoutHandler(Steps.EscalateStep));
```

❌ a step that polls a payment provider in a loop instead of pausing until its webhook arrives.

## 4. Child workflow vs inline steps

Split into a child workflow when the sub-flow has its own independent retry/compensation lifecycle,
its own identity worth tracking on its own (e.g. per line-item), or needs to run in
parallel/fan-out. Stay inline (sequential steps in the parent) when the sub-flow is strictly
sequential and shares the parent's failure handling — no independent lifecycle to gain from
splitting it out.

✅ per-line-item `ItemFulfillmentWorkflow` children fanned out via `AwaitChildren`.

❌ a single child workflow wrapping one strictly-sequential three-step sub-flow that never runs in
parallel and has no separate identity.

## 5. What belongs in `TState`

`TState` holds durable data needed to resume or compensate — IDs, amounts, and decisions already
made. It's not a cache for live/volatile data that a step can re-fetch fresh when it's actually
needed; storing it risks acting on stale data after a crash-recovery replay.

✅ storing a `PaymentId` returned by a charge (needed later to refund).

❌ storing a payment provider's live "processing" status polled once and never refreshed, then
branching on it after recovery days later.

## 6. Retry/backoff policy choices

Reach for a per-step `StepRecovery` override when a step's failure profile differs from the
workflow's default (e.g. a flaky third-party call wants more retries than an in-house one); rely on
`DefaultStepRecovery` otherwise. Reach for `FailoverTo` a compensating step when exhausting retries
should trigger cleanup (e.g. refund), rather than just ending the workflow in a failed state with
nothing rolled back.

✅
```csharp scaffold=skip reason="a settings-builder fragment, shown mid-chain to keep the rule in one line"
.StepRecovery(Steps.ChargePaymentStep, RecoverStrategy.WithMaxRetries(2).FailoverTo(Steps.RefundPaymentStep))
```

❌ relying on default retries for a step whose failure leaves an external system in a
half-completed state, with no failover step to clean it up.

## 7. Testing pointers

Each decision above is provable with `WorkflowTestHarness` — see [testing.md](testing.md) for
detail:

- **Step granularity / retry choices** — a harness test asserting per-step attempt counts against
  the configured `RecoverStrategy`.
- **Pause vs step** — advance a `FakeTimeProvider` past `PauseSettings.Timeout` and call
  `RunPauseTimeoutIfDue()` to assert the workflow lands on the configured handler step.
- **Child workflow vs inline** — the harness's child-group assertions, driving `AwaitChildren`
  fan-out and its `ChildGroupResult` without a real cluster.

None of this needs `ActorSystem` or persistence — the harness applies the same effects
`WorkflowEntityActor` would.
