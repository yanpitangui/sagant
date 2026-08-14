using Sagant;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Settings;

namespace OrderFulfillment.Sample;

/// <summary>
/// End-to-end validation workflow for the engine: multi-service orchestration, retries, step and
/// workflow timeouts, pause/resume with a timeout handler, child-workflow fan-out/fan-in, and a
/// compensation cascade (not just a single undo). Orders whose total exceeds
/// <see cref="ApprovalThreshold"/> pause for manual review before anything is charged. Each line
/// item is fulfilled by its own <see cref="ItemFulfillmentWorkflow"/> child, started by
/// <see cref="FulfillItemsStep"/>.
/// </summary>
public partial class OrderFulfillmentWorkflow : Workflow<OrderState>
{
    public const int ApprovalThreshold = 10_000;

    /// <summary>Kept short, well under a realistic 24h, so the demo UI's pause-then-auto-cancel
    /// path is actually watchable in one sitting.</summary>
    public static readonly TimeSpan ApprovalPauseTimeout = TimeSpan.FromSeconds(20);

    private readonly IPaymentService _payment;
    private readonly INotificationService _notification;
    private readonly FaultInjectionRegistry _faults;

    public OrderFulfillmentWorkflow(
        IPaymentService payment,
        INotificationService notification,
        FaultInjectionRegistry faults)
    {
        _payment = payment;
        _notification = notification;
        _faults = faults;
    }

    public override OrderState EmptyState() => OrderState.Empty();

    public override WorkflowSettings Settings() => WorkflowSettings.Create()
        .Timeout(TimeSpan.FromMinutes(5), Steps.RefundPaymentStep)
        .DefaultStepTimeout(TimeSpan.FromSeconds(5))
        .StepRecovery(Steps.ChargePaymentStep, RecoverStrategy.WithMaxRetries(2).FailoverTo(Steps.RefundPaymentStep))
        // An order asked to cancel unwinds through the same refund path its own failures take,
        // rather than stopping where it stands and leaving the charge outstanding.
        .CancelVia(Steps.CancelOrderStep)
        .Build();

    [WorkflowCommandHandler]
    public CommandEffect<OrderState> Start(PlaceOrder cmd, CommandContext<OrderState> ctx)
    {
        // CustomerId is only ever "" on a genuinely fresh entity (OrderState.Empty()) — Status alone
        // can't tell "never placed" apart from "just placed, first step still in flight", since a
        // small order's own first step (ChargePaymentStep) hasn't run yet either way and Status
        // stays Started until it does. Without this guard, a second PlaceOrder against an id that's
        // already in use overwrites its state and restarts the pipeline from scratch, silently
        // corrupting whatever it was doing.
        if (ctx.State.CustomerId != "")
        {
            return Effects.Reply($"cannot place order: this id is already in use (status {ctx.State.Status})");
        }

        var amount = cmd.Items.Sum(i => i.Amount);
        var state = new OrderState(cmd.CustomerId, amount, cmd.Items, cmd.ShippingAddress, OrderStatus.Started,
            FaultStep: cmd.FaultStep, FaultPermanent: cmd.FaultPermanent);

        // The full order total is known upfront, straight from the command — no step needs to run
        // first to discover whether this order needs approval, unlike the per-item reservation work
        // that follows. Pausing here, before anything is charged or reserved, keeps a paused order
        // from holding any real-world resource while it waits on a human.
        if (amount > ApprovalThreshold)
        {
            var pauseSettings = PauseSettings.WithTimeout(ApprovalPauseTimeout)
                .WithReason("order exceeds approval threshold, awaiting manual review")
                .TimeoutHandler(Steps.AutoCancelStep);
            return Effects.UpdateState(state with { Status = OrderStatus.AwaitingApproval })
                .Pause(pauseSettings).ThenReply("accepted");
        }

        return Effects.UpdateState(state).TransitionTo(Steps.ChargePaymentStep).ThenReply("accepted");
    }

    [WorkflowCommandHandler]
    public CommandEffect<OrderState> Approve(ApproveOrder cmd, CommandContext<OrderState> ctx)
    {
        // AwaitingApproval is the only status Start's pause-for-approval path ever leaves an order
        // in — anywhere else, there's either nothing awaiting approval yet or the order has already
        // moved past it.
        if (ctx.State.Status != OrderStatus.AwaitingApproval)
        {
            return Effects.Reply($"cannot approve order: status is {ctx.State.Status}, expected {OrderStatus.AwaitingApproval}");
        }

        return Effects.TransitionTo(Steps.ChargePaymentStep).ThenReply("approved");
    }

    [WorkflowQuery]
    public QueryEffect GetState(GetOrderState query, QueryContext<OrderState> ctx) =>
        QueryEffects.Reply(ctx.State);

    [WorkflowStep]
    public async Task<StepEffect<OrderState>> ChargePaymentStep(StepContext<OrderState> ctx)
    {
        await MaybeInjectFaultAsync(nameof(ChargePaymentStep), ctx.State);
        var paymentId = await _payment.Charge(ctx.State.CustomerId, ctx.State.Amount);
        var updated = ctx.State with { PaymentId = paymentId, Status = OrderStatus.PaymentCharged };
        return StepEffects.UpdateState(updated).ThenTransitionTo(Steps.FulfillItemsStep);
    }

    [WorkflowStep]
    public StepEffect<OrderState> FulfillItemsStep(StepContext<OrderState> ctx)
    {
        var updated = ctx.State with { Status = OrderStatus.Fulfilling };
        // Scoped to this order, because an item id arrives on the command and a command is not
        // unique: a schedule sends the same one every time it fires, so every occurrence would
        // otherwise await the very same item entity — which the first occurrence already finished,
        // leaving every later order waiting on a child that will never report to it.
        var children = ctx.State.Items.Select(item => StepEffects.Child<ItemFulfillmentWorkflow>(
            ItemWorkflowId(ctx.WorkflowId, item.ItemId),
            new FulfillItem(ctx.State.CustomerId, item.Amount, ctx.State.ShippingAddress),
            // An order in a terminal state (ended, deleted, or externally terminated) has nothing
            // left for any still-running item to do — cascading Terminate keeps a deleted order from
            // leaving orphaned item entities behind.
            ParentClosePolicy.Terminate));

        // AllSuccessful (the default) now means what it says, because a child that failed reports
        // Failed. WaitForAll rather than fail-fast is a business choice: every item gets attempted,
        // so a partial order can be reasoned about, instead of stopping at the first bad one.
        return StepEffects.UpdateState(updated).AwaitChildren(
            children, options => options.AllSuccessful().WaitForAll().ResumeAt(Steps.AfterItemsFulfilledStep));
    }

    /// <summary>
    /// An item's own entity id. Already-scoped ids (the UI writes <c>{orderId}#item-N</c>) are left
    /// alone, so the read model rows placed before the command was sent still line up.
    /// </summary>
    public static string ItemWorkflowId(string orderId, string itemId) =>
        itemId.StartsWith(orderId, StringComparison.Ordinal) ? itemId : $"{orderId}#{itemId}";

    [WorkflowStep]
    public StepEffect<OrderState> AfterItemsFulfilledStep(ChildGroupResult result, StepContext<OrderState> ctx) =>
        // The group's own outcome is the answer now — no re-deriving it from each child's state.
        result.Outcome == GroupOutcome.Succeeded
            ? StepEffects.ThenTransitionTo(Steps.NotifyCustomerStep)
            : StepEffects.ThenTransitionTo(Steps.RefundPaymentStep);

    /// <summary>Demo-only fault injection (see <see cref="FaultInjectionRegistry"/>): throws before
    /// the real service call if <paramref name="stepName"/> was armed by the placing order.
    /// Permanent mode throws on every attempt — the state passed in never changes on a
    /// failed attempt, so this is deterministic across retries with no extra bookkeeping. Transient
    /// mode consumes the one-shot trap on its first hit, so a retry of the same step sails through.
    /// Also worth knowing: <see cref="RecoverStrategy"/> has no backoff/delay setting at all — a
    /// retry is immediate — so this awaits the same simulated latency a real call would take before
    /// throwing. Skipping that latency would make a forced failure resolve instantly while a real
    /// attempt takes ~2s, which would misleadingly read as "retries have no delay."</summary>
    private async Task MaybeInjectFaultAsync(string stepName, OrderState state)
    {
        if (state.FaultStep != stepName)
        {
            return;
        }

        if (!state.FaultPermanent && !_faults.ConsumeOneShot(state.CustomerId, stepName))
        {
            return;
        }

        await DemoLatency.Simulate();
        throw new InvalidOperationException($"{stepName} failed (injected)");
    }

    [WorkflowStep]
    public async Task<StepEffect<OrderState>> NotifyCustomerStep(StepContext<OrderState> ctx)
    {
        // Best-effort: a failed notification shouldn't fail an otherwise-successful order, so the
        // failure is swallowed here directly, bypassing the engine's retry/failover machinery entirely.
        try
        {
            await _notification.Send(ctx.State.CustomerId, $"Your order ({ctx.State.Items.Count} item(s)) has shipped.");
        }
        catch
        {
            // logged/ignored in a real system; not fatal to order fulfillment
        }

        return StepEffects.UpdateState(ctx.State with { Status = OrderStatus.Succeeded }).ThenComplete();
    }

    [WorkflowStep]
    public async Task<StepEffect<OrderState>> RefundPaymentStep(StepContext<OrderState> ctx)
    {
        if (ctx.State.PaymentId is { } paymentId)
        {
            await _payment.Refund(paymentId);
        }

        // How the order got here decides how the run reports: an order that was cancelled — whether
        // by a caller or by the approval timeout — finishes as cancelled; anything else reaching the
        // refund path (an item's own fulfillment failure, or the workflow-level timeout) is a
        // failure, and says so.
        //
        // A sibling item that had already completed its own reservation before another item failed
        // keeps that reservation — unwinding an already-completed child's work back through it is out
        // of scope for this demo.
        return ctx.State.Status == OrderStatus.Cancelled
            ? StepEffects.UpdateState(ctx.State).ThenCancel(ctx.State.FailureReason ?? "order cancelled")
            : StepEffects.UpdateState(ctx.State with { Status = OrderStatus.Failed })
                .ThenFail(ctx.State.FailureReason ?? "order could not be fulfilled");
    }

    [WorkflowStep]
    public StepEffect<OrderState> AutoCancelStep(StepContext<OrderState> ctx)
    {
        var updated = ctx.State with { Status = OrderStatus.Cancelled, FailureReason = "approval timeout" };
        return StepEffects.UpdateState(updated).ThenTransitionTo(Steps.RefundPaymentStep);
    }

    /// <summary>
    /// Where a <c>Cancel</c> lands (see <c>Settings()</c>). Marks the order cancelled and unwinds
    /// through the same refund path everything else uses, so a cancelled order does not leave a
    /// charge outstanding — which is exactly what stopping it abruptly would have done.
    /// </summary>
    [WorkflowStep]
    public StepEffect<OrderState> CancelOrderStep(WorkflowCancellation cancellation, StepContext<OrderState> ctx)
    {
        var updated = ctx.State with
        {
            Status = OrderStatus.Cancelled,
            FailureReason = cancellation.Reason ?? "cancelled",
        };
        return StepEffects.UpdateState(updated).ThenTransitionTo(Steps.RefundPaymentStep);
    }
}
