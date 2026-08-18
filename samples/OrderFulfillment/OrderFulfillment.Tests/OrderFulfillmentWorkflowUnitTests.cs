using Sagant.Effects;
using Sagant.Testing;
using OrderFulfillment.Sample;

namespace OrderFulfillment.Tests;

/// <summary>
/// Same workflow, same fakes as <see cref="OrderFulfillmentWorkflowTests"/> — but driven through
/// <see cref="WorkflowTestHarness{TWorkflow,TState}"/>, with no real <c>ClusterSharding</c> region
/// behind it, in-mem or otherwise. No <c>ActorSystem</c>, no persistence, no <c>await Task.Delay</c>
/// polling loops (retries run back-to-back with no simulated wait — see the harness's own doc
/// comment) — every test here runs in milliseconds, including
/// <see cref="ItemFulfillmentWorkflow"/>'s own retry-then-failover-to-<c>ReleaseItemStep</c> cascade.
/// These tests are about the workflows' own step/command *logic*, including how the parent drives
/// its item children via <see cref="WorkflowTestHarness{TWorkflow,TState}.WithChild{TChildWorkflow,TChildState}"/>.
/// </summary>
public class OrderFulfillmentWorkflowUnitTests
{
    private static WorkflowTestHarness<OrderFulfillmentWorkflow, OrderState> CreateHarness(
        FakePaymentService? payment = null,
        FakeNotificationService? notification = null,
        FaultInjectionRegistry? faults = null) =>
        new(new OrderFulfillmentWorkflow(
            payment ?? new FakePaymentService(),
            notification ?? new FakeNotificationService(),
            faults ?? new FaultInjectionRegistry()),
            instanceId: OrderId);

    /// <summary>The order instance these tests drive. Named, because an item's entity id is scoped to
    /// its order and a test asserting on children has to know which order they belong to.</summary>
    private const string OrderId = "order-1";

    /// <summary>The id an item's own entity gets, scoped to its order the same way the workflow
    /// scopes it — an item id arrives on the command, and a command is not unique to one order.</summary>
    private static string ItemId(string itemId) =>
        OrderFulfillmentWorkflow.ItemWorkflowId(OrderId, itemId);

    private static WorkflowTestHarness<ItemFulfillmentWorkflow, ItemState> CreateItemHarness(
        FakeInventoryService? inventory = null, FakeShippingService? shipping = null) =>
        new(new ItemFulfillmentWorkflow(inventory ?? new FakeInventoryService(), shipping ?? new FakeShippingService()));

    [Fact]
    public async Task HappyPath_TwoItems_RunsToEndAsSucceeded()
    {
        var notification = new FakeNotificationService();
        var itemA = CreateItemHarness();
        var itemB = CreateItemHarness();
        var harness = CreateHarness(notification: notification)
            .WithChild(ItemId("item-a"), itemA)
            .WithChild(ItemId("item-b"), itemB);

        var items = new[] { new OrderLineItem("item-a", 300), new OrderLineItem("item-b", 200) };
        var afterFulfill = await harness.RunUntilStop(new PlaceOrder("cust-1", items, "1 Main St"));

        // Stops at the AwaitChildren transition — nothing left to drive without delivering the
        // children's own terminal state, same as WorkflowTestHarnessChildrenTests' pattern.
        Assert.Equal(Sagant.Protocol.WorkflowStatus.Running, harness.Status);
        Assert.NotNull(harness.State.PaymentId);
        Assert.Equal(OrderStatus.Fulfilling, harness.State.Status);
        Assert.IsType<Transition.AwaitChildrenTransition>(afterFulfill.Transition);

        await itemA.RunUntilStop(new FulfillItem("cust-1", 300, "1 Main St"));
        await itemB.RunUntilStop(new FulfillItem("cust-1", 200, "1 Main St"));
        Assert.Equal(ItemStatus.Shipped, itemA.State.Status);
        Assert.Equal(ItemStatus.Shipped, itemB.State.Status);

        await harness.DeliverChildLifecycle(ItemId("item-a"));
        await harness.DeliverChildLifecycle(ItemId("item-b"));

        Assert.Equal(OrderStatus.Succeeded, harness.State.Status);
        Assert.Single(notification.Sent);
        Assert.Equal(Sagant.Protocol.WorkflowStatus.Finished, harness.Status);
    }

    [Fact]
    public async Task OneItemFails_WholeOrderRefundsAndFails()
    {
        var payment = new FakePaymentService();
        var itemA = CreateItemHarness();
        var itemB = CreateItemHarness(inventory: new FakeInventoryService
        {
            ReserveOverride = (_, _) => throw new InvalidOperationException("out of stock"),
        });
        var harness = CreateHarness(payment: payment)
            .WithChild(ItemId("item-a"), itemA)
            .WithChild(ItemId("item-b"), itemB);

        var items = new[] { new OrderLineItem("item-a", 300), new OrderLineItem("item-b", 200) };
        await harness.RunUntilStop(new PlaceOrder("cust-2", items, "1 Main St"));

        await itemA.RunUntilStop(new FulfillItem("cust-2", 300, "1 Main St"));
        // ReserveItemStep's own RecoverStrategy (2 retries, then failover to ReleaseItemStep) runs
        // the same decision the real actor would, just with no simulated wait — 3 throwing attempts
        // resolve immediately, ending this child at ItemStatus.Failed via its own compensation.
        await itemB.RunUntilStop(new FulfillItem("cust-2", 200, "1 Main St"));
        Assert.Equal(ItemStatus.Failed, itemB.State.Status);

        await harness.DeliverChildLifecycle(ItemId("item-a"));
        await harness.DeliverChildLifecycle(ItemId("item-b"));

        Assert.Equal(OrderStatus.Failed, harness.State.Status);
        Assert.Contains(payment.Refunded, id => id == harness.State.PaymentId);
    }

    [Fact]
    public async Task LargeOrder_Start_PausesBeforeChargingInsteadOfImmediately()
    {
        var harness = CreateHarness();
        var items = new[] { new OrderLineItem("item-a", OrderFulfillmentWorkflow.ApprovalThreshold + 1) };

        var effect = harness.RunCommand(new PlaceOrder("cust-3", items, "1 Main St"));

        Assert.IsType<Transition.PauseTransition>(effect.Transition);
        Assert.Equal(OrderStatus.AwaitingApproval, harness.State.Status);
        Assert.Null(harness.State.PaymentId);
    }

    [Fact]
    public async Task LargeOrder_ApproveCommand_RunsRestOfChainToAwaitChildren()
    {
        var harness = CreateHarness();
        var items = new[] { new OrderLineItem("item-a", OrderFulfillmentWorkflow.ApprovalThreshold + 1) };
        harness.State = harness.State with
        {
            CustomerId = "cust-4", Amount = items[0].Amount, Items = items,
            ShippingAddress = "1 Main St", Status = OrderStatus.AwaitingApproval,
        };

        var final = await harness.RunUntilStop(new ApproveOrder());

        Assert.Equal(OrderStatus.Fulfilling, harness.State.Status);
        Assert.NotNull(harness.State.PaymentId);
        Assert.IsType<Transition.AwaitChildrenTransition>(final.Transition);
    }

    [Fact]
    public async Task PermanentFault_OnChargeStep_ThrowsEveryAttempt()
    {
        var harness = CreateHarness();
        var items = new[] { new OrderLineItem("item-a", 500) };
        harness.State = harness.State with
        {
            CustomerId = "cust-5",
            Amount = 500,
            Items = items,
            FaultStep = nameof(OrderFulfillmentWorkflow.ChargePaymentStep),
            FaultPermanent = true,
        };

        // ChargePaymentStep's RecoverStrategy allows 2 retries before failing over — permanent mode
        // never disarms, so all 3 attempts throw and the harness itself fails over to
        // RefundPaymentStep, swallowing the exception on the way.
        var effect = await harness.RunStep(OrderFulfillmentWorkflow.Steps.ChargePaymentStep);

        Assert.Equal(
            new Transition.StepTransition(nameof(OrderFulfillmentWorkflow.RefundPaymentStep), null),
            effect.Transition);
    }

    [Fact]
    public async Task TransientFault_OnChargeStep_ThrowsOnceThenSucceeds()
    {
        var faults = new FaultInjectionRegistry();
        var harness = CreateHarness(faults: faults);
        var items = new[] { new OrderLineItem("item-a", 500) };
        harness.State = harness.State with
        {
            CustomerId = "cust-6",
            Amount = 500,
            Items = items,
            FaultStep = nameof(OrderFulfillmentWorkflow.ChargePaymentStep),
            FaultPermanent = false,
        };

        // First attempt consumes the one-shot trap and throws; the harness retries automatically per
        // ChargePaymentStep's RecoverStrategy, and the second attempt sails through — one RunStep call.
        var effect = await harness.RunStep(OrderFulfillmentWorkflow.Steps.ChargePaymentStep);

        Assert.NotNull(harness.State.PaymentId);
        Assert.Equal(
            new Transition.StepTransition(nameof(OrderFulfillmentWorkflow.FulfillItemsStep), null),
            effect.Transition);
    }

    [Fact]
    public async Task RefundPaymentStep_FromMidFlightState_EndsFailed()
    {
        // Jump straight into the compensation branch — no need to actually fail a real step first.
        var payment = new FakePaymentService();
        var harness = CreateHarness(payment: payment);
        harness.State = harness.State with
        {
            CustomerId = "cust-7",
            Amount = 500,
            PaymentId = "payment-cust-7",
            Status = OrderStatus.Fulfilling,
        };

        var final = await harness.RunUntilStop(OrderFulfillmentWorkflow.Steps.RefundPaymentStep);

        Assert.Equal(OrderStatus.Failed, harness.State.Status);
        Assert.Contains("payment-cust-7", payment.Refunded);
        Assert.IsType<Transition.TerminalTransition>(final.Transition);
    }
}
