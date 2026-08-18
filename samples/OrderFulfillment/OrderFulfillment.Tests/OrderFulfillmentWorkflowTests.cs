using Sagant.Clients;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Protocol;
using Sagant.Runtime.Akka;
using Sagant.Runtime.Akka.Serialization;
using Akka.Actor;
using Akka.TestKit;
using OrderFulfillment.Sample;
using OrderFulfillment.Tests.Support;

namespace OrderFulfillment.Tests;

/// <summary>
/// Integration tests that go through the real path: <see cref="WorkflowRef{TWorkflow, TState}"/>
/// over a real, self-joined <c>ClusterSharding</c> shard region (see
/// <see cref="WorkflowClusterTestHarness{TWorkflow, TState}"/>) — not a bare actor. These exercise
/// the workflow's own orchestration/compensation logic end-to-end through the same path production
/// traffic takes, including a real <see cref="ItemFulfillmentWorkflow"/> child running as its own
/// sharded entity, standing in for nothing.
///
/// Exception: <see cref="LargeOrder_PauseTimesOutWithoutApproval_AutoCancelsWithoutCharging"/>
/// needs deterministic control over a timer, which requires swapping in a virtual-time
/// <see cref="TestScheduler"/> — incompatible with a real cluster (see the harness's doc comment),
/// so that one test talks to a bare entity actor instead.
/// </summary>
public class OrderFulfillmentWorkflowTests : Akka.TestKit.Xunit2.TestKit
{
    public OrderFulfillmentWorkflowTests() : base(RawActorConfig)
    {
    }

    private const string RawActorConfig = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.scheduler.implementation = "Akka.TestKit.TestScheduler, Akka.TestKit"
        akka.loglevel = OFF
        """;

    private TestScheduler Scheduler => (TestScheduler)Sys.Scheduler;

    private static Task<WorkflowClusterTestHarness<OrderFulfillmentWorkflow, OrderState>> StartAsync(
        FakeInventoryService inventory,
        FakePaymentService payment,
        FakeShippingService shipping,
        FakeNotificationService notification) =>
        WorkflowClusterTestHarness<OrderFulfillmentWorkflow, OrderState>.StartAsync(
            // ActorSystem names allow only word chars + non-leading '-'; test method names carry
            // no meaning here anyway (entity id is what's asserted on), so use a fresh GUID.
            $"sys-{Guid.NewGuid():N}",
            () => new OrderFulfillmentWorkflow(payment, notification, new FaultInjectionRegistry()),
            configureExtra: builder => builder.WithWorkflow<ItemFulfillmentWorkflow, ItemState>(
                () => new ItemFulfillmentWorkflow(inventory, shipping)));

    private static async Task<OrderState> AwaitStatusAsync(
        IWorkflowHandle<OrderFulfillmentWorkflow> workflow,
        OrderStatus status,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            var state = await workflow.Query<GetOrderState, OrderState>(new GetOrderState(), TimeSpan.FromSeconds(5));
            if (state.Status == status)
            {
                return state;
            }

            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(50, cts.Token);
        }
    }

    [Fact]
    public async Task HappyPath_TwoItems_GoesThroughAllStepsAndSucceeds()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-1");

        var items = new[] { new OrderLineItem("order-1#item-0", 300), new OrderLineItem("order-1#item-1", 200) };
        var accepted = await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-1", items, "1 Main St"), TimeSpan.FromSeconds(5));
        Assert.Equal("accepted", accepted);

        var final = await AwaitStatusAsync(workflow, OrderStatus.Succeeded, TimeSpan.FromSeconds(10));

        Assert.Equal(OrderStatus.Succeeded, final.Status);
        Assert.NotNull(final.PaymentId);
        Assert.Single(notification.Sent);
    }

    /// <summary>
    /// The pattern a workflow author copies to check their own <c>TState</c>/commands: build a
    /// realistic value, round-trip it through the <see cref="ActorSystem"/> <c>WithWorkflow</c> set
    /// up (the same one <see cref="WorkflowRuntimeStateSerializer"/>/<c>SagantSerializer</c> are
    /// bound to), and assert the result matches. <c>OrderState</c> is a plain record, carrying no
    /// serialization attributes of its own — exactly the case
    /// <see cref="SerializationRoundTripAssertions.AssertRoundTrips{T}"/> exists to prove works.
    /// </summary>
    [Fact]
    public async Task OrderState_And_PlaceOrder_RoundTripThroughTheRealActorSystem()
    {
        await using var harness = await StartAsync(
            new FakeInventoryService(), new FakePaymentService(), new FakeShippingService(), new FakeNotificationService());

        var items = new List<OrderLineItem> { new("order-9#item-0", 300), new("order-9#item-1", 200) };
        var state = new OrderState("cust-9", 500, items, "1 Main St", OrderStatus.Fulfilling, PaymentId: "pay-9");
        var roundTrippedState = SerializationRoundTripAssertions.AssertRoundTrips(harness.System, state);
        Assert.Equal(state with { Items = [] }, roundTrippedState with { Items = [] });
        Assert.Equal(state.Items, roundTrippedState.Items);

        var command = new PlaceOrder("cust-9", items, "1 Main St");
        var roundTrippedCommand = SerializationRoundTripAssertions.AssertRoundTrips(harness.System, command);
        Assert.Equal(command with { Items = [] }, roundTrippedCommand with { Items = [] });
        Assert.Equal(command.Items, roundTrippedCommand.Items);
    }

    [Fact]
    public async Task ChargePaymentFails_EndsFailedWithoutReservingAnyItem()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService { ChargeOverride = (_, _) => throw new InvalidOperationException("card declined") };
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-2");

        var items = new[] { new OrderLineItem("order-2#item-0", 500) };
        await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-2", items, "1 Main St"), TimeSpan.FromSeconds(5));

        var final = await AwaitStatusAsync(workflow, OrderStatus.Failed, TimeSpan.FromSeconds(10));

        Assert.Equal(OrderStatus.Failed, final.Status);
        Assert.Null(final.PaymentId);
        Assert.Empty(payment.Refunded); // never actually charged, nothing to refund
        Assert.Empty(inventory.Released); // charge fails before any item is ever reserved
    }

    [Fact]
    public async Task OneItemFailsToReserve_RefundsPaymentAndEndsWholeOrderFailed()
    {
        var inventory = new FakeInventoryService
        {
            ReserveOverride = (customerId, amount) => amount == 200
                ? throw new InvalidOperationException("out of stock")
                : Task.FromResult($"reservation-{customerId}-{amount}"),
        };
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-3");

        var items = new[] { new OrderLineItem("order-3#item-0", 300), new OrderLineItem("order-3#item-1", 200) };
        await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-3", items, "1 Main St"), TimeSpan.FromSeconds(5));

        var final = await AwaitStatusAsync(workflow, OrderStatus.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(OrderStatus.Failed, final.Status);
        Assert.NotNull(final.PaymentId);
        Assert.Contains(final.PaymentId!, payment.Refunded);
    }

    [Fact]
    public async Task NotificationFailure_IsNonFatal_OrderStillSucceeds()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService { ThrowOnSend = true };
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-4");

        var items = new[] { new OrderLineItem("order-4#item-0", 500) };
        await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-4", items, "1 Main St"), TimeSpan.FromSeconds(5));

        var final = await AwaitStatusAsync(workflow, OrderStatus.Succeeded, TimeSpan.FromSeconds(5));

        Assert.Equal(OrderStatus.Succeeded, final.Status);
        Assert.Empty(notification.Sent);
    }

    [Fact]
    public async Task LargeOrder_PausesForApproval_ThenResumesAfterApproveCommand()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-5");

        var items = new[] { new OrderLineItem("order-5#item-0", OrderFulfillmentWorkflow.ApprovalThreshold + 1) };
        await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-5", items, "1 Main St"), TimeSpan.FromSeconds(5));

        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            while (await workflow.GetStatus(TimeSpan.FromSeconds(5)) != WorkflowStatus.Paused)
            {
                cts.Token.ThrowIfCancellationRequested();
                await Task.Delay(50, cts.Token);
            }
        }

        var approved = await workflow.Request<ApproveOrder, string>(new ApproveOrder(), TimeSpan.FromSeconds(5));
        Assert.Equal("approved", approved);

        var final = await AwaitStatusAsync(workflow, OrderStatus.Succeeded, TimeSpan.FromSeconds(5));

        Assert.Equal(OrderStatus.Succeeded, final.Status);
        Assert.NotNull(final.PaymentId);
    }

    /// <summary>
    /// The point of graceful cancellation: a cancelled order unwinds through its refund path rather
    /// than stopping where it stands. Terminating it instead would leave the charge outstanding.
    /// </summary>
    [Fact]
    public async Task CancelledOrder_UnwindsThroughRefund_AndReportsCancelled()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-cancel");

        var items = new[] { new OrderLineItem("order-cancel#item-0", 100) };
        await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-cancel", items, "1 Main St"), TimeSpan.FromSeconds(5));

        // Wait until the charge has actually happened, so cancelling has something to unwind. The
        // budget covers a shared runner running every one of this class's clusters at once, where
        // the same work takes several times as long as it does on an idle machine.
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        {
            while (payment.Charged.IsEmpty)
            {
                cts.Token.ThrowIfCancellationRequested();
                await Task.Delay(50, cts.Token);
            }
        }

        await workflow.Cancel("customer changed their mind", TimeSpan.FromSeconds(5));

        var final = await AwaitStatusAsync(workflow, OrderStatus.Cancelled, TimeSpan.FromSeconds(30));
        Assert.Equal(OrderStatus.Cancelled, final.Status);

        // The unwind actually ran: the charge was genuinely refunded, fully settled.
        Assert.NotEmpty(payment.Refunded);
    }

    [Fact]
    public async Task LargeOrder_PauseTimesOutWithoutApproval_AutoCancelsWithoutCharging()
    {
        var payment = new FakePaymentService();
        var notification = new FakeNotificationService();
        var actor = Sys.ActorOf(Props.Create(() =>
            new WorkflowEntityActor<OrderFulfillmentWorkflow, OrderState>(
                nameof(LargeOrder_PauseTimesOutWithoutApproval_AutoCancelsWithoutCharging),
                () => new OrderFulfillmentWorkflow(payment, notification, new FaultInjectionRegistry()),
                ActorRefs.Nobody)));

        var items = new[] { new OrderLineItem("item-0", OrderFulfillmentWorkflow.ApprovalThreshold + 1) };
        await actor.Ask<string>(
            new PlaceOrder("cust-6", items, "1 Main St"), TimeSpan.FromSeconds(5));

        await AwaitConditionAsync(
            async () => (await actor.Ask<WorkflowStatusReply>(new GetStatus(), TimeSpan.FromSeconds(5))).Status == WorkflowStatus.Paused,
            TimeSpan.FromSeconds(5));

        Scheduler.Advance(OrderFulfillmentWorkflow.ApprovalPauseTimeout + TimeSpan.FromSeconds(1));

        OrderState? final = null;
        await AwaitConditionAsync(async () =>
        {
            final = await actor.Ask<OrderState>(new GetOrderState(), TimeSpan.FromSeconds(5));
            return final.Status is OrderStatus.Cancelled;
        }, TimeSpan.FromSeconds(10));

        Assert.Equal(OrderStatus.Cancelled, final!.Status);
        Assert.Equal("approval timeout", final.FailureReason);
        Assert.Null(final.PaymentId);
        Assert.Empty(payment.Refunded); // never charged — the pause happens before ChargePaymentStep
    }

    [Fact]
    public async Task InjectedFault_Permanent_ExhaustsRetriesAndTriggersCompensation()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-fault-permanent");

        var items = new[] { new OrderLineItem("order-fault-permanent#item-0", 500) };
        await workflow.Request<PlaceOrder, string>(
            new PlaceOrder("cust-fault-1", items, "1 Main St",
                FaultStep: nameof(OrderFulfillmentWorkflow.ChargePaymentStep), FaultPermanent: true),
            timeout: TimeSpan.FromSeconds(5));

        var final = await AwaitStatusAsync(workflow, OrderStatus.Failed, TimeSpan.FromSeconds(10));

        Assert.Equal(OrderStatus.Failed, final.Status);
        Assert.Null(final.PaymentId);
        Assert.Empty(payment.Refunded); // never actually charged, nothing to refund
    }

    [Fact]
    public async Task InjectedFault_Transient_FailsOnceThenRetrySucceeds()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-fault-transient");

        var items = new[] { new OrderLineItem("order-fault-transient#item-0", 500) };
        await workflow.Request<PlaceOrder, string>(
            new PlaceOrder("cust-fault-2", items, "1 Main St",
                FaultStep: nameof(OrderFulfillmentWorkflow.ChargePaymentStep), FaultPermanent: false),
            timeout: TimeSpan.FromSeconds(5));

        var final = await AwaitStatusAsync(workflow, OrderStatus.Succeeded, TimeSpan.FromSeconds(10));

        Assert.Equal(OrderStatus.Succeeded, final.Status);
        Assert.NotNull(final.PaymentId);
    }

    /// <summary>Matches what the UI actually offers (see <c>_OrderDetailPartial.cshtml</c>'s own
    /// guard) — delete is only ever reachable for a terminal order. The engine also supports
    /// deleting a still-running order — cascading to any still-<c>Pending</c>
    /// <c>ParentClosePolicy.Terminate</c> child via <c>ChildOrchestrator.SendDelete</c> — but that's
    /// an engine-level capability the sample's own UI never exercises, and is exercised by the
    /// engine's own test suite (<c>WorkflowDeleteTests</c>/<c>ChildWorkflowPruningTests</c>) rather
    /// than repeated here.</summary>
    [Fact]
    public async Task DeletingSucceededOrder_PurgesItAndItsItemChild()
    {
        var inventory = new FakeInventoryService();
        var payment = new FakePaymentService();
        var shipping = new FakeShippingService();
        var notification = new FakeNotificationService();
        await using var harness = await StartAsync(inventory, payment, shipping, notification);
        var workflow = harness.Ref("order-delete");
        var itemId = "order-delete#item-0";

        var items = new[] { new OrderLineItem(itemId, 500) };
        await workflow.Request<PlaceOrder, string>(new PlaceOrder("cust-delete", items, "1 Main St"), TimeSpan.FromSeconds(5));
        await AwaitStatusAsync(workflow, OrderStatus.Succeeded, TimeSpan.FromSeconds(10));

        // Succeeds without throwing — a subsequent GetStatus against this same id isn't a reliable
        // way to observe "still deleted": any message to a purged id reactivates a brand-new,
        // EmptyState entity (ClusterSharding has no persistent "this id is gone forever" concept),
        // exactly why the sample's own read model tombstones a deleted order in Postgres, so it never
        // has to query the live entity again after a delete (see OrderReadModelRepository.SoftDeleteAsync).
        await workflow.Delete(timeout: TimeSpan.FromSeconds(5));

        // The item itself already ended Completed (ChildStatus, per FulfillItemsStep's own doc
        // comment) well before the order was deleted — ParentClosePolicy.Terminate only ever acts on
        // a still-Pending child (see ApplyParentClosePolicyToChildren), so it's untouched here, still
        // reachable and still Completed. Confirms the order's own delete doesn't reach into an
        // already-finished child's own journal.
        var itemHandle = harness.Client.For<ItemFulfillmentWorkflow>(itemId);
        Assert.Equal(WorkflowStatus.Finished, await itemHandle.GetStatus(TimeSpan.FromSeconds(5)));
    }
}
