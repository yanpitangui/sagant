namespace OrderFulfillment.Sample;

/// <summary>One line item of an order, assigned its full child-workflow id
/// (<c>"{orderId}#item-{n}"</c>) by <see cref="OrderPlacementService"/> before the order is even
/// placed — a workflow step has no way to learn its own entity id from inside
/// <see cref="Sagant.Workflow{TState}"/>, so the id has to arrive pre-built from outside, the same
/// way the order's own id does.</summary>
public sealed record OrderLineItem(string ItemId, int Amount);

/// <summary><paramref name="FaultStep"/>/<paramref name="FaultPermanent"/> exist for the demo UI's
/// fault-injection form (see <see cref="FaultInjectionRegistry"/>) — a real caller never sets these.
/// <paramref name="FaultStep"/> is the target step's method name (e.g. <c>nameof(OrderFulfillmentWorkflow.ChargePaymentStep)</c>),
/// or <c>null</c> for no injected failure. Scoped to <see cref="OrderFulfillmentWorkflow"/>'s own
/// parent-level steps only — an individual item's <see cref="ItemFulfillmentWorkflow"/> always
/// succeeds in this demo.</summary>
public sealed record PlaceOrder(
    string CustomerId,
    IReadOnlyList<OrderLineItem> Items,
    string ShippingAddress,
    string? FaultStep = null,
    bool FaultPermanent = true);

/// <summary>External approval for orders paused pending manual review (see <c>ApprovalThreshold</c>).</summary>
public sealed record ApproveOrder;

/// <summary>Read-only query — the engine treats <c>OrderState</c> as opaque, so exposing it (or
/// any slice of it) is the workflow author's job, not something the engine does generically.
/// Handled by a <c>[WorkflowQuery]</c>: this is what the live SSE UI polls to show a step
/// executing/retrying/compensating in real time, and a query dispatches immediately instead of
/// waiting for the current step to settle, which is what makes watching it live possible.</summary>
public sealed record GetOrderState;

/// <summary>Starts one <see cref="ItemFulfillmentWorkflow"/> instance — sent as the
/// <c>ChildStart</c> command by <see cref="OrderFulfillmentWorkflow.FulfillItemsStep"/>, one per
/// order line item.</summary>
public sealed record FulfillItem(string CustomerId, int Amount, string ShippingAddress);

/// <summary>Read-only query for one item's own state — same role as <see cref="GetOrderState"/>,
/// one level down, and a <c>[WorkflowQuery]</c> for the same reason. Used by the read-model
/// repository to render an item's own step pipeline in the UI's inline-nested child view.</summary>
public sealed record GetItemState;
