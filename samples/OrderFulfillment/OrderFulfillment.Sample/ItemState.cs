namespace OrderFulfillment.Sample;

public enum ItemStatus
{
    Started,
    Reserved,
    Shipped,
    Failed,
}

/// <summary>One order line item's own durable state — the <c>TState</c> for
/// <see cref="ItemFulfillmentWorkflow"/>, a separate workflow entity per item, parented under the
/// order that spawned it (see <see cref="OrderFulfillmentWorkflow.FulfillItemsStep"/>).</summary>
public sealed record ItemState(
    string CustomerId,
    int Amount,
    string ShippingAddress,
    ItemStatus Status,
    string? ReservationId = null,
    string? ShipmentId = null)
{
    public static ItemState Empty() =>
        new(CustomerId: "", Amount: 0, ShippingAddress: "", Status: ItemStatus.Started);
}
