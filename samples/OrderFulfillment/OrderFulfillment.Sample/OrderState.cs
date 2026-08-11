namespace OrderFulfillment.Sample;

public enum OrderStatus
{
    Started,
    AwaitingApproval,
    PaymentCharged,
    Fulfilling,
    Succeeded,
    Cancelled,
    Failed,
}

public sealed record OrderState(
    string CustomerId,
    int Amount,
    IReadOnlyList<OrderLineItem> Items,
    string ShippingAddress,
    OrderStatus Status,
    string? PaymentId = null,
    string? FailureReason = null,
    /// <summary>Demo-only fault injection, set from <see cref="PlaceOrder"/> — see
    /// <see cref="FaultInjectionRegistry"/>.</summary>
    string? FaultStep = null,
    bool FaultPermanent = true)
{
    public static OrderState Empty() =>
        new(CustomerId: "", Amount: 0, Items: [], ShippingAddress: "", Status: OrderStatus.Started);
}
