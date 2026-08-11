using Sagant.Clients;

namespace OrderFulfillment.Sample;

/// <summary>Thin helper both the "place order" form and the demo host's startup seeding go
/// through — generates a fresh order id, a customer id, and one child-workflow id per line item,
/// writes them all into <see cref="OrderReadModelRepository"/> *before* sending
/// <see cref="PlaceOrder"/> (so the notification-driven updates in
/// <see cref="WorkflowEventLoggerActor"/> always have a row to land in — see
/// <see cref="OrderReadModelRepository.PlaceOrderAsync"/>), then sends the command through the same
/// <see cref="IWorkflowClient"/> path production traffic would use.</summary>
public sealed class OrderPlacementService(IWorkflowClient client, OrderReadModelRepository repo)
{
    public async Task<string> PlaceAsync(
        IReadOnlyList<int> itemAmounts, string? faultStep = null, bool faultPermanent = true, TimeSpan? timeout = null)
    {
        var shortId = Guid.NewGuid().ToString("N")[..8];
        var orderId = $"order-{shortId}";
        var customerId = $"cust-{shortId}";
        var items = itemAmounts
            .Select((amount, i) => new OrderLineItem($"{orderId}#item-{i}", amount))
            .ToArray();

        await repo.PlaceOrderAsync(orderId, customerId, items, "1 Main St");

        var workflow = client.For<OrderFulfillmentWorkflow>(orderId);
        await workflow.Request<PlaceOrder, string>(
            new PlaceOrder(customerId, items, "1 Main St", faultStep, faultPermanent),
            timeout: timeout ?? TimeSpan.FromSeconds(10));

        return orderId;
    }

    public Task<string> ApproveAsync(string orderId, TimeSpan? timeout = null) =>
        client.For<OrderFulfillmentWorkflow>(orderId)
            .Request<ApproveOrder, string>(new ApproveOrder(), timeout ?? TimeSpan.FromSeconds(10));

    /// <summary>Soft-deletes the read-model row directly, right after the engine confirms the
    /// delete — doesn't wait on <see cref="WorkflowEventLoggerActor"/>'s own
    /// <c>WorkflowEnded(Deleted)</c> handler to do it. That handler still runs too (from the
    /// notification this delete's own <c>WorkflowEvent.RunFinished</c> publish
    /// triggers) and calls the same repository method again — <see cref="OrderReadModelRepository.SoftDeleteAsync"/>
    /// is idempotent (an <c>UPDATE ... SET deleted_at = now()</c>, harmless to run twice) — but the
    /// caller here can't afford to wait on a separate actor's mailbox and pub-sub propagation before
    /// re-rendering the page the button was clicked on, the same reason <see cref="PlaceAsync"/>
    /// writes its own read-model row synchronously instead of waiting on a notification too.</summary>
    public async Task DeleteAsync(string orderId, TimeSpan? timeout = null)
    {
        await client.For<OrderFulfillmentWorkflow>(orderId).Delete(timeout: timeout ?? TimeSpan.FromSeconds(10));
        await repo.SoftDeleteAsync(orderId);
    }
}
