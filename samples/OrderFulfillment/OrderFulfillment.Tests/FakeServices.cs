using OrderFulfillment.Sample;

namespace OrderFulfillment.Tests;

public sealed class FakeInventoryService : IInventoryService
{
    public Func<string, int, Task<string>>? ReserveOverride;
    public readonly List<string> Released = new();

    public Task<string> Reserve(string customerId, int amount) =>
        ReserveOverride?.Invoke(customerId, amount) ?? Task.FromResult($"reservation-{customerId}");

    public Task Release(string reservationId)
    {
        Released.Add(reservationId);
        return Task.CompletedTask;
    }
}

public sealed class FakePaymentService : IPaymentService
{
    public Func<string, int, Task<string>>? ChargeOverride;
    public readonly List<string> Refunded = new();
    public readonly System.Collections.Concurrent.ConcurrentBag<string> Charged = new();

    public Task<string> Charge(string customerId, int amount)
    {
        Charged.Add(customerId);
        return ChargeOverride?.Invoke(customerId, amount) ?? Task.FromResult($"payment-{customerId}");
    }

    public Task Refund(string paymentId)
    {
        Refunded.Add(paymentId);
        return Task.CompletedTask;
    }
}

public sealed class FakeShippingService : IShippingService
{
    public Func<string, string, Task<string>>? ScheduleOverride;
    public readonly List<string> Cancelled = new();

    public Task<string> Schedule(string customerId, string address) =>
        ScheduleOverride?.Invoke(customerId, address) ?? Task.FromResult($"shipment-{customerId}");

    public Task Cancel(string shipmentId)
    {
        Cancelled.Add(shipmentId);
        return Task.CompletedTask;
    }
}

public sealed class FakeNotificationService : INotificationService
{
    public readonly List<(string CustomerId, string Message)> Sent = new();
    public bool ThrowOnSend;

    public Task Send(string customerId, string message)
    {
        if (ThrowOnSend)
        {
            throw new InvalidOperationException("notification service unavailable");
        }

        Sent.Add((customerId, message));
        return Task.CompletedTask;
    }
}
