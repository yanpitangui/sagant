namespace OrderFulfillment.Sample;

/// <summary>Simulated external-call latency for the demo UI, tuned to be watchable: ~2s with jitter
/// (so parallel-ish steps across different orders don't all resolve in lockstep) is slow enough for
/// a human watching the live page to actually see each step "running," without being annoying to
/// sit through.</summary>
internal static class DemoLatency
{
    public static Task Simulate() =>
        Task.Delay(TimeSpan.FromSeconds(2) + TimeSpan.FromMilliseconds(Random.Shared.Next(-400, 400)));
}

public interface IInventoryService
{
    Task<string> Reserve(string customerId, int amount);

    Task Release(string reservationId);
}

public interface IPaymentService
{
    Task<string> Charge(string customerId, int amount);

    Task Refund(string paymentId);
}

public interface IShippingService
{
    Task<string> Schedule(string customerId, string address);

    Task Cancel(string shipmentId);
}

public interface INotificationService
{
    Task Send(string customerId, string message);
}

/// <summary>Always succeeds after a simulated delay (see <see cref="DemoLatency"/>) — good enough
/// for the demo host; tests use their own scriptable fakes to exercise failure/retry/compensation
/// paths without waiting on it.</summary>
public sealed class SimulatedInventoryService : IInventoryService
{
    public async Task<string> Reserve(string customerId, int amount)
    {
        await DemoLatency.Simulate();
        return $"reservation-{Guid.NewGuid():N}";
    }

    public async Task Release(string reservationId) => await DemoLatency.Simulate();
}

public sealed class SimulatedPaymentService : IPaymentService
{
    public async Task<string> Charge(string customerId, int amount)
    {
        await DemoLatency.Simulate();
        return $"payment-{Guid.NewGuid():N}";
    }

    public async Task Refund(string paymentId) => await DemoLatency.Simulate();
}

public sealed class SimulatedShippingService : IShippingService
{
    public async Task<string> Schedule(string customerId, string address)
    {
        await DemoLatency.Simulate();
        return $"shipment-{Guid.NewGuid():N}";
    }

    public async Task Cancel(string shipmentId) => await DemoLatency.Simulate();
}

public sealed class SimulatedNotificationService : INotificationService
{
    public async Task Send(string customerId, string message) => await DemoLatency.Simulate();
}
