using System.Collections.Concurrent;

namespace OrderFulfillment.Sample;

/// <summary>
/// Demo-only fault injection for the sample UI: lets a "place order" request force a specific step
/// to fail, either permanently (every attempt — exhausts retries, triggers the compensation
/// cascade) or transiently (fails once, the engine's own retry then succeeds). A step body only
/// ever sees the workflow's own state — never its own entity id —
/// so this is keyed off <see cref="OrderState.CustomerId"/> instead; the sample always generates a
/// fresh customer id per order, so that's unique enough in practice. Not durable and not meant to
/// be — a restart just clears every armed trap, which is fine for a demo.
/// </summary>
public sealed class FaultInjectionRegistry
{
    private readonly ConcurrentDictionary<(string CustomerId, string StepName), byte> _consumed = new();

    /// <summary>True the first time called for a given (customerId, stepName) pair, false every
    /// time after — lets a transient fault fail exactly once before the engine's own retry
    /// succeeds.</summary>
    public bool ConsumeOneShot(string customerId, string stepName) =>
        _consumed.TryAdd((customerId, stepName), 0);
}
