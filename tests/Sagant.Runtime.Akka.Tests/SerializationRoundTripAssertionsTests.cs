using Sagant.Runtime.Akka.Serialization;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Proves <see cref="SerializationRoundTripAssertions"/> catches what it exists to catch: a
/// collection expression targeting <c>IReadOnlyList&lt;T&gt;</c> compiles to an internal list type
/// with no public constructor — Newtonsoft writes it, then cannot read it back. A failure like that
/// surfaces on recovery in production; this is the same check, run at test time.
/// </summary>
public class SerializationRoundTripAssertionsTests
{
    private sealed record Item(string Name);

    private sealed record OrderState(IReadOnlyList<Item> Items);

    [Fact]
    public void AnOrdinaryState_RoundTrips()
    {
        using var system = ActorSystem.Create("round-trip-ok");
        var state = new OrderState(new List<Item> { new("widget") });

        var roundTripped = SerializationRoundTripAssertions.AssertRoundTrips(system, state);

        // OrderState's record-generated equality compares Items by reference (List<T> has no
        // structural Equals of its own), so two independently deserialized lists carrying identical
        // contents still compare unequal that way. xUnit's own Assert.Equal on the list itself
        // compares element-wise, which is what this asserts on directly.
        Assert.Equal(state.Items, roundTripped.Items);
    }

    /// <summary>A collection expression assigns <c>IReadOnlyList&lt;T&gt;</c> a value the compiler
    /// lowers to a type with no public constructor.</summary>
    [Fact]
    public void AStateBuiltFromACollectionExpression_FailsHere_NotOnRecovery()
    {
        using var system = ActorSystem.Create("round-trip-break");
        var state = new OrderState([new("widget")]);

        Assert.ThrowsAny<Exception>(() => SerializationRoundTripAssertions.AssertRoundTrips(system, state));
    }
}
