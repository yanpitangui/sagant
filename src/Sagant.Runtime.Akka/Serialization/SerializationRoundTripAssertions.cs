using Akka.Actor;

namespace Sagant.Runtime.Akka.Serialization;

/// <summary>
/// Turns a state or command's serialization break into a test failure at the point it's introduced,
/// ahead of the first production recovery that would otherwise find it — a state built from a
/// collection expression, say, which the default JSON serializer writes happily and cannot read
/// back. <c>TState</c> and a workflow's own commands are the consumer's types, riding whatever
/// serializer the consumer's <see cref="ActorSystem"/> resolves for them (Newtonsoft by default,
/// unless something else claims the type via <c>serialization-bindings</c>) — this exercises exactly
/// that path, the one recovery actually takes.
/// </summary>
public static class SerializationRoundTripAssertions
{
    /// <summary>
    /// Serializes <paramref name="value"/> through <paramref name="system"/>'s configured serializer
    /// and reads the result back, returning it for the caller's own equality assertion. A workflow
    /// state built entirely from records and primitives compares equal via a plain
    /// <c>Assert.Equal(value, roundTripped)</c>; one carrying a collection member needs that member
    /// asserted on its own, the same as anywhere else record-generated equality meets a collection
    /// with no structural <c>Equals</c> of its own (a plain <c>List&lt;T&gt;</c>, say). Throws
    /// whatever exception the serializer itself throws on a genuine break — the same exception
    /// recovery would raise, here at test time, at the first restart that would otherwise hit it.
    /// </summary>
    public static T AssertRoundTrips<T>(ActorSystem system, T value)
        where T : notnull
    {
        var serializer = system.Serialization.FindSerializerFor(value);
        var bytes = serializer.ToBinary(value);
        return (T)serializer.FromBinary(bytes, value.GetType());
    }
}
