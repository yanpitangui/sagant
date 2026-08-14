namespace Sagant.Testing;

/// <summary>Thrown when a value does not come back from a round trip as what went in.</summary>
public sealed class SerializationRoundTripException(string message, Exception? inner = null)
    : Exception(message, inner);

/// <summary>
/// Checks that a value survives being written and read back.
///
/// Worth checking because of when the alternative fails. A workflow's state and its commands are
/// written to a journal and read back on recovery, and a schedule goes further — it stores the
/// command it will send and replays it every time it fires. A value that writes cleanly and cannot
/// be read leaves an instance durable and unrecoverable at once, and says so at a restart rather
/// than at the call that wrote it.
///
/// The failure is rarely in the type a caller is thinking about. A collection expression assigned to
/// an <see cref="IReadOnlyList{T}"/> member compiles to a type with no public constructor, which a
/// JSON serializer writes happily and cannot reconstruct. Nothing about the declaration hints at it.
///
/// <para>This uses the same serializer the runtime is configured with, which is what makes it worth
/// anything: a check against a serializer nobody runs proves nothing. A test asserts here on the
/// state and commands its workflow actually uses, so the failure lands at build time on a named
/// type.</para>
/// </summary>
public static class SerializationRoundTrip
{
    /// <summary>
    /// Writes <paramref name="value"/> through <paramref name="serialize"/>, reads it back through
    /// <paramref name="deserialize"/>, and requires the result to equal what went in.
    ///
    /// The two delegates come from whatever the runtime uses — for the Akka runtime, an
    /// <c>ActorSystem</c>'s own <c>Serialization</c>. Keeping them as parameters is what lets this
    /// live in the runtime-agnostic testing package.
    /// </summary>
    /// <exception cref="SerializationRoundTripException">The value could not be written, could not be
    /// read back, or came back carrying something different.</exception>
    /// <remarks>
    /// The comparison is made by writing the result a second time and requiring the same bytes, which
    /// holds for a serializer whose output is settled by the value alone. One that orders a
    /// dictionary differently from run to run would report a difference where there is none.
    /// </remarks>
    public static void Assert<T>(
        T value,
        Func<T, byte[]> serialize,
        Func<byte[], T> deserialize)
        where T : notnull
    {
        byte[] written;
        try
        {
            written = serialize(value);
        }
        catch (Exception ex)
        {
            throw new SerializationRoundTripException(
                $"{typeof(T).Name} could not be written. A value a workflow persists has to be "
                + "writable by the configured serializer.", ex);
        }

        T read;
        try
        {
            read = deserialize(written);
        }
        catch (Exception ex)
        {
            throw new SerializationRoundTripException(
                $"{typeof(T).Name} was written but could not be read back, which is the failure that "
                + "surfaces at recovery rather than at the write. A frequent cause is a collection "
                + "expression assigned to an IReadOnlyList<T> member: it compiles to a type with no "
                + "public constructor. An array or a List<T> reads back.", ex);
        }

        // Compared by writing the result again rather than by equality, because a record's generated
        // equality compares a collection member by reference: an array that round-tripped perfectly
        // would report as different, and most workflow state holds a collection. Writing again asks
        // the question that matters — whether what came back still says the same thing.
        byte[] rewritten;
        try
        {
            rewritten = serialize(read);
        }
        catch (Exception ex)
        {
            throw new SerializationRoundTripException(
                $"{typeof(T).Name} read back into something that could no longer be written, so it "
                + "would survive one recovery and fail the next.", ex);
        }

        if (!written.AsSpan().SequenceEqual(rewritten))
        {
            throw new SerializationRoundTripException(
                $"{typeof(T).Name} came back carrying something different from what was written."
                + $"{Environment.NewLine}  wrote: {value}{Environment.NewLine}  read:  {read}");
        }
    }
}
