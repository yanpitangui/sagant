using Akka.Actor;
using Akka.TestKit.Xunit2;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// What a status query answers with once the answer has to cross a node boundary.
///
/// An <c>Ask</c> reply is written with whatever serializer fits the object handed over and read back
/// from the bytes plus a manifest. That is the path exercised here: serialize, then deserialize the
/// way the receiving node does, knowing only what the sender wrote down.
///
/// A single node never takes that path — a local reply is handed over as the object it already is —
/// so the cost of losing this is a caller that works right up until the instance it asks about
/// happens to live somewhere else.
/// </summary>
public class WorkflowStatusReplySerializationTests : TestKit
{
    /// <summary>Serializes exactly as a reply to a remote asker does, then reads it back from the
    /// bytes and the manifest alone.</summary>
    private object AcrossTheWire(object value)
    {
        var serialization = ((ExtendedActorSystem)Sys).Serialization;
        var serializer = serialization.FindSerializerFor(value);

        return serialization.Deserialize(
            serializer.ToBinary(value),
            serializer.Identifier,
            global::Akka.Serialization.Serialization.ManifestFor(serializer, value));
    }

    [Fact]
    public void AStatusReply_ComesBackAsAStatus()
    {
        foreach (var status in Enum.GetValues<WorkflowStatus>())
        {
            var back = AcrossTheWire(new WorkflowStatusReply(status));

            var reply = Assert.IsType<WorkflowStatusReply>(back);
            Assert.Equal(status, reply.Status);
        }
    }

    /// <summary>
    /// Why the reply is a record rather than the enum on its own. A bare enum carries nothing on the
    /// wire beyond the number behind it, so the asker gets an integer and an <c>Ask</c> typed to a
    /// status throws instead of answering.
    ///
    /// Asserted rather than described, so replacing the wrapper with the bare value fails here rather
    /// than in whatever deployment first has two nodes.
    /// </summary>
    [Fact]
    public void ABareStatus_DoesNotComeBackAsOne()
    {
        var back = AcrossTheWire(WorkflowStatus.Paused);

        Assert.False(
            back is WorkflowStatus,
            $"a bare status survived the wire as {back?.GetType().Name}, so the reply no longer needs "
            + $"{nameof(WorkflowStatusReply)} to wrap it");
    }
}
