using Akka.Actor;
using Akka.Serialization;
using Akka.TestKit.Xunit2;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Testing;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The round-trip check against a real <c>ActorSystem</c>'s own serializer, and the case that
/// motivated it.
///
/// A workflow's state and its commands are written to a journal and read back on recovery, so a
/// value that writes cleanly and cannot be read leaves an instance durable and unrecoverable at the
/// same time — and says so at a restart, long after the write was accepted.
/// </summary>
public class SerializationRoundTripTests : TestKit
{
    private sealed record Item(string Sku, int Amount);

    private sealed record OrderWithArray(string CustomerId, IReadOnlyList<Item> Items);

    private static ChildWorkflowRelationship Member(string relationshipId, string childId, ChildStatus status) =>
        new(relationshipId, "OrderWorkflow", "order-1", "ItemWorkflow", childId, "group-1",
            Generation: 0, status, Result: null, Failure: null, TraceParent: null,
            ParentClosePolicy.Abandon, Command: new Item("SKU-1", 1), ResultTraceParent: null);

    private void RoundTrip<T>(T value) where T : notnull
    {
        var serialization = ((ExtendedActorSystem)Sys).Serialization;
        SerializationRoundTrip.Assert(
            value,
            v => serialization.FindSerializerFor(v).ToBinary(v),
            bytes => (T)serialization.FindSerializerFor(typeof(T)).FromBinary(bytes, typeof(T)));
    }

    /// <summary>
    /// The engine's own resume-step input. It is written to a journal as that step's input, so a
    /// version of it a serializer cannot restore breaks recovery of every instance that ever awaited
    /// children — and, because a tag query deserializes what it reads, takes the deadline projection
    /// down with it. It reached production behaviour with only Outcome and WorkflowIds visible, which
    /// left the constructor nothing to rebuild its members from.
    /// </summary>
    [Fact]
    public void AChildGroupResult_SurvivesARoundTrip()
    {
        var members = new List<ChildWorkflowRelationship>
        {
            Member("rel-1", "item-1", ChildStatus.Completed),
        };

        var value = new ChildGroupResult(GroupOutcome.Succeeded, members);
        RoundTrip(value);
    }

    /// <summary>A restored one answers the same questions the original did.</summary>
    [Fact]
    public void ARestoredChildGroupResult_StillKnowsItsMembers()
    {
        var members = new List<ChildWorkflowRelationship>
        {
            Member("rel-1", "item-1", ChildStatus.Completed),
            Member("rel-2", "item-2", ChildStatus.Failed),
        };

        var serialization = ((ExtendedActorSystem)Sys).Serialization;
        var original = new ChildGroupResult(GroupOutcome.Failed, members);
        var bytes = serialization.FindSerializerFor(original).ToBinary(original);
        var restored = (ChildGroupResult)serialization
            .FindSerializerFor(typeof(ChildGroupResult))
            .FromBinary(bytes, typeof(ChildGroupResult));

        Assert.Equal(GroupOutcome.Failed, restored.Outcome);
        Assert.Equal(new[] { "item-1", "item-2" }, restored.WorkflowIds);
        Assert.Equal(ChildStatus.Completed, restored.GetStatus("item-1"));
        Assert.Equal(ChildStatus.Failed, restored.GetStatus("item-2"));
    }

    [Fact]
    public void APlainRecord_SurvivesARoundTrip() =>
        RoundTrip(new Item("SKU-1", 2));

    [Fact]
    public void ARecordHoldingAnArray_SurvivesARoundTrip() =>
        RoundTrip(new OrderWithArray("customer-1", new[] { new Item("SKU-1", 2) }));

    [Fact]
    public void ARecordHoldingAList_SurvivesARoundTrip() =>
        RoundTrip(new OrderWithArray("customer-1", new List<Item> { new("SKU-1", 2) }));

    /// <summary>
    /// The case this exists for. A collection expression assigned to an
    /// <see cref="IReadOnlyList{T}"/> member compiles to a compiler-generated type with no public
    /// constructor: the default serializer writes it and cannot read it back. Nothing about the
    /// declaration hints at it, and the failure otherwise surfaces at a recovery, far downstream.
    /// </summary>
    [Fact]
    public void ARecordHoldingACollectionExpression_IsCaughtHereRatherThanAtRecovery()
    {
        IReadOnlyList<Item> viaCollectionExpression = [new Item("SKU-1", 2)];
        var value = new OrderWithArray("customer-1", viaCollectionExpression);

        var failure = Assert.Throws<SerializationRoundTripException>(() => RoundTrip(value));
        Assert.Contains("could not be read back", failure.Message);
        Assert.Contains("collection expression", failure.Message);
    }
}
