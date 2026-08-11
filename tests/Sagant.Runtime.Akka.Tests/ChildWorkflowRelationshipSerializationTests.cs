using Sagant.Effects;
using Sagant.Protocol;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Proves the assumption every heterogeneous child-group type from here on depends on: a
/// <c>Children</c> list where two *different* elements independently carry two *different* concrete
/// types in their own polymorphic <see cref="ChildWorkflowRelationship.Result"/> slot round-trip
/// through the real Akka.Persistence serializer with each element's own concrete type preserved.
/// Proves more than <see cref="WorkflowRuntimeStateSerializationTests"/> already does — that test
/// covers one polymorphic field; this one covers a collection whose elements are independently
/// polymorphic among each other, the shape a heterogeneous child group's <c>Children</c> list
/// actually takes in production.
/// </summary>
public class ChildWorkflowRelationshipSerializationTests
{
    private sealed record InventoryState(bool Reserved);

    private sealed record PaymentState(decimal AmountCharged);

    private sealed record StartCommand(string WorkflowId);

    [Fact]
    public void ChildrenList_RoundTripsThroughRealSerializer_PreservingHeterogeneousResultTypes()
    {
        using var system = ActorSystem.Create("child-relationship-serialization-test");

        var relationships = new List<ChildWorkflowRelationship>
        {
            new("rel-1", "OrderWorkflow", "order-1", "InventoryWorkflow", "inv-1", "group-0", 0,
                ChildStatus.Completed, new InventoryState(Reserved: true), null, null, ParentClosePolicy.Abandon,
                new StartCommand("inv-1")),
            new("rel-2", "OrderWorkflow", "order-1", "PaymentWorkflow", "pay-1", "group-0", 0,
                ChildStatus.Completed, new PaymentState(AmountCharged: 42.50m), null, null, ParentClosePolicy.Abandon,
                new StartCommand("pay-1")),
            new("rel-3", "OrderWorkflow", "order-1", "ShippingWorkflow", "ship-1", "group-0", 0,
                ChildStatus.Failed, null, new WorkflowFailure("card declined"), null, ParentClosePolicy.Abandon,
                new StartCommand("ship-1")),
        };
        var state = new WorkflowRuntimeState<string>(
            UserState: "order-state", CurrentStepName: null, CurrentStepInput: null,
            RetryCount: 0, Status: WorkflowStatus.Running, Children: relationships);

        var serializer = system.Serialization.FindSerializerFor(state);
        var bytes = serializer.ToBinary(state);
        var roundTripped = (WorkflowRuntimeState<string>)serializer.FromBinary(bytes, state.GetType());

        Assert.NotNull(roundTripped.Children);

        var inventory = roundTripped.Children!.Single(r => r.ChildWorkflowId == "inv-1");
        Assert.IsType<InventoryState>(inventory.Result);
        Assert.True(((InventoryState)inventory.Result!).Reserved);
        Assert.Equal(ChildStatus.Completed, inventory.Status);

        // The genuinely heterogeneous assertion: a second element, same list, same round trip,
        // carrying an entirely different concrete Result type — proves the serializer preserves
        // each element's own type independently.
        var payment = roundTripped.Children!.Single(r => r.ChildWorkflowId == "pay-1");
        Assert.IsType<PaymentState>(payment.Result);
        Assert.Equal(42.50m, ((PaymentState)payment.Result!).AmountCharged);
        Assert.Equal(ChildStatus.Completed, payment.Status);

        var shipping = roundTripped.Children!.Single(r => r.ChildWorkflowId == "ship-1");
        Assert.Equal(ChildStatus.Failed, shipping.Status);
        Assert.Null(shipping.Result);
        Assert.Equal("card declined", shipping.Failure!.Message);
    }
}
