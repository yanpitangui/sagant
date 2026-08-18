using Sagant;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Settings;

namespace OrderFulfillment.Sample;

/// <summary>
/// One order line item's own reserve-then-ship pipeline, run as its own durable workflow entity
/// (child of an <see cref="OrderFulfillmentWorkflow"/> — see <see cref="OrderFulfillmentWorkflow.FulfillItemsStep"/>).
/// Compensates itself (<see cref="ReleaseItemStep"/>) on its own failure, then finishes as
/// <c>Failed</c> — so its parent's group policy sees the failure directly, with nothing to
/// re-derive from this workflow's own <see cref="ItemState.Status"/>.
/// </summary>
public partial class ItemFulfillmentWorkflow : Workflow<ItemState>
{
    private readonly IInventoryService _inventory;
    private readonly IShippingService _shipping;

    public ItemFulfillmentWorkflow(IInventoryService inventory, IShippingService shipping)
    {
        _inventory = inventory;
        _shipping = shipping;
    }

    public override ItemState EmptyState() => ItemState.Empty();

    public override WorkflowSettings Settings() => WorkflowSettings.Create()
        .DefaultStepTimeout(TimeSpan.FromSeconds(5))
        .StepRecovery(Steps.ReserveItemStep, RecoverStrategy.WithMaxRetries(2).FailoverTo(Steps.ReleaseItemStep))
        .StepRecovery(Steps.ArrangeShipmentStep, RecoverStrategy.WithMaxRetries(2).FailoverTo(Steps.ReleaseItemStep))
        .Build();

    [WorkflowCommandHandler]
    public CommandEffect<ItemState> Start(FulfillItem cmd, CommandContext<ItemState> ctx)
    {
        var state = new ItemState(cmd.CustomerId, cmd.Amount, cmd.ShippingAddress, ItemStatus.Started);
        return Effects.UpdateState(state).TransitionTo(Steps.ReserveItemStep).ThenReply("accepted");
    }

    [WorkflowQuery]
    public QueryEffect GetState(GetItemState query, QueryContext<ItemState> ctx) =>
        QueryEffects.Reply(ctx.State);

    [WorkflowStep]
    public async Task<StepEffect<ItemState>> ReserveItemStep(StepContext<ItemState> ctx)
    {
        var reservationId = await _inventory.Reserve(ctx.State.CustomerId, ctx.State.Amount);
        var updated = ctx.State with { ReservationId = reservationId, Status = ItemStatus.Reserved };
        return StepEffects.UpdateState(updated).ThenTransitionTo(Steps.ArrangeShipmentStep);
    }

    [WorkflowStep]
    public async Task<StepEffect<ItemState>> ArrangeShipmentStep(StepContext<ItemState> ctx)
    {
        var shipmentId = await _shipping.Schedule(ctx.State.CustomerId, ctx.State.ShippingAddress);
        var updated = ctx.State with { ShipmentId = shipmentId, Status = ItemStatus.Shipped };
        return StepEffects.UpdateState(updated).ThenComplete();
    }

    [WorkflowStep]
    public async Task<StepEffect<ItemState>> ReleaseItemStep(StepContext<ItemState> ctx)
    {
        if (ctx.State.ReservationId is { } reservationId)
        {
            await _inventory.Release(reservationId);
        }

        // Compensation ran, but this item did not get fulfilled — so the run reports Failed, plainly,
        // and its parent's group policy sees exactly that failure — an honest report, standing apart
        // from a graceful end that happened to leave the item unshipped.
        return StepEffects.UpdateState(ctx.State with { Status = ItemStatus.Failed })
            .ThenFail("item could not be fulfilled; its reservation was released");
    }
}
