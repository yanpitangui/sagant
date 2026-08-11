using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace OrderFulfillment.Sample.Pages;

public sealed class IndexModel(OrderReadModelRepository repo, OrderPlacementService placement) : PageModel
{
    public IReadOnlyList<OrderSnapshot> Orders { get; private set; } = Array.Empty<OrderSnapshot>();

    public OrderSnapshot? SelectedOrder { get; private set; }

    public string? SelectedOrderId { get; private set; }

    public async Task OnGetAsync(string? order)
    {
        Orders = await repo.SnapshotListAsync();
        SelectedOrderId = order ?? Orders.FirstOrDefault()?.WorkflowId;
        SelectedOrder = SelectedOrderId is null ? null : await repo.SnapshotOfAsync(SelectedOrderId);
    }

    /// <summary><paramref name="itemAmounts"/> is one amount per "+ add item" row in the form (see
    /// wwwroot/app.js) — always at least one.</summary>
    public async Task<IActionResult> OnPostPlaceAsync(int[] itemAmounts, string? faultStep, string faultMode)
    {
        var orderId = await placement.PlaceAsync(
            itemAmounts,
            string.IsNullOrEmpty(faultStep) ? null : faultStep,
            faultMode == "permanent");
        return new JsonResult(new { orderId });
    }
}
