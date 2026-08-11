using Microsoft.AspNetCore.Mvc.RazorPages;

namespace OrderFulfillment.Sample.Pages;

public sealed class OrderListFragmentModel(OrderReadModelRepository repo) : PageModel
{
    public IReadOnlyList<OrderSnapshot> Orders { get; private set; } = Array.Empty<OrderSnapshot>();

    public string? SelectedOrderId { get; private set; }

    public async Task OnGetAsync(string? selected)
    {
        Orders = await repo.SnapshotListAsync();
        SelectedOrderId = selected;
    }
}
