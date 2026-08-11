using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace OrderFulfillment.Sample.Pages;

public sealed class OrderDetailFragmentModel(OrderReadModelRepository repo, OrderPlacementService placement) : PageModel
{
    public OrderSnapshot? Order { get; private set; }

    public async Task OnGetAsync(string id) => Order = await repo.SnapshotOfAsync(id);

    public async Task<IActionResult> OnPostApproveAsync(string id)
    {
        await placement.ApproveAsync(id);
        Order = await repo.SnapshotOfAsync(id);
        return Page();
    }

    /// <summary>Only reachable from the UI for a terminal order (see <c>_OrderDetailPartial.cshtml</c>'s
    /// own guard on when the Delete button renders) — <see cref="Sagant.Clients.IWorkflowHandle{TWorkflow}.Delete"/>
    /// works at any status, this sample just doesn't offer it for one still in flight.</summary>
    public async Task<IActionResult> OnPostDeleteAsync(string id)
    {
        await placement.DeleteAsync(id);
        Order = await repo.SnapshotOfAsync(id);
        return Page();
    }
}
