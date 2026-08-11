using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Settings;

namespace Sagant.Docs.Tests.Fixtures;

/// <summary>
/// The vocabulary the documentation speaks. Every type named in a `csharp` block in `README.md` or
/// `docs/*.md` lives here, under the exact name the docs use, so a snippet compiles as written.
///
/// These are deliberately minimal — a doc snippet is checked for whether it still matches Sagant's
/// public surface, so a fixture only needs enough shape for that. Behavior stays out.
///
/// Adding a type here is the cost of naming a new type in the docs. That cost is the point: it keeps
/// the docs to a vocabulary a reader can actually assemble.
/// </summary>
public enum OrderStatus
{
    Pending,
    Charged,
    Failed,
    Done,
}

public sealed record OrderState(
    string CustomerId,
    int Amount,
    string? PaymentId,
    OrderStatus Status,
    IReadOnlyList<LineItemState> LineItems)
{
    public static OrderState Empty() =>
        new("unknown", 0, null, OrderStatus.Pending, Array.Empty<LineItemState>());
}

public sealed record PlaceOrder(int Amount, string CustomerId = "customer-1");

public sealed record GetProgress;

public enum Tier
{
    Standard,
    Vip,
}

public interface ICustomerService
{
    Task<Tier> GetTier(string customerId);
}

/// <summary>Overloaded because the docs charge with and without a cancellation token, and one
/// guideline example charges by amount alone.</summary>
public interface IPaymentService
{
    Task<string> Charge(string customerId, int amount, CancellationToken cancellationToken);

    Task<string> Charge(string customerId, int amount);

    Task Charge(int amount);

    Task Refund(string paymentId, CancellationToken cancellationToken);
}

public sealed class RealPaymentService : IPaymentService
{
    public Task<string> Charge(string customerId, int amount, CancellationToken cancellationToken) =>
        Task.FromResult("payment-1");

    public Task<string> Charge(string customerId, int amount) => Task.FromResult("payment-1");

    public Task Charge(int amount) => Task.CompletedTask;

    public Task Refund(string paymentId, CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>Named by a testing example that configures how many attempts fail before one works.</summary>
public sealed class FlakyPaymentService(int failuresBeforeSuccess) : IPaymentService
{
    private int _attempts;

    public Task<string> Charge(string customerId, int amount, CancellationToken cancellationToken) =>
        Charge(customerId, amount);

    public Task<string> Charge(string customerId, int amount) =>
        _attempts++ < failuresBeforeSuccess
            ? Task.FromException<string>(new InvalidOperationException("declined"))
            : Task.FromResult("payment-1");

    public Task Charge(int amount) => Task.CompletedTask;

    public Task Refund(string paymentId, CancellationToken cancellationToken) => Task.CompletedTask;
}

public interface IInventoryService
{
    Task<bool> Reserve(string sku, int quantity);
}

// ── the child workflow the fan-out documentation uses ────────────────────────────────────────────

public sealed record LineItemState(string Sku, int Quantity)
{
    public LineItemState() : this("unknown", 0) { }
}

public sealed record ProcessLineItem(string Sku, int Quantity);

public partial class LineItemWorkflow(IInventoryService? inventory = null) : Workflow<LineItemState>
{
    public override LineItemState EmptyState() => new();

    private readonly IInventoryService? _inventory = inventory;

    [WorkflowCommandHandler]
    public CommandEffect<LineItemState> Start(ProcessLineItem cmd) =>
        Effects.TransitionTo(Steps.ReserveStock, cmd).ThenReply("accepted");

    [WorkflowStep]
    public async Task<StepEffect<LineItemState>> ReserveStock(ProcessLineItem item)
    {
        if (_inventory is not null)
        {
            await _inventory.Reserve(item.Sku, item.Quantity);
        }

        return StepEffects.UpdateState(new LineItemState(item.Sku, item.Quantity)).ThenComplete();
    }
}

// ── the pause-driven workflow the testing documentation uses ─────────────────────────────────────

public sealed record ApprovalState(bool Approved)
{
    public ApprovalState() : this(false) { }
}

public sealed record SubmitForApproval;

public partial class ApprovalWorkflow : Workflow<ApprovalState>
{
    public override ApprovalState EmptyState() => new();

    public override WorkflowSettings Settings() => WorkflowSettings.Create()
        .Build();

    [WorkflowCommandHandler]
    public CommandEffect<ApprovalState> Submit(SubmitForApproval cmd) =>
        Effects.TransitionTo(Steps.AwaitApproval).ThenReply("accepted");

    [WorkflowStep]
    public StepEffect<ApprovalState> AwaitApproval() =>
        StepEffects.ThenPause(PauseSettings.WithTimeout(TimeSpan.FromMinutes(30)).TimeoutHandler(Steps.AutoReject));

    [WorkflowStep]
    public StepEffect<ApprovalState> AutoReject() => StepEffects.ThenFail("nobody approved it");
}

// ── the minimal workflow the integration guide starts from ───────────────────────────────────────

public sealed record EchoPing(string Text);

public sealed record EchoState(string Value)
{
    public EchoState() : this("initial") { }
}

public partial class EchoWorkflow : Workflow<EchoState>
{
    public override EchoState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<EchoState> Handle(EchoPing ping) =>
        Effects.TransitionTo(Steps.EchoStep, ping.Text).ThenReply("accepted");

    [WorkflowStep]
    public Task<StepEffect<EchoState>> EchoStep(string text) =>
        Task.FromResult(StepEffects.UpdateState(new EchoState(text)).ThenComplete());
}
