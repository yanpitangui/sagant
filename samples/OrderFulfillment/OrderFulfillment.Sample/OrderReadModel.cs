using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using Npgsql;

namespace OrderFulfillment.Sample;

public enum StepRunStatus
{
    Running,
    Succeeded,
    Failed,
}

public sealed record StepRun(string Name, int Attempt, StepRunStatus Status, string? Error);

/// <summary>Coarse, UI-facing bucket derived from a workflow's own status word plus its paused flag
/// — shared vocabulary between <see cref="OrderStatus"/> and <see cref="ItemStatus"/> (which use
/// different words for the same shape: "Succeeded" vs "Shipped", for instance) so the Razor
/// partials render an order and any of its item children with the exact same status-coloring logic,
/// never switching on which concrete enum produced a node.</summary>
public enum DisplayState
{
    Running,
    Paused,
    Succeeded,
    Failed,
}

/// <summary>Immutable snapshot of one workflow's live view (an order, or one of its item children),
/// handed to the Razor components — never mutated in place, so a component can hold onto one across
/// a render without a lock. Recursive: an order's own <see cref="Children"/> are its item
/// <see cref="ItemFulfillmentWorkflow"/> instances, rendered inline nested under whichever step
/// spawned them (see <c>_OrderDetailPartial.cshtml</c>).</summary>
public sealed record OrderSnapshot(
    string WorkflowId,
    string CustomerId,
    int Amount,
    string StatusText,
    DisplayState State,
    string? FailureReason,
    bool Paused,
    string? PauseReason,
    DateTimeOffset? AutoCancelAt,
    bool Deleted,
    IReadOnlyList<StepRun> Steps,
    IReadOnlyList<string> Log,
    IReadOnlyList<OrderSnapshot> Children);

[Table("workflow_views")]
internal sealed class WorkflowViewRow
{
    [PrimaryKey, Column("workflow_id")] public string WorkflowId { get; set; } = "";
    [Column("parent_workflow_id")] public string? ParentWorkflowId { get; set; }
    [Column("workflow_type")] public string WorkflowType { get; set; } = "";
    [Column("status")] public string Status { get; set; } = "";
    [Column("failure_reason")] public string? FailureReason { get; set; }
    [Column("paused")] public bool Paused { get; set; }
    [Column("pause_reason")] public string? PauseReason { get; set; }
    [Column("auto_cancel_at")] public DateTimeOffset? AutoCancelAt { get; set; }
}

[Table("orders")]
internal sealed class OrderRow
{
    [PrimaryKey, Column("order_id")] public string OrderId { get; set; } = "";
    [Column("customer_id")] public string CustomerId { get; set; } = "";
    [Column("amount")] public int Amount { get; set; }
    [Column("shipping_address")] public string ShippingAddress { get; set; } = "";
    [Column("created_at")] public DateTimeOffset CreatedAt { get; set; }
    [Column("deleted_at")] public DateTimeOffset? DeletedAt { get; set; }
}

[Table("order_items")]
internal sealed class OrderItemRow
{
    [Column("order_id")] public string OrderId { get; set; } = "";
    [PrimaryKey, Column("item_workflow_id")] public string ItemWorkflowId { get; set; } = "";
    [Column("amount")] public int Amount { get; set; }
}

[Table("step_runs")]
internal sealed class StepRunRow
{
    [PrimaryKey(1), Column("workflow_id")] public string WorkflowId { get; set; } = "";
    [PrimaryKey(2), Column("step_name")] public string StepName { get; set; } = "";
    [PrimaryKey(3), Column("attempt")] public int Attempt { get; set; }
    [Column("status")] public string Status { get; set; } = "";
    [Column("error")] public string? Error { get; set; }
    [Column("started_at")] public DateTimeOffset StartedAt { get; set; }
}

[Table("event_log")]
internal sealed class EventLogRow
{
    [PrimaryKey(1), Column("workflow_id")] public string WorkflowId { get; set; } = "";
    [PrimaryKey(2), Column("at")] public DateTimeOffset At { get; set; }
    [PrimaryKey(3), Column("line")] public string Line { get; set; } = "";
}

/// <summary>
/// Durable, Postgres-backed replacement for the old in-memory <c>OrderStore</c>/<c>OrderRegistryActor</c>
/// pair — every replica reads and writes the same shared tables (see
/// <c>OrderFulfillment.AppHost/init-scripts/001-orders-schema.sql</c>), so there's no per-replica
/// gap to backfill on startup and no cluster-singleton registry needed to paper over one. A short-lived
/// <see cref="LinqToDB.Data.DataConnection"/> per call, exactly like this sample's other
/// simulated-service calls — a demo's request volume never makes connection-per-call pooling
/// overhead worth optimizing away.
/// </summary>
public sealed class OrderReadModelRepository(string connectionString)
{
    private static readonly HashSet<string> SuccessWords = ["Succeeded", "Shipped"];
    private static readonly HashSet<string> FailureWords = ["Failed", "Cancelled"];

    private DataConnection Connect() => new(new DataOptions().UsePostgreSQL(connectionString));

    /// <summary>Registers an order and every one of its line items in one shot, before the
    /// <c>PlaceOrder</c> command is even sent — see <see cref="OrderPlacementService"/>. Every
    /// workflow id this order's run will ever publish a notification for already has its
    /// <c>workflow_views</c> row by the time that notification can possibly arrive, so
    /// <see cref="RecordStepStarted"/>/<see cref="RecordLogLine"/> never need a "create on first
    /// sight" fallback the way the old in-memory <c>OrderStore</c> did.</summary>
    public async Task PlaceOrderAsync(string orderId, string customerId, IReadOnlyList<OrderLineItem> items, string shippingAddress)
    {
        await using var db = Connect();
        await using var tx = await db.BeginTransactionAsync();

        var amount = items.Sum(i => i.Amount);
        await db.InsertAsync(new WorkflowViewRow
        {
            WorkflowId = orderId, ParentWorkflowId = null, WorkflowType = nameof(OrderFulfillmentWorkflow),
            Status = nameof(OrderStatus.Started),
        });
        await db.InsertAsync(new OrderRow
        {
            OrderId = orderId, CustomerId = customerId, Amount = amount, ShippingAddress = shippingAddress,
            CreatedAt = DateTimeOffset.UtcNow,
        });

        foreach (var item in items)
        {
            await db.InsertAsync(new WorkflowViewRow
            {
                WorkflowId = item.ItemId, ParentWorkflowId = orderId, WorkflowType = nameof(ItemFulfillmentWorkflow),
                Status = nameof(ItemStatus.Started),
            });
            await db.InsertAsync(new OrderItemRow { OrderId = orderId, ItemWorkflowId = item.ItemId, Amount = item.Amount });
        }

        await tx.CommitAsync();
    }

    public async Task RecordStepStarted(string workflowId, string stepName, int attempt, DateTimeOffset at)
    {
        await using var db = Connect();
        await db.InsertOrReplaceAsync(new StepRunRow
        {
            WorkflowId = workflowId, StepName = stepName, Attempt = attempt,
            Status = nameof(StepRunStatus.Running), StartedAt = at,
        });
    }

    public async Task RecordStepCompleted(string workflowId, string stepName, int attempt)
    {
        await using var db = Connect();
        await db.GetTable<StepRunRow>()
            .Where(s => s.WorkflowId == workflowId && s.StepName == stepName && s.Attempt == attempt)
            .Set(s => s.Status, nameof(StepRunStatus.Succeeded))
            .UpdateAsync();
    }

    public async Task RecordStepFailed(string workflowId, string stepName, int attempt, string error)
    {
        await using var db = Connect();
        await db.GetTable<StepRunRow>()
            .Where(s => s.WorkflowId == workflowId && s.StepName == stepName && s.Attempt == attempt)
            .Set(s => s.Status, nameof(StepRunStatus.Failed))
            .Set(s => s.Error, error)
            .UpdateAsync();
    }

    /// <summary>Idempotent insert keyed by (workflow, timestamp, line) — see this file's own schema
    /// doc comment on why that's a safe natural key across the N replicas that all receive the same
    /// broadcast <c>WorkflowFeedItem</c> (see <c>WorkflowEventPubSubBridge</c>).</summary>
    public async Task RecordLogLine(string workflowId, DateTimeOffset at, string line)
    {
        await using var db = Connect();

        // Every column is part of the key, so a conflict means this exact line is already recorded
        // and there is nothing to write. Merge expresses that as INSERT ... ON CONFLICT DO NOTHING;
        // an upsert would need a non-key column to assign, which this table has none of.
        //
        // Idempotence is what lets the same line arrive more than once: from several replicas
        // handling one cluster-wide broadcast, and again when a replica replays the recorded event
        // feed on startup.
        await EnsuringPresent(() => db.GetTable<EventLogRow>()
            .Merge()
            .Using(new[] { new EventLogRow { WorkflowId = workflowId, At = at, Line = line } })
            .OnTargetKey()
            .InsertWhenNotMatched()
            .MergeAsync());
    }

    /// <summary>
    /// Registers an order this replica did not place, so the rows every other write hangs off exist.
    ///
    /// <see cref="PlaceOrderAsync"/> covers an order placed through the UI, which is every order the
    /// sample had until a schedule started placing them: a schedule sends its target a command
    /// directly, so the first this read model hears of such an order is an event about it. Merge
    /// expresses that as INSERT ... ON CONFLICT DO NOTHING, so an order that was placed normally is
    /// left exactly as it is.
    /// </summary>
    public async Task EnsureOrderRegistered(string orderId, string customerId)
    {
        await using var db = Connect();

        await EnsuringPresent(() => db.GetTable<WorkflowViewRow>()
            .Merge()
            .Using(new[]
            {
                new WorkflowViewRow
                {
                    WorkflowId = orderId, ParentWorkflowId = null,
                    WorkflowType = nameof(OrderFulfillmentWorkflow), Status = nameof(OrderStatus.Started),
                },
            })
            .OnTargetKey()
            .InsertWhenNotMatched()
            .MergeAsync());

        await EnsuringPresent(() => db.GetTable<OrderRow>()
            .Merge()
            .Using(new[]
            {
                new OrderRow
                {
                    OrderId = orderId, CustomerId = customerId, Amount = 0, ShippingAddress = string.Empty,
                    CreatedAt = DateTimeOffset.UtcNow,
                },
            })
            .OnTargetKey()
            .InsertWhenNotMatched()
            .MergeAsync());
    }

    /// <summary>
    /// Registers an item this replica did not place, so the rows every other write hangs off exist.
    /// The order it belongs to is the part of its id before the separator, which is how
    /// <c>OrderFulfillmentWorkflow</c> scopes an item to its order.
    ///
    /// The amount is unknown here: an event says an item ran, without saying what it was for. It is what
    /// <see cref="PlaceOrderAsync"/> records for an order placed through the UI, and zero for one a
    /// schedule placed — which the UI shows as the item existing, with nothing claimed about its size.
    /// </summary>
    public async Task EnsureItemRegistered(string itemWorkflowId, string orderId)
    {
        await using var db = Connect();

        await EnsuringPresent(() => db.GetTable<WorkflowViewRow>()
            .Merge()
            .Using(new[]
            {
                new WorkflowViewRow
                {
                    WorkflowId = itemWorkflowId, ParentWorkflowId = orderId,
                    WorkflowType = nameof(ItemFulfillmentWorkflow), Status = nameof(ItemStatus.Started),
                },
            })
            .OnTargetKey()
            .InsertWhenNotMatched()
            .MergeAsync());

        await EnsuringPresent(() => db.GetTable<OrderItemRow>()
            .Merge()
            .Using(new[] { new OrderItemRow { OrderId = orderId, ItemWorkflowId = itemWorkflowId, Amount = 0 } })
            .OnTargetKey()
            .InsertWhenNotMatched()
            .MergeAsync());
    }

    /// <summary>
    /// Runs a write whose only purpose is that a row ends up present, treating "it is already there"
    /// as the outcome itself, no different from a fresh insert succeeding.
    ///
    /// Every replica watches the same cluster-wide event stream, so all of them react to one event and
    /// all of them try to register the same row. A merge does not settle that on its own: two of them
    /// can each find nothing matching and each go on to insert, and the second is told the key is
    /// taken — which is true, and is what was wanted.
    /// </summary>
    private static async Task EnsuringPresent(Func<Task> write)
    {
        try
        {
            await write();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
        }
    }

    public async Task RefreshStatus(string workflowId, string status, string? failureReason)
    {
        await using var db = Connect();
        await db.GetTable<WorkflowViewRow>()
            .Where(w => w.WorkflowId == workflowId)
            .Set(w => w.Status, status)
            .Set(w => w.FailureReason, failureReason)
            .UpdateAsync();
    }

    public async Task SetPaused(string workflowId, string? reason, DateTimeOffset? autoCancelAt)
    {
        await using var db = Connect();
        await db.GetTable<WorkflowViewRow>()
            .Where(w => w.WorkflowId == workflowId)
            .Set(w => w.Paused, true)
            .Set(w => w.PauseReason, reason)
            .Set(w => w.AutoCancelAt, autoCancelAt)
            .UpdateAsync();
    }

    public async Task SetResumed(string workflowId)
    {
        await using var db = Connect();
        await db.GetTable<WorkflowViewRow>()
            .Where(w => w.WorkflowId == workflowId)
            .Set(w => w.Paused, false)
            .Set(w => w.PauseReason, (string?)null)
            .Set(w => w.AutoCancelAt, (DateTimeOffset?)null)
            .UpdateAsync();
    }

    public async Task SoftDeleteAsync(string orderId)
    {
        await using var db = Connect();
        await db.GetTable<OrderRow>()
            .Where(o => o.OrderId == orderId)
            .Set(o => o.DeletedAt, DateTimeOffset.UtcNow)
            .UpdateAsync();
    }

    /// <summary>List view — every non-deleted order, no item children (the list only ever shows
    /// top-level orders; items are visible inside their parent's own detail view).</summary>
    public async Task<IReadOnlyList<OrderSnapshot>> SnapshotListAsync()
    {
        await using var db = Connect();
        var orders =
            from o in db.GetTable<OrderRow>()
            join v in db.GetTable<WorkflowViewRow>() on o.OrderId equals v.WorkflowId
            where o.DeletedAt == null
            orderby o.CreatedAt
            select new { o, v };

        var rows = await orders.ToListAsync();
        return rows.Select(r => ToSnapshot(r.v, r.o.CustomerId, r.o.Amount, deleted: false, [], [], [])).ToArray();
    }

    /// <summary>Detail view for one order, including its item children's own step pipelines and
    /// logs — one level deep, matching <c>ItemFulfillmentWorkflow</c>'s own shape (it never spawns
    /// children of its own). Returns a tombstone snapshot (<c>Deleted: true</c>, no steps/log) for a
    /// soft-deleted order, with something real to show, so navigating straight to a deleted order's
    /// URL explains why it's gone, past a bare "not found."</summary>
    public async Task<OrderSnapshot?> SnapshotOfAsync(string orderId)
    {
        await using var db = Connect();
        var order = await db.GetTable<OrderRow>().Where(o => o.OrderId == orderId).FirstOrDefaultAsync();
        var view = await db.GetTable<WorkflowViewRow>().Where(v => v.WorkflowId == orderId).FirstOrDefaultAsync();
        if (order is null || view is null)
        {
            return null;
        }

        if (order.DeletedAt is not null)
        {
            return ToSnapshot(view, order.CustomerId, order.Amount, deleted: true, [], [], []);
        }

        var (steps, log) = await LoadStepsAndLogAsync(db, orderId);

        var itemIds = await db.GetTable<OrderItemRow>().Where(i => i.OrderId == orderId).Select(i => i.ItemWorkflowId).ToListAsync();
        var children = new List<OrderSnapshot>(itemIds.Count);
        foreach (var itemId in itemIds)
        {
            var itemView = await db.GetTable<WorkflowViewRow>().Where(v => v.WorkflowId == itemId).FirstOrDefaultAsync();
            if (itemView is null)
            {
                continue;
            }

            var itemRow = await db.GetTable<OrderItemRow>().Where(i => i.ItemWorkflowId == itemId).FirstAsync();
            var (itemSteps, itemLog) = await LoadStepsAndLogAsync(db, itemId);
            children.Add(ToSnapshot(itemView, order.CustomerId, itemRow.Amount, deleted: false, itemSteps, itemLog, []));
        }

        return ToSnapshot(view, order.CustomerId, order.Amount, deleted: false, steps, log, children);
    }

    private static async Task<(IReadOnlyList<StepRun> Steps, IReadOnlyList<string> Log)> LoadStepsAndLogAsync(DataConnection db, string workflowId)
    {
        var steps = await db.GetTable<StepRunRow>()
            .Where(s => s.WorkflowId == workflowId)
            .OrderBy(s => s.StartedAt)
            .Select(s => new StepRun(s.StepName, s.Attempt, Enum.Parse<StepRunStatus>(s.Status), s.Error))
            .ToListAsync();

        var log = await db.GetTable<EventLogRow>()
            .Where(e => e.WorkflowId == workflowId)
            .OrderBy(e => e.At)
            .Select(e => e.Line)
            .ToListAsync();

        return (steps, log);
    }

    private static OrderSnapshot ToSnapshot(
        WorkflowViewRow view, string customerId, int amount, bool deleted,
        IReadOnlyList<StepRun> steps, IReadOnlyList<string> log, IReadOnlyList<OrderSnapshot> children)
    {
        var state = view.Paused
            ? DisplayState.Paused
            : SuccessWords.Contains(view.Status)
                ? DisplayState.Succeeded
                : FailureWords.Contains(view.Status)
                    ? DisplayState.Failed
                    : DisplayState.Running;

        return new OrderSnapshot(
            view.WorkflowId, customerId, amount, view.Status, state, view.FailureReason,
            view.Paused, view.PauseReason, view.AutoCancelAt, deleted, steps, log, children);
    }
}
