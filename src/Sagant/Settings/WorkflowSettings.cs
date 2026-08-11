using Sagant.Descriptors;

namespace Sagant.Settings;

/// <summary>
/// Per-step timeout/recovery override, layered on top of <see cref="WorkflowSettings.DefaultStepTimeout"/>
/// and <see cref="WorkflowSettings.DefaultStepRecoverStrategy"/>.
/// </summary>
public sealed record StepSettings(string StepName, TimeSpan? Timeout, RecoverStrategy? RecoverStrategy);

/// <summary>
/// Per-query timeout override, layered on top of <see cref="WorkflowSettings.DefaultQueryTimeout"/>.
/// Keyed by the query type's name, the same compile-time literal the generator bakes into
/// <see cref="Descriptors.QueryDescriptor{TState}.QueryTypeName"/>.
/// </summary>
public sealed record QuerySettings(string QueryTypeName, TimeSpan? Timeout);

/// <summary>
/// Workflow-wide configuration: overall timeout, default step timeout/recovery, and per-step
/// overrides — the business-level knobs a workflow author sets in code (retry budgets, how long a
/// step is allowed to run). Deployment-level knobs — how instances are distributed, when an idle one
/// is unloaded, how much is buffered — belong to whichever runtime is driving, and live on its own
/// registration API instead.
/// </summary>
public sealed record WorkflowSettings(
    TimeSpan? WorkflowTimeout,
    RecoverStrategy? WorkflowRecoverStrategy,
    TimeSpan? DefaultStepTimeout,
    RecoverStrategy? DefaultStepRecoverStrategy,
    IReadOnlyList<StepSettings> StepSettings,
    /// <summary>
    /// How many caller-supplied idempotency keys (see <c>WorkflowEnvelope.IdempotencyKey</c>) this
    /// workflow instance remembers at once — oldest evicted first once full (see
    /// <see cref="Idempotency.IdempotencyLedger"/>). Per-workflow-<em>type</em> business decision
    /// (how many distinct in-flight-retriable commands an instance should remember), same tier as
    /// <see cref="DefaultStepTimeout"/> — a decision the workflow author makes as part of the
    /// workflow's own settings record.
    /// </summary>
    int IdempotencyLedgerCapacity = 50,
    /// <summary>
    /// How many transport-level producer incarnations (see <c>SeqNrLedger</c>) this workflow instance
    /// remembers a highest-applied-sequence-number for at once — oldest evicted first once full.
    /// Same tier as <see cref="IdempotencyLedgerCapacity"/>: a business-level decision about how much
    /// redelivery-dedup history to keep, made as part of the workflow's own settings record.
    /// </summary>
    int SeqNrDedupCapacity = 16,
    /// <summary>
    /// Whether a finalized <c>AwaitChildren</c> group's terminal-status members (<c>Completed</c>/
    /// <c>Failed</c>/<c>Cancelled</c>/<c>Terminated</c>) are dropped from the runtime driver's
    /// per-instance child history once that group finalizes. <c>ParentClosePolicy</c> only ever acts
    /// on a still-<c>Pending</c>/<c>TerminationRequested</c> member, so pruning is safe for that
    /// logic — but it does mean diagnostics/queries lose the historical record of a pruned child.
    /// Defaults to <c>false</c>: this trades away real history for a memory-footprint win that not
    /// every workflow needs, so it is opt-in.
    /// </summary>
    bool PruneFinalizedChildren = false,
    /// <summary>
    /// How long a query handler is allowed to run before the runtime stops waiting on it, replies
    /// with a <see cref="Protocol.WorkflowQueryTimeoutException"/>, and cancels its token. A caller's
    /// own request timeout completes the caller's wait and sends nothing to the workflow instance, so
    /// this is the only bound that reaches the handler.
    ///
    /// <c>null</c> means <see cref="BuiltInQueryTimeout"/>. Every query is bounded: because queries
    /// dispatch immediately, a bound is what keeps a caller retrying against a slow dependency from
    /// stacking handlers on an instance: each retry starts a fresh handler while the abandoned ones
    /// keep running, so bounding them settles in-flight query concurrency at arrival-rate × timeout.
    /// </summary>
    TimeSpan? DefaultQueryTimeout = null,
    /// <summary>Per-query-type timeout overrides, layered over
    /// <see cref="DefaultQueryTimeout"/>.</summary>
    IReadOnlyList<QuerySettings>? QuerySettings = null,
    /// <summary>
    /// The step a <c>Cancel</c> routes to, giving the workflow a chance to unwind before it finishes.
    /// A workflow-wide decision — how this saga compensates — so a caller asking for cancellation
    /// needs to know only that it wants the run stopped.
    ///
    /// <c>null</c> means there is nothing to unwind, and a <c>Cancel</c> then finishes the run
    /// immediately. It still reports <c>Cancelled</c>: what was asked for is worth recording even
    /// where there was no work to do about it.
    /// </summary>
    string? CancellationStepName = null)
{
    /// <summary>Applied to a query with neither a per-query override nor a
    /// <see cref="DefaultQueryTimeout"/>. A query that genuinely needs longer sets its own.</summary>
    public static readonly TimeSpan BuiltInQueryTimeout = TimeSpan.FromSeconds(30);

    public static readonly WorkflowSettings Default = new(null, null, null, null, Array.Empty<StepSettings>());

    public static WorkflowSettingsBuilder Create() => new();
}

/// <summary>
/// Fluent builder for <see cref="WorkflowSettings"/>. Step-name-keyed overrides accumulate: calling
/// <see cref="StepTimeout"/> and <see cref="StepRecovery"/> for the same step merges into one
/// <see cref="Sagant.StepSettings"/> entry.
/// </summary>
public sealed class WorkflowSettingsBuilder
{
    private TimeSpan? _workflowTimeout;
    private RecoverStrategy? _workflowRecoverStrategy;
    private TimeSpan? _defaultStepTimeout;
    private RecoverStrategy? _defaultStepRecoverStrategy;
    private int _idempotencyLedgerCapacity = 50;
    private int _seqNrDedupCapacity = 16;
    private bool _pruneFinalizedChildren;
    private TimeSpan? _defaultQueryTimeout;
    private string? _cancellationStepName;

    private readonly Dictionary<string, TimeSpan?> _stepTimeouts = new();
    private readonly Dictionary<string, RecoverStrategy?> _stepRecoverStrategies = new();
    private readonly List<string> _stepOrder = new();
    private readonly Dictionary<string, TimeSpan?> _queryTimeouts = new();
    private readonly List<string> _queryOrder = new();

    public WorkflowSettingsBuilder Timeout(TimeSpan timeout)
    {
        _workflowTimeout = timeout;
        return this;
    }

    public WorkflowSettingsBuilder Timeout<TWorkflow, TInput>(TimeSpan timeout, StepRef<TWorkflow, TInput> failoverStep, TInput failoverStepInput)
    {
        _workflowTimeout = timeout;
        _workflowRecoverStrategy = new RecoverStrategy(0, failoverStep.Name, failoverStepInput);
        return this;
    }

    public WorkflowSettingsBuilder Timeout<TWorkflow>(TimeSpan timeout, StepRef<TWorkflow, NoInput> failoverStep)
    {
        _workflowTimeout = timeout;
        _workflowRecoverStrategy = new RecoverStrategy(0, failoverStep.Name, null);
        return this;
    }

    public WorkflowSettingsBuilder DefaultStepTimeout(TimeSpan timeout)
    {
        _defaultStepTimeout = timeout;
        return this;
    }

    public WorkflowSettingsBuilder DefaultStepRecovery(RecoverStrategy recoverStrategy)
    {
        _defaultStepRecoverStrategy = recoverStrategy;
        return this;
    }

    public WorkflowSettingsBuilder StepTimeout<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, TimeSpan timeout)
    {
        TrackStep(step.Name);
        _stepTimeouts[step.Name] = timeout;
        return this;
    }

    public WorkflowSettingsBuilder StepRecovery<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, RecoverStrategy recoverStrategy)
    {
        TrackStep(step.Name);
        _stepRecoverStrategies[step.Name] = recoverStrategy;
        return this;
    }

    public WorkflowSettingsBuilder IdempotencyLedgerCapacity(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "Idempotency ledger capacity must be positive — a non-positive capacity would silently disable dedup.");
        }

        _idempotencyLedgerCapacity = capacity;
        return this;
    }

    public WorkflowSettingsBuilder SeqNrDedupCapacity(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "SeqNr dedup ledger capacity must be positive — a non-positive capacity would silently disable dedup.");
        }

        _seqNrDedupCapacity = capacity;
        return this;
    }

    public WorkflowSettingsBuilder PruneFinalizedChildren(bool prune = true)
    {
        _pruneFinalizedChildren = prune;
        return this;
    }

    /// <summary>Names the step a <c>Cancel</c> unwinds through — see
    /// <see cref="WorkflowSettings.CancellationStepName"/>.</summary>
    /// <summary>
    /// For a cancellation step that reads the request — its input is a
    /// <see cref="Protocol.WorkflowCancellation"/>, so a step declared to take anything else is
    /// caught here, at compile time.
    /// </summary>
    public WorkflowSettingsBuilder CancelVia<TWorkflow>(StepRef<TWorkflow, Protocol.WorkflowCancellation> step)
    {
        _cancellationStepName = step.Name;
        return this;
    }

    /// <summary>For a cancellation step that doesn't need the reason.</summary>
    public WorkflowSettingsBuilder CancelVia<TWorkflow>(StepRef<TWorkflow, NoInput> step)
    {
        _cancellationStepName = step.Name;
        return this;
    }

    /// <summary>Bounds every query handler that has no override of its own — see
    /// <see cref="WorkflowSettings.DefaultQueryTimeout"/>.</summary>
    public WorkflowSettingsBuilder DefaultQueryTimeout(TimeSpan timeout)
    {
        _defaultQueryTimeout = timeout;
        return this;
    }

    /// <summary>Bounds one query type, layered over <see cref="DefaultQueryTimeout"/>.
    /// <typeparamref name="TQuery"/>'s name is the key, matching the literal the generator bakes into
    /// the query descriptor.</summary>
    public WorkflowSettingsBuilder QueryTimeout<TQuery>(TimeSpan timeout)
        where TQuery : notnull
    {
        var name = typeof(TQuery).Name;
        if (!_queryTimeouts.ContainsKey(name))
        {
            _queryOrder.Add(name);
        }

        _queryTimeouts[name] = timeout;
        return this;
    }


    public WorkflowSettings Build()
    {
        var stepSettings = _stepOrder
            .Select(name => new StepSettings(
                name,
                _stepTimeouts.GetValueOrDefault(name),
                _stepRecoverStrategies.GetValueOrDefault(name)))
            .ToList();

        var querySettings = _queryOrder
            .Select(name => new QuerySettings(name, _queryTimeouts.GetValueOrDefault(name)))
            .ToList();

        return new WorkflowSettings(
            _workflowTimeout,
            _workflowRecoverStrategy,
            _defaultStepTimeout,
            _defaultStepRecoverStrategy,
            stepSettings,
            _idempotencyLedgerCapacity,
            _seqNrDedupCapacity,
            _pruneFinalizedChildren,
            _defaultQueryTimeout,
            querySettings,
            _cancellationStepName);
    }

    private void TrackStep(string stepName)
    {
        if (!_stepTimeouts.ContainsKey(stepName) && !_stepRecoverStrategies.ContainsKey(stepName))
        {
            _stepOrder.Add(stepName);
        }
    }
}
