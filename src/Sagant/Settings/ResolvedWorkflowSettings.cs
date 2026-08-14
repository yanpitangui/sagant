using System.Collections.Frozen;

namespace Sagant.Settings;

/// <summary>
/// <see cref="WorkflowSettings"/> flattened once into the lookups a runtime driver actually performs,
/// with per-step and per-query overrides already layered over their defaults.
///
/// Exists because settings are immutable per workflow type but <see cref="Workflow{TState}.Settings"/>
/// is a virtual method a workflow is free to build fresh on every call — and a driver reads it many
/// times per transition (resolving a step timeout, a recover strategy, a ledger capacity). Resolving
/// once per instance turns that into field reads, and turns each override lookup from a scan of the
/// override list into a dictionary hit.
///
/// Also the single place override layering is decided, so a driver cannot get "step-specific value,
/// else default" subtly different from another driver's version of the same rule.
/// </summary>
public sealed class ResolvedWorkflowSettings
{
    private readonly FrozenDictionary<string, StepSettings> _steps;
    private readonly FrozenDictionary<string, TimeSpan?> _queryTimeouts;

    private ResolvedWorkflowSettings(WorkflowSettings settings)
    {
        Source = settings;
        WorkflowTimeout = settings.WorkflowTimeout;
        WorkflowRecoverStrategy = settings.WorkflowRecoverStrategy;
        IdempotencyLedgerCapacity = settings.IdempotencyLedgerCapacity;
        SeqNrDedupCapacity = settings.SeqNrDedupCapacity;
        PruneFinalizedChildren = settings.PruneFinalizedChildren;
        CancellationStepName = settings.CancellationStepName;
        HoldTimeout = settings.HoldTimeout;
        HoldTimeoutStepName = settings.HoldTimeoutStepName;

        _defaultStepTimeout = settings.DefaultStepTimeout;
        _defaultStepRecoverStrategy = settings.DefaultStepRecoverStrategy;
        _defaultQueryTimeout = settings.DefaultQueryTimeout ?? WorkflowSettings.BuiltInQueryTimeout;

        // Last entry wins for a duplicated key: the builder already merges per-step overrides into one
        // entry, and a hand-constructed record with duplicates has no meaningful earlier-wins rule.
        _steps = settings.StepSettings
            .GroupBy(s => s.StepName)
            .ToFrozenDictionary(g => g.Key, g => g.Last());
        _queryTimeouts = (settings.QuerySettings ?? Array.Empty<QuerySettings>())
            .GroupBy(q => q.QueryTypeName)
            .ToFrozenDictionary(g => g.Key, g => g.Last().Timeout);
    }

    private readonly TimeSpan? _defaultStepTimeout;
    private readonly RecoverStrategy? _defaultStepRecoverStrategy;
    private readonly TimeSpan _defaultQueryTimeout;

    /// <summary>The record this was flattened from, for anything needing a value not surfaced here.</summary>
    public WorkflowSettings Source { get; }

    public TimeSpan? WorkflowTimeout { get; }

    public RecoverStrategy? WorkflowRecoverStrategy { get; }

    public int IdempotencyLedgerCapacity { get; }

    public int SeqNrDedupCapacity { get; }

    public bool PruneFinalizedChildren { get; }

    /// <summary>See <see cref="WorkflowSettings.CancellationStepName"/>.</summary>
    public string? CancellationStepName { get; }

    /// <summary>See <see cref="WorkflowSettings.HoldTimeout"/>.</summary>
    public TimeSpan? HoldTimeout { get; }

    /// <summary>See <see cref="WorkflowSettings.HoldTimeoutStepName"/>.</summary>
    public string? HoldTimeoutStepName { get; }

    public static ResolvedWorkflowSettings From(WorkflowSettings settings) => new(settings);

    /// <summary>Step-specific timeout if one is configured, else <see cref="WorkflowSettings.DefaultStepTimeout"/>.</summary>
    public TimeSpan? StepTimeoutFor(string stepName) =>
        (_steps.TryGetValue(stepName, out var step) ? step.Timeout : null) ?? _defaultStepTimeout;

    /// <summary>Step-specific recover strategy if one is configured, else
    /// <see cref="WorkflowSettings.DefaultStepRecoverStrategy"/>.</summary>
    public RecoverStrategy? RecoverStrategyFor(string stepName) =>
        (_steps.TryGetValue(stepName, out var step) ? step.RecoverStrategy : null) ?? _defaultStepRecoverStrategy;

    /// <summary>Query-specific timeout if one is configured, else
    /// <see cref="WorkflowSettings.DefaultQueryTimeout"/>, else
    /// <see cref="WorkflowSettings.BuiltInQueryTimeout"/>. Never <c>null</c> — every query is bounded
    /// (guarantee Q2).</summary>
    public TimeSpan QueryTimeoutFor(string queryTypeName) =>
        (_queryTimeouts.TryGetValue(queryTypeName, out var timeout) ? timeout : null) ?? _defaultQueryTimeout;
}
