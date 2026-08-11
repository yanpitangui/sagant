using Sagant.Settings;
using Sagant.Effects;

namespace Sagant;

/// <summary>
/// Base class for a durable, step-orchestrated workflow. Extend this, declare state via
/// <typeparamref name="TState"/>, and add handlers of three kinds:
/// <list type="bullet">
/// <item><c>[WorkflowCommandHandler]</c> — synchronous, takes a <see cref="CommandContext{TState}"/>,
/// returns <see cref="CommandEffect{TState}"/>. Decides; never does I/O.</item>
/// <item><c>[WorkflowStep]</c> — takes a <see cref="StepContext{TState}"/>, returns
/// <see cref="StepEffect{TState}"/>. Where I/O, retries and timeouts live.</item>
/// <item><c>[WorkflowQuery]</c> — takes a <see cref="QueryContext{TState}"/>, returns
/// <see cref="QueryEffect"/>. Read-only, may be async, may run while a step is executing.</item>
/// </list>
///
/// A workflow instance holds no per-invocation state of its own: state arrives as a value on the
/// handler's context, so a step suspended at an <c>await</c> and a handler dispatched while it waits
/// cannot observe each other. Anything an instance does cache in a field is not durable and does not
/// survive recovery or relocation — dependencies injected at construction are what fields are for.
/// </summary>
public abstract class Workflow<TState>
{
    /// <summary>
    /// This workflow's CLR type name — the single canonical source every span
    /// (<see cref="Protocol.WorkflowDiagnostics.ActivitySource"/>) and metric
    /// (<see cref="Protocol.WorkflowDiagnostics.Meter"/>) tag, and every
    /// <see cref="Protocol.WorkflowFeedItem.WorkflowType"/>, reads workflow type from.
    /// <c>Sagant.SourceGenerators.StepRegistryGenerator</c> overrides this with a compile-time
    /// string literal on every concrete workflow class (the same generated file that implements
    /// <c>IWorkflowStepDispatcher</c>/<c>IWorkflowCommandDispatcher</c>/
    /// <see cref="Descriptors.IWorkflowTypeInfo"/>) — zero reflection, matches the project's other
    /// generated dispatch tables. Declared here, on the base class, because
    /// <c>StepDescriptor{TState}.Invoke</c>/<c>CommandDescriptor{TState}.Invoke</c> (core, shared by
    /// every runtime driver) read it off a <c>Workflow&lt;TState&gt;</c>-typed parameter — the base
    /// class shared by every concrete workflow — so the member has to live here for that
    /// polymorphic call to resolve at all. This <see cref="GetType"/>-based body serves as the
    /// fallback for a hand-written stand-in workflow that implements the dispatcher interfaces
    /// itself, standing in for generator output (as several test fixtures in this repo do).
    /// </summary>
    public virtual string WorkflowTypeName => GetType().Name;

    /// <summary>
    /// The state a fresh (never-persisted) workflow instance starts with.
    ///
    /// Abstract because only the workflow knows what "empty" means for its own state, and because
    /// there is no safe stand-in: <c>default(TState)</c> is <c>null</c> for the record types state
    /// is usually written as, which surfaces as a null-reference error inside the first step that
    /// reads it — far from the declaration that caused it. Requiring it moves that to a compile
    /// error at the workflow that omitted it.
    /// </summary>
    public abstract TState EmptyState();

    /// <summary>Builder for command-handler effects.</summary>
    protected EffectsBuilder<TState> Effects => new();

    /// <summary>Builder for step-handler effects.</summary>
    protected StepEffectsBuilder<TState> StepEffects => new();

    /// <summary>Builder for query-handler effects.</summary>
    protected QueryEffectsBuilder QueryEffects => QueryEffectsBuilder.Instance;

    /// <summary>Override to configure step/workflow/query timeouts and retries.</summary>
    public virtual WorkflowSettings Settings() => WorkflowSettings.Default;
}
