using System.Linq;
using Sagant.Descriptors;
using Sagant.Settings;
namespace Sagant.Effects;

/// <summary>
/// Fluent builder for step-handler effects. Obtain via <see cref="Workflow{TState}.StepEffects"/>.
/// No <c>Reply</c>/<c>Error</c> — steps are internal orchestration only.
/// </summary>
public sealed class StepEffectsBuilder<TState>
{
    private PersistenceEffect<TState> _persistence = PersistenceEffect<TState>.NoPersistence.Instance;

    public StepEffectsBuilder<TState> UpdateState(TState newState)
    {
        _persistence = new PersistenceEffect<TState>.UpdateState(newState);
        return this;
    }

    public StepEffect<TState> ThenPause() =>
        Build(new Transition.PauseTransition(null, null));

    public StepEffect<TState> ThenPause(string reason) =>
        Build(new Transition.PauseTransition(reason, null));

    public StepEffect<TState> ThenPause(PauseSettings settings) =>
        Build(new Transition.PauseTransition(settings.Reason, settings));

    public StepEffect<TState> ThenTransitionTo<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, TInput input)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.StepTransition(step.Name, input));

    public StepEffect<TState> ThenTransitionTo<TWorkflow>(StepRef<TWorkflow, NoInput> step)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.StepTransition(step.Name, null));

    /// <summary>
    /// Begin a fresh cycle at <paramref name="step"/>, reclaiming the history recorded so far — see
    /// <see cref="Transition.RestartTransition"/> for what an instance keeps across one.
    ///
    /// This is how a workflow with no natural end stays bounded: recorded events accumulate for as
    /// long as an instance lives, so a perpetual run needs a point where its past becomes
    /// reclaimable. Pair it with <c>UpdateState</c> to carry a cycle's conclusion forward.
    /// </summary>
    public StepEffect<TState> ThenRestartAt<TWorkflow, TInput>(StepRef<TWorkflow, TInput> step, TInput input, string? reason = null)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.RestartTransition(step.Name, input, reason));

    /// <inheritdoc cref="ThenRestartAt{TWorkflow, TInput}"/>
    public StepEffect<TState> ThenRestartAt<TWorkflow>(StepRef<TWorkflow, NoInput> step, string? reason = null)
        where TWorkflow : Workflow<TState> =>
        Build(new Transition.RestartTransition(step.Name, null, reason));

    /// <summary>Finish successfully.</summary>
    public StepEffect<TState> ThenComplete() =>
        Build(new Transition.TerminalTransition(global::Sagant.Protocol.WorkflowOutcome.Completed.Instance));

    /// <summary>
    /// Finish as failed, with <paramref name="message"/> describing why. The runtime fills in which
    /// step this came from and how many attempts had run.
    /// </summary>
    public StepEffect<TState> ThenFail(string message) =>
        Build(new Transition.TerminalTransition(
            new global::Sagant.Protocol.WorkflowOutcome.Failed(new global::Sagant.Protocol.WorkflowFailure(message))));

    /// <summary>
    /// Finish as cancelled — the run was asked to stop and has now unwound. Normally the last thing a
    /// cancellation step does.
    /// </summary>
    public StepEffect<TState> ThenCancel() =>
        Build(new Transition.TerminalTransition(new global::Sagant.Protocol.WorkflowOutcome.Cancelled(null)));

    /// <summary>Finish as cancelled, recording why.</summary>
    public StepEffect<TState> ThenCancel(string reason) =>
        Build(new Transition.TerminalTransition(new global::Sagant.Protocol.WorkflowOutcome.Cancelled(reason)));

    /// <summary>
    /// Finish as failed, capturing <paramref name="exception"/> — its type, stack trace and whole
    /// inner chain — so a caller inspecting the failure later sees exactly what was thrown, in full,
    /// unflattened.
    /// </summary>
    public StepEffect<TState> ThenFail(Exception exception) =>
        Build(new Transition.TerminalTransition(
            new global::Sagant.Protocol.WorkflowOutcome.Failed(
                global::Sagant.Protocol.WorkflowFailure.FromException(exception))));

    public StepEffect<TState> ThenDelete() =>
        Build(new Transition.DeleteTransition(null));

    public StepEffect<TState> ThenDelete(string reason) =>
        Build(new Transition.DeleteTransition(reason));

    /// <summary>
    /// Describes one child workflow to start — <typeparamref name="TWorkflow"/> resolves its
    /// <see cref="Descriptors.IWorkflowTypeInfo.WorkflowTypeName"/> at compile time, no reflection,
    /// no instance required (nothing's been constructed yet — this only describes intent). Pass the
    /// result to <see cref="AwaitChildren(System.Collections.Generic.IEnumerable{ChildStart}, string)"/>.
    /// <paramref name="workflowId"/> is this child's durable identity and doubles as the lookup key
    /// for its result later — see <see cref="ChildStart"/>'s own doc comment.
    ///
    /// An instance member, so it reads as <c>StepEffects.Child&lt;T&gt;(...)</c>, matching every
    /// other call on this builder.
    /// </summary>
    public ChildStart Child<TWorkflow>(
        string workflowId, object command, ParentClosePolicy parentClosePolicy = ParentClosePolicy.Abandon)
        where TWorkflow : Descriptors.IWorkflowTypeInfo =>
        new(TWorkflow.WorkflowTypeName, workflowId, command, parentClosePolicy);

    /// <summary>
    /// Start <paramref name="children"/> (potentially different workflow types each — the general,
    /// heterogeneous model this feature is built on) and durably wait for their outcomes. The common
    /// case: all must succeed, fail fast on the first failure, terminate the rest.
    /// </summary>
    public StepEffect<TState> AwaitChildren<TWorkflow>(
        IEnumerable<ChildStart> children, StepRef<TWorkflow, ChildGroupResult> resumeStep) =>
        AwaitChildren(children, options => options.ResumeAt(resumeStep));

    /// <summary>Configured form — see <see cref="ChildGroupOptions"/> for every knob and its
    /// default.</summary>
    public StepEffect<TState> AwaitChildren(IEnumerable<ChildStart> children, Action<ChildGroupOptions> configure)
    {
        var list = children as IReadOnlyList<ChildStart> ?? children.ToList();
        var duplicate = list.GroupBy(c => c.WorkflowId).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
        {
            throw new ArgumentException(
                $"Duplicate child WorkflowId '{duplicate.Key}' — WorkflowId must be unique within one AwaitChildren group.",
                nameof(children));
        }

        var options = new ChildGroupOptions();
        configure(options);
        var built = options.Build();

        return Build(new Transition.AwaitChildrenTransition(
            built.GroupId, list, built.CompletionPolicy, built.FailurePolicy, built.RemainingChildrenPolicy,
            built.ResumeStepName, built.Timeout, built.TimeoutStepName));
    }

    /// <summary>
    /// Convenience over the general <see cref="AwaitChildren{TWorkflow}(System.Collections.Generic.IEnumerable{ChildStart}, Sagant.Descriptors.StepRef{TWorkflow, ChildGroupResult})"/>
    /// for the common case where every child is the same workflow type — fixes
    /// <typeparamref name="TWorkflow"/> once for the whole group, applying it to every element via
    /// <see cref="Child{TWorkflow}"/> internally. Produces the exact same <c>AwaitChildrenTransition</c>
    /// the general path would; nothing about persistence, delivery, or the result type differs for a
    /// homogeneous group.
    /// </summary>
    public StepEffect<TState> AwaitChildren<TWorkflow, TResumeWorkflow>(
        IEnumerable<(string WorkflowId, object Command)> children,
        StepRef<TResumeWorkflow, ChildGroupResult> resumeStep)
        where TWorkflow : Descriptors.IWorkflowTypeInfo =>
        AwaitChildren(children.Select(c => Child<TWorkflow>(c.WorkflowId, c.Command)), resumeStep);

    /// <summary>Configured form of the homogeneous convenience overload above.</summary>
    public StepEffect<TState> AwaitChildren<TWorkflow>(
        IEnumerable<(string WorkflowId, object Command)> children, Action<ChildGroupOptions> configure)
        where TWorkflow : Descriptors.IWorkflowTypeInfo =>
        AwaitChildren(children.Select(c => Child<TWorkflow>(c.WorkflowId, c.Command)), configure);

    private StepEffect<TState> Build(Transition transition) =>
        new(_persistence, transition);
}
