using Sagant.Descriptors;

namespace Sagant.Effects;

/// <summary>
/// Fluent configuration for <c>StepEffectsBuilder{TState}.AwaitChildren</c>. Every policy defaults
/// to the common case if its corresponding method is never called —
/// <see cref="AllSuccessful"/>/<see cref="FailFast"/>/<see cref="TerminateRemaining"/> — so the
/// fully-configured form and the two-argument common-case overload produce identical transitions
/// when nothing here is called at all. <see cref="ResumeAt"/> is the only required call.
/// </summary>
public sealed class ChildGroupOptions
{
    private CompletionPolicy _completionPolicy = CompletionPolicy.AllSuccessful;
    private FailurePolicy _failurePolicy = FailurePolicy.FailFast;
    private RemainingChildrenPolicy _remainingChildrenPolicy = RemainingChildrenPolicy.Terminate;
    private string? _resumeStepName;
    private string? _groupId;
    private TimeSpan? _timeout;
    private string? _timeoutStepName;

    public ChildGroupOptions AllSuccessful() { _completionPolicy = CompletionPolicy.AllSuccessful; return this; }
    public ChildGroupOptions AllCompleted() { _completionPolicy = CompletionPolicy.AllCompleted; return this; }

    public ChildGroupOptions FailFast() { _failurePolicy = FailurePolicy.FailFast; return this; }
    public ChildGroupOptions WaitForAll() { _failurePolicy = FailurePolicy.WaitForAll; return this; }

    public ChildGroupOptions TerminateRemaining() { _remainingChildrenPolicy = RemainingChildrenPolicy.Terminate; return this; }
    public ChildGroupOptions ContinueRemaining() { _remainingChildrenPolicy = RemainingChildrenPolicy.Continue; return this; }

    /// <summary>Required — the step to invoke, with a typed <c>ChildGroupResult</c> input, once this
    /// group's policy is satisfied.</summary>
    public ChildGroupOptions ResumeAt<TWorkflow>(StepRef<TWorkflow, ChildGroupResult> step)
    {
        _resumeStepName = step.Name;
        return this;
    }

    /// <summary>Optional — explicit group id for a workflow that wants to refer to this group by a
    /// human-meaningful name later. Omit it and the runtime driver generates a durable one.</summary>
    public ChildGroupOptions GroupId(string groupId) { _groupId = groupId; return this; }

    /// <summary>
    /// Optional — how long this group waits before <paramref name="timeoutStep"/> decides what to do
    /// about children that never finished. Omit it and the parent waits for them however long they
    /// take, which is the default.
    ///
    /// The step takes the group's result so far, so it sees which children settled and which are
    /// still outstanding, and decides from there — compensate, carry on without them, or end the run.
    /// </summary>
    public ChildGroupOptions Timeout<TWorkflow>(TimeSpan timeout, StepRef<TWorkflow, ChildGroupResult> timeoutStep)
    {
        _timeout = timeout;
        _timeoutStepName = timeoutStep.Name;
        return this;
    }

    internal (string? GroupId, CompletionPolicy CompletionPolicy, FailurePolicy FailurePolicy, RemainingChildrenPolicy RemainingChildrenPolicy, string ResumeStepName, TimeSpan? Timeout, string? TimeoutStepName) Build()
    {
        if (_resumeStepName is null)
        {
            throw new InvalidOperationException($"{nameof(ResumeAt)} is required — call it before building the group.");
        }

        return (_groupId, _completionPolicy, _failurePolicy, _remainingChildrenPolicy, _resumeStepName,
            _timeout, _timeoutStepName);
    }
}
