namespace Sagant.Effects;

using Sagant.Descriptors;
using Sagant.Protocol;

/// <summary>The resume step's first, cheapest signal — did the group's configured
/// <c>CompletionPolicy</c> hold, or did <c>FailurePolicy</c> finalize it as failed.</summary>
public enum GroupOutcome { Succeeded, Failed }

/// <summary>
/// The one and only resume-step input type for an <c>AwaitChildren</c> group, homogeneous or
/// heterogeneous alike. Built by the runtime driver from a group's persisted
/// <c>ChildWorkflowRelationship</c> members once its policy is satisfied — a workflow author never
/// constructs one directly.
/// </summary>
public sealed class ChildGroupResult
{
    private IReadOnlyDictionary<string, ChildWorkflowRelationship>? _byId;

    public ChildGroupResult(GroupOutcome outcome, IReadOnlyList<ChildWorkflowRelationship> members)
    {
        Outcome = outcome;
        Members = members ?? throw new ArgumentNullException(nameof(members));
    }

    public GroupOutcome Outcome { get; }

    /// <summary>
    /// The group's members. A member is always read by id — through <see cref="Get{TWorkflow, TState}"/>,
    /// <see cref="TryGet{TWorkflow, TState}"/>, <see cref="GetAll{TWorkflow, TState}"/>, or
    /// <see cref="WorkflowIds"/>. A member's place in this list carries no meaning.
    ///
    /// Public because this value is persisted: it reaches the resume step as that step's input, which
    /// a runtime writes to its journal and reads back on recovery. A member a serializer cannot see is
    /// a member it cannot restore, and the constructor would then be handed nothing to build from —
    /// so what the lookups below read has to be visible here.
    /// </summary>
    public IReadOnlyList<ChildWorkflowRelationship> Members { get; }

    /// <summary>Built lazily, on first use, so restoring one of these costs nothing until something
    /// asks about a member.</summary>
    private IReadOnlyDictionary<string, ChildWorkflowRelationship> ById =>
        _byId ??= Members.ToDictionary(m => m.ChildWorkflowId);

    public IReadOnlyList<string> WorkflowIds => Members.Select(m => m.ChildWorkflowId).ToList();

    public ChildStatus GetStatus(string workflowId) => Resolve(workflowId).Status;

    public WorkflowFailure? GetFailure(string workflowId) => Resolve(workflowId).Failure;

    /// <summary>Throws <see cref="ChildNotInGroupException"/>, <see cref="ChildWorkflowTypeMismatchException"/>,
    /// or <see cref="ChildResultNotAvailableException"/> — see each type's own doc comment for which
    /// condition produces which. Never an unqualified <see cref="InvalidCastException"/>.</summary>
    public TState Get<TWorkflow, TState>(string workflowId) where TWorkflow : IWorkflowTypeInfo
    {
        var member = Resolve(workflowId);
        if (member.ChildWorkflowType != TWorkflow.WorkflowTypeName)
        {
            throw new ChildWorkflowTypeMismatchException(workflowId, TWorkflow.WorkflowTypeName, member.ChildWorkflowType);
        }

        if (member.Status != ChildStatus.Completed)
        {
            throw new ChildResultNotAvailableException(workflowId, member.Status);
        }

        return (TState)member.Result!;
    }

    /// <summary>Never throws — returns <c>false</c> for every reason <see cref="Get{TWorkflow, TState}"/>
    /// would throw (not present, wrong type, or not completed), standard .NET <c>TryXxx</c>
    /// contract.</summary>
    public bool TryGet<TWorkflow, TState>(string workflowId, out TState? state) where TWorkflow : IWorkflowTypeInfo
    {
        if (ById.TryGetValue(workflowId, out var member)
            && member.ChildWorkflowType == TWorkflow.WorkflowTypeName
            && member.Status == ChildStatus.Completed)
        {
            state = (TState)member.Result!;
            return true;
        }

        state = default;
        return false;
    }

    /// <summary>Homogeneous convenience — every member must match <typeparamref name="TWorkflow"/>
    /// and be <see cref="ChildStatus.Completed"/>, or this throws the same exception
    /// <see cref="Get{TWorkflow, TState}"/> would for whichever member fails first.</summary>
    public IReadOnlyDictionary<string, TState> GetAll<TWorkflow, TState>() where TWorkflow : IWorkflowTypeInfo
    {
        var result = new Dictionary<string, TState>();
        foreach (var workflowId in WorkflowIds)
        {
            result[workflowId] = Get<TWorkflow, TState>(workflowId);
        }

        return result;
    }

    private ChildWorkflowRelationship Resolve(string workflowId) =>
        ById.TryGetValue(workflowId, out var member) ? member : throw new ChildNotInGroupException(workflowId);
}
