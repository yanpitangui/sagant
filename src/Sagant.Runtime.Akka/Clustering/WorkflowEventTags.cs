using System.Collections.Immutable;
using Sagant.Execution;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// The tags <c>WorkflowEntityActor</c> writes on every event, and the names a reader selects a stream
/// by.
///
/// A tag is a stream selector, held to a small fixed set: the read side takes one tag per query with
/// no way to combine two, so a dimension worth filtering on — a customer, an amount — belongs in a
/// column of a consumer's own projection. Bounding the set by the number of workflow types keeps the
/// journal's tag index small.
/// </summary>
public static class WorkflowEventTags
{
    /// <summary>Every workflow event, whatever type produced it.</summary>
    public const string All = "sagant";

    /// <summary>
    /// Carried by the events the deadline machinery writes about its own workings — a bucket's
    /// contents, a ticker's position, a projection's position. None of them is a workflow event and
    /// nothing reads this stream; it exists so every row this engine writes carries a tag, since a
    /// journal that indexes tags has a query over that index and an untagged row is a shape it may
    /// not expect.
    /// </summary>
    public const string Internal = "sagant-internal";

    /// <summary>Prefix of the per-type tag; the workflow's durable type name follows.</summary>
    public const string TypePrefix = "sagant:";

    /// <summary>The tag carrying every instance of <paramref name="workflowTypeName"/> — e.g.
    /// <c>sagant:OrderFulfillmentWorkflow</c>. Pass it to
    /// <see cref="Sagant.Clients.IWorkflowEventFeed.Subscribe"/> to follow one workflow type.</summary>
    public static string ForWorkflowType(string workflowTypeName) => TypePrefix + workflowTypeName;

    /// <summary>Both tags an instance of <paramref name="workflowTypeName"/> writes.</summary>
    public static IImmutableSet<string> For(string workflowTypeName) =>
        ImmutableHashSet.Create(All, ForWorkflowType(workflowTypeName));

    /// <summary>
    /// Every deadline-moving event, whatever instance produced it. One query against the journal
    /// carries the whole deadline stream.
    /// </summary>
    public const string Deadline = "sagant-deadline";

    /// <summary>
    /// Whether <paramref name="event"/> moves one of the deadlines an outside wake can fire, and so
    /// belongs in the deadline stream.
    ///
    /// Kept in step with <see cref="WorkflowDeadlineFold"/>, which computes the change this tag lets
    /// a reader see. Every case is named here explicitly, so checking the two against each other is
    /// comparing two lists.
    /// </summary>
    public static bool MovesADeadline(WorkflowEvent @event) => @event switch
    {
        // Arms.
        WorkflowEvent.WorkflowDeadlineSet => true,
        WorkflowEvent.RunPaused => true,

        // A hold arms one when the settings name a deadline, and ends the pause it came from either
        // way, so both belong here whatever they happen to carry.
        WorkflowEvent.RunSuspended => true,
        WorkflowEvent.RunParked => true,

        // Disarms — every event that puts the instance back to running, plus the terminal ones.
        WorkflowEvent.StepStarted => true,
        WorkflowEvent.RunResumed => true,
        WorkflowEvent.ChildrenAwaited => true,

        // A group that resolves stops being worth waking for.
        WorkflowEvent.ChildGroupFinalized => true,
        WorkflowEvent.RunRestarted => true,
        WorkflowEvent.RunFinished => true,
        WorkflowEvent.RunDeleted => true,

        _ => false,
    };

    /// <summary>
    /// Every tag an instance of <paramref name="workflowTypeName"/> writes on a deadline-moving
    /// event.
    ///
    /// One tag for the whole deadline stream, with no shard number in it: a reader that needs to
    /// spread the work partitions what it reads after reading it (see
    /// <c>WorkflowDeadlineProjection</c>), so how many readers or lanes exist is settled at read time
    /// and never reaches the journal.
    /// </summary>
    public static IImmutableSet<string> ForDeadlineEvent(string workflowTypeName) =>
        ImmutableHashSet.Create(All, ForWorkflowType(workflowTypeName), Deadline);
}
