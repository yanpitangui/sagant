using System.Collections.Immutable;

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

    /// <summary>Prefix of the per-type tag; the workflow's durable type name follows.</summary>
    public const string TypePrefix = "sagant:";

    /// <summary>The tag carrying every instance of <paramref name="workflowTypeName"/> — e.g.
    /// <c>sagant:OrderFulfillmentWorkflow</c>. Pass it to
    /// <see cref="Sagant.Clients.IWorkflowEventFeed.Subscribe"/> to follow one workflow type.</summary>
    public static string ForWorkflowType(string workflowTypeName) => TypePrefix + workflowTypeName;

    /// <summary>Both tags an instance of <paramref name="workflowTypeName"/> writes.</summary>
    public static IImmutableSet<string> For(string workflowTypeName) =>
        ImmutableHashSet.Create(All, ForWorkflowType(workflowTypeName));
}
