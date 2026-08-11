namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Splits the persistence id <see cref="WorkflowClusterShardingExtensions.WithWorkflow{TWorkflow, TState}"/>
/// builds — <c>{WorkflowTypeName}-{entityId}</c> — back into its two halves.
///
/// The two are distinct and the difference matters: <c>ClusterSharding</c> routes on the plain entity
/// id, so that is what a caller hands to <c>IWorkflowClient.For</c> and what a child reports back to.
/// The persistence id is the type-prefixed form the journal is keyed by.
///
/// Because the type is a prefix, a listing can filter by workflow type from the id alone, reading no
/// event bodies at all.
/// </summary>
public static class WorkflowPersistenceId
{
    /// <summary>The workflow's durable type name.</summary>
    public static string WorkflowTypeOf(string persistenceId)
    {
        var separator = persistenceId.IndexOf('-');
        return separator > 0 ? persistenceId[..separator] : persistenceId;
    }

    /// <summary>The routable entity id, ready to hand back to <c>IWorkflowClient.For</c>.</summary>
    public static string EntityIdOf(string persistenceId)
    {
        // The FIRST separator only: a type name holds no '-', and an entity id holds several when it
        // is a GUID.
        var separator = persistenceId.IndexOf('-');
        return separator >= 0 ? persistenceId[(separator + 1)..] : persistenceId;
    }
}
