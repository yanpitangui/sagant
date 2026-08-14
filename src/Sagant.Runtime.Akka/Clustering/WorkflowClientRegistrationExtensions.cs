using Sagant.Clients;
using Akka.Actor;
using Sagant.Descriptors;
using Microsoft.Extensions.DependencyInjection;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Per-<c>typeof(TWorkflow)</c> factory for a strongly-typed handle, closed over the workflow's
/// actual <c>TState</c> — built once at <c>WithWorkflow&lt;TWorkflow,TState&gt;</c> registration
/// time, read by <see cref="WorkflowClient"/> at every <c>For&lt;TWorkflow&gt;</c> call. This is
/// the one place both type parameters are ever paired up — every call site after registration
/// needs only <c>TWorkflow</c>.
/// </summary>
internal delegate object WorkflowHandleFactory(IActorRef shardRegion, IActorRef producerAdapter, string entityId);

/// <summary>
/// One instance per <see cref="ActorSystem"/> — an <see cref="IExtension"/>, because it has to be
/// reachable from two places that don't share a container: the
/// <c>WithWorkflow</c> registration callback (which only has the <see cref="ActorSystem"/>, via
/// <c>AddStartup</c>, not an <see cref="IServiceProvider"/>) and <see cref="WorkflowClient"/>
/// (constructed through DI). <see cref="WorkflowHandleRegistryProvider"/> is the standard
/// Akka.NET "one instance of X per ActorSystem" accessor for it.
/// </summary>
internal sealed class WorkflowHandleRegistry : IExtension
{
    private readonly Dictionary<Type, WorkflowHandleFactory> _factories = new();
    private readonly Dictionary<Type, (IActorRef ShardRegion, IActorRef ProducerAdapter)> _targets = new();
    private readonly Dictionary<string, Type> _typesByName = new();

    public void Register<TWorkflow, TState>(IActorRef shardRegion, IActorRef producerAdapter)
        where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>, IWorkflowTypeInfo
    {
        _targets[typeof(TWorkflow)] = (shardRegion, producerAdapter);
        _factories[typeof(TWorkflow)] = (region, producer, entityId) =>
            new WorkflowHandle<TWorkflow, TState>(new WorkflowRef<TWorkflow, TState>(region, producer, entityId));
        _typesByName[TWorkflow.WorkflowTypeName] = typeof(TWorkflow);
    }

    public IWorkflowHandle<TWorkflow> Resolve<TWorkflow>(string entityId) where TWorkflow : class
    {
        if (!_factories.TryGetValue(typeof(TWorkflow), out var factory) || !_targets.TryGetValue(typeof(TWorkflow), out var targets))
        {
            throw new InvalidOperationException(
                $"No workflow of type '{typeof(TWorkflow).Name}' is registered. " +
                $"Did you forget to call WithWorkflow<{typeof(TWorkflow).Name}, TState>(...) during host configuration?");
        }

        return (IWorkflowHandle<TWorkflow>)factory(targets.ShardRegion, targets.ProducerAdapter, entityId);
    }

    /// <summary>
    /// Type-erased counterpart to <see cref="Resolve{TWorkflow}"/>, for a caller holding the workflow
    /// type as a runtime value. Backs <see cref="IWorkflowClient.For(string, string)"/>.
    ///
    /// Reaches the same registration by one extra string→<see cref="Type"/> lookup, so a name no
    /// <c>WithWorkflow</c> call registered fails here exactly as an unregistered
    /// <c>TWorkflow</c> fails in <see cref="Resolve{TWorkflow}"/>.
    /// </summary>
    public IWorkflowHandle Resolve(string workflowType, string entityId)
    {
        if (!_typesByName.TryGetValue(workflowType, out var type)
            || !_factories.TryGetValue(type, out var factory)
            || !_targets.TryGetValue(type, out var targets))
        {
            throw new InvalidOperationException(
                $"No workflow is registered under the type name '{workflowType}'. " +
                "Did you forget to call WithWorkflow<TWorkflow, TState>(...) during host configuration?");
        }

        return (IWorkflowHandle)factory(targets.ShardRegion, targets.ProducerAdapter, entityId);
    }

    /// <summary>
    /// Resolves the shard region and producer adapter behind the persisted <c>WorkflowType</c> string
    /// a <see cref="Protocol.ChildWorkflowRelationship"/> carries, since a child actor only ever has
    /// that string, never a compile-time <c>TWorkflow</c> for its parent (or vice versa). Serves the
    /// runtime's own child-lifecycle plumbing, where the refs are what is wanted and a handle would
    /// be a layer to unwrap. Reuses <see cref="_targets"/> as the single source of truth for the
    /// actual `(ShardRegion, ProducerAdapter)` pair.
    /// </summary>
    internal bool TryResolveByTypeName(string workflowTypeName, out (IActorRef ShardRegion, IActorRef ProducerAdapter) targets)
    {
        if (_typesByName.TryGetValue(workflowTypeName, out var type) && _targets.TryGetValue(type, out targets))
        {
            return true;
        }

        targets = default;
        return false;
    }
}

/// <summary>
/// Standard Akka.NET <see cref="ExtensionIdProvider{T}"/> accessor for the one
/// <see cref="WorkflowHandleRegistry"/> per <see cref="ActorSystem"/> — lazily creates it on first
/// <see cref="Apply"/>, exactly like e.g. <c>Akka.Cluster.Cluster.Get(system)</c>.
/// </summary>
internal sealed class WorkflowHandleRegistryProvider : ExtensionIdProvider<WorkflowHandleRegistry>
{
    public static readonly WorkflowHandleRegistryProvider Instance = new();

    public override WorkflowHandleRegistry CreateExtension(ExtendedActorSystem system) => new();
}

public static class WorkflowClientRegistrationExtensions
{
    /// <summary>
    /// Registers <see cref="IWorkflowClient"/> for DI resolution. Call it on the
    /// <see cref="IServiceCollection"/> returned by <c>AddAkka(...)</c> — i.e.
    /// <c>services.AddAkka(name, builder => builder.WithWorkflow&lt;...&gt;(...)).AddWorkflowClient()</c>.
    /// Akka.Hosting 1.5.70 doesn't invoke the <c>AkkaConfigurationBuilder</c> callback synchronously:
    /// <c>AddAkka</c> registers a lazy <c>ActorSystem</c> factory and only runs the callback when
    /// something first resolves <c>ActorSystem</c> from DI (in practice, during
    /// <c>host.StartAsync()</c>) — which, with <c>Microsoft.Extensions.Hosting.HostApplicationBuilder</c>,
    /// is always after <c>hostBuilder.Build()</c> has already called <c>IServiceCollection.MakeReadOnly()</c>.
    /// A call to <c>IServiceCollection.AddSingleton</c> made from inside that callback throws at that
    /// point, and bypassing the read-only guard via reflection would not help either, since the
    /// already-built <see cref="IServiceProvider"/> snapshots its descriptors at construction, before
    /// the callback ever runs. Calling <c>AddWorkflowClient</c> after <c>AddAkka</c> avoids the
    /// problem entirely, since <c>AddAkka</c> itself returns synchronously, well before <c>Build()</c>.
    /// <see cref="IWorkflowClient"/>'s actual construction is still fully lazy (via the factory
    /// below) — by the time anything resolves it, <c>WithWorkflow</c>'s <c>AddStartup</c> callback
    /// (see <c>WorkflowClusterShardingExtensions</c>) has already populated the per-
    /// <see cref="ActorSystem"/> <see cref="WorkflowHandleRegistry"/> this reads from.
    /// </summary>
    public static IServiceCollection AddWorkflowClient(this IServiceCollection services)
    {
        services.AddSingleton<IWorkflowClient>(sp =>
        {
            var system = sp.GetRequiredService<ActorSystem>();
            var registry = WorkflowHandleRegistryProvider.Instance.Apply(system);
            return new WorkflowClient(registry);
        });

        return services;
    }
}
