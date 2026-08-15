using System.Collections.Concurrent;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Configuration;
using Sagant.Idempotency;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Settings;

namespace Sagant.Runtime.Akka.Execution;

/// <summary>
/// Everything a <see cref="WorkflowEntityActor{TWorkflow, TState}"/> derives that is the same for
/// every instance one registration drives: resolved settings, the tag sets its events carry, the
/// empty dedup ledgers a fresh envelope starts from, and the grace window a hand-off allows.
///
/// It exists because these are expensive to derive and identical across instances.
/// <see cref="ResolvedWorkflowSettings.From"/> builds two <see cref="System.Collections.Frozen.FrozenDictionary{TKey, TValue}"/>s —
/// a structure built to be read, at the cost of being slow to construct — and the tag sets are two
/// immutable sets built from one string. Deriving that per activation is the largest thing an
/// activation does, and under idle passivation an instance activates whenever it is next addressed.
///
/// A registration is the scope: settings and dispatch tables belong to the workflow
/// <em>instance</em> a factory produced, and one class can be constructed several times with
/// different settings, which the actor test fixtures do. One <c>WithWorkflow</c> call resolves this
/// once and every entity it starts reads it; an actor constructed directly derives its own.
/// </summary>
internal sealed class WorkflowTypeProfile<TState>
{
    /// <summary>Akka's own default, which applies to a deployment whose config carries no
    /// <c>ClusterSharding</c> section.</summary>
    private static readonly TimeSpan DefaultHandoffTimeout = TimeSpan.FromSeconds(60);

    /// <summary>How far under the hand-off timeout the grace ceiling sits, so a step finishing at the
    /// last moment still has room to persist before Sharding stops waiting.</summary>
    private static readonly TimeSpan HandoffHeadroom = TimeSpan.FromSeconds(10);

    /// <summary>Floor for the ceiling, for a deployment that configured a very short hand-off.</summary>
    private static readonly TimeSpan MinimumGraceCeiling = TimeSpan.FromSeconds(5);

    private WorkflowTypeProfile(
        string workflowTypeName,
        ResolvedWorkflowSettings settings,
        IImmutableSet<string> eventTags,
        IImmutableSet<string> deadlineEventTags,
        SeqNrLedger emptySeqNrLedger,
        IdempotencyLedger emptyIdempotencyLedger,
        TimeSpan graceCeiling)
    {
        WorkflowTypeName = workflowTypeName;
        Settings = settings;
        EventTags = eventTags;
        DeadlineEventTags = deadlineEventTags;
        EmptySeqNrLedger = emptySeqNrLedger;
        EmptyIdempotencyLedger = emptyIdempotencyLedger;
        GraceCeiling = graceCeiling;
    }

    public string WorkflowTypeName { get; }

    public ResolvedWorkflowSettings Settings { get; }

    /// <summary>Tags carried by every event an instance writes.</summary>
    public IImmutableSet<string> EventTags { get; }

    /// <summary><see cref="EventTags"/> plus the deadline tag, carried by the events that move a
    /// deadline.</summary>
    public IImmutableSet<string> DeadlineEventTags { get; }

    /// <summary>The ledger a fresh envelope starts from. Immutable, and <c>Record</c> returns a new
    /// one, so every instance of this registration starts from this same value.</summary>
    public SeqNrLedger EmptySeqNrLedger { get; }

    /// <inheritdoc cref="EmptySeqNrLedger"/>
    public IdempotencyLedger EmptyIdempotencyLedger { get; }

    /// <summary>
    /// The longest grace an in-flight step may be given across a hand-off. What actually prevents two
    /// live copies of an entity is <c>ClusterSharding</c>'s coordinator protocol: the new region
    /// activates the shard once the old one confirms it stopped, or once Sharding stops waiting
    /// (<c>akka.cluster.sharding.handoff-timeout</c>). This stays under that ceiling, so a grace window
    /// always expires before the coordinator proceeds regardless.
    /// </summary>
    public TimeSpan GraceCeiling { get; }

    /// <summary>
    /// Derives the profile from one workflow instance and the <c>ActorSystem</c> config the deployment
    /// is running under.
    /// </summary>
    public static WorkflowTypeProfile<TState> For(Workflow<TState> workflow, Config config)
    {
        var settings = ResolvedWorkflowSettings.From(workflow.Settings());
        var handoffTimeout = config.GetTimeSpan(
            "akka.cluster.sharding.handoff-timeout", DefaultHandoffTimeout, allowInfinite: false);
        var ceiling = handoffTimeout - HandoffHeadroom;

        return new WorkflowTypeProfile<TState>(
            workflow.WorkflowTypeName,
            settings,
            WorkflowEventTags.For(workflow.WorkflowTypeName),
            WorkflowEventTags.ForDeadlineEvent(workflow.WorkflowTypeName),
            SeqNrLedger.Empty(settings.SeqNrDedupCapacity),
            IdempotencyLedger.Empty(settings.IdempotencyLedgerCapacity),
            ceiling > MinimumGraceCeiling ? ceiling : MinimumGraceCeiling);
    }
}

/// <summary>
/// The profile each registration on this <see cref="ActorSystem"/> resolved for its own entities.
///
/// A registration is the unit a profile is keyed by: what an entity runs on comes from the workflow
/// <em>instance</em> its factory produced, and one class can be constructed several times with
/// different settings and different dispatch tables. So a profile is read by entities of the
/// registration that wrote it, and an actor built outside one derives its own — see
/// <see cref="ResolveOrDerive{TWorkflow, TState}"/>.
/// </summary>
internal sealed class WorkflowTypeProfileRegistry : IExtension
{
    // Written once per registration during host start and read on every activation after that.
    private readonly ConcurrentDictionary<Type, object> _profiles = new();

    public void Register<TWorkflow, TState>(WorkflowTypeProfile<TState> profile) =>
        _profiles[typeof(TWorkflow)] = profile;

    /// <summary>
    /// The profile <typeparamref name="TWorkflow"/>'s registration resolved, or one derived from
    /// <paramref name="workflow"/> for this instance alone where nothing registered it. A derived
    /// profile belongs to the caller that asked for it and stays out of the dictionary: with this type
    /// unclaimed, the next instance of the same class may be a differently-configured workflow.
    /// </summary>
    public WorkflowTypeProfile<TState> ResolveOrDerive<TWorkflow, TState>(Workflow<TState> workflow, Config config) =>
        _profiles.TryGetValue(typeof(TWorkflow), out var profile)
            ? (WorkflowTypeProfile<TState>)profile
            : WorkflowTypeProfile<TState>.For(workflow, config);
}

/// <summary>
/// Standard Akka.NET <see cref="ExtensionIdProvider{T}"/> accessor for the one
/// <see cref="WorkflowTypeProfileRegistry"/> per <see cref="ActorSystem"/>.
/// </summary>
internal sealed class WorkflowTypeProfileRegistryProvider : ExtensionIdProvider<WorkflowTypeProfileRegistry>
{
    public static readonly WorkflowTypeProfileRegistryProvider Instance = new();

    public override WorkflowTypeProfileRegistry CreateExtension(ExtendedActorSystem system) => new();
}
