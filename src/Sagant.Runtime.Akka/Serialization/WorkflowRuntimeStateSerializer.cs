using Akka.Actor;
using Akka.Serialization;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Serialization;

/// <summary>
/// The HOCON binding <see cref="WorkflowRuntimeStateSerializer"/> needs to actually run: naming the
/// serializer once, then binding it to <typeparamref name="TState"/>'s own two closed generic types —
/// <c>WorkflowRuntimeState&lt;TState&gt;</c> (the snapshot) and <c>WorkflowEvent.UserStateChanged&lt;TState&gt;</c>
/// (the per-transition event carrying a state change) — an open generic type name in HOCON only ever
/// matches itself, so binding either type needs its own line per closed
/// <typeparamref name="TState"/>. A driver that registers a workflow type through
/// <c>WithWorkflow</c> gets this for free, once per <typeparamref name="TState"/> it registers (see
/// <c>WorkflowClusterShardingExtensions</c>); a caller that constructs
/// <see cref="WorkflowEntityActor{TWorkflow, TState}"/> directly — bypassing that fluent registration,
/// the way this codebase's own actor test kit does for speed and isolation — has to add it itself,
/// the same way it already supplies its own journal/snapshot-store plugin HOCON.
/// </summary>
public static class WorkflowRuntimeStateSerializerSetup
{
    public static string HoconFor<TState>() => $$"""
        akka.actor {
          serializers {
            workflow-runtime-state = "Sagant.Runtime.Akka.Serialization.WorkflowRuntimeStateSerializer, Sagant.Runtime.Akka"
          }
          serialization-bindings {
            "{{typeof(WorkflowRuntimeState<TState>).AssemblyQualifiedName}}" = workflow-runtime-state
            "{{typeof(WorkflowEvent.UserStateChanged<TState>).AssemblyQualifiedName}}" = workflow-runtime-state
          }
        }
        """;
}

/// <summary>
/// Serializes <c>WorkflowRuntimeState&lt;TState&gt;</c> snapshots and <c>WorkflowEvent.UserStateChanged&lt;TState&gt;</c>
/// events — bound per workflow type in <c>WorkflowClusterShardingExtensions.WithWorkflow</c>, scoped
/// to those two closed generic types, so this never touches how anything else on the
/// <see cref="ActorSystem"/> serializes. Both wrap a bare <typeparamref name="TState"/> somewhere
/// inside them, so both need the same treatment: a workflow author's own state can carry the same
/// readonly-collection shape <c>WorkflowRuntimeState.Children</c> did, and the default json serializer
/// would refuse it there too.
///
/// <see cref="SagantCodec"/> does the actual byte-level work — the same codec
/// <see cref="SagantSerializer"/> uses. What's different here is the manifest: a closed generic per
/// <typeparamref name="TState"/> has no fixed set of cases to name the way <c>WorkflowEvent</c>'s
/// hierarchy does, so this rides <see cref="Serializer.IncludeManifest"/>'s CLR-name manifest instead
/// of a string one.
/// </summary>
public sealed class WorkflowRuntimeStateSerializer : Serializer
{
    /// <summary>
    /// Outside Akka's reserved 0–40 range and every identifier this codebase otherwise assigns —
    /// there being only the one custom serializer so far, any value clear of that reserved band
    /// works, but a value can't be a small, easily-collided number given a deployment might load
    /// other libraries with their own custom serializers on the same <see cref="ActorSystem"/>.
    /// </summary>
    public const int SerializerIdentifier = 1_090_100;

    public WorkflowRuntimeStateSerializer(ExtendedActorSystem system) : base(system)
    {
    }

    public override int Identifier => SerializerIdentifier;

    public override bool IncludeManifest => true;

    public override byte[] ToBinary(object obj) => SagantCodec.ToBinary(obj);

    public override object FromBinary(byte[] bytes, Type? type) => SagantCodec.FromBinary(bytes, type!);
}
