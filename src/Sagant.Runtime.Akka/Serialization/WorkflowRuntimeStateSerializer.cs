using Akka.Actor;
using Akka.Serialization;
using Newtonsoft.Json;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Serialization;

/// <summary>
/// The HOCON binding <see cref="WorkflowRuntimeStateSerializer"/> needs to actually run: naming the
/// serializer once, then binding it to one closed <c>WorkflowRuntimeState&lt;TState&gt;</c>. A driver
/// that registers a workflow type through <c>WithWorkflow</c> gets this for free, once per
/// <typeparamref name="TState"/> it registers (see <c>WorkflowClusterShardingExtensions</c>); a
/// caller that constructs <see cref="WorkflowEntityActor{TWorkflow, TState}"/> directly — bypassing
/// that fluent registration, the way this codebase's own actor test kit does for speed and isolation
/// — has to add it itself, the same way it already supplies its own journal/snapshot-store plugin
/// HOCON.
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
          }
        }
        """;
}

/// <summary>
/// Serializes <c>WorkflowRuntimeState&lt;TState&gt;</c> snapshots — bound per workflow type in
/// <c>WorkflowClusterShardingExtensions.WithWorkflow</c>, scoped to that one closed generic type, so
/// this never touches how anything else on the <see cref="ActorSystem"/> serializes.
///
/// Newtonsoft still does the work — <see cref="JsonSerializerSettings.TypeNameHandling"/> still needs
/// <see cref="TypeNameHandling.All"/> so a workflow author's own polymorphic
/// <c>Result</c>/<c>Command</c>/<c>CurrentStepInput</c> values round-trip. What differs from the
/// ActorSystem's default json serializer is <see cref="JsonSerializerSettings.PreserveReferencesHandling"/>:
/// left off here, because a workflow's own persisted state is a value snapshot with no expected
/// reference cycles, and turning it on is what makes Newtonsoft refuse a readonly, no-default-
/// constructor collection like <c>ImmutableDictionary&lt;TKey,TValue&gt;</c> — see
/// <c>WorkflowRuntimeState.Children</c>.
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

    private readonly JsonSerializerSettings _settings = new()
    {
        TypeNameHandling = TypeNameHandling.All,
    };

    public WorkflowRuntimeStateSerializer(ExtendedActorSystem system) : base(system)
    {
    }

    public override int Identifier => SerializerIdentifier;

    public override bool IncludeManifest => true;

    public override byte[] ToBinary(object obj) =>
        System.Text.Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj, obj.GetType(), _settings));

    public override object FromBinary(byte[] bytes, Type? type) =>
        JsonConvert.DeserializeObject(System.Text.Encoding.UTF8.GetString(bytes), type!, _settings)!;
}
