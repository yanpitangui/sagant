using System.Runtime.Serialization;
using Akka.Actor;
using Akka.Serialization;
using Sagant.Runtime.Akka.Serialization;

namespace Sagant.Scheduling.Serialization;

/// <summary>
/// Stable-manifest serializer for the schedule protocol's own command/query/reply messages —
/// <see cref="StartSchedule"/>, <see cref="PauseSchedule"/>, <see cref="ResumeSchedule"/>,
/// <see cref="TriggerSchedule"/>, <see cref="CancelSchedule"/>, <see cref="GetScheduleStatus"/>, and
/// <see cref="ScheduleStatus"/>. Each is sent live over the wire to a <c>ScheduleWorkflow</c> entity
/// the same way any workflow's own commands are, so without a binding here they fall to whatever
/// default serializer the hosting <c>ActorSystem</c> configures — this gives them the same short,
/// rename-safe manifest and MessagePack encoding <see cref="SagantSerializer"/> gives the engine's
/// own protocol messages.
///
/// <see cref="StartSchedule.Spec"/>/<see cref="StartSchedule.TargetCommand"/> and
/// <see cref="ScheduleState.Spec"/>/<see cref="ScheduleState.TargetCommand"/> need no entry of their
/// own here: nested inside one of these types, they are values <see cref="SagantCodec"/>'s own
/// polymorphic resolver already tags on the wire, the same way a workflow author's own step input or
/// child command does.
/// </summary>
public sealed class ScheduleSerializer : SerializerWithStringManifest
{
    /// <summary>
    /// Clear of Akka's reserved 0–40 band and every identifier this codebase otherwise assigns
    /// (<see cref="WorkflowRuntimeStateSerializer.SerializerIdentifier"/>,
    /// <see cref="SagantSerializer.SerializerIdentifier"/>).
    /// </summary>
    public const int SerializerIdentifier = 1_090_102;

    private static readonly (string Manifest, Type Type)[] Registrations =
    [
        ("start-schedule", typeof(StartSchedule)),
        ("pause-schedule", typeof(PauseSchedule)),
        ("resume-schedule", typeof(ResumeSchedule)),
        ("trigger-schedule", typeof(TriggerSchedule)),
        ("cancel-schedule", typeof(CancelSchedule)),
        ("get-schedule-status", typeof(GetScheduleStatus)),
        ("schedule-status", typeof(ScheduleStatus)),
    ];

    private static readonly IReadOnlyDictionary<string, Type> ManifestToType =
        Registrations.ToDictionary(r => r.Manifest, r => r.Type);

    private static readonly IReadOnlyDictionary<Type, string> TypeToManifest =
        Registrations.ToDictionary(r => r.Type, r => r.Manifest);

    internal static readonly Type[] BindingRoots = Registrations.Select(r => r.Type).ToArray();

    public ScheduleSerializer(ExtendedActorSystem system) : base(system)
    {
    }

    public override int Identifier => SerializerIdentifier;

    public override string Manifest(object o) =>
        TypeToManifest.TryGetValue(o.GetType(), out var manifest)
            ? manifest
            : throw new SerializationException(
                $"ScheduleSerializer has no manifest registered for {o.GetType()}. Add it to {nameof(ScheduleSerializer)}.{nameof(Registrations)}.");

    public override byte[] ToBinary(object obj) => SagantCodec.ToBinary(obj);

    public override object FromBinary(byte[] bytes, string manifest) =>
        ManifestToType.TryGetValue(manifest, out var type)
            ? SagantCodec.FromBinary(bytes, type)
            : throw new SerializationException(
                $"ScheduleSerializer has no type registered for manifest '{manifest}'. " +
                $"Was this written by a newer version that added a case this one doesn't know?");
}

/// <summary>
/// The HOCON binding <see cref="ScheduleSerializer"/> needs to actually run — naming it once, then
/// binding <see cref="ScheduleSerializer.BindingRoots"/> to it. A fixed set of types, so the same
/// HOCON every time regardless of what workflows an application registers alongside scheduling.
/// </summary>
public static class ScheduleSerializerSetup
{
    public static readonly string Hocon = BuildHocon();

    private static string BuildHocon()
    {
        var bindings = string.Join(
            "\n    ",
            ScheduleSerializer.BindingRoots.Select(t => $"\"{t.AssemblyQualifiedName}\" = sagant-schedule"));

        return $$"""
            akka.actor {
              serializers {
                sagant-schedule = "Sagant.Scheduling.Serialization.ScheduleSerializer, Sagant.Scheduling.Akka"
              }
              serialization-bindings {
                {{bindings}}
              }
            }
            """;
    }
}
