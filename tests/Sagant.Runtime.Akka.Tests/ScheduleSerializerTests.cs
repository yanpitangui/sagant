using Sagant.Scheduling;
using Sagant.Scheduling.Serialization;
using Akka.Actor;
using Akka.Configuration;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Proves every schedule protocol message <see cref="ScheduleSerializer"/> claims to cover genuinely
/// resolves to it, round-trips through a real <see cref="ActorSystem"/>, and gets a manifest stable
/// enough to survive a rename — the same guarantees <c>SagantSerializerTests</c> proves for the
/// engine's own protocol messages.
/// </summary>
public class ScheduleSerializerTests
{
    private static ActorSystem NewSystem() =>
        ActorSystem.Create("schedule-serializer-test", ConfigurationFactory.ParseString(ScheduleSerializerSetup.Hocon));

    [Theory]
    [MemberData(nameof(EveryCoveredInstance))]
    public void EveryCoveredType_ResolvesToScheduleSerializer(object instance)
    {
        using var system = NewSystem();

        var serializer = system.Serialization.FindSerializerFor(instance);

        Assert.IsType<ScheduleSerializer>(serializer);
    }

    /// <summary>Serialize, then read back purely from the bytes and the manifest string — the same
    /// two things a remote node or a recovering journal has.</summary>
    [Theory]
    [MemberData(nameof(EveryCoveredInstance))]
    public void EveryCoveredType_RoundTripsFromBytesAndManifestAlone(object instance)
    {
        using var system = NewSystem();
        var serializer = (ScheduleSerializer)system.Serialization.FindSerializerFor(instance);

        var bytes = serializer.ToBinary(instance);
        var manifest = serializer.Manifest(instance);
        var roundTripped = serializer.FromBinary(bytes, manifest);

        Assert.Equal(instance.GetType(), roundTripped.GetType());
    }

    /// <summary>No two covered types share a manifest string — a collision would make
    /// <see cref="ScheduleSerializer.FromBinary(byte[], string)"/> reconstruct the wrong type.</summary>
    [Fact]
    public void NoTwoManifestsCollide()
    {
        using var system = ActorSystem.Create("schedule-manifest-check");
        var serializer = new ScheduleSerializer((ExtendedActorSystem)system);

        var manifests = EveryCoveredInstance()
            .Select(row => serializer.Manifest(row[0]!))
            .ToList();

        Assert.Equal(manifests.Count, manifests.Distinct().Count());
    }

    /// <summary>An unregistered manifest string on read is a named, immediate failure — never a
    /// silent wrong-type reconstruction.</summary>
    [Fact]
    public void AnUnknownManifest_FailsLoudlyOnRead()
    {
        using var system = ActorSystem.Create("schedule-unknown-manifest");
        var serializer = new ScheduleSerializer((ExtendedActorSystem)system);

        Assert.Throws<System.Runtime.Serialization.SerializationException>(
            () => serializer.FromBinary([], "a-case-nobody-registered"));
    }

    /// <summary>A polymorphic value nested inside a covered type — <see cref="StartSchedule.Spec"/>
    /// and <see cref="StartSchedule.TargetCommand"/> — carries its own concrete type on the wire and
    /// comes back as that same concrete type, with no binding of its own needed.</summary>
    [Fact]
    public void StartSchedule_RoundTripsItsNestedPolymorphicFields()
    {
        using var system = NewSystem();
        var serializer = (ScheduleSerializer)system.Serialization.FindSerializerFor(new StartSchedule(
            new EverySpec(TimeSpan.FromMinutes(1)), "SomeWorkflow", new DemoCommand("payload")));
        var original = new StartSchedule(
            new EverySpec(TimeSpan.FromMinutes(1)), "SomeWorkflow", new DemoCommand("payload"));

        var bytes = serializer.ToBinary(original);
        var manifest = serializer.Manifest(original);
        var roundTripped = (StartSchedule)serializer.FromBinary(bytes, manifest);

        Assert.Equal(original.Spec, roundTripped.Spec);
        Assert.Equal(original.TargetWorkflowType, roundTripped.TargetWorkflowType);
        Assert.Equal(original.TargetCommand, roundTripped.TargetCommand);
    }

    private sealed record DemoCommand(string Payload);

    public static IEnumerable<object?[]> EveryCoveredInstance()
    {
        yield return [new StartSchedule(new EverySpec(TimeSpan.FromMinutes(1)), "SomeWorkflow", new DemoCommand("payload"))];
        yield return [new PauseSchedule()];
        yield return [new ResumeSchedule()];
        yield return [new TriggerSchedule()];
        yield return [new CancelSchedule()];
        yield return [new GetScheduleStatus()];
        yield return [new ScheduleStatus(false, DateTimeOffset.UtcNow, 1, "entity-1", 0)];
    }
}
