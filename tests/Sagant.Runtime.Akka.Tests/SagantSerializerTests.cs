using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Serialization;
using Akka.Actor;
using Akka.Configuration;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Proves every type <see cref="SagantSerializer"/> claims to cover genuinely resolves to it, round-
/// trips through a real <see cref="ActorSystem"/>, and gets a manifest stable enough to survive a
/// rename — a short string per case, decoupled from the CLR type name recovery would otherwise
/// depend on.
/// </summary>
public class SagantSerializerTests
{
    private static ActorSystem NewSystem() =>
        ActorSystem.Create("sagant-serializer-test", ConfigurationFactory.ParseString(SagantSerializerSetup.Hocon));

    /// <summary>Every registered type resolves to <see cref="SagantSerializer"/> itself.</summary>
    [Theory]
    [MemberData(nameof(EveryCoveredInstance))]
    public void EveryCoveredType_ResolvesToSagantSerializer(object instance)
    {
        using var system = NewSystem();

        var serializer = system.Serialization.FindSerializerFor(instance);

        Assert.IsType<SagantSerializer>(serializer);
    }

    /// <summary>Serialize, then read back purely from the bytes and the manifest string — the same
    /// two things a remote node or a recovering journal has.</summary>
    [Theory]
    [MemberData(nameof(EveryCoveredInstance))]
    public void EveryCoveredType_RoundTripsFromBytesAndManifestAlone(object instance)
    {
        using var system = NewSystem();
        var serializer = (SagantSerializer)system.Serialization.FindSerializerFor(instance);

        var bytes = serializer.ToBinary(instance);
        var manifest = serializer.Manifest(instance);
        var roundTripped = serializer.FromBinary(bytes, manifest);

        Assert.Equal(instance.GetType(), roundTripped.GetType());
    }

    /// <summary>No two covered types share a manifest string — a collision would make
    /// <see cref="SagantSerializer.FromBinary(byte[], string)"/> reconstruct the wrong type.</summary>
    [Fact]
    public void NoTwoManifestsCollide()
    {
        using var system = ActorSystem.Create("manifest-check");
        var serializer = new SagantSerializer((global::Akka.Actor.ExtendedActorSystem)system);

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
        using var system = ActorSystem.Create("sagant-unknown-manifest");
        var serializer = new SagantSerializer((global::Akka.Actor.ExtendedActorSystem)system);

        Assert.Throws<System.Runtime.Serialization.SerializationException>(
            () => serializer.FromBinary([], "workflow-event.a-case-nobody-registered"));
    }

    public static IEnumerable<object?[]> EveryCoveredInstance()
    {
        var cause = new TransitionCause.Control("test");
        var failure = new WorkflowFailure("boom");
        var relationship = new ChildWorkflowRelationship(
            "parent:group-1:child-1", "Parent", "parent-1", "Child", "child-1", "group-1", 0,
            ChildStatus.Pending, null, null, null, ParentClosePolicy.Terminate, new object());
        var group = new ChildGroupState(
            "group-1", 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Continue, "OnDone", false);

        // WorkflowEvent — every concrete case.
        yield return [new WorkflowEvent.WorkflowDeadlineSet(DateTimeOffset.UtcNow)];
        yield return [new WorkflowEvent.RunStayed(cause)];
        yield return [new WorkflowEvent.StepStarted("Step", "input", DateTimeOffset.UtcNow, null, cause)];
        yield return [new WorkflowEvent.StepRetryScheduled(1, DateTimeOffset.UtcNow, null, cause)];
        yield return [new WorkflowEvent.RunPaused("reason", DateTimeOffset.UtcNow, null, null, null, cause)];
        yield return [new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, cause)];
        yield return [new WorkflowEvent.RunDeleted(null, cause)];
        yield return [new WorkflowEvent.RunRestarted("Step", null, "reason", null, null, cause)];
        yield return [new WorkflowEvent.RunSuspended(cause, DateTimeOffset.UtcNow)];
        yield return [new WorkflowEvent.RunParked(failure, null, cause, DateTimeOffset.UtcNow)];
        yield return [new WorkflowEvent.RunResumed(null, cause)];
        // .ToList() deliberately: a bare collection expression like [relationship] compiles to a
        // compiler-internal single-element list type Newtonsoft can write but cannot read back.
        // WorkflowTransitionPlanner already builds these the same way, for the same reason.
        yield return [new WorkflowEvent.ChildrenAwaited("group-1", new[] { relationship }.ToList(), group, 1, null, cause)];
        yield return [new WorkflowEvent.ChildMemberUpdated("rel-1", ChildStatus.Completed, null, null, null)];
        yield return [new WorkflowEvent.ChildGroupFinalized("group-1", new List<string>(), false)];
        yield return [new WorkflowEvent.ParentClosePolicyApplied(new[] { "rel-1" }.ToList())];
        yield return [new WorkflowEvent.ParentRelationshipSet(relationship)];
        yield return [new WorkflowEvent.SeqNrRecorded("producer-1", 1)];
        yield return [new WorkflowEvent.IdempotencyRecorded("key-1", new Reply.NoReply())];

        // WorkflowOutcome — every case.
        yield return [WorkflowOutcome.Completed.Instance];
        yield return [new WorkflowOutcome.Failed(failure)];
        yield return [WorkflowOutcome.TimedOut.Instance];
        yield return [new WorkflowOutcome.Cancelled("reason")];
        yield return [new WorkflowOutcome.Terminated("reason")];

        // TransitionCause — every case.
        yield return [new TransitionCause.Command("StartWorkflow")];
        yield return [new TransitionCause.StepSucceeded("Step", 1, TimeSpan.FromSeconds(1))];
        yield return [new TransitionCause.StepFailed("Step", 1, "error", TimeSpan.FromSeconds(1), true)];
        yield return [new TransitionCause.Control("kind")];

        // Reply — every case.
        yield return [new Reply.ReplyValue("value", null)];
        yield return [new Reply.ErrorValue("error")];
        yield return [new Reply.NoReply()];

        // Standalone protocol types.
        yield return [group];
        yield return [relationship];
        yield return [failure];

        // Protocol messages.
        yield return [new Suspend("reason")];
        yield return [new Resume()];
        yield return [new Terminate("reason")];
        yield return [new Cancel("reason")];
        yield return [new Delete("reason")];
        yield return [new GetState()];
        yield return [new Done()];
        yield return [new Wake(WorkflowTimerKind.Workflow)];
        yield return [new GetStatus()];
        yield return [new WorkflowStatusReply(WorkflowStatus.Running)];
        yield return [new WorkflowCancellation("reason")];
    }
}
