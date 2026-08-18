using System.Runtime.Serialization;
using Akka.Actor;
using Akka.Serialization;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Serialization;

/// <summary>
/// Stable-manifest serializer for the engine's own closed types. A CLR type name is the wrong
/// manifest for a persisted event: renaming a type or moving it between assemblies breaks recovery
/// silently, since the old name is what every already-written event carries.
///
/// <see cref="Manifest"/> writes a short string per case instead — <see cref="Registrations"/> is the
/// one place naming every case this serializer covers, so a missing entry fails loudly: an unknown
/// manifest on read, or an unregistered type on write, each a named exception.
///
/// <see cref="SagantCodec"/> does the actual byte-level work (MessagePack): the manifest identifies
/// the outer object, so nothing needs its own type tag there, but a workflow author's own polymorphic
/// values nested inside one of these types — <c>StepStarted.Input</c>, <c>ChildMemberUpdated.Result</c>,
/// and the rest — still carry one, since those stay the consumer's own types, outside this closed set.
///
/// Scoped to <c>WorkflowEvent</c>'s hierarchy, <c>ChildWorkflowRelationship</c>, <c>ChildGroupState</c>,
/// and the protocol messages. <c>Sagant.Runtime.Akka</c>'s own internal transport messages
/// (<c>ChildLifecycleNotification</c>, <c>WorkflowEnvelope</c>, and the rest) stay outside it — one
/// driver's implementation detail, sent live over the wire and never journal-persisted the way a
/// <c>WorkflowEvent</c> is. <c>WorkflowEvent.UserStateChanged&lt;TState&gt;</c> is generic —
/// <see cref="WorkflowRuntimeStateSerializer"/> covers what a closed generic needs, bound
/// per registered workflow type, the same way <c>WorkflowRuntimeState&lt;TState&gt;</c> itself is.
/// </summary>
public sealed class SagantSerializer : SerializerWithStringManifest
{
    public const int SerializerIdentifier = 1_090_101;

    private static readonly (string Manifest, Type Type)[] Registrations =
    [
        // WorkflowEvent — every concrete, non-generic case.
        ("workflow-event.workflow-deadline-set", typeof(WorkflowEvent.WorkflowDeadlineSet)),
        ("workflow-event.run-stayed", typeof(WorkflowEvent.RunStayed)),
        ("workflow-event.step-started", typeof(WorkflowEvent.StepStarted)),
        ("workflow-event.step-retry-scheduled", typeof(WorkflowEvent.StepRetryScheduled)),
        ("workflow-event.run-paused", typeof(WorkflowEvent.RunPaused)),
        ("workflow-event.run-finished", typeof(WorkflowEvent.RunFinished)),
        ("workflow-event.run-deleted", typeof(WorkflowEvent.RunDeleted)),
        ("workflow-event.run-restarted", typeof(WorkflowEvent.RunRestarted)),
        ("workflow-event.run-suspended", typeof(WorkflowEvent.RunSuspended)),
        ("workflow-event.run-parked", typeof(WorkflowEvent.RunParked)),
        ("workflow-event.run-resumed", typeof(WorkflowEvent.RunResumed)),
        ("workflow-event.children-awaited", typeof(WorkflowEvent.ChildrenAwaited)),
        ("workflow-event.child-member-updated", typeof(WorkflowEvent.ChildMemberUpdated)),
        ("workflow-event.child-group-finalized", typeof(WorkflowEvent.ChildGroupFinalized)),
        ("workflow-event.parent-close-policy-applied", typeof(WorkflowEvent.ParentClosePolicyApplied)),
        ("workflow-event.parent-relationship-set", typeof(WorkflowEvent.ParentRelationshipSet)),
        ("workflow-event.seq-nr-recorded", typeof(WorkflowEvent.SeqNrRecorded)),
        ("workflow-event.idempotency-recorded", typeof(WorkflowEvent.IdempotencyRecorded)),

        // WorkflowOutcome — every case.
        ("workflow-outcome.completed", typeof(WorkflowOutcome.Completed)),
        ("workflow-outcome.failed", typeof(WorkflowOutcome.Failed)),
        ("workflow-outcome.timed-out", typeof(WorkflowOutcome.TimedOut)),
        ("workflow-outcome.cancelled", typeof(WorkflowOutcome.Cancelled)),
        ("workflow-outcome.terminated", typeof(WorkflowOutcome.Terminated)),

        // TransitionCause — every case.
        ("transition-cause.command", typeof(TransitionCause.Command)),
        ("transition-cause.step-succeeded", typeof(TransitionCause.StepSucceeded)),
        ("transition-cause.step-failed", typeof(TransitionCause.StepFailed)),
        ("transition-cause.control", typeof(TransitionCause.Control)),

        // Reply — every case.
        ("reply.reply-value", typeof(Reply.ReplyValue)),
        ("reply.error-value", typeof(Reply.ErrorValue)),
        ("reply.no-reply", typeof(Reply.NoReply)),

        // Standalone protocol types.
        ("child-group-state", typeof(ChildGroupState)),
        ("child-workflow-relationship", typeof(ChildWorkflowRelationship)),
        ("workflow-failure", typeof(WorkflowFailure)),

        // Protocol messages.
        ("suspend", typeof(Suspend)),
        ("resume", typeof(Resume)),
        ("terminate", typeof(Terminate)),
        ("cancel", typeof(Cancel)),
        ("delete", typeof(Delete)),
        ("get-state", typeof(GetState)),
        ("done", typeof(Done)),
        ("wake", typeof(Wake)),
        ("get-status", typeof(GetStatus)),
        ("workflow-status-reply", typeof(WorkflowStatusReply)),
        ("workflow-cancellation", typeof(WorkflowCancellation)),
    ];

    private static readonly IReadOnlyDictionary<string, Type> ManifestToType =
        Registrations.ToDictionary(r => r.Manifest, r => r.Type);

    private static readonly IReadOnlyDictionary<Type, string> TypeToManifest =
        Registrations.ToDictionary(r => r.Type, r => r.Manifest);

    /// <summary>
    /// What <see cref="SagantSerializerSetup"/> binds in <c>serialization-bindings</c> — narrower than
    /// <see cref="Registrations"/>'s full concrete-case list, since Akka resolves a binding by walking
    /// a type's own base types and interfaces for the most specific match: binding the closed
    /// hierarchies' abstract roots (<c>WorkflowEvent</c>, <c>WorkflowOutcome</c>, <c>TransitionCause</c>,
    /// <c>Reply</c>) once each covers every concrete case under them, the same way one line already
    /// covers many. The remaining types share no common base, so each gets its own line.
    /// </summary>
    internal static readonly Type[] BindingRoots =
    [
        typeof(WorkflowEvent), typeof(WorkflowOutcome), typeof(TransitionCause), typeof(Reply),
        typeof(ChildGroupState), typeof(ChildWorkflowRelationship), typeof(WorkflowFailure),
        typeof(Suspend), typeof(Resume), typeof(Terminate), typeof(Cancel), typeof(Delete),
        typeof(GetState), typeof(Done), typeof(Wake), typeof(GetStatus), typeof(WorkflowStatusReply),
        typeof(WorkflowCancellation),
    ];

    public SagantSerializer(ExtendedActorSystem system) : base(system)
    {
    }

    public override int Identifier => SerializerIdentifier;

    public override string Manifest(object o) =>
        TypeToManifest.TryGetValue(o.GetType(), out var manifest)
            ? manifest
            : throw new SerializationException(
                $"SagantSerializer has no manifest registered for {o.GetType()}. Add it to {nameof(SagantSerializer)}.{nameof(Registrations)}.");

    public override byte[] ToBinary(object obj) => SagantCodec.ToBinary(obj);

    public override object FromBinary(byte[] bytes, string manifest) =>
        ManifestToType.TryGetValue(manifest, out var type)
            ? SagantCodec.FromBinary(bytes, type)
            : throw new SerializationException(
                $"SagantSerializer has no type registered for manifest '{manifest}'. " +
                $"Was this written by a newer version that added a case this one doesn't know?");
}

/// <summary>
/// The HOCON binding <see cref="SagantSerializer"/> needs to actually run — naming it once, then
/// binding <see cref="SagantSerializer.BindingRoots"/> to it. Covers a fixed, non-generic set of
/// types, the same HOCON regardless of which <c>TState</c> a caller is registering — a single
/// addition per <see cref="global::Akka.Actor.ActorSystem"/>, where
/// <see cref="WorkflowRuntimeStateSerializerSetup"/> needs one per <c>TState</c>.
/// </summary>
public static class SagantSerializerSetup
{
    public static readonly string Hocon = BuildHocon();

    private static string BuildHocon()
    {
        var bindings = string.Join(
            "\n    ",
            SagantSerializer.BindingRoots.Select(t => $"\"{t.AssemblyQualifiedName}\" = sagant"));

        return $$"""
            akka.actor {
              serializers {
                sagant = "Sagant.Runtime.Akka.Serialization.SagantSerializer, Sagant.Runtime.Akka"
              }
              serialization-bindings {
                {{bindings}}
              }
            }
            """;
    }
}
