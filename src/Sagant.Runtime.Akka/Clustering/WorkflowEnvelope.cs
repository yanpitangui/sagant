using Akka.Actor;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Routing wrapper <see cref="WorkflowRef{TWorkflow, TState}"/> sends everything through — carries
/// the target entity id alongside the actual command. Under <c>ClusterSharding</c>,
/// <see cref="WorkflowMessageExtractor"/> strips this off before delivery, so
/// <see cref="WorkflowEntityActor{TWorkflow, TState}"/> only ever sees the unwrapped
/// <see cref="Message"/> — the entity actor has no *routing/entity-id* awareness. It does know
/// about <c>Akka.Delivery</c>'s confirm/dedup protocol for the business-command path, though — see
/// <c>WorkflowEntityActor.HandleDelivery</c>.
///
/// Also doubles as the payload type for <c>Akka.Delivery</c> business-command traffic (see
/// <c>WorkflowProducerAdapter</c>): <see cref="ReplyTo"/> carries the original caller's ephemeral
/// promise-actor ref, since <c>Akka.Delivery</c> provides no reply-to correlation of its own — the
/// <c>Sender</c> a <see cref="WorkflowEntityActor{TWorkflow, TState}"/> sees on a
/// <c>ConsumerController.Delivery&lt;T&gt;</c> is always the internal <c>ConsumerController</c> itself.
/// <c>null</c> for fire-and-forget <c>Send</c>, and always <c>null</c> for the
/// still-plain <c>Suspend</c>/<c>Resume</c>/<c>Terminate</c>/<c>GetStatus</c> path (Akka's own
/// implicit <c>Sender</c> chaining still works there — see the design doc's Scope section).
/// <see cref="IdempotencyKey"/> is caller-supplied, opt-in, only meaningful on the Delivery path.
/// </summary>
public sealed record WorkflowEnvelope(
    string EntityId,
    object Message,
    IActorRef? ReplyTo = null,
    string? IdempotencyKey = null,
    /// <summary>
    /// Set only when this envelope is a child-start — the receiving actor persists this as its own
    /// <c>ParentRelationship</c> in the same atomic write as applying <see cref="Message"/>'s effect
    /// (see <c>WorkflowEntityActor</c>'s extended <c>PersistEnvelopeThen</c>). <c>null</c> for every
    /// ordinary external command, so a workflow started that way ends up with no parent.
    /// </summary>
    ChildWorkflowRelationship? ParentRelationship = null,

    /// <summary>
    /// Caller-supplied context travelling with this command, recorded against whatever it causes.
    /// Opaque to the engine — see <c>IWorkflowHandle.Send</c>.
    /// </summary>
    IReadOnlyDictionary<string, string>? Metadata = null,

    /// <summary>
    /// The sender's own ambient <c>Activity.Current</c> at the moment this command left
    /// <c>WorkflowRef</c> — <c>null</c> when nothing was listening. Consumed the same way
    /// <see cref="ChildWorkflowRelationship.TraceParent"/> already is: a fresh entity's very first
    /// activity links back to it (see <c>StepTracingContext.ConsumeParentLink</c>), so a workflow
    /// started by an ordinary <c>Send</c>/<c>Request</c> reads as one trace with whatever sent it,
    /// the same as a child spawned through <c>AwaitChildren</c> already does. Set only by
    /// <c>WorkflowRef.Send</c>/<c>Ask</c>/<c>RunAndAwaitResult</c> — the plain <c>Tell</c>/<c>Ask</c>
    /// control-command lane (<c>Suspend</c>/<c>Resume</c>/<c>Terminate</c>/<c>GetStatus</c>) and
    /// <c>Query</c> leave it <c>null</c>, since none of them can be a workflow's first command.
    /// </summary>
    string? TraceParent = null);
