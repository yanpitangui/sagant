using Akka.Actor;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.Execution;

/// <summary>
/// The sends a parent workflow makes to its <c>AwaitChildren</c>-started children: child-start,
/// terminate, delete, and a child's own report back to its parent. What those sends should be is
/// decided upstream — relationship records and close policy come from
/// <c>WorkflowTransitionPlanner</c>/<c>ChildGroupPolicy</c> in core; this is only the delivery.
/// Deciding *when* these run —
/// e.g. that a group has just finalized and the parent should resume via a
/// <c>Transition.StepTransition</c> — stays on <c>WorkflowEntityActor.ApplyChildLifecycleNotification</c>,
/// since that's tied directly to the actor's own persist/transition machinery. A failed
/// <see cref="TrySendChildStart"/> (an unregistered child workflow type — a permanent configuration
/// error that no redelivery could ever fix) is reported back to the actor for the same reason: only
/// the actor can drive the resulting <c>Transition.EndTransition</c> through
/// <c>PersistEnvelopeThen</c>.
/// </summary>
internal sealed class ChildOrchestrator<TState>(WorkflowHandleRegistry registry)
{
    /// <summary>
    /// The single call site for a child-start send, shared by the caller's original send and its own
    /// recovery redelivery. Reads <see cref="ChildWorkflowRelationship.Command"/> off the relationship
    /// itself — this relationship's own persisted state is the single source of truth for what to
    /// (re)send. Returns <c>false</c> with <paramref name="unregisteredTypeError"/> set when no
    /// workflow of the relationship's child type is registered on this <c>ActorSystem</c> at all — the
    /// caller is expected to end its own workflow with that message, since this collaborator has no
    /// persist machinery of its own to do so.
    /// </summary>
    public bool TrySendChildStart(ChildWorkflowRelationship relationship, out string? unregisteredTypeError)
    {
        if (!registry.TryResolveByTypeName(relationship.ChildWorkflowType, out var targets))
        {
            unregisteredTypeError =
                $"Cannot start child '{relationship.ChildWorkflowId}': no workflow of type '{relationship.ChildWorkflowType}' is registered.";
            return false;
        }

        var envelope = new WorkflowEnvelope(
            relationship.ChildWorkflowId, relationship.Command, ReplyTo: null,
            IdempotencyKey: relationship.RelationshipId, ParentRelationship: relationship);
        targets.ProducerAdapter.Tell(new WorkflowProducerAdapter.Enqueue(relationship.ChildWorkflowId, envelope), ActorRefs.NoSender);
        unregisteredTypeError = null;
        return true;
    }

    /// <summary>
    /// The engine's own <c>ParentClosePolicy</c>/<c>RemainingChildrenPolicy</c> cascade, sent with no
    /// live caller present to resend it on a timeout, so it must reach the child reliably on its own
    /// — rides the same producer adapter and <c>Akka.Delivery</c> pipeline
    /// <see cref="TrySendChildStart"/> uses for the opposite direction.
    /// </summary>
    /// <summary>
    /// <see cref="SendTerminate"/>'s graceful counterpart: asks the child to unwind first. Rides the same reliable pipeline for the same reason — the engine sends
    /// this itself, with no caller to retry it.
    /// </summary>
    public void SendCancel(ChildWorkflowRelationship relationship, string? reason)
    {
        if (!registry.TryResolveByTypeName(relationship.ChildWorkflowType, out var targets))
        {
            return;
        }

        var envelope = new WorkflowEnvelope(relationship.ChildWorkflowId, new Cancel(reason ?? "parent cancelled"));
        targets.ProducerAdapter.Tell(new WorkflowProducerAdapter.Enqueue(relationship.ChildWorkflowId, envelope), ActorRefs.NoSender);
    }

    public void SendTerminate(ChildWorkflowRelationship relationship)
    {
        if (!registry.TryResolveByTypeName(relationship.ChildWorkflowType, out var targets))
        {
            return;
        }

        var envelope = new WorkflowEnvelope(
            relationship.ChildWorkflowId, new Terminate("parent group finalized"));
        targets.ProducerAdapter.Tell(new WorkflowProducerAdapter.Enqueue(relationship.ChildWorkflowId, envelope), ActorRefs.NoSender);
    }

    /// <summary>
    /// <see cref="SendTerminate"/>'s counterpart for a parent that is itself being deleted (either via
    /// the business-level <c>Transition.DeleteTransition</c> or the external <see cref="Delete"/>
    /// command) — deleting a parent purges its owned subtree, so children under
    /// <c>ParentClosePolicy.Terminate</c> get <see cref="Delete"/> in turn. Same reliable
    /// producer/consumer pipeline, no live caller to resend it.
    /// </summary>
    public void SendDelete(ChildWorkflowRelationship relationship)
    {
        if (!registry.TryResolveByTypeName(relationship.ChildWorkflowType, out var targets))
        {
            return;
        }

        var envelope = new WorkflowEnvelope(
            relationship.ChildWorkflowId, new Delete("parent deleted"));
        targets.ProducerAdapter.Tell(new WorkflowProducerAdapter.Enqueue(relationship.ChildWorkflowId, envelope), ActorRefs.NoSender);
    }

    /// <summary>
    /// Reports this instance's own terminal outcome to whichever workflow is waiting on it, via the
    /// same registry+producer-adapter path <see cref="TrySendChildStart"/> uses for the opposite
    /// direction. <paramref name="traceParent"/> is the caller's currently-resolved trace parent, the
    /// source for backward-linking a group's <c>ResumeAt</c> step activity to each member's final
    /// trace.
    /// </summary>
    public void SendChildLifecycleNotification(
        ChildWorkflowRelationship relationship, WorkflowOutcome? outcome, TState result, string? traceParent)
    {
        // The child's own outcome decides how the parent sees it, which is what makes
        // CompletionPolicy.AllSuccessful mean what its name says: a child that failed reports Failed,
        // exactly that, whatever grace it stopped with.
        var childStatus = outcome switch
        {
            WorkflowOutcome.Completed => ChildStatus.Completed,
            WorkflowOutcome.Failed => ChildStatus.Failed,
            WorkflowOutcome.TimedOut => ChildStatus.Failed,
            WorkflowOutcome.Terminated => ChildStatus.Terminated,
            // Deleted without ever finishing: gone, with no result to report.
            null => ChildStatus.Cancelled,
            _ => ChildStatus.Failed,
        };

        var failure = outcome switch
        {
            WorkflowOutcome.Failed f => f.Cause,
            WorkflowOutcome.TimedOut => new WorkflowFailure("Workflow timed out", ExceptionType: typeof(TimeoutException).FullName),
            _ => null,
        };

        if (!registry.TryResolveByTypeName(relationship.ParentWorkflowType, out var targets))
        {
            // The parent's workflow type isn't registered on this node/ActorSystem — this child has
            // no further recourse. Same class of permanent, unrecoverable configuration error as
            // TrySendChildStart's own guard: no redelivery mechanism could fix a type that genuinely
            // isn't registered anywhere.
            return;
        }

        var notification = new ChildLifecycleNotification(
            relationship.RelationshipId, relationship.ChildWorkflowId, relationship.Generation, childStatus,
            childStatus == ChildStatus.Completed ? result : null, failure, traceParent);
        var envelope = new WorkflowEnvelope(relationship.ParentWorkflowId, notification);
        targets.ProducerAdapter.Tell(new WorkflowProducerAdapter.Enqueue(relationship.ParentWorkflowId, envelope), ActorRefs.NoSender);
    }

}
