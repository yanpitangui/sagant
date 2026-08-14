using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Cluster.Sharding.Delivery;
using Sagant.Protocol;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Sits between <see cref="WorkflowRef{TWorkflow, TState}"/> (called from arbitrary caller
/// threads/<c>Task</c>s) and one <c>ShardingProducerController&lt;WorkflowEnvelope&gt;</c> per
/// workflow type per <see cref="ActorSystem"/>. <c>Akka.Delivery</c> is pull-based — the controller
/// only accepts a send when it has handed out a <see cref="ShardingProducerController.RequestNext{T}"/>
/// — so this actor holds a small bounded FIFO of pending sends and forwards the head as demand
/// arrives, exposing a plain <c>Ask</c>-based API back out. Backpressure is purely local: acceptance
/// into this queue (not consumer confirmation) is what completes/faults the caller's <c>Task</c> —
/// see the design doc's Error handling section on why (a new, deliberate caller-visible failure mode
/// that doesn't exist for a plain <c>Tell</c>).
/// </summary>
public sealed class WorkflowProducerAdapter : ReceiveActor
{
    public sealed record RegisterProducerController(IActorRef ProducerController);

    public sealed record Enqueue(string EntityId, WorkflowEnvelope Envelope);

    /// <summary>
    /// Spawns a fresh <see cref="ReplyWaiterActor"/> child and replies with its <see cref="IActorRef"/>.
    /// Sent by <see cref="WorkflowRef{TWorkflow, TState}.Ask{TCommand, TReply}"/> before it enqueues a
    /// business command whose reply it needs to observe — the returned ref is what gets embedded as
    /// <see cref="WorkflowEnvelope.ReplyTo"/> on that command.
    /// </summary>
    public sealed record CreateReplyWaiter(Action<object?> OnReply, Action<Exception> OnFailure);

    public sealed class ProducerBufferFullException(int capacity)
        : Exception($"Workflow producer buffer is full (capacity {capacity}) — the entity/consumer side may be down or falling behind.");

    // Uses the fully qualified `global::Akka.Actor.Props.Create` because the static method below
    // is itself named `Props`, so an unqualified `Props` inside this class binds to that method
    // group. `Akka.Actor.Props.Create` written without `global::` also hits this project's
    // namespace shadow, where `Akka` as the first identifier of a dotted expression resolves to
    // the sibling `Sagant.Runtime.Akka` namespace; `global::` reaches the top-level Akka.NET
    // namespace directly (see AkkaSchedulerTimeProvider.cs).
    public static Props Props(int bufferCapacity) =>
        global::Akka.Actor.Props.Create(() => new WorkflowProducerAdapter(bufferCapacity));

    private readonly int _bufferCapacity;
    private readonly Queue<(string EntityId, WorkflowEnvelope Envelope, IActorRef? ReplyTo)> _pending = new();
    private IActorRef? _sendNextTo;

    public WorkflowProducerAdapter(int bufferCapacity)
    {
        _bufferCapacity = bufferCapacity;

        Receive<RegisterProducerController>(msg => msg.ProducerController.Tell(new ShardingProducerController.Start<WorkflowEnvelope>(Self)));

        Receive<ShardingProducerController.RequestNext<WorkflowEnvelope>>(next =>
        {
            _sendNextTo = next.SendNextTo;
            DrainIfPossible();
        });

        Receive<Enqueue>(msg =>
        {
            // Enqueued with no sender by a caller that wants the command delivered and has nothing to
            // do with the acknowledgement — a child start, a parent notification. Akka.Delivery is what
            // carries those to their entity, so the reply here would be a message with no reader.
            var replyTo = Sender.Equals(Context.System.DeadLetters) ? null : Sender;

            if (_sendNextTo is { } sendNextTo)
            {
                sendNextTo.Tell(new ShardingEnvelope(msg.EntityId, msg.Envelope));
                _sendNextTo = null;
                replyTo?.Tell(Done.Instance);
                return;
            }

            if (_pending.Count >= _bufferCapacity)
            {
                replyTo?.Tell(new Status.Failure(new ProducerBufferFullException(_bufferCapacity)));
                return;
            }

            _pending.Enqueue((msg.EntityId, msg.Envelope, replyTo));
        });

        Receive<CreateReplyWaiter>(msg =>
        {
            var waiter = Context.ActorOf(global::Akka.Actor.Props.Create(() => new ReplyWaiterActor(msg.OnReply, msg.OnFailure)));
            Sender.Tell(waiter);
        });
    }

    private void DrainIfPossible()
    {
        if (_sendNextTo is not { } sendNextTo || _pending.Count == 0)
        {
            return;
        }

        var (entityId, envelope, replyTo) = _pending.Dequeue();
        sendNextTo.Tell(new ShardingEnvelope(entityId, envelope));
        _sendNextTo = null;
        replyTo?.Tell(Done.Instance);
    }

    /// <summary>
    /// Bridges an eventual, unsolicited <c>Tell</c> from some other actor (the workflow entity's business
    /// reply, sent to whatever <c>IActorRef</c> ends up in <c>WorkflowEnvelope.ReplyTo</c>) into a
    /// completed .NET <c>Task</c>, for callers like <see cref="WorkflowRef{TWorkflow, TState}"/> that live
    /// outside the actor system. Built entirely from ordinary, fully public Akka API (spawned via
    /// the adapter's own <c>Context.ActorOf</c>, plain <c>ReceiveActor</c>/<c>PoisonPill</c>), avoiding
    /// a dependency on Akka's own <c>[InternalApi]</c>-marked promise-actor-ref machinery (the same
    /// building block Akka's own <c>Ask&lt;T&gt;()</c> is built on) that could shift without notice
    /// across a version bump — accepted at the cost of one extra actor per business-command reply.
    /// One-shot: completes (via whichever callback fires first) and stops itself on the first message it
    /// receives, or on an explicit <c>PoisonPill</c> if the caller gives up waiting first (timeout/cancel).
    /// </summary>
    private sealed class ReplyWaiterActor : ReceiveActor
    {
        public ReplyWaiterActor(Action<object?> onReply, Action<Exception> onFailure)
        {
            ReceiveAny(msg =>
            {
                if (msg is Status.Failure failure)
                {
                    onFailure(failure.Cause);
                }
                else
                {
                    onReply(msg);
                }

                Context.Stop(Self);
            });
        }
    }
}
