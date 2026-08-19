using System.Diagnostics;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Descriptors;
using Akka.Actor;

namespace Sagant.Runtime.Akka.Clustering;

/// <summary>
/// Typed client for one workflow instance. Business commands (<see cref="Send{TCommand}"/>/
/// <see cref="Ask{TCommand, TReply}"/>/<see cref="RunAndAwaitResult"/>) route through
/// <paramref name="producerAdapter"/> — see <see cref="WorkflowProducerAdapter"/> — into
/// <c>Akka.Delivery</c> for at-least-once delivery. Control commands
/// (<see cref="Suspend"/>/<see cref="Resume"/>/<see cref="Terminate"/>/<see cref="GetStatus"/>) keep
/// going straight to <paramref name="shardRegion"/> via plain <c>Tell</c>/<c>Ask</c>, unaffected.
///
/// Every underlying Akka <c>Ask</c> here is given <see cref="Timeout.InfiniteTimeSpan"/> explicitly,
/// never left to default — a null timeout falls back to Akka's own configured
/// <c>akka.actor.ask-timeout</c>, which would fire underneath a caller who passed a
/// <see cref="CancellationToken"/> precisely because they wanted to wait longer. The token is the one
/// and only thing that ever bounds a wait on this type.
/// </summary>
internal sealed class WorkflowRef<TWorkflow, TState>
    where TWorkflow : Workflow<TState>, IWorkflowStepDispatcher<TState>, IWorkflowCommandDispatcher<TState>, IWorkflowQueryDispatcher<TState>, IWorkflowChildResultDispatcher<TState>
{
    private readonly IActorRef _shardRegion;
    private readonly IActorRef _producerAdapter;
    private readonly TimeSpan _watchRetryInterval;

    /// <summary><paramref name="watchRetryInterval"/> bounds each <see cref="RunAndAwaitResult"/>
    /// attempt — see that method's own doc comment for why it retries at all. Internal to
    /// <see cref="WorkflowRef{TWorkflow, TState}"/>, invisible past <c>IWorkflowHandle</c>; a test
    /// injects a short one so the retry itself is fast to observe, production takes the
    /// default.</summary>
    public WorkflowRef(
        IActorRef shardRegion, IActorRef producerAdapter, string entityId, TimeSpan? watchRetryInterval = null)
    {
        _shardRegion = shardRegion;
        _producerAdapter = producerAdapter;
        EntityId = entityId;
        _watchRetryInterval = watchRetryInterval ?? TimeSpan.FromSeconds(10);
    }

    public string EntityId { get; }

    public async ValueTask Send<TCommand>(
        TCommand command, string? idempotencyKey = null,
        IReadOnlyDictionary<string, string>? metadata = null) where TCommand : notnull
    {
        var envelope = new WorkflowEnvelope(
            EntityId, command, ReplyTo: null, IdempotencyKey: idempotencyKey, Metadata: metadata,
            TraceParent: Activity.Current?.Id);
        await _producerAdapter.Ask<Done>(new WorkflowProducerAdapter.Enqueue(EntityId, envelope));
    }

    /// <summary>
    /// Enqueues <paramref name="command"/> through <see cref="_producerAdapter"/> exactly like
    /// <see cref="Send{TCommand}"/>, but also waits for the business reply and returns it.
    ///
    /// The <c>Done</c> that <see cref="_producerAdapter"/>'s own <c>Ask</c> resolves with (below) is
    /// only an acknowledgement that the command was accepted into the delivery buffer — never the
    /// business reply itself. The business reply is <c>Tell</c>'d directly to whatever
    /// <see cref="IActorRef"/> sits in <see cref="WorkflowEnvelope.ReplyTo"/> by
    /// <c>WorkflowEntityActor.SendReplyTo</c>, once the workflow actually handles the command — so
    /// this method needs a second, independent way to observe that reply, created *before* the
    /// enqueue even happens (so it can be embedded in the envelope the adapter receives).
    ///
    /// That second ref comes from asking <see cref="_producerAdapter"/> — a real actor with a real
    /// <c>Context</c> — to spawn a small <c>ReplyWaiterActor</c> child
    /// (<see cref="WorkflowProducerAdapter.CreateReplyWaiter"/>) that bridges the eventual, unsolicited
    /// <c>Tell</c> from the entity into this method's <paramref name="cancellationToken"/>-aware
    /// <see cref="Task{TResult}"/>. This is deliberately built from ordinary, fully
    /// public Akka API only (<c>Context.ActorOf</c>, plain <c>ReceiveActor</c>, <c>PoisonPill</c>),
    /// avoiding Akka's own <c>Ask&lt;T&gt;()</c>, which is backed by <c>[InternalApi]</c>-marked
    /// promise-actor-ref plumbing not meant to be depended on directly from outside Akka.dll. The
    /// waiter is a genuine, addressable child actor, so it needs no manual temp-actor-path
    /// registration dance to be resolvable from another cluster node — its <see cref="ActorPath"/> is
    /// a normal child path under <see cref="_producerAdapter"/>, valid the instant it's spawned, the
    /// same as any other actor.
    /// The waiter is always stopped in <c>finally</c> below (via <c>PoisonPill</c>) once this method
    /// is done with it — a no-op if it already self-stopped after delivering the real reply.
    /// </summary>
    public async Task<TReply> Ask<TCommand, TReply>(
        TCommand command, string? idempotencyKey = null,
        CancellationToken cancellationToken = default, IReadOnlyDictionary<string, string>? metadata = null)
        where TCommand : notnull
    {
        // tcs carries the raw, un-cast object? the waiter relays -- the (TReply) cast happens below,
        // AFTER awaiting tcs.Task, on this calling thread/continuation, never inside
        // ReplyWaiterActor's own message handler, where a cast failure (mismatched TReply, or a
        // handler that starts returning a different type) would throw on the actor's thread. Akka's
        // default supervisor strategy RESTARTS a child actor that throws out of its receive handler,
        // so Context.Stop(Self) would never run, tcs would never complete, and the caller would just
        // hang until cancellationToken fires -- no hint the real cause was an invalid cast. Keeping
        // the cast out here, on the awaiting side, produces the same immediate InvalidCastException
        // behavior as plain IActorRef.Ask<TReply>(), whose cast likewise happens outside any actor,
        // on the continuation observing the promise.
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiterRef = await _producerAdapter.Ask<IActorRef>(
            new WorkflowProducerAdapter.CreateReplyWaiter(
                OnReply: value => tcs.TrySetResult(value),
                OnFailure: ex => tcs.TrySetException(ex)),
            Timeout.InfiniteTimeSpan, cancellationToken);

        try
        {
            var envelope = new WorkflowEnvelope(
                EntityId, command, waiterRef, idempotencyKey, Metadata: metadata, TraceParent: Activity.Current?.Id);
            await _producerAdapter.Ask<Done>(
                new WorkflowProducerAdapter.Enqueue(EntityId, envelope), Timeout.InfiniteTimeSpan, cancellationToken);

            var raw = await tcs.Task.WaitAsync(cancellationToken);
            return (TReply)raw!;
        }
        finally
        {
            // If control reaches here due to a cancellation/enqueue failure, with no real reply
            // received, the waiter is still alive, sitting on a reply that will never come (or one that
            // already arrived after we gave up) — stop it explicitly here so it doesn't leak for the
            // life of the ActorSystem. Harmless no-op if the waiter already self-stopped after
            // delivering a real reply.
            waiterRef.Tell(PoisonPill.Instance);
        }
    }

    /// <summary>
    /// Sends a read-only query straight to <see cref="_shardRegion"/>, the same lane
    /// <see cref="Suspend"/>/<see cref="Resume"/>/<see cref="GetStatus"/> already use — deliberately
    /// bypassing <see cref="_producerAdapter"/>. Akka.Delivery exists to guarantee a command lands
    /// exactly once; a query writes nothing, so guaranteeing its delivery buys nothing while costing
    /// a producer buffer slot, a sequence number and a confirmation round trip on a path a live
    /// dashboard may poll hard.
    ///
    /// <c>ShardingConsumerController</c>'s <c>AllowBypass</c> is what lets a message that isn't part
    /// of the delivery protocol reach the entity with <see cref="IActorRef"/> preserved — the same
    /// mechanism the control commands rely on (see <c>WorkflowClusterShardingExtensions</c>).
    /// </summary>
    public Task<TReply> Query<TQuery, TReply>(TQuery query, CancellationToken cancellationToken = default)
        where TQuery : notnull =>
        _shardRegion.Ask<TReply>(new WorkflowEnvelope(EntityId, query), Timeout.InfiniteTimeSpan, cancellationToken);

    public Task<Done> Suspend(string? reason = null, CancellationToken cancellationToken = default) =>
        _shardRegion.Ask<Done>(new WorkflowEnvelope(EntityId, new Suspend(reason)), Timeout.InfiniteTimeSpan, cancellationToken);

    public Task<Done> Resume(CancellationToken cancellationToken = default) =>
        _shardRegion.Ask<Done>(new WorkflowEnvelope(EntityId, new Resume()), Timeout.InfiniteTimeSpan, cancellationToken);

    public Task<Done> Terminate(string? reason = null, CancellationToken cancellationToken = default) =>
        _shardRegion.Ask<Done>(new WorkflowEnvelope(EntityId, new Terminate(reason)), Timeout.InfiniteTimeSpan, cancellationToken);

    public Task<Done> Cancel(string? reason = null, CancellationToken cancellationToken = default) =>
        _shardRegion.Ask<Done>(new WorkflowEnvelope(EntityId, new Cancel(reason)), Timeout.InfiniteTimeSpan, cancellationToken);

    public Task<Done> Delete(string? reason = null, CancellationToken cancellationToken = default) =>
        _shardRegion.Ask<Done>(new WorkflowEnvelope(EntityId, new Delete(reason)), Timeout.InfiniteTimeSpan, cancellationToken);

    /// <summary>
    /// Asks for <see cref="WorkflowStatusReply"/> and hands back the status inside it. The wrapper is
    /// what survives the trip: an entity on another node answers over the wire, where a bare enum
    /// travels as its number and arrives as one.
    /// </summary>
    public async Task<WorkflowStatus> GetStatus(CancellationToken cancellationToken = default)
    {
        var reply = await _shardRegion.Ask<WorkflowStatusReply>(
            new WorkflowEnvelope(EntityId, new GetStatus()), Timeout.InfiniteTimeSpan, cancellationToken);

        return reply.Status;
    }

    public Task<Done> Wake(WorkflowTimerKind kind, CancellationToken cancellationToken = default) =>
        _shardRegion.Ask<Done>(new WorkflowEnvelope(EntityId, new Wake(kind)), Timeout.InfiniteTimeSpan, cancellationToken);

    /// <summary>
    /// Sends <paramref name="command"/>, then waits for the workflow to reach a terminal status
    /// and returns its final state, going beyond a mere acknowledgement that the command was accepted.
    /// Takes `object` for the command parameter: this method is already generic over `TState`, and
    /// there's no single natural `TCommand` to add for a rarely-used method without an awkward third
    /// type parameter (Send/Ask above take a generic `TCommand` because they don't carry this
    /// constraint).
    ///
    /// The initial send goes through <see cref="_producerAdapter"/> (same at-least-once delivery as
    /// <see cref="Send{TCommand}"/>/<see cref="Ask{TCommand, TReply}"/>). Waiting for the enqueue's own
    /// <c>Done</c> ack before moving on to <see cref="WatchForCompletion{TState}"/> means a caller only
    /// starts watching once the command is durably queued for delivery — a stronger guarantee than a
    /// plain fire-and-forget <c>Tell</c> gives, which merely dispatches into the ether.
    ///
    /// <see cref="WatchForCompletion{TState}"/>'s registration on the entity side lives only in that
    /// incarnation's own memory (<c>_pendingCompletionWatchers</c>) — a relocation or crash while a
    /// caller is mid-wait drops it. Bounding each attempt to <see cref="_watchRetryInterval"/> and
    /// re-asking on a timeout is what recovers from that: a re-ask is always safe regardless of why the
    /// previous one went unanswered, since the entity's own reply comes straight off its durably
    /// persisted status either way — still running answers nothing new (the retry just re-registers),
    /// already finished answers immediately off that persisted state, whichever incarnation now holds
    /// it. Only the read-only completion check repeats here, on this loop; the command itself is sent
    /// exactly once, in the initial enqueue above — see
    /// <see cref="WorkflowEntityActor{TWorkflow, TState}.HandleWatchForCompletion"/>.
    /// </summary>
    public async Task<WorkflowResult<TState>> RunAndAwaitResult(
        object command, string? idempotencyKey = null, CancellationToken cancellationToken = default)
    {
        var envelope = new WorkflowEnvelope(
            EntityId, command, ReplyTo: null, IdempotencyKey: idempotencyKey, TraceParent: Activity.Current?.Id);
        await _producerAdapter.Ask<Done>(
            new WorkflowProducerAdapter.Enqueue(EntityId, envelope), Timeout.InfiniteTimeSpan, cancellationToken);

        while (true)
        {
            try
            {
                return await _shardRegion.Ask<WorkflowResult<TState>>(
                    new WorkflowEnvelope(EntityId, new WatchForCompletion<TState>()), _watchRetryInterval, cancellationToken);
            }
            catch (AskTimeoutException)
            {
                // No answer within this attempt's window — ask again. Covers both a run that's
                // genuinely still going and a registration a relocation dropped; see the doc comment
                // above for why the same retry is the right move either way.
            }
        }
    }
}
