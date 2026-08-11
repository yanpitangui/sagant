using Sagant.Execution;

namespace Sagant.Protocol;

/// <summary>
/// Where a consumer had read up to. Opaque: the runtime encodes its own position into
/// <see cref="Value"/> — an Akka <c>Offset</c>, a partition offset, an array index — and a consumer
/// stores the string verbatim and hands it back to resume from there.
/// </summary>
public readonly record struct WorkflowFeedPosition(string Value);

/// <summary>
/// One recorded fact about a workflow instance, as delivered to whatever is watching it.
///
/// Two transports carry these, and they differ only in what they promise. A runtime publishes
/// in-process as each batch is written — immediate, best-effort, unresumable — and reads the same
/// events back from durable storage for a consumer that needs every one of them. Because both carry
/// this same type, a consumer writes one <c>switch</c> and picks a transport by the guarantee it
/// needs.
/// </summary>
/// <param name="Position">Where to resume from, on a transport that supports resuming.
/// <c>null</c> on the in-process transport, which encodes in the type that it cannot be resumed.</param>
/// <param name="EntityId">The instance's routable id — what
/// <c>IWorkflowClient.For&lt;TWorkflow&gt;</c> takes.</param>
/// <param name="WorkflowType">The workflow's durable type name.</param>
/// <param name="SequenceNr">This event's position within the instance's own stream, counting from 1
/// and dense. Together with <paramref name="EntityId"/> it identifies the event, so a consumer that
/// sees one twice can recognise it.</param>
/// <param name="Timestamp">When the event was recorded.</param>
/// <param name="Event">The fact itself.</param>
public sealed record WorkflowFeedItem(
    WorkflowFeedPosition? Position,
    string EntityId,
    string WorkflowType,
    long SequenceNr,
    DateTimeOffset Timestamp,
    WorkflowEvent Event);
