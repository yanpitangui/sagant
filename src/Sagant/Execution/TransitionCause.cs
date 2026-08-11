namespace Sagant.Execution;

/// <summary>
/// What drove a transition, handed to <see cref="WorkflowTransitionPlanner.Plan{TState}"/> by the
/// driver that ran the handler. The planner turns it into the events that open the batch, so a
/// consumer reading the event stream can say what caused each change.
///
/// The cause belongs to the whole batch: every event a transition writes shares one command, one
/// step outcome, one operator action. Carrying it once keeps a fact that describes the batch out of
/// each row inside it.
///
/// Closed hierarchy (all cases nested sealed records) so the planner matches exhaustively and the
/// compiler reports a new case.
/// </summary>
public abstract record TransitionCause
{
    private TransitionCause()
    {
    }

    /// <summary>
    /// Caller-supplied context, opaque to the engine and carried through to consumers untouched —
    /// an acting user, a correlation id, a source system.
    ///
    /// It is a schemaless dictionary inside a serialized event, so it is unindexed, unversioned and
    /// invisible to a query over columns. Its job is to <em>carry</em> context to a projection, which
    /// lifts whatever it cares about into real columns of its own.
    ///
    /// It also lives as long as the instance's recorded events do, surviving everything short of a
    /// full purge, so anything erasable on request belongs elsewhere.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>An external command reached its handler and produced this transition.</summary>
    public sealed record Command(string CommandType) : TransitionCause;

    /// <summary>A step returned successfully and its effect produced this transition.
    /// <paramref name="Duration"/> is the figure measured inside <c>StepDescriptor.Invoke</c>, the
    /// same one <c>sagant.step.duration</c> records.</summary>
    public sealed record StepSucceeded(string StepName, int Attempt, TimeSpan Duration) : TransitionCause;

    /// <summary>
    /// A step attempt failed. Raised for every failed attempt, whichever route follows it — a retry,
    /// a failover, or the end of the run.
    ///
    /// <paramref name="Error"/> is the only place a retried attempt's error survives, since
    /// <see cref="Protocol.WorkflowOutcome.Failed"/> carries the terminal failure alone.
    /// <paramref name="WillRetry"/> is the decision the planner has already reached, carried here
    /// because a consumer reads one event at a time with no view of the batch around it.
    /// </summary>
    public sealed record StepFailed(
        string StepName,
        int Attempt,
        string Error,
        TimeSpan Duration,
        bool WillRetry) : TransitionCause;

    /// <summary>An operator action produced this transition — a suspend, resume, terminate, or a
    /// deadline firing. <paramref name="Kind"/> names which.</summary>
    public sealed record Control(string Kind) : TransitionCause;
}
