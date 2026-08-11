namespace Sagant.Protocol;

/// <summary>
/// What went wrong, as data: the exception's type, message, stack trace and inner chain, each kept
/// as its own field.
///
/// A record, because this is part of persisted state. Writing an exception object down and reading
/// it back is unreliable: its type may be missing in the version that reads it, exception
/// serialization varies by serializer, and stack traces are usually lost. Capturing the parts that
/// matter keeps a failure readable after a restart, a redeploy, or a move to another machine, exactly
/// as it read the moment it was thrown.
///
/// One-way by design: this stays data. The original exception type is gone by the time a failure is
/// read back, so any reconstruction would carry a different type under the same name, and
/// <c>catch (PaymentDeclinedException)</c> would silently miss. Read the fields — that is what they
/// are for, and why the outcome is typed at all.
/// </summary>
/// <param name="Message">What went wrong.</param>
/// <param name="ExceptionType">Full name of the exception type, when the failure came from one. A
/// step that exhausted its budget on timeouts reports <c>System.TimeoutException</c> here, which is
/// how a step-level timeout is told apart from other failures — it is not a workflow-level
/// <see cref="WorkflowOutcome.TimedOut"/>.</param>
/// <param name="StackTrace">The stack trace as captured at the throw site, when there was one.</param>
/// <param name="Inner">The next failure in the <see cref="Exception.InnerException"/> chain, captured
/// the same way, so a wrapped driver error keeps its detail.</param>
/// <param name="StepName">The step that failed. Filled in by the runtime when a workflow author fails
/// the run from a handler without naming it.</param>
/// <param name="Attempts">How many attempts ran before giving up. Filled in by the runtime.</param>
public sealed record WorkflowFailure(
    string Message,
    string? ExceptionType = null,
    string? StackTrace = null,
    WorkflowFailure? Inner = null,
    string? StepName = null,
    int Attempts = 0)
{
    /// <summary>
    /// Captures <paramref name="exception"/> and its inner chain. <paramref name="maxDepth"/> bounds
    /// how far the chain is walked, since this record is persisted on every failing transition and a
    /// deeply nested chain would otherwise grow the envelope without bound.
    /// </summary>
    public static WorkflowFailure FromException(
        Exception exception, string? stepName = null, int attempts = 0, int maxDepth = 8) =>
        new(
            exception.Message,
            exception.GetType().FullName,
            exception.StackTrace,
            maxDepth > 1 && exception.InnerException is { } inner
                ? FromException(inner, maxDepth: maxDepth - 1)
                : null,
            stepName,
            attempts);

    public override string ToString() =>
        ExceptionType is null ? Message : $"{ExceptionType}: {Message}";
}
