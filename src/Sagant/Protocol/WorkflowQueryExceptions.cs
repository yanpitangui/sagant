namespace Sagant.Protocol;

/// <summary>
/// A query handler ran past <see cref="Settings.WorkflowSettings.DefaultQueryTimeout"/> (or its
/// per-query override), so the runtime stopped waiting, replied with this, and cancelled the
/// handler's token. The handler itself only unwinds if it observes that token — this bounds how long
/// the workflow instance stays on the hook, which is the part the runtime controls.
/// </summary>
public sealed class WorkflowQueryTimeoutException : Exception
{
    public WorkflowQueryTimeoutException(string message) : base(message)
    {
    }
}
