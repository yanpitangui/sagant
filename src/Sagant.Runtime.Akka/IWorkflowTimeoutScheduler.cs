using Akka.Actor;

namespace Sagant.Runtime.Akka;

/// <summary>
/// Schedules the in-process timeouts a resident entity holds: the step deadline, retry backoff, the
/// workflow and pause deadlines, the graceful-shutdown grace window, query timeouts and the
/// keep-alive tick. Each is armed fresh on every activation from a persisted absolute deadline (see
/// <see cref="WorkflowRuntimeState{TState}"/>), so recovery reproduces the remaining wait.
///
/// Waking a <em>passivated</em> entity for a deadline belongs to
/// <see cref="Sagant.Execution.IWorkflowDeadlineScheduler"/>, which bounds the lateness guarantee
/// <c>D8</c> describes. This seam stays in-process, and replacing it affects every timeout listed
/// above at once.
/// </summary>
public interface IWorkflowTimeoutScheduler
{
    ICancelable ScheduleTimeout(TimeSpan delay, IActorRef target, object message);
}

/// <summary>Default implementation: <see cref="IScheduler.ScheduleTellOnceCancelable"/>.</summary>
public sealed class NativeWorkflowTimeoutScheduler : IWorkflowTimeoutScheduler
{
    private readonly IScheduler _scheduler;

    public NativeWorkflowTimeoutScheduler(IScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    public ICancelable ScheduleTimeout(TimeSpan delay, IActorRef target, object message)
    {
        var effectiveDelay = delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
        var cancelable = new Cancelable(_scheduler);
        _scheduler.ScheduleTellOnce(effectiveDelay, target, message, ActorRefs.NoSender, cancelable);
        return cancelable;
    }
}
