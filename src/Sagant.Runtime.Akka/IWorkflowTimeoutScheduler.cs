using Akka.Actor;

namespace Sagant.Runtime.Akka;

/// <summary>
/// Schedules a durable-enough timeout: the default implementation uses Akka.NET's ordinary
/// scheduler (armed fresh on every actor activation from a persisted absolute deadline — see
/// <see cref="WorkflowRuntimeState{TState}"/> — so a timer surviving passivation isn't needed, just
/// recomputed). Pluggable so a future implementation can back onto
/// <c>Aaronontheweb/akka-reminders</c> for timers that must fire even while the entity is fully
/// passivated.
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
