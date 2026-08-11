namespace Sagant.Runtime.Akka;

/// <summary>
/// Bridges Akka's own clock (<see cref="global::Akka.Actor.IScheduler"/>, which is also an
/// <see cref="global::Akka.Actor.ITimeProvider"/>) onto the BCL's <see cref="TimeProvider"/> surface, so
/// <see cref="WorkflowEntityActor{TWorkflow,TState}"/> exposes the same <see cref="TimeProvider"/>
/// clock abstraction <c>Sagant.Testing.WorkflowTestHarness</c> uses. A passthrough view of Akka's
/// own clock: <see cref="GetUtcNow"/> forwards straight through to the wrapped scheduler, so swapping
/// in <c>Akka.TestKit.TestScheduler</c> via <c>akka.scheduler.implementation</c> (as the timeout test
/// suites already do) moves this in lockstep with <c>Scheduler.Advance()</c>, with no separate
/// test-only time seam to wire up.
/// </summary>
internal sealed class AkkaSchedulerTimeProvider(global::Akka.Actor.IScheduler scheduler) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => scheduler.Now;
}
