using Sagant;
using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Runtime.Akka.ChaosTests.Support;

public sealed record RestartingState(int Cycle, bool Settled)
{
    public RestartingState() : this(0, false)
    {
    }
}

public sealed record BeginCycling(int Cycles);

public sealed record GetCycle;

/// <summary>
/// Runs a bounded number of restart cycles, then settles.
///
/// A restart is the durability path with the narrowest safety margin: it makes an instance's history
/// reclaimable while the instance keeps running, so what a crash finds on disk depends on how far
/// through that the process got. The cycle counter is carried forward through
/// <c>UpdateState</c>, which makes recovery checkable — a recovered instance either knows how many
/// cycles it completed or it does not.
/// </summary>
public partial class RestartingWorkflow : Workflow<RestartingState>
{
    private readonly int _cycles;

    /// <param name="cycles">How many restarts to perform before settling. Taken as a constructor
    /// argument so a node rebuilt after a crash runs the same workflow definition.</param>
    public RestartingWorkflow(int cycles) => _cycles = cycles;

    public override RestartingState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<RestartingState> Handle(BeginCycling command) =>
        Effects.TransitionTo(Steps.Loop).ThenReply("accepted");

    /// <summary>
    /// The instance's own view of how far it has cycled.
    ///
    /// A restart reclaims the events behind it, including the state change of the cycle it closed,
    /// so state ends up held entirely in the snapshot, with none of it left in the journal. That is
    /// the whole point of <c>E11</c> — and it means a reader that folds the journal cannot see it.
    /// Asking the instance is how a caller observes a restarting workflow's state.
    /// </summary>
    [WorkflowQuery]
    public QueryEffect GetCycle(GetCycle query, QueryContext<RestartingState> ctx) =>
        QueryEffects.Reply(ctx.State);

    [WorkflowStep]
    public Task<StepEffect<RestartingState>> Loop(StepContext<RestartingState> ctx)
    {
        var next = ctx.State with { Cycle = ctx.State.Cycle + 1 };

        // Each cycle releases the history behind it, so an instance that has looped many times holds
        // roughly one cycle's worth of events however long it has run (E11).
        return Task.FromResult(next.Cycle < _cycles
            ? StepEffects.UpdateState(next).ThenRestartAt(Steps.Loop, $"cycle {next.Cycle}")
            : StepEffects.UpdateState(next with { Settled = true }).ThenPause("cycled out"));
    }
}
