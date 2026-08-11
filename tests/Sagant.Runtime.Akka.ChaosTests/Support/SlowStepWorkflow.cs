using Sagant;
using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Runtime.Akka.ChaosTests.Support;

public sealed record SlowStepState(bool Finished, int Runs)
{
    public SlowStepState() : this(false, 0)
    {
    }
}

public sealed record BeginSlowWork(int Millis);

public sealed record GetSlowState;

/// <summary>
/// A workflow whose step takes long enough to still be running when its node is asked to leave.
///
/// The step counts its own executions, which is what makes the outcome legible: a step abandoned
/// mid-flight and re-run on another node reports two, while one allowed to finish before handoff
/// reports one. Guarantee <c>R1</c> makes re-running safe, so this measures whether graceful
/// shutdown avoids paying for it.
/// </summary>
public partial class SlowStepWorkflow : Workflow<SlowStepState>
{
    private readonly TimeSpan _stepDuration;

    public SlowStepWorkflow(TimeSpan stepDuration) => _stepDuration = stepDuration;

    public override SlowStepState EmptyState() => new();

    [WorkflowCommandHandler]
    public CommandEffect<SlowStepState> Handle(BeginSlowWork command) =>
        Effects.TransitionTo(Steps.SlowStep).ThenReply("accepted");

    [WorkflowQuery]
    public QueryEffect Read(GetSlowState query, QueryContext<SlowStepState> ctx) =>
        QueryEffects.Reply(ctx.State);

    [WorkflowStep]
    public async Task<StepEffect<SlowStepState>> SlowStep(StepContext<SlowStepState> ctx)
    {
        // The runtime cancels this token when it stops waiting on the attempt, so a shutdown that
        // abandons the step shows up here as the delay being cut short.
        await Task.Delay(_stepDuration, ctx.CancellationToken);

        return StepEffects
            .UpdateState(ctx.State with { Finished = true, Runs = ctx.State.Runs + 1 })
            .ThenPause("slow work done");
    }
}
