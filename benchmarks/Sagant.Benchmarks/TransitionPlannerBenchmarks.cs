using BenchmarkDotNet.Attributes;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Settings;

namespace Sagant.Benchmarks;

/// <summary>
/// What one transition costs to plan — the work <c>WorkflowEntityActor</c> does before it ever
/// touches the journal, on every step start, command, and control message an instance handles.
/// </summary>
[MemoryDiagnoser]
public class TransitionPlannerBenchmarks
{
    private WorkflowRuntimeState<string> _envelope = null!;
    private ResolvedWorkflowSettings _settings = null!;
    private WorkflowInstanceIdentity _identity;
    private Transition.StepTransition _transition = null!;
    private TransitionCause _cause = null!;

    [GlobalSetup]
    public void Setup()
    {
        var settings = new WorkflowSettings(
            WorkflowTimeout: TimeSpan.FromMinutes(5),
            WorkflowRecoverStrategy: null,
            DefaultStepTimeout: TimeSpan.FromSeconds(5),
            DefaultStepRecoverStrategy: null,
            StepSettings: [new StepSettings("ChargePaymentStep", TimeSpan.FromSeconds(10), null)],
            CancellationStepName: "CancelOrderStep");

        _settings = ResolvedWorkflowSettings.From(settings);
        _identity = new WorkflowInstanceIdentity("OrderWorkflow-order-1", "order-1", "OrderWorkflow");
        _transition = new Transition.StepTransition("ChargePaymentStep", null);
        _cause = new TransitionCause.Control("bench");
        _envelope = new WorkflowRuntimeState<string>("state", null, null, 0, WorkflowStatus.Running);
    }

    /// <summary>A step-to-step transition that leaves state alone — the common case in a chain with
    /// no business state change on this hop.</summary>
    [Benchmark(Baseline = true)]
    public TransitionPlan<string> PlanStepTransition() =>
        WorkflowTransitionPlanner.Plan(
            _envelope, _transition, PersistenceEffect<string>.NoPersistence.Instance,
            DateTimeOffset.UtcNow, _settings, _identity, _cause);

    /// <summary>The same transition, carrying a state update — the added cost of one
    /// <c>UserStateChanged</c> event.</summary>
    [Benchmark]
    public TransitionPlan<string> PlanStepTransitionWithStateWrite() =>
        WorkflowTransitionPlanner.Plan(
            _envelope, _transition, new PersistenceEffect<string>.UpdateState("new-state"),
            DateTimeOffset.UtcNow, _settings, _identity, _cause);
}
