using BenchmarkDotNet.Attributes;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Benchmarks;

/// <summary>
/// What one child report costs a parent awaiting a group of <see cref="GroupSize"/> children — the
/// path guarantee H5 names ("a fan-out's per-report cost stays flat as the group grows"). A report
/// runs three things in sequence on the real path: reading whether the group has resolved
/// (<see cref="ChildGroupPolicy.TallyGroup"/>, O(1) — <see cref="ChildGroupState"/> carries its own
/// running tally), the fold that records the report (<see cref="FoldOneReport"/>), and — only for the
/// report that resolves the group — evaluating the outcome from that same tally. Benchmarked here in
/// isolation, so a regression in any one of them shows up on its own.
///
/// <see cref="GroupSize"/> exercises <see cref="FoldOneReport"/> alone now: it grows
/// <c>WorkflowRuntimeState.Children</c>, the map size <see cref="ChildGroupPolicy.TallyGroup"/> is
/// independent of, reading <see cref="ChildGroupState"/>'s own count directly instead.
/// <c>TallyGroup</c>/<c>EvaluateGroupOutcome</c> stay in this <c>GroupSize</c>-parameterized suite
/// anyway, so a future regression that reintroduces a scan shows up as their numbers growing with
/// size again.
/// </summary>
[MemoryDiagnoser]
public class ChildFanOutBenchmarks
{
    [Params(100, 1_000, 10_000)]
    public int GroupSize { get; set; }

    private WorkflowRuntimeState<string> _envelope = null!;
    private string _reportedRelationshipId = null!;
    private WorkflowEvent.ChildMemberUpdated _reportEvent = null!;
    private ChildGroupState _group = null!;

    [GlobalSetup]
    public void Setup()
    {
        const string groupId = "g1";
        var children = new List<ChildWorkflowRelationship>(GroupSize);
        for (var i = 0; i < GroupSize; i++)
        {
            children.Add(new ChildWorkflowRelationship(
                RelationshipId: $"parent:{groupId}:child-{i}",
                ParentWorkflowType: "Parent",
                ParentWorkflowId: "parent-1",
                ChildWorkflowType: "Child",
                ChildWorkflowId: $"child-{i}",
                GroupId: groupId,
                Generation: 0,
                Status: ChildStatus.Pending,
                Result: null,
                Failure: null,
                TraceParent: null,
                ParentClosePolicy: ParentClosePolicy.Terminate,
                Command: new object()));
        }

        _group = new ChildGroupState(
            groupId, Generation: 0, CompletionPolicy.AllSuccessful, FailurePolicy.WaitForAll,
            RemainingChildrenPolicy.Continue, "OnDone", Finalized: false, null, null, Total: GroupSize);

        // Seeded through the real fold path, so Children is exactly what a live instance holds: an
        // ImmutableDictionary built by WorkflowEventFold.Concat, keyed by RelationshipId.
        _envelope = WorkflowEventFold.Apply(
            new WorkflowRuntimeState<string>("state", null, null, 0, WorkflowStatus.Running),
            new WorkflowEvent.ChildrenAwaited(groupId, children, _group, 1, null, new TransitionCause.Control("seed")));

        // The middle child: an ordinary report, well clear of whatever a first-or-last id's position
        // might do to a scan.
        _reportedRelationshipId = $"parent:{groupId}:child-{GroupSize / 2}";
        _reportEvent = new WorkflowEvent.ChildMemberUpdated(_reportedRelationshipId, ChildStatus.Completed, "result", null, null);
    }

    /// <summary>The read side of a report: does this settle the group, and how.</summary>
    [Benchmark]
    public ChildGroupPolicy.ChildGroupTally TallyGroup() =>
        ChildGroupPolicy.TallyGroup(_group, ChildStatus.Completed);

    /// <summary>The write side: recording one member's report against the group's current state.</summary>
    [Benchmark]
    public WorkflowRuntimeState<string> FoldOneReport() =>
        WorkflowEventFold.Apply(_envelope, _reportEvent);

    /// <summary>What a resolving report's tally is turned into.</summary>
    [Benchmark]
    public GroupOutcome? EvaluateGroupOutcome()
    {
        var tally = ChildGroupPolicy.TallyGroup(_group, ChildStatus.Completed);
        return ChildGroupPolicy.EvaluateGroupOutcome(_group, tally);
    }
}
