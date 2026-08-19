using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;

namespace Sagant.Tests.Execution;

/// <summary>
/// <see cref="WorkflowDeadlineFold"/> tells anything outside an instance which deadlines that
/// instance is waiting on, reading the same events <see cref="WorkflowEventFold"/> reads. Two
/// functions over one stream drift as events are added, and the cost of drift is asymmetric: a
/// missed arm leaves an instance nobody will ever wake, and a missed disarm wakes one forever.
///
/// So the central test is the agreement itself, checked at every prefix of every sequence: the
/// deadlines this fold reports must be the deadlines the state fold holds.
/// </summary>
public class WorkflowDeadlineFoldTests
{
    private static readonly TransitionCause TestCause = new TransitionCause.Control("Test");

    private sealed record OrderState(string Value);

    private static readonly DateTimeOffset Now = new(2026, 8, 12, 12, 0, 0, TimeSpan.Zero);

    private static WorkflowRuntimeState<OrderState> Fresh() =>
        new(new OrderState("initial"), CurrentStepName: null, CurrentStepInput: null,
            RetryCount: 0, Status: WorkflowStatus.Running);

    /// <summary>
    /// The invariant, checked after every single event along the way — an index built by
    /// following the stream is correct only if it is correct at each point it observes.
    ///
    /// A live instance's armed set is exactly the two instants the state fold holds. A terminal one
    /// arms nothing: the state fold keeps <see cref="WorkflowRuntimeState{TState}.WorkflowDeadline"/>
    /// past the end because guarantee <c>D3</c> makes it sticky and a reader of the history is
    /// entitled to see what the run was bounded by, while an index exists to decide who to wake, and
    /// a finished run is nobody.
    /// </summary>
    [Theory]
    [MemberData(nameof(EventSequences))]
    public void ArmedDeadlines_MatchTheStateFold_AtEveryPrefix(WorkflowEvent[] events)
    {
        var state = Fresh();
        var armed = new Dictionary<(WorkflowTimerKind Kind, string? Discriminator), DateTimeOffset>();

        foreach (var e in events)
        {
            state = WorkflowEventFold.Apply(state, e);

            foreach (var change in WorkflowDeadlineFold.Changes(e))
            {
                switch (change)
                {
                    case WorkflowDeadlineChange.Arm arm:
                        armed[(arm.Kind, arm.Discriminator)] = arm.DueUtc;
                        break;
                    case WorkflowDeadlineChange.Disarm disarm:
                        armed.Remove((disarm.Kind, disarm.Discriminator));
                        break;
                }
            }

            var terminal = state.Status is WorkflowStatus.Finished or WorkflowStatus.Deleted;

            // Mirrors WorkflowTransitionPlanner: the workflow timer runs until the run is terminal,
            // and the pause timer runs while the status is Paused.
            Assert.Equal(
                terminal ? null : state.WorkflowDeadline,
                Lookup(armed, WorkflowTimerKind.Workflow));
            Assert.Equal(
                state.Status == WorkflowStatus.Paused ? state.PauseDeadline : null,
                Lookup(armed, WorkflowTimerKind.Pause));
            Assert.Equal(
                state.Status == WorkflowStatus.Suspended ? state.HoldDeadline : null,
                Lookup(armed, WorkflowTimerKind.Hold));

            // One wait per live group, keyed by the group so two awaited at once keep their own.
            var liveGroups = (state.ChildGroups?.Values ?? [])
                .Where(g => g is { Finalized: false, Deadline: not null })
                .ToDictionary(g => g.GroupId, g => g.Deadline!.Value);
            var armedGroups = armed
                .Where(a => a.Key.Kind == WorkflowTimerKind.ChildGroup)
                .ToDictionary(a => a.Key.Discriminator!, a => a.Value);
            Assert.Equal(liveGroups, armedGroups);
        }
    }

    private static DateTimeOffset? Lookup(
        Dictionary<(WorkflowTimerKind Kind, string? Discriminator), DateTimeOffset> armed,
        WorkflowTimerKind kind) =>
        armed.TryGetValue((kind, null), out var due) ? due : null;

    [Fact]
    public void APauseWithNoTimeout_ClearsWhateverAPreviousPauseArmed()
    {
        var armed = WorkflowDeadlineFold.Changes(
            new WorkflowEvent.RunPaused("waiting", Now, Now.AddHours(4), "OnTimeout", null, TestCause));
        var arm = Assert.IsType<WorkflowDeadlineChange.Arm>(Assert.Single(armed));
        Assert.Equal(WorkflowTimerKind.Pause, arm.Kind);
        Assert.Equal(Now.AddHours(4), arm.DueUtc);

        // Pausing also ends any hold, since an instance is in one place and it is now paused.
        var cleared = WorkflowDeadlineFold.Changes(
            new WorkflowEvent.RunPaused("waiting for a person", Now, null, null, null, TestCause));
        Assert.All(cleared, c => Assert.IsType<WorkflowDeadlineChange.Disarm>(c));
        Assert.Contains(cleared, c => c is WorkflowDeadlineChange.Disarm { Kind: WorkflowTimerKind.Pause });
        Assert.Contains(cleared, c => c is WorkflowDeadlineChange.Disarm { Kind: WorkflowTimerKind.Hold });
    }

    /// <summary>
    /// Holding a paused instance ends the pause wake, because the pause is over — an operator now
    /// decides when it runs again. The workflow deadline it was already counting down survives, which
    /// is what <c>TimeoutHandles.CancelForSuspend</c> does on the live side.
    /// </summary>
    [Fact]
    public void AHeldInstance_LosesItsPauseWakeAndKeepsItsWorkflowDeadline()
    {
        foreach (var held in new WorkflowEvent[]
                 {
                     new WorkflowEvent.RunSuspended(TestCause, Now),
                     new WorkflowEvent.RunParked(new WorkflowFailure("stuck"), null, TestCause, Now),
                 })
        {
            Assert.All(
                WorkflowDeadlineFold.Changes(held),
                change => Assert.IsType<WorkflowDeadlineChange.Disarm>(change));
            Assert.DoesNotContain(
                WorkflowDeadlineFold.Changes(held),
                c => c is WorkflowDeadlineChange.Disarm { Kind: WorkflowTimerKind.Workflow });
        }
    }

    /// <summary>A hold that names a deadline is something a wake can fire, so it arms one.</summary>
    [Fact]
    public void AHoldWithADeadline_ArmsAWake()
    {
        foreach (var held in new WorkflowEvent[]
                 {
                     new WorkflowEvent.RunSuspended(TestCause, Now, null, Now.AddDays(3), "OnAbandoned"),
                     new WorkflowEvent.RunParked(
                         new WorkflowFailure("stuck"), null, TestCause, Now, Now.AddDays(3), "OnAbandoned"),
                 })
        {
            var arm = Assert.Single(
                WorkflowDeadlineFold.Changes(held).OfType<WorkflowDeadlineChange.Arm>());
            Assert.Equal(WorkflowTimerKind.Hold, arm.Kind);
            Assert.Equal(Now.AddDays(3), arm.DueUtc);
        }
    }

    private static ChildGroupState Group(string groupId, DateTimeOffset? deadline, string? timeoutStep) =>
        new(groupId, Generation: 0, CompletionPolicy.AllSuccessful, FailurePolicy.FailFast,
            RemainingChildrenPolicy.Terminate, "OnDone", Finalized: false, deadline, timeoutStep);

    public static TheoryData<WorkflowEvent[]> EventSequences() => new()
    {
        // A plain run: a workflow deadline, one step, done.
        new WorkflowEvent[]
        {
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(30)),
            new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), "trace-1", TestCause),
            new WorkflowEvent.UserStateChanged<OrderState>(new OrderState("charged")),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, "trace-2", TestCause),
        },
        // Pause with a timeout, then the timeout handler runs.
        new WorkflowEvent[]
        {
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddDays(30)),
            new WorkflowEvent.StepStarted("AwaitApproval", null, null, null, TestCause),
            new WorkflowEvent.RunPaused("awaiting approval", Now, Now.AddDays(7), "OnTimeout", null, TestCause),
            new WorkflowEvent.StepStarted("OnTimeout", null, null, null, TestCause),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        },
        // Pause, then a command resumes it early.
        new WorkflowEvent[]
        {
            new WorkflowEvent.RunPaused("awaiting approval", Now, Now.AddDays(7), "OnTimeout", null, TestCause),
            new WorkflowEvent.RunResumed(Now.AddSeconds(30), TestCause),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        },
        // Pause with no timeout at all, which waits on a command alone.
        new WorkflowEvent[]
        {
            new WorkflowEvent.RunPaused("awaiting approval", Now, null, null, null, TestCause),
            new WorkflowEvent.StepStarted("Continue", null, null, null, TestCause),
        },
        // Held, then released, with the workflow deadline running throughout.
        new WorkflowEvent[]
        {
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddHours(2)),
            new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
            new WorkflowEvent.RunParked(new WorkflowFailure("gateway down"), null, TestCause, Now),
            new WorkflowEvent.RunSuspended(TestCause, Now),
            new WorkflowEvent.RunResumed(Now.AddSeconds(35), TestCause),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        },
        // A fresh cycle drops the previous one's deadlines, then establishes its own.
        new WorkflowEvent[]
        {
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(30)),
            new WorkflowEvent.RunPaused("waiting", Now, Now.AddMinutes(10), "OnTimeout", null, TestCause),
            new WorkflowEvent.RunRestarted("Begin", null, "next cycle", Now.AddSeconds(5), null, TestCause),
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(90)),
            new WorkflowEvent.RunPaused("waiting", Now, Now.AddMinutes(70), "OnTimeout", null, TestCause),
        },
        // Deleted while paused.
        new WorkflowEvent[]
        {
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(30)),
            new WorkflowEvent.RunPaused("waiting", Now, Now.AddMinutes(10), "OnTimeout", null, TestCause),
            new WorkflowEvent.RunDeleted(null, TestCause),
        },
        // Held with a deadline, then released by an operator before it lands.
        new WorkflowEvent[]
        {
            new WorkflowEvent.WorkflowDeadlineSet(Now.AddHours(4)),
            new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
            new WorkflowEvent.RunSuspended(TestCause, Now, null, Now.AddDays(3), "OnAbandoned"),
            new WorkflowEvent.RunResumed(Now.AddSeconds(35), TestCause),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        },
        // Parked with a deadline nobody came back for, so the hold's own step runs.
        new WorkflowEvent[]
        {
            new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
            new WorkflowEvent.RunParked(
                new WorkflowFailure("gateway down"), null, TestCause, Now, Now.AddDays(3), "OnAbandoned"),
            new WorkflowEvent.StepStarted("OnAbandoned", null, null, null, TestCause),
            new WorkflowEvent.RunFinished(
                new WorkflowOutcome.Failed(new WorkflowFailure("abandoned")), null, TestCause),
        },
        // A pause, then an operator holds it before the pause deadline lands.
        new WorkflowEvent[]
        {
            new WorkflowEvent.RunPaused("awaiting approval", Now, Now.AddDays(7), "OnTimeout", null, TestCause),
            new WorkflowEvent.RunSuspended(TestCause, Now, null, Now.AddDays(1), "OnAbandoned"),
            new WorkflowEvent.RunResumed(Now.AddSeconds(30), TestCause),
        },
        // Two groups awaited at once, each with its own wait, resolving one at a time.
        new WorkflowEvent[]
        {
            new WorkflowEvent.StepStarted("Fan", null, null, null, TestCause),
            new WorkflowEvent.ChildrenAwaited(
                "items", [], Group("items", Now.AddHours(2), "OnItemsLate"), 1, null, TestCause),
            new WorkflowEvent.ChildrenAwaited(
                "notify", [], Group("notify", Now.AddHours(6), "OnNotifyLate"), 2, null, TestCause),
            new WorkflowEvent.ChildGroupFinalized("items", [], false),
            new WorkflowEvent.ChildGroupFinalized("notify", [], false),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        },
        // A group that waits for its children however long they take arms nothing.
        new WorkflowEvent[]
        {
            new WorkflowEvent.ChildrenAwaited("items", [], Group("items", null, null), 1, null, TestCause),
            new WorkflowEvent.ChildGroupFinalized("items", [], false),
        },
        // Retries move the step's own deadline, which is no concern of this fold's.
        new WorkflowEvent[]
        {
            new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
            new WorkflowEvent.StepRetryScheduled(1, Now.AddSeconds(35), Now.AddSeconds(30), TestCause),
            new WorkflowEvent.StepRetryScheduled(2, Now.AddSeconds(65), Now.AddSeconds(60), TestCause),
            new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        },
    };
}
