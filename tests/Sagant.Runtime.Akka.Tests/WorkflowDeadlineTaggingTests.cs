using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The deadline stream is a tag written by the entity actor and a fold read by whatever follows that
/// tag. They live in separate files and are edited for separate reasons, so an event carrying a
/// deadline change that the tag leaves out is a change computed from an event the reader never sees.
/// These assert the two lists agree.
/// </summary>
public class WorkflowDeadlineTaggingTests
{
    private static readonly TransitionCause TestCause = new TransitionCause.Control("Test");
    private static readonly DateTimeOffset Now = new(2026, 8, 12, 12, 0, 0, TimeSpan.Zero);

    public static TheoryData<WorkflowEvent> EveryEventKind() => new()
    {
        new WorkflowEvent.WorkflowDeadlineSet(Now.AddMinutes(30)),
        new WorkflowEvent.StepStarted("Charge", null, Now.AddSeconds(5), null, TestCause),
        new WorkflowEvent.StepRetryScheduled(1, Now.AddSeconds(35), Now.AddSeconds(30), TestCause),
        new WorkflowEvent.RunPaused("waiting", Now, Now.AddDays(7), "OnTimeout", null, TestCause),
        new WorkflowEvent.RunPaused("waiting", Now, null, null, null, TestCause),
        new WorkflowEvent.RunResumed(Now.AddSeconds(30), TestCause),
        new WorkflowEvent.RunRestarted("Begin", null, "cycle", Now.AddSeconds(5), null, TestCause),
        new WorkflowEvent.RunSuspended(TestCause),
        new WorkflowEvent.RunParked(new WorkflowFailure("stuck"), null, TestCause),
        new WorkflowEvent.RunStayed(TestCause),
        new WorkflowEvent.RunFinished(WorkflowOutcome.Completed.Instance, null, TestCause),
        new WorkflowEvent.RunDeleted(null, TestCause),
        new WorkflowEvent.SeqNrRecorded("producer-1", 4),
    };

    /// <summary>
    /// The direction that matters. An event whose change the reader never receives leaves an instance
    /// armed forever, or leaves one nobody will wake.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryEventKind))]
    public void AnEventThatChangesADeadline_CarriesTheDeadlineTag(WorkflowEvent @event)
    {
        if (WorkflowDeadlineFold.Changes(@event).Count > 0)
        {
            Assert.True(
                WorkflowEventTags.MovesADeadline(@event),
                $"{@event.GetType().Name} changes a deadline, so it belongs in the deadline stream.");
        }
    }

    /// <summary>
    /// The other direction. A tagged event the fold ignores costs read volume alone, so this is the
    /// softer of the two — it keeps the tag from quietly widening back towards every event.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryEventKind))]
    public void ATaggedEvent_ChangesADeadline(WorkflowEvent @event)
    {
        if (WorkflowEventTags.MovesADeadline(@event))
        {
            Assert.NotEmpty(WorkflowDeadlineFold.Changes(@event));
        }
    }

    [Fact]
    public void ADeadlineEventCarriesTheOrdinaryTagsToo()
    {
        var tags = WorkflowEventTags.ForDeadlineEvent("OrderWorkflow");

        Assert.Contains(WorkflowEventTags.All, tags);
        Assert.Contains(WorkflowEventTags.ForWorkflowType("OrderWorkflow"), tags);
        Assert.Contains(WorkflowEventTags.Deadline, tags);
    }

    /// <summary>
    /// The deadline tag carries no reader-side detail — no shard, no lane. How a reader spreads the
    /// work is settled where it reads, so the journal never has to agree with it and changing it
    /// strands nothing already written.
    /// </summary>
    [Fact]
    public void TheDeadlineTag_IsTheSameWhicheverInstanceWroteIt() =>
        Assert.Equal(
            WorkflowEventTags.ForDeadlineEvent("OrderWorkflow"),
            WorkflowEventTags.ForDeadlineEvent("OrderWorkflow"));
}
