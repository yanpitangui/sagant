using Microsoft.Extensions.Time.Testing;
using Sagant.Clients;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Testing;

namespace Sagant.Scheduling.Tests;

/// <summary>
/// A schedule's own logic — when it fires, what it skips, when it stops — driven with a fake clock
/// and a recording client. No `ActorSystem`, no persistence, no deadline scheduler: waiting is a
/// pause with a deadline, and the harness compares that deadline against the clock it was given, so
/// everything here is settled by arithmetic.
///
/// What is deliberately absent is "does a passivated schedule actually wake", which belongs to the
/// runtime rather than to the schedule.
/// </summary>
public class ScheduleWorkflowTests
{
    private static readonly DateTimeOffset Start = new(2026, 8, 14, 12, 0, 0, TimeSpan.Zero);

    private sealed record Reconcile(string Scope);

    /// <summary>Records what a schedule started, and answers whatever status a test wants for the
    /// previous occurrence.</summary>
    private sealed class RecordingClient : IWorkflowClient
    {
        public List<(string Type, string EntityId, object Command)> Started { get; } = [];

        public WorkflowStatus PreviousStatus { get; set; } = WorkflowStatus.Finished;

        public IWorkflowHandle<TWorkflow> For<TWorkflow>(string entityId) where TWorkflow : class =>
            throw new NotSupportedException("A schedule addresses its target by type name.");

        public IWorkflowHandle For(string workflowType, string entityId) =>
            new RecordingHandle(this, workflowType, entityId);

        private sealed class RecordingHandle(RecordingClient owner, string type, string entityId) : IWorkflowHandle
        {
            public string EntityId => entityId;

            public ValueTask Send<TCommand>(
                TCommand command, CancellationToken cancellationToken = default, string? idempotencyKey = null,
                IReadOnlyDictionary<string, string>? metadata = null) where TCommand : notnull
            {
                owner.Started.Add((type, entityId, command));
                return ValueTask.CompletedTask;
            }

            public Task<WorkflowStatus> GetStatus(
                TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
                Task.FromResult(owner.PreviousStatus);

            public Task<TReply> Request<TCommand, TReply>(
                TCommand command, TimeSpan? timeout = null, CancellationToken cancellationToken = default,
                string? idempotencyKey = null, IReadOnlyDictionary<string, string>? metadata = null)
                where TCommand : notnull => throw new NotSupportedException();

            public Task<TReply> Query<TQuery, TReply>(
                TQuery query, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
                where TQuery : notnull => throw new NotSupportedException();

            public Task<Done> Suspend(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Resume(TimeSpan? timeout = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Terminate(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Cancel(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Delete(string? reason = null, TimeSpan? timeout = null, CancellationToken ct = default) =>
                throw new NotSupportedException();

            public Task<Done> Wake(WorkflowTimerKind kind, TimeSpan? timeout = null, CancellationToken ct = default) =>
                Task.FromResult(Done.Instance);

            public Task<WorkflowResult<TState>> RunAndAwaitResult<TState>(
                object command, TimeSpan timeout, string? idempotencyKey = null, CancellationToken ct = default) =>
                throw new NotSupportedException();
        }
    }

    private static (WorkflowTestHarness<ScheduleWorkflow, ScheduleState> Harness, RecordingClient Client, FakeTimeProvider Clock)
        Build()
    {
        var clock = new FakeTimeProvider(Start);
        var client = new RecordingClient();
        return (new WorkflowTestHarness<ScheduleWorkflow, ScheduleState>(
            new ScheduleWorkflow(client, clock), timeProvider: clock), client, clock);
    }

    private static StartSchedule Every(TimeSpan interval, OverlapPolicy overlap = OverlapPolicy.Skip,
        TimeSpan? catchUp = null, int? endsAfter = null) =>
        new(new EverySpec(interval), "ReconciliationWorkflow", new Reconcile("orders"), overlap, catchUp, endsAfter);

    [Fact]
    public async Task AScheduleWaitsForItsFirstOccurrence()
    {
        var (harness, client, _) = Build();

        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        Assert.Equal(WorkflowStatus.Paused, harness.Status);
        Assert.Empty(client.Started);
        Assert.Equal(Start.AddHours(1), harness.State.NextFireUtc);
    }

    [Fact]
    public async Task WhenTheOccurrenceArrives_ItStartsTheTarget()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();

        var started = Assert.Single(client.Started);
        Assert.Equal("ReconciliationWorkflow", started.Type);
        Assert.Equal(new Reconcile("orders"), started.Command);
        Assert.Equal(1, harness.State.FireCount);
    }

    /// <summary>
    /// The id is the occurrence's own instant, so a fire that happens twice addresses the same
    /// instance both times and the second is an ordinary duplicate command rather than a second run.
    /// </summary>
    [Fact]
    public async Task TheOccurrenceIdIsDerivedFromTheInstantItWasScheduledFor()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();

        Assert.Equal(Start.AddHours(1).UtcDateTime.ToString("yyyyMMddHHmmss"), client.Started[0].EntityId);
    }

    [Fact]
    public async Task AfterFiring_ItWaitsForTheNextOccurrence()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();
        Assert.Equal(WorkflowStatus.Paused, harness.Status);
        Assert.Equal(Start.AddHours(2), harness.State.NextFireUtc);

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();
        Assert.Equal(2, client.Started.Count);
    }

    /// <summary>
    /// Occurrences are computed from the previous scheduled instant, so a schedule that was away
    /// keeps its original phase — "every hour on the hour" is still on the hour afterwards.
    /// </summary>
    [Fact]
    public async Task ALongGap_LandsOnTheNextOccurrenceAndKeepsItsPhase()
    {
        var (harness, _, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        clock.Advance(TimeSpan.FromHours(5) + TimeSpan.FromMinutes(20));
        await harness.RunPauseTimeoutIfDue();

        Assert.Equal(Start.AddHours(6), harness.State.NextFireUtc);
    }

    [Fact]
    public async Task AnOccurrenceOlderThanTheCatchUpWindow_IsSkipped()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1), catchUp: TimeSpan.FromMinutes(10)));

        clock.Advance(TimeSpan.FromHours(5));
        await harness.RunPauseTimeoutIfDue();

        Assert.Empty(client.Started);
        Assert.Equal(1, harness.State.SkippedCount);
        Assert.Equal(0, harness.State.FireCount);
    }

    [Fact]
    public async Task WithSkipOverlap_AnOccurrenceIsPassedOverWhileThePreviousOneRuns()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1), OverlapPolicy.Skip));

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();
        Assert.Single(client.Started);

        client.PreviousStatus = WorkflowStatus.Running;
        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();

        Assert.Single(client.Started);
        Assert.Equal(1, harness.State.SkippedCount);
    }

    [Fact]
    public async Task WithAllowOverlap_AnOccurrenceRunsRegardless()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1), OverlapPolicy.Allow));

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();

        client.PreviousStatus = WorkflowStatus.Running;
        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();

        Assert.Equal(2, client.Started.Count);
        Assert.Equal(0, harness.State.SkippedCount);
    }

    [Fact]
    public async Task AScheduleWithAnEndCondition_FinishesAfterThatManyOccurrences()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1), endsAfter: 2));

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();
        Assert.Equal(WorkflowStatus.Paused, harness.Status);

        clock.Advance(TimeSpan.FromHours(1));
        await harness.RunPauseTimeoutIfDue();

        Assert.Equal(2, client.Started.Count);
        Assert.Equal(WorkflowStatus.Finished, harness.Status);
    }

    [Fact]
    public async Task AOneShotSchedule_FiresOnceAndFinishes()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(new StartSchedule(
            new OnceAtSpec(Start.AddHours(3)), "ReconciliationWorkflow", new Reconcile("orders")));

        clock.Advance(TimeSpan.FromHours(3));
        await harness.RunPauseTimeoutIfDue();

        Assert.Single(client.Started);
        Assert.Equal(WorkflowStatus.Finished, harness.Status);
    }

    [Fact]
    public async Task AHeldSchedule_ResumesAtTheNextOccurrenceRatherThanFiringWhatItSleptThrough()
    {
        var (harness, client, clock) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        harness.RunCommand(new PauseSchedule());
        clock.Advance(TimeSpan.FromHours(5));
        await harness.RunUntilStop(new ResumeSchedule());

        Assert.Empty(client.Started);
        Assert.Equal(Start.AddHours(6), harness.State.NextFireUtc);
    }

    [Fact]
    public async Task ACancelledSchedule_Finishes()
    {
        var (harness, _, _) = Build();
        await harness.RunUntilStop(Every(TimeSpan.FromHours(1)));

        harness.RunCommand(new CancelSchedule());

        Assert.Equal(WorkflowStatus.Finished, harness.Status);
    }

    [Fact]
    public async Task ASpecWithNoOccurrenceAtAll_FinishesImmediately()
    {
        var (harness, client, _) = Build();

        // Settles in the command itself: with nothing to wait for there is no step to run.
        harness.RunCommand(new StartSchedule(
            new OnceAtSpec(Start.AddHours(-1)), "ReconciliationWorkflow", new Reconcile("orders")));

        Assert.Equal(WorkflowStatus.Finished, harness.Status);
        Assert.Empty(client.Started);
    }
}
