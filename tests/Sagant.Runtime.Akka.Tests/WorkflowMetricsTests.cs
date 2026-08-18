using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Akka.Actor;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Covers every <see cref="WorkflowDiagnostics"/> instrument recorded from
/// <see cref="WorkflowEntityActor{TWorkflow, TState}"/> — the step/command instruments (recorded
/// automatically from <see cref="Sagant.Descriptors.StepDescriptor{TState}.Invoke"/>/
/// <see cref="Sagant.Descriptors.CommandDescriptor{TState}.Invoke"/> in core, see
/// <c>Sagant.Tests.WorkflowDiagnosticsMetricsTests</c> for the harness/core-level equivalent) and
/// the status-change instruments this actor calls explicitly. Mirrors <see cref="WorkflowTracingTests"/>'s
/// <c>MeterListener</c>-flavored setup — but the <see cref="WorkflowDiagnostics.Meter"/> it listens
/// on is process-global with no per-test session, and the metrics themselves deliberately carry no
/// per-instance tag at all (see <see cref="WorkflowDiagnostics.RecordStatusChange"/>'s doc comment)
/// — spans, by contrast, scope via <c>workflow.persistence_id</c> (see
/// <see cref="WorkflowTracingTests"/>). Every test below passes a <c>workflowTypeName</c> unique to itself into
/// <see cref="WorkflowActorTestKit.CreateActor"/> and filters on it, so a concurrently-running test
/// elsewhere in this assembly recording the exact same instrument (e.g. another test's workflow
/// also reaching <c>ThenEnd()</c>) can never be mistaken for this test's own measurement.
/// </summary>
public class WorkflowMetricsTests : WorkflowActorTestKit, IDisposable
{
    private sealed record Measurement(string InstrumentName, double Value, IReadOnlyDictionary<string, object?> Tags);

    public WorkflowMetricsTests() : base(Config)
    {
        _measurements = new ConcurrentBag<Measurement>();
        _listener = new MeterListener();
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == WorkflowDiagnostics.SourceName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
            _measurements.Add(new Measurement(instrument.Name, measurement, ToDict(tags))));
        _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            _measurements.Add(new Measurement(instrument.Name, measurement, ToDict(tags))));
        _listener.Start();
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private readonly MeterListener _listener;
    private readonly ConcurrentBag<Measurement> _measurements;

    private static Dictionary<string, object?> ToDict(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            dict[tag.Key] = tag.Value;
        }

        return dict;
    }

    /// <summary>Every assertion below filters on this in addition to the instrument name — see the
    /// class doc comment for why that's required for a process-global <c>Meter</c>.</summary>
    private IEnumerable<Measurement> For(string workflowTypeName, string instrumentName) =>
        _measurements.Where(m => m.InstrumentName == instrumentName && (string?)m.Tags["workflow.type"] == workflowTypeName);

    // Same explicit-interface Dispose split as WorkflowTracingTests — TestKit's own Dispose() isn't
    // virtual, so xUnit must reach both via the IDisposable reference.
    void IDisposable.Dispose()
    {
        _listener.Dispose();
        base.Dispose();
    }

    [Fact]
    public void StepSuccess_RecordsStepDuration_TaggedOkWithAttemptOne()
    {
        var script = Script()
            .Step("ChargePayment", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("ChargePayment")).ThenReply("accepted"));

        const string persistenceId = nameof(StepSuccess_RecordsStepDuration_TaggedOkWithAttemptOne);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var m = Assert.Single(For(persistenceId, "sagant.step.duration"));
            Assert.Equal("ChargePayment", m.Tags["step.name"]);
            Assert.Equal(1, m.Tags["attempt"]);
            Assert.Equal("ok", m.Tags["outcome"]);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void CommandHandled_RecordsCommandCount()
    {
        var script = Script()
            .Command<StartWorkflow>((state, _) => new EffectsBuilder<TestState>().UpdateState(state).Reply("accepted"));

        const string persistenceId = nameof(CommandHandled_RecordsCommandCount);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var m = Assert.Single(For(persistenceId, "sagant.command.count"));
            Assert.Equal(nameof(StartWorkflow), m.Tags["command.type"]);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepFailure_WillRetry_RecordsErrorDurationAndRetryScheduled()
    {
        var attempts = 0;
        var script = Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempts++;
                return attempts < 3
                    ? Task.FromException<StepEffect<TestState>>(new InvalidOperationException("flaky"))
                    : Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete());
            })
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("FlakyStep")).ThenReply("accepted"));

        var settings = WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(5).FailoverTo(Step("FlakyStep")))
            .Build();
        const string persistenceId = nameof(StepFailure_WillRetry_RecordsErrorDurationAndRetryScheduled);
        var actor = CreateActor(persistenceId, script, settings, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var durations = For(persistenceId, "sagant.step.duration").ToList();
            Assert.Equal(3, durations.Count);
            Assert.Equal(2, durations.Count(m => (string?)m.Tags["outcome"] == "error"));
            Assert.Equal(1, durations.Count(m => (string?)m.Tags["outcome"] == "ok"));

            var retries = For(persistenceId, "sagant.step.retry").ToList();
            Assert.Equal(2, retries.Count);
            Assert.All(retries, m => Assert.Equal("FlakyStep", m.Tags["step.name"]));
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepFailure_RetriesExhausted_DoesNotRecordRetryScheduledForFinalFailure()
    {
        var script = Script()
            .Step("AlwaysFails", (_, _) => Task.FromException<StepEffect<TestState>>(new InvalidOperationException("nope")))
            .Step("Failover", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("AlwaysFails")).ThenReply("accepted"));

        var settings = WorkflowSettings.Create()
            .DefaultStepRecovery(RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("Failover")))
            .Build();
        const string persistenceId = nameof(StepFailure_RetriesExhausted_DoesNotRecordRetryScheduledForFinalFailure);
        var actor = CreateActor(persistenceId, script, settings, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            // MaxRetries(1) => 2 attempts total (original + 1 retry), only the first failure
            // schedules a retry; the second exhausts the budget and fails over instead.
            var durations = For(persistenceId, "sagant.step.duration").Where(m => (string?)m.Tags["step.name"] == "AlwaysFails").ToList();
            Assert.Equal(2, durations.Count);

            Assert.Single(For(persistenceId, "sagant.step.retry"));
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void Pause_RecordsWorkflowPaused()
    {
        var script = Script()
            .Step("ReviewOrder", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause("waiting")))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("ReviewOrder")).ThenReply("accepted"));

        const string persistenceId = nameof(Pause_RecordsWorkflowPaused);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() => Assert.Single(For(persistenceId, "sagant.workflow.paused")), TimeSpan.FromSeconds(10));

        // No spurious extra Running->Running "resumed" firing off the initial StepTransition that
        // got us to ReviewOrder in the first place.
        Assert.Empty(For(persistenceId, "sagant.workflow.resumed"));
    }

    private sealed record Approve;

    [Fact]
    public void PauseThenBusinessResume_RecordsPauseDuration()
    {
        var script = Script()
            .Step("ReviewOrder", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenPause("waiting")))
            .Step("Finish", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("ReviewOrder")).ThenReply("accepted"))
            .Command<Approve>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Finish")).ThenReply("approved"));

        const string persistenceId = nameof(PauseThenBusinessResume_RecordsPauseDuration);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() => Assert.Single(For(persistenceId, "sagant.workflow.paused")), TimeSpan.FromSeconds(10));

        actor.Tell(new Approve(), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() => Assert.Single(For(persistenceId, "sagant.workflow.pause.duration")), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void CompletingRun_RecordsFinishedTaggedCompleted()
    {
        var script = Script()
            .Step("LastStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("LastStep")).ThenReply("accepted"));

        const string persistenceId = nameof(CompletingRun_RecordsFinishedTaggedCompleted);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var m = Assert.Single(For(persistenceId, "sagant.workflow.finished"));
            Assert.Equal("completed", m.Tags["outcome"]);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void SuspendThenResume_RecordsSuspendedThenResumed()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("SlowStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("SlowStep")).ThenReply("accepted"));

        const string persistenceId = nameof(SuspendThenResume_RecordsSuspendedThenResumed);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Suspend(), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(() => Assert.Single(For(persistenceId, "sagant.workflow.suspended")), TimeSpan.FromSeconds(10));

        actor.Tell(new Resume(), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(() => Assert.Single(For(persistenceId, "sagant.workflow.resumed")), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void Terminate_RecordsFinishedTaggedTerminated()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("SlowStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("SlowStep")).ThenReply("accepted"));

        const string persistenceId = nameof(Terminate_RecordsFinishedTaggedTerminated);
        var actor = CreateActor(persistenceId, script, workflowTypeName: persistenceId);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        actor.Tell(new Terminate("stop"), TestActor);
        ExpectMsg<Done>();

        AwaitAssert(() =>
        {
            var m = Assert.Single(For(persistenceId, "sagant.workflow.finished"));
            Assert.Equal("terminated", m.Tags["outcome"]);
        }, TimeSpan.FromSeconds(10));
    }
}
