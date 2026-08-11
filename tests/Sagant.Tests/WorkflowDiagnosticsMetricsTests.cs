using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Testing;

namespace Sagant.Tests;

/// <summary>
/// Covers the metrics <see cref="WorkflowDiagnostics"/> records automatically from
/// <see cref="StepDescriptor{TState}.Invoke"/>/<see cref="CommandDescriptor{TState}.Invoke"/> and
/// from <see cref="WorkflowTestHarness{TWorkflow,TState}"/>'s own status-change tracking — no
/// Akka.NET involved (see <see cref="WorkflowRuntimeStateSerializationTests"/>-style zero-Akka
/// tests elsewhere in this project). <c>WorkflowEntityActor</c>'s own additional instruments
/// (paused/suspended/resumed/ended via control commands it alone exposes) are covered by
/// <c>Sagant.Runtime.Akka.Tests.WorkflowMetricsTests</c> instead.
/// </summary>
public class WorkflowDiagnosticsMetricsTests
{
    private sealed record Measurement(string InstrumentName, double Value, IReadOnlyDictionary<string, object?> Tags);

    private static (MeterListener Listener, ConcurrentBag<Measurement> Measurements) Listen()
    {
        var measurements = new ConcurrentBag<Measurement>();
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == WorkflowDiagnostics.SourceName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
            measurements.Add(new Measurement(instrument.Name, measurement, ToDict(tags))));
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            measurements.Add(new Measurement(instrument.Name, measurement, ToDict(tags))));
        listener.Start();
        return (listener, measurements);
    }

    private static Dictionary<string, object?> ToDict(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            dict[tag.Key] = tag.Value;
        }

        return dict;
    }

    private sealed class MetricsFakeWorkflow : Workflow<string>, IWorkflowStepDispatcher<string>, IWorkflowCommandDispatcher<string>, IWorkflowQueryDispatcher<string>, IWorkflowChildResultDispatcher<string>
    {
        public override string EmptyState() => string.Empty;

        private static readonly Dictionary<string, StepDescriptor<string>> StepDescriptors = new()
        {
            ["Ok"] = new("Ok", typeof(NoInput), static (w, _, _) => ((MetricsFakeWorkflow)w).OkStep()),
            ["Boom"] = new("Boom", typeof(NoInput), static (w, _, _) => ((MetricsFakeWorkflow)w).BoomStep()),
        };

        private static readonly Dictionary<Type, CommandDescriptor<string>> CommandDescriptors = new()
        {
            [typeof(Ping)] = new(typeof(Ping), nameof(Ping), static (w, ctx, cmd) => ((MetricsFakeWorkflow)w).PingHandler((Ping)cmd)),
        };

        bool IWorkflowStepDispatcher<string>.TryGetStep(string stepName, out StepDescriptor<string> descriptor) =>
            StepDescriptors.TryGetValue(stepName, out descriptor);

        IReadOnlyCollection<string> IWorkflowStepDispatcher<string>.StepNames => StepDescriptors.Keys;

        bool IWorkflowQueryDispatcher<string>.TryGetQuery(Type queryType, out QueryDescriptor<string> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<string>.TryGetChildResultHandler(out ChildResultDescriptor<string> descriptor) { descriptor = default; return false; }

        bool IWorkflowCommandDispatcher<string>.TryGetHandler(Type commandType, out CommandDescriptor<string> descriptor) =>
            CommandDescriptors.TryGetValue(commandType, out descriptor);

        public Task<StepEffect<string>> OkStep() => Task.FromResult(StepEffects.ThenComplete());

        public Task<StepEffect<string>> BoomStep() => throw new InvalidOperationException("boom");

        public CommandEffect<string> PingHandler(Ping cmd) => Effects.Reply("pong");
    }

    private sealed record Ping;

    [Fact]
    public async Task StepDescriptorInvoke_Success_RecordsStepDurationTaggedOk()
    {
        var (listener, measurements) = Listen();
        using var _ = listener;

        var workflow = new MetricsFakeWorkflow();
        ((IWorkflowStepDispatcher<string>)workflow).TryGetStep("Ok", out var descriptor);

        await descriptor.Invoke(workflow, "state", null, attempt: 1, CancellationToken.None);

        var m = Assert.Single(measurements, m =>
            m.InstrumentName == "sagant.step.duration" && (string?)m.Tags["workflow.type"] == nameof(MetricsFakeWorkflow));
        Assert.Equal("Ok", m.Tags["step.name"]);
        Assert.Equal(1, m.Tags["attempt"]);
        Assert.Equal("ok", m.Tags["outcome"]);
    }

    [Fact]
    public async Task StepDescriptorInvoke_Throws_RecordsStepDurationTaggedError_ThenRethrows()
    {
        var (listener, measurements) = Listen();
        using var _ = listener;

        var workflow = new MetricsFakeWorkflow();
        ((IWorkflowStepDispatcher<string>)workflow).TryGetStep("Boom", out var descriptor);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            descriptor.Invoke(workflow, "state", null, attempt: 3, CancellationToken.None));

        var m = Assert.Single(measurements, m =>
            m.InstrumentName == "sagant.step.duration" && (string?)m.Tags["workflow.type"] == nameof(MetricsFakeWorkflow));
        Assert.Equal("Boom", m.Tags["step.name"]);
        Assert.Equal(3, m.Tags["attempt"]);
        Assert.Equal("error", m.Tags["outcome"]);
    }

    [Fact]
    public void CommandDescriptorInvoke_Success_RecordsCommandCount()
    {
        var (listener, measurements) = Listen();
        using var _ = listener;

        var workflow = new MetricsFakeWorkflow();
        ((IWorkflowCommandDispatcher<string>)workflow).TryGetHandler(typeof(Ping), out var descriptor);

        descriptor.Invoke(workflow, "state", new Ping());

        var m = Assert.Single(measurements, m =>
            m.InstrumentName == "sagant.command.count" && (string?)m.Tags["workflow.type"] == nameof(MetricsFakeWorkflow));
        Assert.Equal(nameof(Ping), m.Tags["command.type"]);
    }

    private sealed record CounterState(int Value);

    private sealed record Begin;

    private sealed class PauseEndWorkflow : Workflow<CounterState>, IWorkflowStepDispatcher<CounterState>, IWorkflowCommandDispatcher<CounterState>, IWorkflowQueryDispatcher<CounterState>, IWorkflowChildResultDispatcher<CounterState>
    {
        public static class Steps
        {
            public static readonly StepRef<PauseEndWorkflow, NoInput> WaitThenEnd = new("WaitThenEnd");
            public static readonly StepRef<PauseEndWorkflow, NoInput> Finish = new("Finish");
        }

        private static readonly Dictionary<string, StepDescriptor<CounterState>> StepDescriptors = new()
        {
            ["WaitThenEnd"] = new("WaitThenEnd", typeof(NoInput), static (w, _, _) => ((PauseEndWorkflow)w).WaitStep()),
            ["Finish"] = new("Finish", typeof(NoInput), static (w, _, _) => ((PauseEndWorkflow)w).FinishStep()),
        };

        private static readonly Dictionary<Type, CommandDescriptor<CounterState>> CommandDescriptors = new()
        {
            [typeof(Begin)] = new(typeof(Begin), nameof(Begin), static (w, ctx, _) => ((PauseEndWorkflow)w).Start()),
        };

        public override CounterState EmptyState() => new(0);

        bool IWorkflowStepDispatcher<CounterState>.TryGetStep(string stepName, out StepDescriptor<CounterState> descriptor) =>
            StepDescriptors.TryGetValue(stepName, out descriptor);

        IReadOnlyCollection<string> IWorkflowStepDispatcher<CounterState>.StepNames => StepDescriptors.Keys;

        bool IWorkflowQueryDispatcher<CounterState>.TryGetQuery(Type queryType, out QueryDescriptor<CounterState> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<CounterState>.TryGetChildResultHandler(out ChildResultDescriptor<CounterState> descriptor) { descriptor = default; return false; }

        bool IWorkflowCommandDispatcher<CounterState>.TryGetHandler(Type commandType, out CommandDescriptor<CounterState> descriptor) =>
            CommandDescriptors.TryGetValue(commandType, out descriptor);

        public CommandEffect<CounterState> Start() => Effects.TransitionTo(Steps.WaitThenEnd);

        public Task<StepEffect<CounterState>> WaitStep() =>
            Task.FromResult(StepEffects.ThenPause("waiting"));

        public Task<StepEffect<CounterState>> FinishStep() =>
            Task.FromResult(StepEffects.ThenComplete());
    }

    // WorkflowDiagnostics.Meter is process-global with no per-test session, and — unlike a span,
    // which every test can scope via a unique persistence id — these status-change metrics
    // deliberately carry no per-instance tag (see WorkflowDiagnostics.RecordStatusChange's doc
    // comment). PauseEndWorkflow's own type name is this file's disambiguator: it's declared only
    // here, so no concurrently-running test elsewhere in this assembly (e.g. WorkflowTestHarnessTests'
    // CounterWorkflow also reaching ThenEnd()) can be mistaken for these measurements — every
    // assertion below filters on it explicitly rather than relying on Single() over the whole bag.
    private static IEnumerable<Measurement> For(ConcurrentBag<Measurement> measurements, string instrumentName) =>
        measurements.Where(m => m.InstrumentName == instrumentName && (string?)m.Tags["workflow.type"] == nameof(PauseEndWorkflow));

    [Fact]
    public async Task Harness_PauseTransition_RecordsWorkflowPaused()
    {
        var (listener, measurements) = Listen();
        using var _ = listener;

        var harness = new WorkflowTestHarness<PauseEndWorkflow, CounterState>(new PauseEndWorkflow());
        harness.RunCommand(new Begin());
        await harness.RunStep(PauseEndWorkflow.Steps.WaitThenEnd);

        Assert.Single(For(measurements, "sagant.workflow.paused"));
        Assert.Empty(For(measurements, "sagant.workflow.resumed"));
    }

    [Fact]
    public async Task Harness_CompletingRun_RecordsFinishedTaggedCompleted()
    {
        var (listener, measurements) = Listen();
        using var _ = listener;

        var harness = new WorkflowTestHarness<PauseEndWorkflow, CounterState>(new PauseEndWorkflow());
        await harness.RunStep(PauseEndWorkflow.Steps.Finish);

        var m = Assert.Single(For(measurements, "sagant.workflow.finished"));
        Assert.Equal("completed", m.Tags["outcome"]);
    }
}
