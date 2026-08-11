using Sagant.Protocol;
using Sagant.Settings;
using Sagant.Effects;
using System.Collections.Concurrent;
using System.Diagnostics;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowTracingTests : WorkflowActorTestKit, IDisposable
{
    public WorkflowTracingTests() : base(Config)
    {
        _capturedActivities = new ConcurrentBag<Activity>();
        _startedActivities = new ConcurrentBag<Activity>();
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == WorkflowDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = a => _startedActivities.Add(a),
            ActivityStopped = a => _capturedActivities.Add(a),
        };
        ActivitySource.AddActivityListener(_listener);
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private readonly ActivityListener _listener;
    private readonly ConcurrentBag<Activity> _capturedActivities;
    private readonly ConcurrentBag<Activity> _startedActivities;

    // Explicit interface impl: TestKit's own public Dispose() (which shuts down the ActorSystem)
    // isn't virtual, so a same-named public method here would just hide it — xUnit always
    // disposes via the IDisposable reference, so this guarantees both run, in the right order.
    void IDisposable.Dispose()
    {
        _listener.Dispose();
        base.Dispose();
    }

    [Fact]
    public void HandlingCommand_StartsActivityNamedAfterWorkflowAndCommand()
    {
        var script = Script()
            .Step("Step", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Step")).ThenReply("accepted"));

        const string persistenceId = nameof(HandlingCommand_StartsActivityNamedAfterWorkflowAndCommand);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            Assert.Contains(_capturedActivities, a =>
                a.OperationName.EndsWith("StartWorkflow") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepExecution_StartsChildActivityTaggedWithStepName_MarkedOkOnSuccess()
    {
        var script = Script()
            .Step("ChargePayment", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("ChargePayment")).ThenReply("accepted"));

        const string persistenceId = nameof(StepExecution_StartsChildActivityTaggedWithStepName_MarkedOkOnSuccess);
        var actor = CreateActor(persistenceId, script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var stepActivity = Assert.Single(_capturedActivities, a =>
                a.OperationName.EndsWith("Step.ChargePayment") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
            Assert.Equal("ChargePayment", stepActivity.GetTagItem("workflow.step"));
            Assert.Equal(ActivityStatusCode.Ok, stepActivity.Status);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void TraceContext_SurvivesActorRestart_FirstSpanAfterRecoveryLinksToPreCrashTrace()
    {
        var neverCompletes = new TaskCompletionSource<StepEffect<TestState>>();
        var script = Script()
            .Step("SlowStep", (_, _) => neverCompletes.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("SlowStep")).ThenReply("accepted"));

        // The ActivitySource is process-global, and other test classes running concurrently may
        // use the same step name ("SlowStep" collides with WorkflowEntityActorTests's
        // CrashRecovery test) — every span this test cares about must be scoped to this specific
        // persistenceId, not matched by operation name alone.
        const string persistenceId = nameof(TraceContext_SurvivesActorRestart_FirstSpanAfterRecoveryLinksToPreCrashTrace);
        var actor1 = CreateActor(persistenceId, script);
        actor1.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // StartStep's span is created synchronously before PipeTo even fires, so it's already
        // started (though never stopped, since the step never completes) by the time we get here.
        string? preCrashTraceId = null;
        AwaitAssert(() =>
        {
            preCrashTraceId = _startedActivities
                .FirstOrDefault(a => a.OperationName.EndsWith("Step.SlowStep") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId)
                ?.TraceId.ToString();
            Assert.NotNull(preCrashTraceId);
        }, TimeSpan.FromSeconds(10));

        Watch(actor1);
        Sys.Stop(actor1);
        ExpectTerminated(actor1);

        CreateActor(persistenceId, script);

        AwaitAssert(() =>
        {
            var linked = _startedActivities.FirstOrDefault(a =>
                (string?)a.GetTagItem("workflow.persistence_id") == persistenceId &&
                a.TraceId.ToString() != preCrashTraceId &&
                a.Links.Any(l => l.Context.TraceId.ToString() == preCrashTraceId));
            Assert.NotNull(linked);
        }, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void StepRetry_ProducesOneActivityPerAttempt_FailedAttemptsMarkedError()
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
        const string persistenceId = nameof(StepRetry_ProducesOneActivityPerAttempt_FailedAttemptsMarkedError);
        var actor = CreateActor(persistenceId, script, settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var stepActivities = _capturedActivities
                .Where(a => a.OperationName.EndsWith("Step.FlakyStep") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId)
                .ToList();
            Assert.Equal(3, stepActivities.Count);
            Assert.Equal(2, stepActivities.Count(a => a.Status == ActivityStatusCode.Error));
            Assert.Equal(1, stepActivities.Count(a => a.Status == ActivityStatusCode.Ok));

            // Siblings, not a chain: attempt 2 isn't nested inside attempt 1 (which already ended,
            // Error, by the time attempt 2 starts) — every retry shares the SAME parent (whatever
            // triggered FlakyStep in the first place, here the StartWorkflow command), or a trace
            // waterfall view would misrepresent independent sequential retries as one attempt
            // containing the next.
            var parentIds = stepActivities.Select(a => a.ParentSpanId).Distinct().ToList();
            var attemptSpanIds = stepActivities.Select(a => a.SpanId).ToHashSet();
            Assert.Single(parentIds);
            Assert.DoesNotContain(parentIds[0], attemptSpanIds);
        }, TimeSpan.FromSeconds(10));
    }

    private sealed record Ping1;
    private sealed record Peek;
    private sealed record Ping2;

    [Fact]
    public void PureQueryCommand_DoesNotAdvanceTraceChain_NextRealCommandParentsOffPriorRealCommand()
    {
        // Ping1/Ping2 each persist (UpdateState), so they're not no-op effects and legitimately
        // advance the chain. Peek replies with neither persistence nor a transition — a pure
        // query, like a sample's GetOrderState — and must not become Ping2's parent.
        var script = Script()
            .Command<Ping1>((state, _) => new EffectsBuilder<TestState>().UpdateState(state).Reply("pong1"))
            .Command<Peek>((_, _) => new EffectsBuilder<TestState>().Reply("peeked"))
            .Command<Ping2>((state, _) => new EffectsBuilder<TestState>().UpdateState(state).Reply("pong2"));

        const string persistenceId = nameof(PureQueryCommand_DoesNotAdvanceTraceChain_NextRealCommandParentsOffPriorRealCommand);
        var actor = CreateActor(persistenceId, script);

        actor.Tell(new Ping1(), TestActor);
        ExpectMsg<string>();
        actor.Tell(new Peek(), TestActor);
        ExpectMsg<string>();
        actor.Tell(new Ping2(), TestActor);
        ExpectMsg<string>();

        AwaitAssert(() =>
        {
            var ping1 = Assert.Single(_capturedActivities, a => a.OperationName.EndsWith("Ping1") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
            var peek = Assert.Single(_capturedActivities, a => a.OperationName.EndsWith("Peek") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);
            var ping2 = Assert.Single(_capturedActivities, a => a.OperationName.EndsWith("Ping2") && (string?)a.GetTagItem("workflow.persistence_id") == persistenceId);

            Assert.Equal(ping1.SpanId, ping2.ParentSpanId);
            Assert.NotEqual(peek.SpanId, ping2.ParentSpanId);
        }, TimeSpan.FromSeconds(10));
    }
}
