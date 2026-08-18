using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Persistence.Query.InMemory;
using Akka.Streams;
using Sagant.Clients;
using Sagant.Effects;
using Sagant.Execution;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// The read side of the visibility seam, against a real journal: listing instances by status and type
/// without holding an id, and reading one instance's whole history back.
///
/// Both answer from recorded events alone, which is what makes them work for an instance nobody has
/// an <c>IActorRef</c> for.
/// </summary>
public class JournalVisibilityTests : WorkflowActorTestKit
{
    public JournalVisibilityTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private IReadJournal ReadJournal =>
        PersistenceQuery.Get(Sys).ReadJournalFor<InMemoryReadJournal>(InMemoryReadJournal.Identifier);

    private JournalWorkflowVisibilityQuery Visibility => new(ReadJournal, Sys.Materializer());

    private JournalWorkflowEventFeed Feed => new(ReadJournal, Sys.Materializer());

    /// <summary>A finished run stays answerable from its events — the case
    /// <c>IWorkflowClient.For(id)</c> covers only while an entity is alive to ask.</summary>
    [Fact]
    public async Task FinishedWorkflow_IsListedWithItsOutcome()
    {
        var actor = CreateActor("VisibilityFinished", Script()
            .Step("OnlyStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("OnlyStep")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var record = await Visibility.GetAsync("VisibilityFinished");
            Assert.NotNull(record);
            Assert.Equal(WorkflowStatus.Finished, record!.Status);
            Assert.IsType<WorkflowOutcome.Completed>(record.Outcome);
            Assert.NotNull(record.EndedAt);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>Filtering by status is the question that has no answer today: it needs every instance
    /// enumerated, which no id-keyed lookup can do.</summary>
    [Fact]
    public async Task ListAsync_FiltersByStatus()
    {
        var paused = CreateActor("VisibilityPaused", Script()
            .Step("PausingStep", (_, _) => Task.FromResult(
                new StepEffectsBuilder<TestState>().ThenPause("awaiting approval")))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("PausingStep")).ThenReply("accepted")));
        paused.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var records = new List<WorkflowVisibilityRecord>();
            await foreach (var record in Visibility.ListAsync(
                new WorkflowVisibilityFilter(Statuses: new[] { WorkflowStatus.Paused })))
            {
                records.Add(record);
            }

            Assert.Contains(records, r => r.EntityId == "VisibilityPaused");
            Assert.All(records, r => Assert.Equal(WorkflowStatus.Paused, r.Status));
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>Guarantee V6: a child started through <c>AwaitChildren</c> reports its parent's id and
    /// type in its own listing, so a caller can answer "which run does this belong to" from the
    /// visibility query alone.</summary>
    [Fact]
    public async Task ChildInstance_ReportsItsParentInTheVisibilityRecord()
    {
        const string childPersistenceId = nameof(ChildInstance_ReportsItsParentInTheVisibilityRecord) + "Child";
        const string parentPersistenceId = nameof(ChildInstance_ReportsItsParentInTheVisibilityRecord) + "Parent";

        var childActor = CreateActor(childPersistenceId, Script()
            .Step("Run", (state, _) => Task.FromResult(new StepEffectsBuilder<TestState>().UpdateState(state).ThenComplete()))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("Run")).ThenReply("accepted")));

        var parentScript = Script()
            .Step("StartChildren", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().AwaitChildren(
                new[] { new StepEffectsBuilder<TestState>().Child<ScriptableWorkflow>(childPersistenceId, new StartWorkflow(1)) },
                Step<ChildGroupResult>("OnResolved"))))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>().TransitionTo(Step("StartChildren")).ThenReply("accepted"));
        var parentActor = CreateAltActor(parentPersistenceId, parentScript);

        var childRelay = Sys.ActorOf(Props.Create(() => new RelayProducerAdapter(childActor)));
        var parentRelay = Sys.ActorOf(Props.Create(() => new RelayProducerAdapter(parentActor)));

        var registry = WorkflowHandleRegistryProvider.Instance.Apply(Sys);
        registry.Register<ScriptableWorkflow, TestState>(CreateTestProbe().Ref, childRelay);
        registry.Register<AltScriptableWorkflow, TestState>(CreateTestProbe().Ref, parentRelay);

        parentActor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var record = await Visibility.GetAsync(childPersistenceId);
            Assert.NotNull(record);
            Assert.Equal(parentPersistenceId, record!.ParentWorkflowId);
            Assert.Equal(nameof(AltScriptableWorkflow), record.ParentWorkflowType);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>A workflow with no parent — the overwhelmingly common case — reports both parent
    /// fields as plain <c>null</c>.</summary>
    [Fact]
    public async Task WorkflowWithNoParent_ReportsNoParentFields()
    {
        var actor = CreateActor("VisibilityNoParent", Script()
            .Step("OnlyStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("OnlyStep")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var record = await Visibility.GetAsync("VisibilityNoParent");
            Assert.NotNull(record);
            Assert.Null(record!.ParentWorkflowId);
            Assert.Null(record.ParentWorkflowType);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>One instance's history, including the retry that a run recovers from — the detail
    /// that survives nowhere else once the run goes on to succeed.</summary>
    [Fact]
    public async Task ReadEntity_ReportsTheRetriedAttemptsError()
    {
        var attempt = 0;
        var settings = Sagant.Settings.WorkflowSettings.Create()
            .StepRecovery(Step("FlakyStep"),
                Sagant.Settings.RecoverStrategy.WithMaxRetries(1).FailoverTo(Step("FlakyStep")))
            .Build();

        var actor = CreateActor("VisibilityRetry", Script()
            .Step("FlakyStep", (_, _) =>
            {
                attempt++;
                return attempt == 1
                    ? Task.FromException<StepEffect<TestState>>(new InvalidOperationException("boom"))
                    : Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete());
            })
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("FlakyStep")).ThenReply("accepted")), settings);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var causes = new List<TransitionCause>();
            await foreach (var item in Feed.ReadEntity("VisibilityRetry"))
            {
                if (item.Event is WorkflowEvent.CausedEvent caused)
                {
                    causes.Add(caused.Cause);
                }
            }

            var failure = Assert.Single(causes.OfType<TransitionCause.StepFailed>());
            Assert.Equal("FlakyStep", failure.StepName);
            Assert.Contains("boom", failure.Error);
            Assert.True(failure.WillRetry);
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>Every event is written under both tags, so a reader follows all workflows or one
    /// type without knowing any entity id. The actor tags as it persists, so this holds whichever
    /// journal an application configures.</summary>
    [Fact]
    public async Task Read_ByTag_ReturnsEventsWithoutKnowingAnyEntityId()
    {
        var actor = CreateActor("VisibilityTagged", Script()
            .Step("OnlyStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("OnlyStep")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var byTypeTag = new List<WorkflowFeedItem>();
            await foreach (var item in Feed.Read(WorkflowEventTags.ForWorkflowType(nameof(ScriptableWorkflow))))
            {
                byTypeTag.Add(item);
            }

            Assert.Contains(byTypeTag, i => i.EntityId == "VisibilityTagged");

            var byAllTag = new List<WorkflowFeedItem>();
            await foreach (var item in Feed.Read(WorkflowEventTags.All))
            {
                byAllTag.Add(item);
            }

            Assert.Contains(byAllTag, i => i.EntityId == "VisibilityTagged");
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Guarantee G5's point: a restart releases the history behind it, so a workflow that runs
    /// indefinitely stops accumulating events without bound. The instance keeps running under the
    /// same id, and its state survives the cycle.
    /// </summary>
    [Fact]
    public async Task Restart_ReleasesTheHistoryBehindIt_AndKeepsTheInstanceRunning()
    {
        var cycles = 0;
        var actor = CreateActor("VisibilityRestart", Script()
            .Step("Loop", (state, _) =>
            {
                // Three cycles, then settle so the assertion below races nothing.
                cycles++;
                return Task.FromResult(cycles < 3
                    ? new StepEffectsBuilder<TestState>()
                        .UpdateState(new TestState { Value = $"cycle-{cycles}" })
                        .ThenRestartAt(Step("Loop"), "next cycle")
                    : new StepEffectsBuilder<TestState>().ThenPause());
            })
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("Loop")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            var diagnostics = ExpectMsg<Diagnostics<TestState>>();
            Assert.Equal(WorkflowStatus.Paused, diagnostics.Envelope.Status);
            Assert.Equal("cycle-2", diagnostics.Envelope.UserState.Value);

            // Everything before the last restart is gone, so what remains describes the final cycle
            // alone; the other two are gone with it.
            var remaining = new List<WorkflowEvent>();
            await foreach (var item in Feed.ReadEntity("VisibilityRestart"))
            {
                remaining.Add(item.Event);
            }

            // Two restarts ran, so an instance that kept its whole history would hold both. At most
            // the most recent one is left: everything before a restart is released, which is what
            // stops a perpetual workflow accumulating without bound.
            //
            // Whether that final batch has been released yet is a matter of when the journal gets to
            // it, so this asserts the bound the count stays under, treating the exact value as
            // unpredictable.
            Assert.True(
                remaining.OfType<WorkflowEvent.RunRestarted>().Count() <= 1,
                $"expected the earlier cycle's restart to be released, saw: {string.Join(", ", remaining.Select(e => e.GetType().Name))}");
        }, TimeSpan.FromSeconds(10));
    }

    /// <summary>Delivery bookkeeping stays out of the feed, so both transports carry the same
    /// sequence.</summary>
    [Fact]
    public async Task Feed_OmitsDeliveryBookkeeping()
    {
        var actor = CreateActor("VisibilityBookkeeping", Script()
            .Step("OnlyStep", (_, _) => Task.FromResult(new StepEffectsBuilder<TestState>().ThenComplete()))
            .Command<StartWorkflow>((_, _) =>
                new EffectsBuilder<TestState>().TransitionTo(Step("OnlyStep")).ThenReply("accepted")));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        await AwaitAssertAsync(async () =>
        {
            var events = new List<WorkflowEvent>();
            await foreach (var item in Feed.ReadEntity("VisibilityBookkeeping"))
            {
                events.Add(item.Event);
            }

            Assert.NotEmpty(events);
            Assert.Empty(events.OfType<WorkflowEvent.SeqNrRecorded>());
            Assert.Empty(events.OfType<WorkflowEvent.IdempotencyRecorded>());
        }, TimeSpan.FromSeconds(10));
    }
}
