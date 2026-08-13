using Akka.Actor;
using Akka.TestKit;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Tests.Support;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// Cluster sharding decides an entity is idle from the last message it <em>routed</em> to it
/// (<c>Shard.TouchLastMessageTimestamp</c>, reached from <c>DeliverMessage</c>), so an entity working
/// away on a step it started minutes ago looks idle from the outside. These cover the keep-alive that
/// closes that gap: while the entity holds work of its own, it sends itself a message through its
/// shard region, which is the one path that touches that clock.
/// </summary>
public class WorkflowResidencyKeepAliveTests : WorkflowActorTestKit
{
    public WorkflowResidencyKeepAliveTests() : base(Config)
    {
    }

    private const string Config = """
        akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
        akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.inmem"
        akka.loglevel = OFF
        """;

    private TestProbe RegisterRegion()
    {
        var region = CreateTestProbe();
        WorkflowHandleRegistryProvider.Instance.Apply(Sys)
            .Register<ScriptableWorkflow, TestState>(region.Ref, ActorRefs.Nobody);
        return region;
    }

    [Fact]
    public void WhileAStepRuns_TheEntityKeepsItsShardEntryWarm()
    {
        const string entityId = nameof(WhileAStepRuns_TheEntityKeepsItsShardEntryWarm);
        var region = RegisterRegion();
        var hanging = new TaskCompletionSource<StepEffect<TestState>>();

        var script = Script()
            .Step("Slow", (_, _) => hanging.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("Slow"))
                .ThenReply("accepted"));

        var actor = CreateActor(entityId, script, keepAliveInterval: TimeSpan.FromMilliseconds(200));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        // Addressed to this entity's own id, so the shard routes it back down and touches the clock
        // on the way through.
        var first = region.ExpectMsg<WorkflowEnvelope>(TimeSpan.FromSeconds(3));
        Assert.Equal(entityId, first.EntityId);
        Assert.IsType<EntityKeepAlive>(first.Message);

        // Repeating, so a step outlasting any number of idle windows stays resident.
        region.ExpectMsg<WorkflowEnvelope>(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void OnceTheStepSettles_TheEntityStopsKeepingItselfWarm()
    {
        const string entityId = nameof(OnceTheStepSettles_TheEntityStopsKeepingItselfWarm);
        var region = RegisterRegion();
        var hanging = new TaskCompletionSource<StepEffect<TestState>>();

        var script = Script()
            .Step("Slow", (_, _) => hanging.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("Slow"))
                .ThenReply("accepted"));

        var actor = CreateActor(entityId, script, keepAliveInterval: TimeSpan.FromMilliseconds(200));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();
        region.ExpectMsg<WorkflowEnvelope>(TimeSpan.FromSeconds(3));

        hanging.SetResult(new StepEffectsBuilder<TestState>().ThenComplete());
        AwaitAssert(() =>
        {
            actor.Tell(new GetDiagnostics<TestState>(), TestActor);
            Assert.Equal(WorkflowStatus.Finished, ExpectMsg<Diagnostics<TestState>>().Envelope.Status);
        }, TimeSpan.FromSeconds(10));

        // A tick already in flight as the step settled may still land; what matters is that they stop.
        region.ReceiveWhile(TimeSpan.FromMilliseconds(400), m => m);
        region.ExpectNoMsg(TimeSpan.FromSeconds(1));
    }

    /// <summary>
    /// An idle entity is exactly what passivation is for, so the keep-alive stays quiet unless the
    /// entity is holding work — a paused instance goes to sleep as the deployment asked.
    /// </summary>
    [Fact]
    public void AnIdleEntity_KeepsNothingWarm()
    {
        var region = RegisterRegion();

        var script = Script()
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .Pause("waiting for approval")
                .ThenReply("accepted"));

        var actor = CreateActor(
            nameof(AnIdleEntity_KeepsNothingWarm), script, keepAliveInterval: TimeSpan.FromMilliseconds(200));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        region.ExpectNoMsg(TimeSpan.FromSeconds(1));
    }

    /// <summary>
    /// A step waiting out its retry backoff is holding work just as much as one that is running: the
    /// delay lives on an in-memory timer, so an entity passivated during it stops without anything
    /// scheduled to bring it back.
    /// </summary>
    [Fact]
    public void WhileARetryBackoffIsPending_TheEntityKeepsItsShardEntryWarm()
    {
        const string entityId = nameof(WhileARetryBackoffIsPending_TheEntityKeepsItsShardEntryWarm);
        var region = RegisterRegion();

        var script = Script()
            .Step("Flaky", (_, _) => throw new InvalidOperationException("gateway down"))
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("Flaky"))
                .ThenReply("accepted"));

        var settings = Sagant.Settings.WorkflowSettings.Create()
            .StepRecovery(
                Step("Flaky"),
                Sagant.Settings.RecoverStrategy.WithMaxRetries(5)
                    .ThenFail()
                    .WithBackoff(_ => TimeSpan.FromSeconds(30)))
            .Build();

        var actor = CreateActor(entityId, script, settings, keepAliveInterval: TimeSpan.FromMilliseconds(200));
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        var envelope = region.ExpectMsg<WorkflowEnvelope>(TimeSpan.FromSeconds(3));
        Assert.IsType<EntityKeepAlive>(envelope.Message);
    }

    /// <summary>
    /// The keep-alive exists to serve idle passivation, so a deployment that leaves passivation off —
    /// the default — pays nothing for it.
    /// </summary>
    [Fact]
    public void WithNoKeepAliveConfigured_NothingIsSent()
    {
        var region = RegisterRegion();
        var hanging = new TaskCompletionSource<StepEffect<TestState>>();

        var script = Script()
            .Step("Slow", (_, _) => hanging.Task)
            .Command<StartWorkflow>((_, _) => new EffectsBuilder<TestState>()
                .TransitionTo(Step("Slow"))
                .ThenReply("accepted"));

        var actor = CreateActor(nameof(WithNoKeepAliveConfigured_NothingIsSent), script);
        actor.Tell(new StartWorkflow(1), TestActor);
        ExpectMsg<string>();

        region.ExpectNoMsg(TimeSpan.FromSeconds(1));
    }
}
