using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Tests;

public sealed record EchoPing(string Text);

public sealed record EchoPeek;

public sealed record EchoState(string Value)
{
    public static EchoState Empty() => new("initial");
}

// Top-level, not nested: the source generator only handles top-level partial classes today —
// nested-type support is a known gap, not exercised or fixed here. Sagant.Runtime.Akka.Tests'
// WorkflowClientTests has its own identically-shaped copy, deliberately — a real ClusterSharding
// round-trip needs a [WorkflowStep] class the generator actually runs against IN that project;
// test projects don't share generated code with each other.
public partial class EchoWorkflow : Workflow<EchoState>
{
    public override EchoState EmptyState() => EchoState.Empty();

    // Declared synchronously — the generator wraps it in Task.FromResult, so a step with nothing to
    // await doesn't have to say so.
    [WorkflowStep]
    public StepEffect<EchoState> EchoStep(string text, StepContext<EchoState> ctx) =>
        StepEffects.UpdateState(ctx.State with { Value = text }).ThenComplete();

    [WorkflowCommandHandler]
    public CommandEffect<EchoState> Handle(EchoPing ping, CommandContext<EchoState> ctx) =>
        Effects.TransitionTo(Steps.EchoStep, ping.Text).ThenReply("accepted");

    [WorkflowQuery]
    public QueryEffect Peek(EchoPeek peek, QueryContext<EchoState> ctx) =>
        QueryEffects.Reply(ctx.State.Value);
}

/// <summary>
/// Isolates whether the generator's actual compiled output works for a real project-level class,
/// independent of clustering/networking — narrows down where a dispatch failure lives. The real
/// ClusterSharding round-trip (WithWorkflow -> real WorkflowRef -> real shard region) is proven
/// more thoroughly by OrderFulfillment.Tests already, so this deliberately doesn't repeat a
/// cluster-join here.
/// </summary>
public class EchoWorkflowGeneratorSanityTests
{
    [Fact]
    public void EchoWorkflow_ImplementsCommandDispatcher_AndResolvesEchoPingHandler()
    {
        var workflow = new EchoWorkflow();
        IWorkflowCommandDispatcher<EchoState> dispatcher = workflow;

        var found = dispatcher.TryGetHandler(typeof(EchoPing), out var descriptor);

        Assert.True(found);
        Assert.Equal(typeof(EchoPing), descriptor.CommandType);
    }

    [Fact]
    public void EchoWorkflow_ImplementsStepDispatcher_AndResolvesEchoStep()
    {
        var workflow = new EchoWorkflow();
        IWorkflowStepDispatcher<EchoState> dispatcher = workflow;

        var found = dispatcher.TryGetStep("EchoStep", out var descriptor);

        Assert.True(found);
        Assert.Equal("EchoStep", descriptor.Name);
    }

    [Fact]
    public void EchoWorkflow_ImplementsQueryDispatcher_AndResolvesEchoPeek()
    {
        var workflow = new EchoWorkflow();
        IWorkflowQueryDispatcher<EchoState> dispatcher = workflow;

        var found = dispatcher.TryGetQuery(typeof(EchoPeek), out var descriptor);

        Assert.True(found);
        Assert.Equal(typeof(EchoPeek), descriptor.QueryType);
        Assert.Equal("EchoPeek", descriptor.QueryTypeName);
    }

    /// <summary>The generator wraps a synchronously-declared step, so a driver only ever sees the
    /// <c>Task</c> shape regardless of how the step was written.</summary>
    [Fact]
    public async Task EchoWorkflow_SynchronouslyDeclaredStep_IsDrivenAsATask()
    {
        var workflow = new EchoWorkflow();
        IWorkflowStepDispatcher<EchoState> dispatcher = workflow;
        dispatcher.TryGetStep("EchoStep", out var descriptor);

        var effect = await descriptor.Invoke(workflow, EchoState.Empty(), "echoed", attempt: 1, CancellationToken.None);

        var persistence = Assert.IsType<PersistenceEffect<EchoState>.UpdateState>(effect.Persistence);
        Assert.Equal("echoed", persistence.NewState.Value);
    }

    [Fact]
    public async Task EchoWorkflow_QueryReadsStateFromItsContext()
    {
        var workflow = new EchoWorkflow();
        IWorkflowQueryDispatcher<EchoState> dispatcher = workflow;
        dispatcher.TryGetQuery(typeof(EchoPeek), out var descriptor);

        var effect = await descriptor.Invoke(workflow, new EchoState("observed"), new EchoPeek(), CancellationToken.None);

        var reply = Assert.IsType<Reply.ReplyValue>(effect.Reply);
        Assert.Equal("observed", reply.Value);
    }
}
