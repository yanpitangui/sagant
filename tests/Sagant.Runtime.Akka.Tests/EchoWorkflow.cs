using Sagant.Descriptors;
using Sagant.Effects;

namespace Sagant.Runtime.Akka.Tests;

public sealed record EchoPing(string Text);

public sealed record EchoState(string Value)
{
    public EchoState() : this("initial") { }
}

// Top-level: the source generator only handles top-level partial classes today. Nested-type
// support is a known gap, left alone here. A separate, identically-shaped
// copy from Sagant.Tests's EchoWorkflowGeneratorSanityTests.cs — that one exercises the generator's
// dispatcher output directly with zero Akka.NET; this one is what WorkflowClientTests below drives
// through a real ClusterSharding round-trip, which needs its own [WorkflowStep] class in THIS
// project for the generator to actually produce output for (test projects don't share generated
// code across each other).
public partial class EchoWorkflow : Workflow<EchoState>
{
    public override EchoState EmptyState() => new();

    [WorkflowStep]
    public Task<StepEffect<EchoState>> EchoStep(string text) =>
        Task.FromResult(StepEffects.UpdateState(new EchoState(text)).ThenComplete());

    [WorkflowCommandHandler]
    public CommandEffect<EchoState> Handle(EchoPing ping) =>
        Effects.TransitionTo(Steps.EchoStep, ping.Text).ThenReply("accepted");
}
