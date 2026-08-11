using Sagant.Runtime.Akka.Clustering;
using Sagant.Descriptors;
using Sagant.Effects;
using Akka.Actor;
using Akka.TestKit.Xunit2;

namespace Sagant.Runtime.Akka.Tests;

public class WorkflowHandleRegistryTests : TestKit
{
    private sealed class FakeWorkflow : Workflow<string>, IWorkflowStepDispatcher<string>, IWorkflowCommandDispatcher<string>, IWorkflowQueryDispatcher<string>, IWorkflowChildResultDispatcher<string>, IWorkflowTypeInfo
    {
        public override string EmptyState() => string.Empty;

        static string IWorkflowTypeInfo.WorkflowTypeName => "FakeWorkflow";
        public override string WorkflowTypeName => "FakeWorkflow";
        bool IWorkflowStepDispatcher<string>.TryGetStep(string stepName, out StepDescriptor<string> descriptor) { descriptor = default; return false; }
        IReadOnlyCollection<string> IWorkflowStepDispatcher<string>.StepNames => Array.Empty<string>();
        bool IWorkflowCommandDispatcher<string>.TryGetHandler(Type commandType, out CommandDescriptor<string> descriptor) { descriptor = default; return false; }
        bool IWorkflowQueryDispatcher<string>.TryGetQuery(Type queryType, out QueryDescriptor<string> descriptor) { descriptor = default; return false; }

        bool IWorkflowChildResultDispatcher<string>.TryGetChildResultHandler(out ChildResultDescriptor<string> descriptor) { descriptor = default; return false; }
    }

    [Fact]
    public void TryResolveByTypeName_RegisteredType_ReturnsTargets()
    {
        var registry = WorkflowHandleRegistryProvider.Instance.Apply(Sys);
        var shardRegion = CreateTestProbe().Ref;
        var producerAdapter = CreateTestProbe().Ref;
        registry.Register<FakeWorkflow, string>(shardRegion, producerAdapter);

        var resolved = registry.TryResolveByTypeName("FakeWorkflow", out var targets);

        Assert.True(resolved);
        Assert.Equal(shardRegion, targets.ShardRegion);
        Assert.Equal(producerAdapter, targets.ProducerAdapter);
    }

    [Fact]
    public void TryResolveByTypeName_UnregisteredType_ReturnsFalse()
    {
        var registry = WorkflowHandleRegistryProvider.Instance.Apply(Sys);

        Assert.False(registry.TryResolveByTypeName("NeverRegistered", out _));
    }
}
