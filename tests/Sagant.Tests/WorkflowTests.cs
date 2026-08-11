using Sagant.Effects;
using Sagant.Settings;

namespace Sagant.Tests;

public class WorkflowTests
{
    private sealed class TestWorkflow : Workflow<string>
    {
        public override string EmptyState() => string.Empty;

        public EffectsBuilder<string> PublicEffects => Effects;
        public StepEffectsBuilder<string> PublicStepEffects => StepEffects;
        public QueryEffectsBuilder PublicQueryEffects => QueryEffects;
    }

    /// <summary>
    /// A fresh instance starts at whatever the workflow says empty means. There is no default: the
    /// member is abstract precisely because <c>default(TState)</c> is <c>null</c> for the record
    /// types state is usually written as, and a workflow that omitted it would fail inside its first
    /// step rather than at the declaration that caused it.
    /// </summary>
    [Fact]
    public void EmptyState_ComesFromTheWorkflow()
    {
        var workflow = new TestWorkflow();

        Assert.Equal(string.Empty, workflow.EmptyState());
    }

    [Fact]
    public void Settings_DefaultImplementation_ReturnsWorkflowSettingsDefault()
    {
        var workflow = new TestWorkflow();

        Assert.Same(WorkflowSettings.Default, workflow.Settings());
    }

    /// <summary>
    /// A workflow instance carries no state of its own — state reaches a handler through its
    /// context, which is what keeps a step suspended at an await and a handler dispatched while it
    /// waits from observing each other. This asserts the absence structurally: no public or
    /// non-public instance field on the base class holds <c>TState</c>.
    /// </summary>
    [Fact]
    public void Workflow_HoldsNoInstanceStateField()
    {
        var stateFields = typeof(Workflow<string>)
            .GetFields(System.Reflection.BindingFlags.Instance
                | System.Reflection.BindingFlags.Public
                | System.Reflection.BindingFlags.NonPublic)
            .Where(f => f.FieldType == typeof(string))
            .ToList();

        Assert.Empty(stateFields);
    }

    [Fact]
    public void Effects_BuildersAreAvailableToDerivedWorkflows()
    {
        var workflow = new TestWorkflow();

        Assert.NotNull(workflow.PublicEffects);
        Assert.NotNull(workflow.PublicStepEffects);
        Assert.NotNull(workflow.PublicQueryEffects);
    }

    /// <summary>The query builder carries nothing between calls, so every access hands back the one
    /// shared instance rather than allocating per handler invocation.</summary>
    [Fact]
    public void QueryEffects_IsASharedInstance()
    {
        var workflow = new TestWorkflow();

        Assert.Same(workflow.PublicQueryEffects, workflow.PublicQueryEffects);
    }
}
