using Akka.Configuration;
using Sagant.Effects;
using Sagant.Runtime.Akka.Clustering;
using Sagant.Runtime.Akka.Execution;
using Sagant.Settings;

namespace Sagant.Runtime.Akka.Tests;

/// <summary>
/// What every instance of one registration shares. An activation reads these off the profile, so the
/// profile has to carry what deriving them produces.
/// </summary>
public class WorkflowTypeProfileTests
{
    private sealed class ProfiledWorkflow : Workflow<EchoState>
    {
        public override EchoState EmptyState() => new();

        public override string WorkflowTypeName => "ProfiledWorkflow";

        public override WorkflowSettings Settings() => WorkflowSettings.Create()
            .DefaultStepTimeout(TimeSpan.FromSeconds(7))
            .IdempotencyLedgerCapacity(11)
            .SeqNrDedupCapacity(3)
            .Build();
    }

    private static Config ConfigWithHandoff(string handoffTimeout) =>
        ConfigurationFactory.ParseString($"akka.cluster.sharding.handoff-timeout = {handoffTimeout}");

    [Fact]
    public void ItCarriesTheSettingsTheWorkflowDeclared()
    {
        var profile = WorkflowTypeProfile<EchoState>.For(new ProfiledWorkflow(), ConfigWithHandoff("60s"));

        Assert.Equal(TimeSpan.FromSeconds(7), profile.Settings.StepTimeoutFor("AnyStep"));
        Assert.Equal(11, profile.Settings.IdempotencyLedgerCapacity);
        Assert.Equal(3, profile.Settings.SeqNrDedupCapacity);
    }

    /// <summary>The tags an event carries decide which streams a reader can follow it on, so they have
    /// to match what the entity derives for itself.</summary>
    [Fact]
    public void ItCarriesTheSameTagsTheEntityWouldHaveBuilt()
    {
        var profile = WorkflowTypeProfile<EchoState>.For(new ProfiledWorkflow(), ConfigWithHandoff("60s"));

        Assert.Equal(WorkflowEventTags.For("ProfiledWorkflow"), profile.EventTags);
        Assert.Equal(WorkflowEventTags.ForDeadlineEvent("ProfiledWorkflow"), profile.DeadlineEventTags);
        Assert.Equal("ProfiledWorkflow", profile.WorkflowTypeName);
    }

    /// <summary>Both ledgers are sized from settings, and a fresh envelope starts from these.</summary>
    [Fact]
    public void ItSizesTheEmptyLedgersFromSettings()
    {
        var profile = WorkflowTypeProfile<EchoState>.For(new ProfiledWorkflow(), ConfigWithHandoff("60s"));

        Assert.Equal(3, profile.EmptySeqNrLedger.Capacity);
        Assert.Empty(profile.EmptySeqNrLedger.Entries);
        Assert.Equal(11, profile.EmptyIdempotencyLedger.Capacity);
        Assert.Empty(profile.EmptyIdempotencyLedger.Entries);
    }

    /// <summary>
    /// The ceiling tracks the configured hand-off timeout, staying under it by enough for a step
    /// finishing at the last moment to persist. A hand-off too short to leave that room lands on the
    /// floor.
    /// </summary>
    [Theory]
    [InlineData("60s", 50)]
    [InlineData("30s", 20)]
    [InlineData("12s", 5)]
    [InlineData("2s", 5)]
    public void ItKeepsTheGraceCeilingUnderTheHandoffTimeout(string handoffTimeout, int expectedSeconds)
    {
        var profile = WorkflowTypeProfile<EchoState>.For(new ProfiledWorkflow(), ConfigWithHandoff(handoffTimeout));

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), profile.GraceCeiling);
    }

    /// <summary>A deployment that never loaded ClusterSharding has no such key, and Akka's own default
    /// is what the ceiling is derived from there.</summary>
    [Fact]
    public void ItFallsBackToAkkasOwnHandoffDefault()
    {
        var profile = WorkflowTypeProfile<EchoState>.For(new ProfiledWorkflow(), ConfigurationFactory.Empty);

        Assert.Equal(TimeSpan.FromSeconds(50), profile.GraceCeiling);
    }
}
