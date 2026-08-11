using Sagant.Protocol;
using Sagant.Runtime.Akka.Execution;

namespace Sagant.Runtime.Akka.Tests.Execution;

public class SnapshotPolicyTests
{
    [Theory]
    [InlineData(WorkflowStatus.Running)]
    [InlineData(WorkflowStatus.Paused)]
    [InlineData(WorkflowStatus.Suspended)]
    public void NonTerminalStatus_BelowThreshold_DoesNotSnapshot(WorkflowStatus status)
    {
        var policy = new SnapshotPolicy(everyNEvents: 10);

        Assert.False(policy.ShouldSnapshot(status, lastSequenceNr: 1));
        Assert.False(policy.ShouldSnapshot(status, lastSequenceNr: 9));
    }

    [Fact]
    public void NonTerminalStatus_AtThreshold_Snapshots()
    {
        var policy = new SnapshotPolicy(everyNEvents: 10);

        Assert.True(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 10));
    }

    /// <summary>
    /// One transition can write several events, so the sequence number moves in steps wider than one.
    /// The threshold is a distance from the last snapshot, which a step of any width crosses.
    /// </summary>
    [Fact]
    public void ABatchThatJumpsPastTheThreshold_StillSnapshots()
    {
        var policy = new SnapshotPolicy(everyNEvents: 10);

        Assert.False(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 8));
        Assert.True(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 13));
    }

    /// <summary>A save takes time to complete, so the batches after one stay quiet until a full
    /// threshold's worth of events has accumulated past the one already asked for.</summary>
    [Fact]
    public void AfterSnapshotting_WaitsAFullThresholdAgain()
    {
        var policy = new SnapshotPolicy(everyNEvents: 10);

        Assert.True(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 13));
        Assert.False(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 20));
        Assert.False(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 22));
        Assert.True(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 23));
    }

    /// <summary>Recovery adopts the offered snapshot as the baseline, so a short journal tail behind
    /// it leaves the cadence where the previous incarnation left it.</summary>
    [Fact]
    public void RecordSnapshot_MakesAnOfferedSnapshotTheBaseline()
    {
        var policy = new SnapshotPolicy(everyNEvents: 10);
        policy.RecordSnapshot(sequenceNr: 40);

        Assert.False(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 45));
        Assert.True(policy.ShouldSnapshot(WorkflowStatus.Running, lastSequenceNr: 50));
    }

    [Theory]
    [InlineData(WorkflowStatus.Finished)]
    [InlineData(WorkflowStatus.Deleted)]
    public void TerminalStatus_AlwaysSnapshots_RegardlessOfSequenceNr(WorkflowStatus status)
    {
        var policy = new SnapshotPolicy(everyNEvents: 10);

        Assert.True(policy.ShouldSnapshot(status, lastSequenceNr: 1));
        Assert.True(policy.ShouldSnapshot(status, lastSequenceNr: 2));
    }
}
