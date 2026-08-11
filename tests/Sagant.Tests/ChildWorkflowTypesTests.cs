using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Tests;

public class ChildWorkflowTypesTests
{
    [Fact]
    public void ChildStatus_HasExactlySixValues_NoStartedState()
    {
        // Pending covers "not yet terminal, no termination requested" uniformly. TerminationRequested
        // is the one non-terminal value beyond Pending — durably marks "we've committed to sending
        // Terminate to this child" so recovery can redeliver it, distinct from Pending (which never
        // implies an active decision to stop the child). See ChildStatus's own doc comment.
        var values = Enum.GetValues<ChildStatus>();
        Assert.Equal(
            new[] { ChildStatus.Pending, ChildStatus.TerminationRequested, ChildStatus.Completed, ChildStatus.Failed, ChildStatus.Cancelled, ChildStatus.Terminated },
            values);
    }

    [Fact]
    public void ChildFailure_CarriesMessage()
    {
        var failure = new WorkflowFailure("card declined");

        Assert.Equal("card declined", failure.Message);
    }

    [Fact]
    public void ChildGroupState_CarriesPolicyAndFinalizationState()
    {
        var group = new ChildGroupState(
            "group-0", Generation: 0, CompletionPolicy.AllSuccessful, FailurePolicy.FailFast,
            RemainingChildrenPolicy.Terminate, "OnResolved", Finalized: false);

        Assert.Equal("group-0", group.GroupId);
        Assert.False(group.Finalized);
    }
}
