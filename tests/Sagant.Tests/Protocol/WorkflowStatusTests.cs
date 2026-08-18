using Sagant.Protocol;

namespace Sagant.Tests.Protocol;

/// <summary>
/// What an unset <see cref="WorkflowStatus"/> means.
///
/// A status arrives as <c>default</c> in more places than a construction site: a value type nobody
/// assigned, a field a serializer left alone, an id whose history is gone. Whatever the zero value is
/// becomes the answer in all of them at once, which is worth pinning down here, once, for
/// whoever next adds a member to the enum.
/// </summary>
public class WorkflowStatusTests
{
    /// <summary>
    /// The one that costs something to get wrong. A caller asking whether a run is finished before
    /// starting another waits on a live run and gives up on an absent one — so a default that reads as
    /// <see cref="WorkflowStatus.Running"/> makes it wait forever on something that will never report
    /// anything else.
    /// </summary>
    [Fact]
    public void TheDefaultStatus_SaysARunIsAbsent() =>
        Assert.Equal(WorkflowStatus.NotStarted, default);

    /// <summary>
    /// Held apart from the assertion above, since the two are only the same while NotStarted is the
    /// zero value: this one says the enum has no second member sharing it.
    /// </summary>
    [Fact]
    public void NoOtherStatus_SharesTheZeroValue()
    {
        var atZero = Enum.GetValues<WorkflowStatus>().Where(s => (int)s == 0).ToList();

        Assert.Equal([WorkflowStatus.NotStarted], atZero);
    }

    /// <summary>
    /// Every member is distinct, so a status that round-trips through its numeric form comes back as
    /// itself.
    /// </summary>
    [Fact]
    public void EveryStatus_HasItsOwnValue()
    {
        var values = Enum.GetValues<WorkflowStatus>();

        Assert.Equal(values.Length, values.Select(s => (int)s).Distinct().Count());
    }
}
