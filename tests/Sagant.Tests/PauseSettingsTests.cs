using Sagant.Descriptors;
using Sagant.Settings;

namespace Sagant.Tests;

public class PauseSettingsTests
{
    [Fact]
    public void TimeoutHandler_WithoutReason_ProducesSettingsWithNullReason()
    {
        var settings = PauseSettings.WithTimeout(TimeSpan.FromHours(8))
            .TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AcceptanceTimeout"));

        Assert.Equal(TimeSpan.FromHours(8), settings.Timeout);
        Assert.Null(settings.Reason);
        Assert.Equal("AcceptanceTimeout", settings.TimeoutHandlerStepName);
    }

    [Fact]
    public void WithReason_SetsReasonOnBuiltSettings()
    {
        var settings = PauseSettings.WithTimeout(TimeSpan.FromHours(1))
            .WithReason("awaiting manual approval")
            .TimeoutHandler(Ref.Step<DocWorkflowFor<string>>("AutoCancel"));

        Assert.Equal("awaiting manual approval", settings.Reason);
    }
}
