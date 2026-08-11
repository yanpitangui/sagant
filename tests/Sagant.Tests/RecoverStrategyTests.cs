using Sagant.Descriptors;
using Sagant.Settings;

namespace Sagant.Tests;

public class RecoverStrategyTests
{
    [Fact]
    public void MaxRetries_FailoverTo_WithoutInput_SetsStepNameAndNullInput()
    {
        var strategy = RecoverStrategy.WithMaxRetries(3).FailoverTo(Ref.Step<DocWorkflowFor<string>>("CompensateStep"));

        Assert.Equal(3, strategy.MaxRetries);
        Assert.Equal("CompensateStep", strategy.FailoverStepName);
        Assert.Null(strategy.FailoverStepInput);
    }

    [Fact]
    public void MaxRetries_FailoverTo_WithInput_SetsInput()
    {
        var strategy = RecoverStrategy.WithMaxRetries(2).FailoverTo(Ref.Step<DocWorkflowFor<string>, object>("CompensateStep"), 42);

        Assert.Equal(2, strategy.MaxRetries);
        Assert.Equal("CompensateStep", strategy.FailoverStepName);
        Assert.Equal(42, strategy.FailoverStepInput);
    }
}
