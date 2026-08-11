using Sagant.Descriptors;

namespace Sagant.Settings;

/// <summary>
/// Configuration for a paused workflow: how long it may stay paused before the timeout fires and
/// the workflow auto-transitions into <see cref="TimeoutHandlerStepName"/> — a normal step, able to
/// do I/O (e.g. auto-cancel calling a compensating service), reusing the same <c>StepTransition</c>
/// machinery as everything else.
/// </summary>
public sealed record PauseSettings(TimeSpan Timeout, string? Reason, string TimeoutHandlerStepName)
{
    public static PauseSettingsBuilder WithTimeout(TimeSpan timeout) => new(timeout, null);
}

/// <summary>
/// Fluent continuation from <see cref="PauseSettings.WithTimeout"/> — requires a timeout handler
/// step before a <see cref="PauseSettings"/> can be built.
/// </summary>
public sealed class PauseSettingsBuilder
{
    private readonly TimeSpan _timeout;
    private readonly string? _reason;

    internal PauseSettingsBuilder(TimeSpan timeout, string? reason)
    {
        _timeout = timeout;
        _reason = reason;
    }

    public PauseSettingsBuilder WithReason(string reason) => new(_timeout, reason);

    public PauseSettings TimeoutHandler<TWorkflow>(StepRef<TWorkflow, NoInput> step) => new(_timeout, _reason, step.Name);
}
