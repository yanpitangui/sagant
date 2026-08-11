namespace Sagant.Descriptors;

/// <summary>
/// Marker type for a <see cref="StepRef{TWorkflow, TInput}"/> pointing at a step that takes no input.
/// </summary>
public readonly struct NoInput
{
    public static readonly NoInput Instance = default;
}
