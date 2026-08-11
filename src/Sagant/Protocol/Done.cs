namespace Sagant.Protocol;

/// <summary>Acknowledgement reply for lifecycle commands (<see cref="Suspend"/>/<see cref="Resume"/>/<see cref="Terminate"/>).</summary>
public sealed record Done
{
    public static readonly Done Instance = new();
}
