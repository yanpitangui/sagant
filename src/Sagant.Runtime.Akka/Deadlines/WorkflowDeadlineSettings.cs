namespace Sagant.Runtime.Akka.Deadlines;

/// <summary>
/// Knobs for the deadline projection and the in-memory scheduler behind it.
/// </summary>
public sealed class WorkflowDeadlineSettings
{
    /// <summary>
    /// How far out a deadline has to be before it is worth recording externally. A nearer one is
    /// served by the instance's own live timer, which is exact and costs nothing, so this is what
    /// keeps step timeouts and retry backoff out of the index entirely.
    ///
    /// <b>Keep it below the sharding passivation window.</b> An instance that goes idle stays
    /// resident for that whole window, so a deadline landing inside it fires locally, and one
    /// landing after it needs a record here. The margin between the two absorbs the delay between
    /// the write and the projection reading it. The default pairs a one-minute threshold with the
    /// two-minute window <c>WithWorkflow</c> configures.
    /// </summary>
    public TimeSpan ExternalArmThreshold { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Ceiling on wakes handed to the cluster per second. An instance answers a wake as soon as it
    /// has recovered and then starts doing whatever the deadline asked for, so the reply says
    /// nothing about the load that follows it — this is the bound that accounts for that work.
    /// </summary>
    public int MaxWakesPerSecond { get; set; } = 50;

    /// <summary>How far <see cref="MaxWakesPerSecond"/> may be exceeded momentarily.</summary>
    public int WakeBurst { get; set; } = 10;

    /// <summary>
    /// How many wakes may be outstanding at once. Each one waits for an instance to activate and
    /// replay its journal, so this adapts the rate to how slow that currently is: a cluster starting
    /// cold slows the stream on its own.
    /// </summary>
    public int MaxWakesInFlight { get; set; } = 16;

    /// <summary>How long one wake waits before the scheduler moves on. Exceeding it leaves the entry
    /// armed for a later attempt.</summary>
    public TimeSpan WakeTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// How long after firing a wake the same entry comes up again. Firing repeats until the
    /// instance's own events retire the entry, so this is the gap between attempts for one that
    /// stays live.
    /// </summary>
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Ceiling on the backoff, which doubles per attempt. Bounds how often an unreachable instance is
    /// tried while keeping it in the index.
    /// </summary>
    public TimeSpan MaxRetryBackoff { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// How many lanes the projection spreads recorded deadlines across. Each event is hashed to a
    /// lane by its instance's id, so one instance's arms stay in order while unrelated instances
    /// proceed in parallel — recording an arm is a round trip, and doing them one at a time is what
    /// bounds how fast the projection keeps up.
    ///
    /// Read-time only. The journal carries a single deadline tag with no lane in it, so this can be
    /// raised, lowered or changed on a restart, and two readers need not agree.
    /// </summary>
    public int ProjectionLanes { get; set; } = 16;

    /// <summary>
    /// How many applied events pass before the projection's position is snapshotted. The position is
    /// recorded for every event either way; this only decides how often the record behind it is
    /// collapsed, which bounds how much a restart replays before it reaches the latest one.
    /// </summary>
    public int ProjectionCheckpointEvery { get; set; } = 100;

    /// <summary>How many due entries one pass takes, bounding the work a single tick starts.</summary>
    public int MaxWakesPerTick { get; set; } = 500;

    /// <summary>
    /// How many times a bucket retries a wake that goes unanswered before letting the entry go. Used
    /// by the bucket scheduler, where an entry lives inside its one bucket with no index a disarm
    /// could reach — so the attempts are what bound a wake lost in transit. Exhausting them
    /// leaves the instance on guarantee <c>D8</c>'s terms: its deadline fires whenever something next
    /// activates it.
    /// </summary>
    public int MaxWakeAttempts { get; set; } = 5;

    /// <summary>
    /// How many past buckets the ticker catches up on in one pass. A process down for a long stretch
    /// walks every bucket it missed on its way back to the present, and this bounds how much of that
    /// backlog one pass takes on.
    /// </summary>
    public int MaxBucketCatchUp { get; set; } = 240;

    public void Validate()
    {
        if (ExternalArmThreshold <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ExternalArmThreshold), "Must be greater than zero.");
        if (MaxWakesPerSecond < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxWakesPerSecond), "Must be at least 1.");
        if (WakeBurst < 1)
            throw new ArgumentOutOfRangeException(nameof(WakeBurst), "Must be at least 1.");
        if (MaxWakesInFlight < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxWakesInFlight), "Must be at least 1.");
        if (MaxWakesPerTick < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxWakesPerTick), "Must be at least 1.");
        if (RetryBackoff <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(RetryBackoff), "Must be greater than zero.");
        if (MaxRetryBackoff < RetryBackoff)
            throw new ArgumentOutOfRangeException(nameof(MaxRetryBackoff), "Must be at least RetryBackoff.");
        if (MaxWakeAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxWakeAttempts), "Must be at least 1.");
        if (ProjectionCheckpointEvery < 1)
            throw new ArgumentOutOfRangeException(nameof(ProjectionCheckpointEvery), "Must be at least 1.");
        if (ProjectionLanes < 1)
            throw new ArgumentOutOfRangeException(nameof(ProjectionLanes), "Must be at least 1.");
        if (MaxBucketCatchUp < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxBucketCatchUp), "Must be at least 1.");
    }
}
