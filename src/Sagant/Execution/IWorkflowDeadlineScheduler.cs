namespace Sagant.Execution;

/// <summary>
/// Durable identity of one armed wake. Stable across process restarts, so an implementation can
/// store it and match a later disarm against it.
/// </summary>
/// <param name="WorkflowType">The registered type name, as
/// <see cref="Sagant.Clients.IWorkflowClient.For(string, string)"/> resolves it.</param>
/// <param name="EntityId">The instance's durable id.</param>
/// <param name="Kind">Which of the instance's deadlines this wake stands for.</param>
/// <param name="Discriminator">Which one, for a kind an instance can hold several of. Carries the
/// group id for <see cref="WorkflowTimerKind.ChildGroup"/>, since an instance can await two groups at
/// once and each keeps its own deadline. <c>null</c> for every other kind, which an instance holds
/// exactly one of — there is nothing there to tell apart.</param>
public readonly record struct WorkflowDeadlineKey(
    string WorkflowType,
    string EntityId,
    WorkflowTimerKind Kind,
    string? Discriminator = null);

/// <summary>
/// Remembers that an instance has a deadline at an absolute instant, and wakes the instance when
/// that instant arrives — through <see cref="Sagant.Clients.IWorkflowHandle.Wake"/>, which every
/// runtime already implements.
///
/// <para><b>The contract is deliberately weak: at-least-once, may fire late, may fire more than
/// once.</b> The deadline itself is durable in the instance's own journal, so this is a wake backstop
/// and the journal stays the source of truth. A wake that arrives twice activates an instance that
/// re-arms from its own state and goes quiet again. A wake that arrives late fires late, which
/// absolute deadlines already tolerate (guarantee <c>D8</c>). That weakness is what lets a separate
/// durability domain — a job scheduler's own tables, a cloud timer service — serve as an
/// implementation while the engine keeps its own guarantees.</para>
///
/// <para><b>Implementer obligation: keep firing until disarmed.</b> After a wake, re-arm the same key
/// at <c>now + backoff</c> and repeat until <see cref="DisarmAsync"/> arrives. A wake can be dropped
/// in transit, and the disarm is what confirms the instance actually consumed the deadline: it is
/// issued from the instance's own subsequent events. An implementation that fires once and forgets
/// loses the instance silently, which is the one failure this seam exists to prevent.</para>
///
/// <para>Arms are supplied by whatever discovers deadlines for a given runtime, which knows the
/// threshold below which an instance stays resident and handles its own deadline in-process. So an
/// implementation sees long-horizon deadlines alone.</para>
/// </summary>
public interface IWorkflowDeadlineScheduler
{
    /// <summary>
    /// Records that <paramref name="key"/> comes due at <paramref name="dueUtc"/>, replacing any
    /// instant already recorded for that key.
    /// </summary>
    Task ArmAsync(WorkflowDeadlineKey key, DateTimeOffset dueUtc, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops <paramref name="key"/>, ending the re-arm cycle described above. Silent for a key that
    /// holds no arm, so a duplicate disarm is safe.
    /// </summary>
    Task DisarmAsync(WorkflowDeadlineKey key, CancellationToken cancellationToken = default);
}
