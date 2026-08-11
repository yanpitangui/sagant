using Sagant.Protocol;

namespace Sagant.Runtime.Akka;

/// <summary>
/// Test/ops escape hatch: ask a <see cref="WorkflowEntityActor{TWorkflow, TState}"/> for its
/// current runtime envelope and internal counters. Not part of the durable protocol.
/// </summary>
internal sealed record GetDiagnostics<TState>;

internal sealed record Diagnostics<TState>(WorkflowRuntimeState<TState> Envelope);

/// <summary>
/// Registers the sender to be notified with the workflow's final <typeparamref name="TState"/>
/// once it reaches a terminal status (Ended/Deleted/Terminated) — or immediately, if it already
/// has. Backs <c>WorkflowRef.RunAndAwaitResult</c>. Not persisted — a transient, in-memory
/// watcher list; lost on passivation like any other outstanding <c>Ask</c>.
/// </summary>
internal sealed record WatchForCompletion<TState>;
