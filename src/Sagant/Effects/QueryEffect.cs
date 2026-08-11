namespace Sagant.Effects;

/// <summary>
/// The effect a query handler (a method marked <c>[WorkflowQuery]</c>) produces: a reply, and
/// nothing else. There is no <see cref="PersistenceEffect{TState}"/> member and no
/// <see cref="Transition"/> member, so a query cannot express a write at all — that is a
/// compile-time property of this type, not a convention a handler is asked to respect.
///
/// That constraint is what lets a runtime driver dispatch a query immediately, concurrently with a
/// step that is still executing: whole-state persistence means two overlapping writers race over the
/// entirety of <c>TState</c>, and a handler that cannot write cannot join that race. A driver that
/// defers ordinary commands until an in-flight step settles is therefore free to let queries through
/// while it does so.
/// </summary>
public sealed record QueryEffect(Reply Reply);
