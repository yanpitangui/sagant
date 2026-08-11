namespace Sagant.Descriptors;

/// <summary>
/// Marks a method as a read-only query handler. The method must be a public instance method on a
/// <c>partial</c> class deriving from <see cref="Workflow{TState}"/>, taking the query and a
/// <see cref="QueryContext{TState}"/>, and returning <c>QueryEffect</c> or
/// <c>Task&lt;QueryEffect&gt;</c>. Discovered by the source generator at compile time — never by
/// runtime reflection.
///
/// A query differs from a <see cref="WorkflowCommandHandlerAttribute"/> handler in three ways that
/// all follow from it being unable to write (see <c>Effects.QueryEffect</c>):
/// <list type="bullet">
/// <item>It <b>may be asynchronous</b>. A command handler is synchronous because it applies its
/// effect atomically; a query applies nothing, so it can await external work — reading a projection,
/// calling a pricing service — without an effect waiting to land behind it.</item>
/// <item>It <b>dispatches immediately</b>, including while a step is executing. A driver that defers
/// commands until an in-flight step settles does so to keep two writers from racing over the whole
/// state; a query is not a writer.</item>
/// <item>It is <b>bounded by the runtime, not the caller</b>. A caller's request timeout completes
/// the caller's own wait and sends nothing to the workflow instance, so a query carries a
/// server-side timeout of its own — see <c>WorkflowSettings.DefaultQueryTimeout</c>.</item>
/// </list>
///
/// Reach for a query for anything a caller reads: a live status view, progress for a dashboard, a
/// projection join. Reach for a command when the workflow should move.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class WorkflowQueryAttribute : Attribute
{
}
