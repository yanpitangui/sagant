namespace Sagant.Descriptors;

/// <summary>
/// Marks the method a parent runs each time one of its children settles. The method must be a public
/// instance method on a <c>partial</c> class deriving from <see cref="Workflow{TState}"/>, taking a
/// <c>ChildResultContext&lt;TState&gt;</c> and returning <c>ChildResultEffect&lt;TState&gt;</c>.
/// Discovered by the source generator at compile time — never by runtime reflection.
///
/// At most one per workflow: this handler is reached by a child reporting, so there is no message
/// type to dispatch on the way a command or query has. A workflow with several kinds of child
/// switches on <c>ChildResultContext.Relationship</c> or <c>Result</c>.
///
/// Without one, a parent hears about its children only once, at the group's resume step. With one,
/// it sees each child as it settles — enough to accumulate progress into its own state, or to decide
/// the group is over before its <c>CompletionPolicy</c> would have said so.
///
/// It is <b>synchronous and cannot transition</b>, both for the same reason a command handler is
/// synchronous: the effect is applied atomically with the report that triggered it. Anything needing
/// I/O belongs in the group's resume step. See <c>ChildResultEffect</c> for what it can return.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class WorkflowChildResultAttribute : Attribute
{
}
