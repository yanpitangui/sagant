namespace Sagant.Descriptors;

/// <summary>
/// Marks a method as a command handler. The method must be a public instance method on a
/// <c>partial</c> class deriving from <see cref="Workflow{TState}"/>, taking the command and a
/// <see cref="CommandContext{TState}"/>, and returning <c>CommandEffect&lt;TState&gt;</c>
/// synchronously — a command applies its effect atomically, so work that needs I/O belongs in a
/// <see cref="WorkflowStepAttribute"/> handler that the returned effect transitions to. Discovered by the
/// source generator at compile time — never by runtime reflection. Explicit, like
/// <see cref="WorkflowStepAttribute"/> — discovery is attribute-driven, avoiding mistaking an
/// unrelated helper method for an entry point.
///
/// The dispatcher routes a command to whichever handler is registered for its type. It never checks
/// the workflow's current *state* itself: guarding which states a command is valid from is this
/// handler's own job. Inspect the current state (passed in alongside the command) and return a
/// <c>NoPersistence</c>/no-transition <c>CommandEffect</c> — e.g. a rejection reply — when called
/// somewhere it shouldn't be. Without that guard, a command handler that transitions or updates
/// state will do so regardless of what the workflow is currently doing, including after it's
/// already reached a terminal state.
///
/// A runtime driver may additionally guarantee something about *when*, relative to other in-flight
/// work (e.g. a step still executing), a handler actually gets invoked — that's a driver-level
/// concern this attribute makes no promise about either way. See the driver's own documentation for
/// whatever a specific runtime guarantees. A read that must not wait behind in-flight work is a
/// <see cref="WorkflowQueryAttribute"/> handler instead.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class WorkflowCommandHandlerAttribute : Attribute
{
}
