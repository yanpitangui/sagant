namespace Sagant.Protocol;

/// <summary>
/// Engine-level query for a workflow's current <c>TState</c> — same role as <see cref="GetStatus"/>
/// one level down, returning the business state itself, where <see cref="GetStatus"/> returns the
/// engine's lifecycle status.
/// Public, generic across every workflow, no per-workflow handler needed: a runtime driver replies
/// with the raw, currently-persisted state, boxed the same way any command reply already is.
///
/// The reply type isn't expressed in this type itself (there is no single shared "state" type
/// across workflows the way <see cref="WorkflowStatus"/> is) — a caller supplies it explicitly, the
/// same way any command reply already works: <c>handle.Request&lt;GetState, OrderState&gt;(new
/// GetState())</c>. Getting that type wrong fails exactly like any other reply-type mismatch would.
/// </summary>
public sealed record GetState;
