namespace Sagant.Descriptors;

/// <summary>
/// Marks a method as a workflow step. The method must be an instance method on a
/// <c>partial</c> class deriving from <see cref="Workflow{TState}"/>, taking zero or one
/// parameter (the step's input) and returning <c>Task&lt;StepEffect&lt;TState&gt;&gt;</c>.
/// Discovered by the source generator at compile time — never by runtime reflection.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class WorkflowStepAttribute : Attribute
{
    /// <summary>
    /// Overrides the durable step name (what gets persisted/scheduled) independently of the C#
    /// method name. Defaults to the method name when omitted.
    /// </summary>
    public WorkflowStepAttribute(string? name = null)
    {
        Name = name;
    }

    public string? Name { get; }
}
