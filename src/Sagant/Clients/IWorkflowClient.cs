namespace Sagant.Clients;

/// <summary>
/// Single entry point for talking to workflow instances. Resolve via DI
/// (<c>services.GetRequiredService&lt;IWorkflowClient&gt;()</c> after
/// <c>AddWorkflowClient()</c> — see <c>WorkflowClientRegistrationExtensions</c>). Resolves a
/// durable-id lookup into an <see cref="IWorkflowHandle{TWorkflow}"/>, the sole handle type
/// application code deals with.
/// </summary>
public interface IWorkflowClient
{
    /// <summary>
    /// A handle to the workflow instance <paramref name="entityId"/> of type
    /// <typeparamref name="TWorkflow"/>. <typeparamref name="TWorkflow"/> must have been registered
    /// via <c>WithWorkflow&lt;TWorkflow,TState&gt;</c> — if it wasn't, this throws
    /// <see cref="InvalidOperationException"/> immediately, surfacing what is fundamentally a
    /// startup-wiring mistake as soon as it's discovered.
    /// </summary>
    IWorkflowHandle<TWorkflow> For<TWorkflow>(string entityId) where TWorkflow : class;

    /// <summary>
    /// A handle to the workflow instance <paramref name="entityId"/> of the workflow registered
    /// under <paramref name="workflowType"/>. Resolves the same registration
    /// <see cref="For{TWorkflow}"/> does and throws the same
    /// <see cref="InvalidOperationException"/> for a name no <c>WithWorkflow</c> call registered.
    ///
    /// For infrastructure holding a type name as a runtime value — a component that reads instance
    /// ids out of storage, for one. Application code names its workflow at compile time through
    /// <see cref="For{TWorkflow}"/>, where a mistyped name fails the build.
    /// </summary>
    IWorkflowHandle For(string workflowType, string entityId);
}
