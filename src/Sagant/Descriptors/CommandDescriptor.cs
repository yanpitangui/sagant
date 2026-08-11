using System.Diagnostics;
using Sagant.Effects;
using Sagant.Protocol;

namespace Sagant.Descriptors;

/// <summary>
/// Zero-reflection binding of a command's runtime <see cref="Type"/> to a compiled invoker.
/// Emitted by the source generator, one per command-handler method.
/// </summary>
public readonly struct CommandDescriptor<TState>
{
    private readonly Func<Workflow<TState>, CommandContext<TState>, object, CommandEffect<TState>> _invoke;

    public CommandDescriptor(
        Type commandType, string commandTypeName, Func<Workflow<TState>, CommandContext<TState>, object, CommandEffect<TState>> invoke)
    {
        CommandType = commandType;
        CommandTypeName = commandTypeName;
        _invoke = invoke;
    }

    public Type CommandType { get; }

    /// <summary>
    /// The command type's display name, emitted by the source generator as a compile-time string
    /// literal — see <see cref="QueryDescriptor{TState}.QueryTypeName"/> for why span names, metric
    /// tags and notifications read it straight from here at no runtime cost.
    /// </summary>
    public string CommandTypeName { get; }

    /// <summary>
    /// Builds this invocation's <see cref="CommandContext{TState}"/> from <paramref name="state"/>,
    /// then invokes the command handler inside an <see cref="Activity"/> span. See
    /// <see cref="StepDescriptor{TState}.Invoke"/> for why context construction and span lifecycle
    /// both live here, as part of Core: a command handler is synchronous (no I/O; a step is what
    /// I/O is for), so this span never has to survive an await across a message-passing boundary
    /// the way the step's span does, but the same "runtime supplies context, Core owns the
    /// span" split applies here too.
    ///
    /// <paramref name="links"/> mirrors <see cref="StepDescriptor{TState}.Invoke"/>'s own parameter
    /// of the same name — how a runtime attaches a cross-trace <see cref="ActivityLink"/> to this
    /// span (e.g. a fresh child workflow's first command linking back to the parent span that
    /// started it). Optional, same as there.
    /// </summary>
    public CommandEffect<TState> Invoke(
        Workflow<TState> workflow,
        TState state,
        object command,
        ActivityContext parentContext = default,
        IEnumerable<ActivityLink>? links = null,
        Action<Activity>? configureActivity = null)
    {
        using var activity = WorkflowDiagnostics.ActivitySource.StartActivity(
            $"{workflow.WorkflowTypeName}.{CommandTypeName}", ActivityKind.Consumer, parentContext, tags: null, links: links);
        if (activity is not null)
        {
            configureActivity?.Invoke(activity);
        }

        try
        {
            var result = _invoke(workflow, new CommandContext<TState>(state), command);
            // Reached only for a handler that returns successfully — a thrown exception is
            // surfaced on the Activity above via the catch block below, same split as a step's
            // sagant.step.duration "error" outcome.
            WorkflowDiagnostics.RecordCommandHandled(workflow.WorkflowTypeName, CommandTypeName);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddTag("exception.type", ex.GetType().FullName);
            activity?.AddTag("exception.message", ex.Message);
            throw;
        }
    }
}

/// <summary>
/// Implemented (by the source generator) on every workflow class that declares command handlers.
/// Lets the runtime dispatch an incoming command to the right handler method without reflection.
/// </summary>
public interface IWorkflowCommandDispatcher<TState>
{
    bool TryGetHandler(Type commandType, out CommandDescriptor<TState> descriptor);
}
