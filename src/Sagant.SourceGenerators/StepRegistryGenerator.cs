using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Sagant.SourceGenerators;

/// <summary>
/// For every partial class deriving from <c>Sagant.Workflow&lt;TState&gt;</c> that declares
/// one or more <c>[WorkflowStep]</c>, <c>[WorkflowCommandHandler]</c> and/or <c>[WorkflowQuery]</c>
/// methods, emits:
///   - a nested <c>Steps</c> static class with one <c>StepRef&lt;TWorkflow, TInput&gt;</c> field per step
///   - an explicit, zero-reflection implementation of <c>IWorkflowStepDispatcher&lt;TState&gt;</c>
///   - an explicit, zero-reflection implementation of <c>IWorkflowCommandDispatcher&lt;TState&gt;</c>
///   - an explicit, zero-reflection implementation of <c>IWorkflowQueryDispatcher&lt;TState&gt;</c>
/// All three dispatcher interfaces are always implemented together (even where one kind has zero
/// members) since a runtime driver resolves all three regardless. The workflow class may be nested
/// inside one or more containing classes — the generated members are emitted inside the same
/// containing-class chain, each level re-opened as <c>partial</c>.
///
/// Handlers receive their state through a context parameter (<c>StepContext&lt;TState&gt;</c>,
/// <c>CommandContext&lt;TState&gt;</c>, <c>QueryContext&lt;TState&gt;</c>), bound here by type, so a
/// declaration is free to order its parameters either way. Steps and queries may be declared
/// synchronously; the emitted invoker wraps those in <c>Task.FromResult</c> so a driver has one
/// shape to drive.
/// </summary>
[Generator]
public sealed class StepRegistryGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor NotPartialDiagnostic = new(
        id: "SAG001",
        title: "Workflow class with handler methods must be partial",
        messageFormat: "Class '{0}' declares [WorkflowStep]/[WorkflowCommandHandler]/[WorkflowQuery] methods but is not declared 'partial'; the source generator cannot augment it",
        category: "Sagant",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor MutableStateDiagnostic = new(
        id: "SAG002",
        title: "Workflow state should be immutable",
        messageFormat: "Workflow state type '{0}' exposes settable member '{1}'; a handler mutating state in place bypasses the effect that would persist it, leaving the running workflow holding data its journal has never seen",
        category: "Sagant",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor MutableCollectionDiagnostic = new(
        id: "SAG003",
        title: "Workflow state exposes a mutable collection",
        messageFormat: "Workflow state type '{0}' exposes member '{1}' of mutable collection type '{2}'; prefer IReadOnlyList<T>/IReadOnlyDictionary<,>/ImmutableArray<T> so state can only change through an effect",
        category: "Sagant",
        DiagnosticSeverity.Info,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor QueryReturnTypeDiagnostic = new(
        id: "SAG004",
        title: "Query handler must return QueryEffect",
        messageFormat: "[WorkflowQuery] method '{0}' returns '{1}'; a query handler must return QueryEffect or Task<QueryEffect>",
        category: "Sagant",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor AsyncCommandDiagnostic = new(
        id: "SAG005",
        title: "Command handler must be synchronous",
        messageFormat: "[WorkflowCommandHandler] method '{0}' returns '{1}'; a command handler must return CommandEffect<TState> synchronously — move work that needs I/O into a [WorkflowStep], or read it with a [WorkflowQuery]",
        category: "Sagant",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ChildResultReturnTypeDiagnostic = new(
        id: "SAG006",
        title: "Child-result handler must return ChildResultEffect<TState>",
        messageFormat: "[WorkflowChildResult] method '{0}' returns '{1}'; a child-result handler must return ChildResultEffect<TState> synchronously — it is applied in the same write as the child report that triggered it",
        category: "Sagant",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor DuplicateChildResultDiagnostic = new(
        id: "SAG007",
        title: "A workflow declares at most one child-result handler",
        messageFormat: "'{0}' declares {1} [WorkflowChildResult] methods; a child reports with no message type to dispatch on, so one handler serves every child — switch on ChildResultContext.Relationship or Result inside it",
        category: "Sagant",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var candidates = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => node is ClassDeclarationSyntax,
                transform: static (ctx, _) => (ClassDeclarationSyntax)ctx.Node)
            .Where(static c => c.Members.OfType<MethodDeclarationSyntax>()
                .Any(m => m.AttributeLists.SelectMany(al => al.Attributes)
                    .Any(a => a.Name.ToString() is "WorkflowStep" or "WorkflowStepAttribute"
                        or "WorkflowCommandHandler" or "WorkflowCommandHandlerAttribute"
                        or "WorkflowQuery" or "WorkflowQueryAttribute"
                        or "WorkflowChildResult" or "WorkflowChildResultAttribute")));

        var compilationAndClasses = context.CompilationProvider.Combine(candidates.Collect());

        context.RegisterSourceOutput(compilationAndClasses, static (spc, source) =>
            Execute(source.Left, source.Right, spc));
    }

    private static void Execute(Compilation compilation, ImmutableArray<ClassDeclarationSyntax> classes, SourceProductionContext context)
    {
        var workflowBaseType = compilation.GetTypeByMetadataName("Sagant.Workflow`1");
        if (workflowBaseType is null)
        {
            return;
        }

        // Two workflow classes may share one TState; the shape diagnostics below describe the state
        // type, so report each one once rather than once per workflow that uses it.
        var stateTypesAlreadyChecked = new HashSet<string>();

        // One workflow, one generated file, however many `partial` blocks it is written across.
        // Grouping by the declared symbol is what makes that true: a class split in two would
        // otherwise be generated once per block, each emission seeing only the members in front of
        // it and both writing the same hint name.
        var workflows = new Dictionary<INamedTypeSymbol, ClassDeclarationSyntax>(SymbolEqualityComparer.Default);
        foreach (var candidate in classes.Distinct())
        {
            if (compilation.GetSemanticModel(candidate.SyntaxTree).GetDeclaredSymbol(candidate)
                is not INamedTypeSymbol symbol)
            {
                continue;
            }

            // The block carrying the base list is the one that names TState, and the one whose
            // containing chain the generated members are emitted into.
            if (!workflows.TryGetValue(symbol, out var chosen) || candidate.BaseList is not null && chosen.BaseList is null)
            {
                workflows[symbol] = candidate;
            }
        }

        foreach (var (classSymbol, classDecl) in workflows.Select(kv => (kv.Key, kv.Value)))
        {
            var model = compilation.GetSemanticModel(classDecl.SyntaxTree);

            var stateType = FindWorkflowStateType(classSymbol, workflowBaseType);
            if (stateType is null)
            {
                continue;
            }

            // Every block of the class, including any the syntax filter never saw because it carries
            // neither a base list nor an attributed method.
            var allDeclarations = classSymbol.DeclaringSyntaxReferences
                .Select(r => r.GetSyntax())
                .OfType<ClassDeclarationSyntax>()
                .ToList();

            var stepMethods = MethodsAcross(compilation, allDeclarations, HasWorkflowStepAttribute);
            var commandMethods = MethodsAcross(compilation, allDeclarations, HasWorkflowCommandHandlerAttribute);
            var queryMethods = MethodsAcross(compilation, allDeclarations, HasWorkflowQueryAttribute);
            var childResultMethods = MethodsAcross(compilation, allDeclarations, HasWorkflowChildResultAttribute);

            if (stepMethods.Count == 0 && commandMethods.Count == 0 && queryMethods.Count == 0
                && childResultMethods.Count == 0)
            {
                continue;
            }

            // The generated members are emitted inside the same containing-class chain as the
            // workflow class (see GenerateSource) — every level of that chain, not just the
            // workflow class itself, has to be reopened as partial for the generated file to
            // compile.
            var containingChain = GetContainingClassChain(classDecl);
            var notPartial = allDeclarations.Concat(containingChain).FirstOrDefault(c => !IsPartial(c));
            if (notPartial is not null)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    NotPartialDiagnostic, notPartial.Identifier.GetLocation(),
                    compilation.GetSemanticModel(notPartial.SyntaxTree).GetDeclaredSymbol(notPartial)?.Name
                        ?? notPartial.Identifier.Text));
                continue;
            }

            if (stateTypesAlreadyChecked.Add(stateType.ToDisplayString()))
            {
                ReportStateShapeDiagnostics(stateType, classDecl, context);
            }

            var invalidQuery = queryMethods.FirstOrDefault(m => !ReturnsQueryEffect(m.Symbol));
            if (invalidQuery.Symbol is not null)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    QueryReturnTypeDiagnostic, invalidQuery.Syntax.Identifier.GetLocation(),
                    invalidQuery.Symbol.Name, invalidQuery.Symbol.ReturnType.ToDisplayString()));
                continue;
            }

            var invalidChildResult = childResultMethods.FirstOrDefault(m => !ReturnsChildResultEffect(m.Symbol));
            if (invalidChildResult.Symbol is not null)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    ChildResultReturnTypeDiagnostic, invalidChildResult.Syntax.Identifier.GetLocation(),
                    invalidChildResult.Symbol.Name, invalidChildResult.Symbol.ReturnType.ToDisplayString()));
                continue;
            }

            if (childResultMethods.Count > 1)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    DuplicateChildResultDiagnostic, childResultMethods[1].Syntax.Identifier.GetLocation(),
                    classSymbol.Name, childResultMethods.Count));
                continue;
            }

            var asyncCommand = commandMethods.FirstOrDefault(m => IsTaskOf(m.Symbol.ReturnType, out _));
            if (asyncCommand.Symbol is not null)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    AsyncCommandDiagnostic, asyncCommand.Syntax.Identifier.GetLocation(),
                    asyncCommand.Symbol.Name, asyncCommand.Symbol.ReturnType.ToDisplayString()));
                continue;
            }

            var source = GenerateSource(
                classSymbol, stateType, stepMethods, commandMethods, queryMethods, childResultMethods, containingChain);
            context.AddSource($"{classSymbol.Name}.WorkflowGenerated.g.cs", SourceText(source));
        }
    }

    /// <summary>
    /// The attributed methods of a workflow, gathered from every <c>partial</c> block that declares
    /// it. Each block is read against its own tree's semantic model, so a class split across files
    /// resolves the same as one written in a single place.
    /// </summary>
    private static List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> MethodsAcross(
        Compilation compilation, List<ClassDeclarationSyntax> declarations, Func<ISymbol, bool> predicate) =>
        declarations
            .SelectMany(d => MethodsWith(d, compilation.GetSemanticModel(d.SyntaxTree), predicate))
            .ToList();

    private static List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> MethodsWith(
        ClassDeclarationSyntax classDecl, SemanticModel model, Func<ISymbol, bool> predicate) =>
        classDecl.Members.OfType<MethodDeclarationSyntax>()
            .Select(m => (Syntax: m, Symbol: model.GetDeclaredSymbol(m)!))
            .Where(m => m.Symbol is not null && predicate(m.Symbol))
            .ToList();

    private static Microsoft.CodeAnalysis.Text.SourceText SourceText(string source) =>
        Microsoft.CodeAnalysis.Text.SourceText.From(source, Encoding.UTF8);

    private static bool IsPartial(ClassDeclarationSyntax classDecl) =>
        classDecl.Modifiers.Any(SyntaxKind.PartialKeyword);

    /// <summary>The workflow class's containing classes, outermost first, with the workflow class
    /// itself last — a top-level workflow class yields a single-element chain (itself). Used to
    /// mirror the exact nesting the generated file needs to reopen.</summary>
    private static List<ClassDeclarationSyntax> GetContainingClassChain(ClassDeclarationSyntax classDecl)
    {
        var chain = new List<ClassDeclarationSyntax>();
        for (var current = classDecl; current is not null; current = current.Parent as ClassDeclarationSyntax)
        {
            chain.Add(current);
        }

        chain.Reverse();
        return chain;
    }

    private static ITypeSymbol? FindWorkflowStateType(INamedTypeSymbol classSymbol, INamedTypeSymbol workflowBaseType)
    {
        for (var current = classSymbol.BaseType; current is not null; current = current.BaseType)
        {
            if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, workflowBaseType))
            {
                return current.TypeArguments[0];
            }
        }

        return null;
    }

    /// <summary>
    /// Reports SAG002/SAG003 against the state type's own declaration where it's available in
    /// source, falling back to the workflow class that names it. Skips a state type declared outside
    /// this compilation — its shape isn't something this build can act on.
    /// </summary>
    private static void ReportStateShapeDiagnostics(
        ITypeSymbol stateType, ClassDeclarationSyntax classDecl, SourceProductionContext context)
    {
        if (!stateType.Locations.Any(l => l.IsInSource))
        {
            return;
        }

        var location = stateType.Locations.FirstOrDefault(l => l.IsInSource) ?? classDecl.Identifier.GetLocation();
        var stateTypeName = stateType.Name;

        foreach (var member in stateType.GetMembers())
        {
            if (member.DeclaredAccessibility != Accessibility.Public || member.IsStatic || member.IsImplicitlyDeclared)
            {
                continue;
            }

            var (memberType, isSettable) = member switch
            {
                IPropertySymbol p => (p.Type, p.SetMethod is { IsInitOnly: false, DeclaredAccessibility: Accessibility.Public }),
                IFieldSymbol { IsConst: false } f => (f.Type, !f.IsReadOnly),
                _ => (null, false),
            };

            if (memberType is null)
            {
                continue;
            }

            if (isSettable)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    MutableStateDiagnostic, location, stateTypeName, member.Name));
            }

            if (IsMutableCollection(memberType))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    MutableCollectionDiagnostic, location, stateTypeName, member.Name, memberType.ToDisplayString()));
            }
        }
    }

    private static bool IsMutableCollection(ITypeSymbol type) =>
        type.TypeKind == TypeKind.Array
        || (type is INamedTypeSymbol { IsGenericType: true } named
            && named.ConstructedFrom.ToDisplayString() is
                "System.Collections.Generic.List<T>"
                or "System.Collections.Generic.Dictionary<TKey, TValue>"
                or "System.Collections.Generic.HashSet<T>");

    private static bool HasWorkflowStepAttribute(ISymbol methodSymbol) =>
        HasWorkflowStepAttribute(methodSymbol, out _);

    private static bool HasWorkflowStepAttribute(ISymbol methodSymbol, out string? explicitName)
    {
        foreach (var attribute in methodSymbol.GetAttributes())
        {
            if (attribute.AttributeClass?.Name is "WorkflowStepAttribute")
            {
                explicitName = attribute.ConstructorArguments.Length > 0
                    ? attribute.ConstructorArguments[0].Value as string
                    : null;
                return true;
            }
        }

        explicitName = null;
        return false;
    }

    private static bool HasWorkflowCommandHandlerAttribute(ISymbol methodSymbol) =>
        methodSymbol.GetAttributes().Any(a => a.AttributeClass?.Name is "WorkflowCommandHandlerAttribute");

    private static bool HasWorkflowQueryAttribute(ISymbol methodSymbol) =>
        methodSymbol.GetAttributes().Any(a => a.AttributeClass?.Name is "WorkflowQueryAttribute");

    private static bool HasWorkflowChildResultAttribute(ISymbol methodSymbol) =>
        methodSymbol.GetAttributes().Any(a => a.AttributeClass?.Name is "WorkflowChildResultAttribute");

    /// <summary>True when <paramref name="type"/> is <c>Task&lt;T&gt;</c>, yielding its
    /// <paramref name="inner"/> argument.</summary>
    private static bool IsTaskOf(ITypeSymbol type, out ITypeSymbol? inner)
    {
        if (type is INamedTypeSymbol { IsGenericType: true } named
            && named.ConstructedFrom.ToDisplayString() == "System.Threading.Tasks.Task<TResult>")
        {
            inner = named.TypeArguments[0];
            return true;
        }

        inner = null;
        return false;
    }

    private static bool ReturnsQueryEffect(IMethodSymbol method)
    {
        var effectType = IsTaskOf(method.ReturnType, out var inner) ? inner! : method.ReturnType;
        return effectType is { Name: "QueryEffect", ContainingNamespace.Name: "Effects" };
    }

    /// <summary>Synchronous only: a <c>Task</c>-returning handler fails this, and reports SAG006 with
    /// the type it actually returned.</summary>
    private static bool ReturnsChildResultEffect(IMethodSymbol method) =>
        method.ReturnType is { Name: "ChildResultEffect", ContainingNamespace.Name: "Effects" };

    /// <summary>True when <paramref name="type"/> is the named handler context for any state type —
    /// matched on the open generic so a mismatched <c>TState</c> surfaces as a cast error in the
    /// generated file, where the compiler reports it.</summary>
    private static bool IsHandlerContext(ITypeSymbol type, string contextTypeName) =>
        type is INamedTypeSymbol { IsGenericType: true } named
        && named.ConstructedFrom.Name == contextTypeName
        && named.ContainingNamespace.ToDisplayString() == "Sagant";

    /// <summary>
    /// Builds the argument list for one generated invoker, preserving the order the handler declared
    /// its parameters in: the context parameter becomes <c>ctx</c>, whichever other parameter exists
    /// becomes the cast payload. Returns <c>null</c> for the payload type when the handler declared
    /// no payload parameter.
    /// </summary>
    private static (string Args, string? PayloadType, string? PayloadTypeName) BuildInvokerArgs(
        IMethodSymbol method, string contextTypeName, string payloadExpression)
    {
        string? payloadType = null;
        string? payloadTypeName = null;
        var args = new List<string>();

        foreach (var parameter in method.Parameters)
        {
            if (IsHandlerContext(parameter.Type, contextTypeName))
            {
                args.Add("ctx");
                continue;
            }

            payloadType = parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            // Readable, source-shaped name (e.g. "PlaceOrder"), baked into the descriptor as a
            // literal so span names and metric tags never look the type up at runtime.
            payloadTypeName = parameter.Type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
            args.Add($"(({payloadType}){payloadExpression})");
        }

        return (string.Join(", ", args), payloadType, payloadTypeName);
    }

    private static string GenerateSource(
        INamedTypeSymbol classSymbol,
        ITypeSymbol stateType,
        List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> stepMethods,
        List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> commandMethods,
        List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> queryMethods,
        List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> childResultMethods,
        List<ClassDeclarationSyntax> containingChain)
    {
        // Everything but the workflow class itself (the chain's last entry) is a containing class
        // that needs to be reopened as partial around the generated members — see this generator's
        // class-level doc comment.
        var outerContainers = containingChain.Take(containingChain.Count - 1).Select(c => c.Identifier.Text).ToList();
        var ns = classSymbol.ContainingNamespace.IsGlobalNamespace
            ? null
            : classSymbol.ContainingNamespace.ToDisplayString();
        var className = classSymbol.Name;
        var stateTypeName = stateType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        var steps = stepMethods
            .Select(m =>
            {
                HasWorkflowStepAttribute(m.Symbol, out var explicitName);
                var (args, inputType, _) = BuildInvokerArgs(m.Symbol, "StepContext", "input!");
                var isAsync = IsTaskOf(m.Symbol.ReturnType, out _);
                return (
                    MethodName: m.Symbol.Name,
                    DurableName: explicitName ?? m.Symbol.Name,
                    InputType: inputType ?? "global::Sagant.Descriptors.NoInput",
                    Args: args,
                    IsAsync: isAsync);
            })
            .ToList();

        var commands = commandMethods
            .Select(m =>
            {
                var (args, commandType, commandTypeName) = BuildInvokerArgs(m.Symbol, "CommandContext", "cmd");
                return (MethodName: m.Symbol.Name, CommandType: commandType, CommandTypeName: commandTypeName, Args: args);
            })
            .Where(c => c.CommandType is not null)
            .ToList();

        var queries = queryMethods
            .Select(m =>
            {
                var (args, queryType, queryTypeName) = BuildInvokerArgs(m.Symbol, "QueryContext", "query");
                return (MethodName: m.Symbol.Name, QueryType: queryType, QueryTypeName: queryTypeName, Args: args, IsAsync: IsTaskOf(m.Symbol.ReturnType, out _));
            })
            .Where(q => q.QueryType is not null)
            .ToList();

        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        if (ns is not null)
        {
            sb.AppendLine($"namespace {ns};");
            sb.AppendLine();
        }

        foreach (var container in outerContainers)
        {
            sb.AppendLine($"partial class {container}");
            sb.AppendLine("{");
        }

        sb.AppendLine($"partial class {className} : global::Sagant.Descriptors.IWorkflowStepDispatcher<{stateTypeName}>, global::Sagant.Descriptors.IWorkflowCommandDispatcher<{stateTypeName}>, global::Sagant.Descriptors.IWorkflowQueryDispatcher<{stateTypeName}>, global::Sagant.Descriptors.IWorkflowChildResultDispatcher<{stateTypeName}>, global::Sagant.Descriptors.IWorkflowTypeInfo");
        sb.AppendLine("{");

        // A compile-time string literal baked directly into the emitted class — see
        // Workflow<TState>.WorkflowTypeName's doc comment for why this is the canonical source
        // every span/metric tag reads from. Two forms, same literal: the instance override below is
        // what a live Workflow<TState>-typed reference reads (spans/metrics); the static interface
        // member is for call sites with only the generic type parameter and no instance at all — see
        // IWorkflowTypeInfo's own doc comment.
        sb.AppendLine($"    public override string WorkflowTypeName => \"{className}\";");
        sb.AppendLine($"    static string global::Sagant.Descriptors.IWorkflowTypeInfo.WorkflowTypeName => \"{className}\";");
        sb.AppendLine();

        // Steps container
        sb.AppendLine("    public static class Steps");
        sb.AppendLine("    {");
        foreach (var step in steps)
        {
            sb.AppendLine($"        public static readonly global::Sagant.Descriptors.StepRef<{className}, {step.InputType}> {step.MethodName} = new(\"{step.DurableName}\");");
        }
        sb.AppendLine("    }");
        sb.AppendLine();

        // Step descriptor table. Frozen: built once in this static initializer and only ever read
        // afterwards, which is exactly what FrozenDictionary optimizes for — it pays a one-off build
        // cost per closed workflow type to make every subsequent lookup faster, and specializes
        // hardest for the small string-keyed tables a workflow actually produces.
        sb.AppendLine($"    private static readonly global::System.Collections.Frozen.FrozenDictionary<string, global::Sagant.Descriptors.StepDescriptor<{stateTypeName}>> __sagantStepDescriptors =");
        sb.AppendLine($"        global::System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(new global::System.Collections.Generic.Dictionary<string, global::Sagant.Descriptors.StepDescriptor<{stateTypeName}>>");
        sb.AppendLine("        {");
        foreach (var step in steps)
        {
            var call = $"(({className})workflow).{step.MethodName}({step.Args})";
            var body = step.IsAsync ? call : $"global::System.Threading.Tasks.Task.FromResult({call})";
            sb.AppendLine($"            [\"{step.DurableName}\"] = new(\"{step.DurableName}\", typeof({step.InputType}), static (workflow, ctx, input) => {body}),");
        }
        sb.AppendLine("        });");
        sb.AppendLine();

        sb.AppendLine($"    bool global::Sagant.Descriptors.IWorkflowStepDispatcher<{stateTypeName}>.TryGetStep(string stepName, out global::Sagant.Descriptors.StepDescriptor<{stateTypeName}> descriptor) =>");
        sb.AppendLine("        __sagantStepDescriptors.TryGetValue(stepName, out descriptor);");
        sb.AppendLine();

        sb.AppendLine($"    global::System.Collections.Generic.IReadOnlyCollection<string> global::Sagant.Descriptors.IWorkflowStepDispatcher<{stateTypeName}>.StepNames =>");
        sb.AppendLine("        __sagantStepDescriptors.Keys;");
        sb.AppendLine();

        // Command descriptor table — frozen for the same reason as the step table above.
        sb.AppendLine($"    private static readonly global::System.Collections.Frozen.FrozenDictionary<global::System.Type, global::Sagant.Descriptors.CommandDescriptor<{stateTypeName}>> __sagantCommandDescriptors =");
        sb.AppendLine($"        global::System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(new global::System.Collections.Generic.Dictionary<global::System.Type, global::Sagant.Descriptors.CommandDescriptor<{stateTypeName}>>");
        sb.AppendLine("        {");
        foreach (var command in commands)
        {
            sb.AppendLine($"            [typeof({command.CommandType})] = new(typeof({command.CommandType}), \"{command.CommandTypeName}\", static (workflow, ctx, cmd) => (({className})workflow).{command.MethodName}({command.Args})),");
        }
        sb.AppendLine("        });");
        sb.AppendLine();

        sb.AppendLine($"    bool global::Sagant.Descriptors.IWorkflowCommandDispatcher<{stateTypeName}>.TryGetHandler(global::System.Type commandType, out global::Sagant.Descriptors.CommandDescriptor<{stateTypeName}> descriptor) =>");
        sb.AppendLine("        __sagantCommandDescriptors.TryGetValue(commandType, out descriptor);");
        sb.AppendLine();

        // Query descriptor table — frozen for the same reason as the step table above.
        sb.AppendLine($"    private static readonly global::System.Collections.Frozen.FrozenDictionary<global::System.Type, global::Sagant.Descriptors.QueryDescriptor<{stateTypeName}>> __sagantQueryDescriptors =");
        sb.AppendLine($"        global::System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(new global::System.Collections.Generic.Dictionary<global::System.Type, global::Sagant.Descriptors.QueryDescriptor<{stateTypeName}>>");
        sb.AppendLine("        {");
        foreach (var query in queries)
        {
            var call = $"(({className})workflow).{query.MethodName}({query.Args})";
            var body = query.IsAsync ? call : $"global::System.Threading.Tasks.Task.FromResult({call})";
            sb.AppendLine($"            [typeof({query.QueryType})] = new(typeof({query.QueryType}), \"{query.QueryTypeName}\", static (workflow, ctx, query) => {body}),");
        }
        sb.AppendLine("        });");
        sb.AppendLine();

        sb.AppendLine($"    bool global::Sagant.Descriptors.IWorkflowQueryDispatcher<{stateTypeName}>.TryGetQuery(global::System.Type queryType, out global::Sagant.Descriptors.QueryDescriptor<{stateTypeName}> descriptor) =>");
        sb.AppendLine("        __sagantQueryDescriptors.TryGetValue(queryType, out descriptor);");
        sb.AppendLine();

        // At most one, so there is no table to look up — the handler either exists for this workflow
        // or it does not, decided at compile time.
        sb.AppendLine($"    bool global::Sagant.Descriptors.IWorkflowChildResultDispatcher<{stateTypeName}>.TryGetChildResultHandler(out global::Sagant.Descriptors.ChildResultDescriptor<{stateTypeName}> descriptor)");
        sb.AppendLine("    {");
        if (childResultMethods.Count == 1)
        {
            sb.AppendLine($"        descriptor = new(static (workflow, ctx) => (({className})workflow).{childResultMethods[0].Symbol.Name}(ctx));");
            sb.AppendLine("        return true;");
        }
        else
        {
            sb.AppendLine("        descriptor = default;");
            sb.AppendLine("        return false;");
        }

        sb.AppendLine("    }");

        sb.AppendLine("}");

        // Close each containing class wrapper, innermost first.
        for (var i = 0; i < outerContainers.Count; i++)
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }
}
