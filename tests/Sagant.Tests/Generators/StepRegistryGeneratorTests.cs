using Microsoft.CodeAnalysis;

namespace Sagant.Tests.Generators;

public class StepRegistryGeneratorTests
{
    private const string SampleWorkflowSource = """
        using System.Threading.Tasks;
        using Sagant;
        using Sagant.Settings;
        using Sagant.Descriptors;
        using Sagant.Effects;

        namespace TestNamespace;

        public sealed record StartOrder(int Amount);

        public partial class SampleWorkflow : Workflow<string>
        {
            public override string EmptyState() => string.Empty;

            [WorkflowStep]
            public Task<StepEffect<string>> ReserveInventoryStep(int amount) =>
                Task.FromResult(StepEffects.ThenComplete());

            [WorkflowStep]
            public Task<StepEffect<string>> NotifyStep() =>
                Task.FromResult(StepEffects.ThenComplete());

            [WorkflowCommandHandler]
            public CommandEffect<string> Start(StartOrder cmd) =>
                Effects.TransitionTo(Steps.ReserveInventoryStep, cmd.Amount).ThenReply("accepted");
        }
        """;

    [Fact]
    public void Generator_ForPartialWorkflowWithSteps_ProducesNoErrorDiagnostics()
    {
        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(SampleWorkflowSource);

        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));
    }

    [Fact]
    public void Generator_ForPartialWorkflowWithSteps_EmitsStepsClassWithOneFieldPerStep()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(SampleWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("class Steps", combined);
        Assert.Contains("ReserveInventoryStep", combined);
        Assert.Contains("NotifyStep", combined);
        Assert.Contains("StepRef<SampleWorkflow, int>", combined);
        Assert.Contains("StepRef<SampleWorkflow, global::Sagant.Descriptors.NoInput>", combined);
    }

    [Fact]
    public void Generator_ForPartialWorkflowWithSteps_ImplementsIWorkflowStepDispatcher()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(SampleWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("IWorkflowStepDispatcher<string>", combined);
        Assert.Contains("TryGetStep", combined);
        Assert.Contains("StepNames", combined);
    }

    [Fact]
    public void Generator_ForWorkflowWithCommandHandler_ImplementsIWorkflowCommandDispatcher()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(SampleWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("IWorkflowCommandDispatcher<string>", combined);
        Assert.Contains("TryGetHandler", combined);
        Assert.Contains("typeof(global::TestNamespace.StartOrder)", combined);
    }

    [Fact]
    public void Generator_ForPartialWorkflowWithSteps_OverridesWorkflowTypeNameWithCompileTimeConstant()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(SampleWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("""public override string WorkflowTypeName => "SampleWorkflow";""", combined);
    }

    [Fact]
    public void Generator_ForPartialWorkflowWithSteps_ImplementsIWorkflowTypeInfo()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(SampleWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("IWorkflowTypeInfo", combined);
        Assert.Contains("""static string global::Sagant.Descriptors.IWorkflowTypeInfo.WorkflowTypeName => "SampleWorkflow";""", combined);
    }

    [Fact]
    public void Generator_ForNonPartialWorkflowWithSteps_ReportsDiagnostic()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant;
            using Sagant.Settings;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public class NotPartialWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowStep]
                public Task<StepEffect<string>> DoStep() =>
                    Task.FromResult(StepEffects.ThenComplete());
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Severity == DiagnosticSeverity.Error && d.Id == "SAG001");
    }

    [Fact]
    public void Generator_WithExplicitStepName_UsesOverrideAsDurableName()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Settings;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public partial class RenamedStepWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowStep("legacy_step_name")]
                public Task<StepEffect<string>> DoStep() =>
                    Task.FromResult(StepEffects.ThenComplete());
            }
            """;

        var (_, diagnostics, generatedSources) = GeneratorTestHelper.RunGenerator(source);

        Assert.DoesNotContain(diagnostics, d => d.Severity == DiagnosticSeverity.Error);
        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("\"legacy_step_name\"", combined);
    }

    [Fact]
    public void Generator_ForNestedPartialWorkflow_ProducesNoErrorDiagnostics()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Settings;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public partial class Container
            {
                public partial class NestedWorkflow : Workflow<string>
                {
                    public override string EmptyState() => string.Empty;

                    [WorkflowStep]
                    public Task<StepEffect<string>> DoStep() =>
                        Task.FromResult(StepEffects.ThenComplete());
                }
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));
    }

    [Fact]
    public void Generator_ForNestedPartialWorkflow_WrapsGeneratedMembersInContainingPartialClass()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Settings;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public partial class Container
            {
                public partial class NestedWorkflow : Workflow<string>
                {
                    public override string EmptyState() => string.Empty;

                    [WorkflowStep]
                    public Task<StepEffect<string>> DoStep() =>
                        Task.FromResult(StepEffects.ThenComplete());
                }
            }
            """;

        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(source);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("partial class Container", combined);
        Assert.Contains("partial class NestedWorkflow", combined);
    }

    [Fact]
    public void Generator_ForNestedWorkflowWithNonPartialContainer_ReportsDiagnostic()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Settings;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public class Container
            {
                public partial class NestedWorkflow : Workflow<string>
                {
                    public override string EmptyState() => string.Empty;

                    [WorkflowStep]
                    public Task<StepEffect<string>> DoStep() =>
                        Task.FromResult(StepEffects.ThenComplete());
                }
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Severity == DiagnosticSeverity.Error && d.Id == "SAG001");
    }

    [Fact]
    public void Generator_ForWorkflowWithOnlyCommandHandlers_StillImplementsBothDispatcherInterfaces()
    {
        const string source = """
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;
            using Sagant.Settings;

            namespace TestNamespace;

            public sealed record Ping;

            public partial class PingOnlyWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowCommandHandler]
                public CommandEffect<string> HandlePing(Ping cmd) => Effects.Reply("pong");
            }
            """;

        var (_, diagnostics, generatedSources) = GeneratorTestHelper.RunGenerator(source);

        Assert.DoesNotContain(diagnostics, d => d.Severity == DiagnosticSeverity.Error);
        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("IWorkflowStepDispatcher<string>", combined);
        Assert.Contains("IWorkflowCommandDispatcher<string>", combined);
        Assert.Contains("IWorkflowQueryDispatcher<string>", combined);
    }

    private const string ContextWorkflowSource = """
        using System.Threading.Tasks;
        using Sagant;
        using Sagant.Settings;
        using Sagant.Descriptors;
        using Sagant.Effects;

        namespace TestNamespace;

        public sealed record Go(int Amount);
        public sealed record Peek;

        public partial class ContextWorkflow : Workflow<string>
        {
            public override string EmptyState() => string.Empty;

            [WorkflowStep]
            public StepEffect<string> SyncStep(StepContext<string> ctx) =>
                StepEffects.UpdateState(ctx.State + ctx.Attempt).ThenComplete();

            [WorkflowStep]
            public Task<StepEffect<string>> AsyncStep(int amount, StepContext<string> ctx) =>
                Task.FromResult(StepEffects.UpdateState(ctx.State + amount).ThenComplete());

            [WorkflowStep]
            public Task<StepEffect<string>> ContextFirstStep(StepContext<string> ctx, int amount) =>
                Task.FromResult(StepEffects.UpdateState(ctx.State + amount).ThenComplete());

            [WorkflowCommandHandler]
            public CommandEffect<string> Handle(Go cmd, CommandContext<string> ctx) =>
                Effects.TransitionTo(Steps.AsyncStep, cmd.Amount).ThenReply(ctx.State);

            [WorkflowQuery]
            public async Task<QueryEffect> Look(Peek peek, QueryContext<string> ctx)
            {
                await Task.Yield();
                return QueryEffects.Reply(ctx.State);
            }
        }
        """;

    [Fact]
    public void Generator_ForHandlersWithContexts_ProducesNoErrorDiagnostics()
    {
        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(ContextWorkflowSource);

        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));
    }

    /// <summary>The context parameter is bound by type, so a handler is free to declare it before or
    /// after its input — the emitted call preserves whichever order was written.</summary>
    [Fact]
    public void Generator_BindsContextByType_PreservingDeclaredParameterOrder()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(ContextWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("AsyncStep(((int)input!), ctx)", combined);
        Assert.Contains("ContextFirstStep(ctx, ((int)input!))", combined);
    }

    [Fact]
    public void Generator_ForSynchronousStep_WrapsInvokerInTaskFromResult()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(ContextWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("Task.FromResult(((ContextWorkflow)workflow).SyncStep(ctx))", combined);
    }

    [Fact]
    public void Generator_ForQuery_EmitsQueryTableWithCompileTimeTypeName()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(ContextWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Contains("TryGetQuery", combined);
        Assert.Contains("typeof(global::TestNamespace.Peek), \"Peek\"", combined);
    }

    /// <summary>Dispatch tables are built once and only read afterwards, so they're frozen.</summary>
    [Fact]
    public void Generator_EmitsFrozenDispatchTables()
    {
        var (_, _, generatedSources) = GeneratorTestHelper.RunGenerator(ContextWorkflowSource);

        var combined = string.Join("\n---\n", generatedSources);
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(
            combined, @"System\.Collections\.Frozen\.FrozenDictionary\.ToFrozenDictionary").Count);
    }

    [Fact]
    public void Generator_ForMutableStateType_ReportsSag002()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed class MutableState
            {
                public string Value = "initial";
            }

            public partial class MutableStateWorkflow : Workflow<MutableState>
            {
                public override MutableState EmptyState() => new();

                [WorkflowStep]
                public StepEffect<MutableState> DoStep(StepContext<MutableState> ctx) => StepEffects.ThenComplete();
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Id == "SAG002" && d.Severity == DiagnosticSeverity.Warning);
    }

    [Fact]
    public void Generator_ForImmutableStateType_ReportsNoShapeDiagnostics()
    {
        const string source = """
            using System.Collections.Generic;
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed record ImmutableState(string Value, IReadOnlyList<int> Items);

            public partial class ImmutableStateWorkflow : Workflow<ImmutableState>
            {
                public override ImmutableState EmptyState() => new();

                [WorkflowStep]
                public StepEffect<ImmutableState> DoStep(StepContext<ImmutableState> ctx) => StepEffects.ThenComplete();
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.DoesNotContain(diagnostics, d => d.Id is "SAG002" or "SAG003");
    }

    [Fact]
    public void Generator_ForMutableCollectionInState_ReportsSag003()
    {
        const string source = """
            using System.Collections.Generic;
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed record ListState(List<int> Items);

            public partial class ListStateWorkflow : Workflow<ListState>
            {
                public override ListState EmptyState() => new();

                [WorkflowStep]
                public StepEffect<ListState> DoStep(StepContext<ListState> ctx) => StepEffects.ThenComplete();
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Id == "SAG003");
    }

    [Fact]
    public void Generator_ForQueryReturningCommandEffect_ReportsSag004()
    {
        const string source = """
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed record Peek;

            public partial class BadQueryWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowQuery]
                public CommandEffect<string> Look(Peek peek, QueryContext<string> ctx) => Effects.Reply("no");
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Id == "SAG004" && d.Severity == DiagnosticSeverity.Error);
    }

    [Fact]
    public void Generator_ForAsyncCommandHandler_ReportsSag005()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed record Go;

            public partial class AsyncCommandWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowCommandHandler]
                public async Task<CommandEffect<string>> Handle(Go cmd, CommandContext<string> ctx)
                {
                    await Task.Yield();
                    return Effects.Reply("nope");
                }
            }
            """;

        var (_, diagnostics, _) = GeneratorTestHelper.RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Id == "SAG005" && d.Severity == DiagnosticSeverity.Error);
    }

    /// <summary>
    /// A workflow may be written across several <c>partial</c> blocks, and the generated <c>Steps</c>
    /// table has to hold every step regardless of which block declares it. The block carrying the
    /// steps is not necessarily the block carrying <c>: Workflow&lt;TState&gt;</c>.
    /// </summary>
    [Fact]
    public void Generator_ForWorkflowSplitAcrossPartials_IncludesStepsFromEveryBlock()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed record Go;

            public partial class SplitWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowCommandHandler]
                public CommandEffect<string> Handle(Go cmd) =>
                    Effects.TransitionTo(Steps.DeclaredHere).ThenReply("accepted");

                [WorkflowStep]
                public Task<StepEffect<string>> DeclaredHere() =>
                    Task.FromResult(StepEffects.ThenTransitionTo(Steps.DeclaredInTheOtherBlock));
            }

            public partial class SplitWorkflow
            {
                [WorkflowStep]
                public Task<StepEffect<string>> DeclaredInTheOtherBlock() =>
                    Task.FromResult(StepEffects.ThenComplete());
            }
            """;

        var (_, diagnostics, generated) = GeneratorTestHelper.RunGenerator(source);

        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));

        // One file for the workflow, holding both steps — the second block's step is dispatchable
        // and reachable through Steps, which is what the transition above relies on.
        var file = Assert.Single(generated, g => g.Contains("class SplitWorkflow"));
        Assert.Contains("DeclaredHere", file);
        Assert.Contains("DeclaredInTheOtherBlock", file);
    }

    /// <summary>The same split, with the base list on the second block.</summary>
    [Fact]
    public void Generator_ForWorkflowWhoseBaseListIsOnAnotherBlock_StillGenerates()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public partial class BaseLast
            {
                [WorkflowStep]
                public Task<StepEffect<string>> First() => Task.FromResult(StepEffects.ThenComplete());
            }

            public partial class BaseLast : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowStep]
                public Task<StepEffect<string>> Second() => Task.FromResult(StepEffects.ThenComplete());
            }
            """;

        var (_, diagnostics, generated) = GeneratorTestHelper.RunGenerator(source);

        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));

        var file = Assert.Single(generated, g => g.Contains("class BaseLast"));
        Assert.Contains("First", file);
        Assert.Contains("Second", file);
    }

    /// <summary>
    /// The case that fails quietly. A step declared in another block is at least named through
    /// <c>Steps</c>, so leaving it out breaks the build. A command handler is dispatched by message
    /// type and never named, so leaving it out compiles and then finds no handler at runtime.
    /// </summary>
    [Fact]
    public void Generator_ForCommandHandlerInAnotherPartialBlock_IncludesItInTheDispatcher()
    {
        const string source = """
            using System.Threading.Tasks;
            using Sagant;
            using Sagant.Descriptors;
            using Sagant.Effects;

            namespace TestNamespace;

            public sealed record Approve;

            public partial class SplitCommandWorkflow : Workflow<string>
            {
                public override string EmptyState() => string.Empty;

                [WorkflowStep]
                public Task<StepEffect<string>> Work() => Task.FromResult(StepEffects.ThenComplete());
            }

            public partial class SplitCommandWorkflow
            {
                [WorkflowCommandHandler]
                public CommandEffect<string> Approve(Approve cmd) =>
                    Effects.TransitionTo(Steps.Work).ThenReply("accepted");
            }
            """;

        var (_, diagnostics, generated) = GeneratorTestHelper.RunGenerator(source);

        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));

        var file = Assert.Single(generated, g => g.Contains("class SplitCommandWorkflow"));
        Assert.Contains("Approve", file);
        Assert.Contains("IWorkflowCommandDispatcher", file);
    }
}
