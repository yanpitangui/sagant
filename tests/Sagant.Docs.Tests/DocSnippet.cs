using System.Text;
using System.Text.RegularExpressions;

namespace Sagant.Docs.Tests;

/// <summary>How a snippet has to be wrapped before it means anything to a compiler.</summary>
public enum Scaffold
{
    /// <summary>Complete type declarations. Compiled with usings around it and nothing else.</summary>
    File,

    /// <summary>Members of a workflow class — a <c>[WorkflowStep]</c> method and friends.</summary>
    WorkflowMember,

    /// <summary>Statements, as a caller would write them. Wrapped in an async method body.</summary>
    Statements,

    /// <summary>A whole test method. Wrapped in a test class.</summary>
    TestMember,

    /// <summary>Deliberately uncompilable. Carries a reason.</summary>
    Skip,
}

/// <summary>One `csharp` block from the documentation, with where it came from.</summary>
public sealed record DocSnippet(
    string DocFile,
    int Index,
    int Line,
    Scaffold Scaffold,
    string? SkipReason,
    string Code)
{
    /// <summary>What a failure names, so a broken snippet is findable without counting fences.</summary>
    public override string ToString() => $"{DocFile}:{Line} (block {Index}, scaffold={Scaffold.ToString().ToLowerInvariant()})";
}

/// <summary>
/// Pulls `csharp` blocks out of the documentation.
///
/// The scaffold is declared in the fence's info string — ```` ```csharp scaffold=statements ````.
/// Markdown renderers take the first word as the language and ignore the rest, so this is invisible
/// on GitHub while still being explicit in the source.
///
/// A block with no <c>scaffold=</c> is an error — it has no default. A snippet that nothing checks
/// is how the documentation drifted in the first place, so the failure has to happen when the block
/// is written.
/// </summary>
public static class DocSnippetExtractor
{
    private static readonly Regex Fence = new(
        @"^[ \t]*```csharp(?<info>[^\n]*)\n(?<code>.*?)^[ \t]*```",
        RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex ScaffoldTag = new(
        @"scaffold=(?<value>[a-z-]+)", RegexOptions.Compiled);

    private static readonly Regex ReasonTag = new(
        @"reason=""(?<value>[^""]*)""", RegexOptions.Compiled);

    public static IReadOnlyList<DocSnippet> Extract(string docFile, string markdown)
    {
        var snippets = new List<DocSnippet>();
        var index = 0;

        foreach (Match match in Fence.Matches(markdown))
        {
            var info = match.Groups["info"].Value;
            var line = markdown.Take(match.Index).Count(c => c == '\n') + 1;

            var scaffoldTag = ScaffoldTag.Match(info);
            if (!scaffoldTag.Success)
            {
                throw new InvalidOperationException(
                    $"{docFile}:{line} — csharp block {index} has no scaffold. Tag the fence, e.g. " +
                    "```csharp scaffold=statements. Valid: file, workflow-member, statements, " +
                    "test-member, skip (which also needs reason=\"...\").");
            }

            var scaffold = scaffoldTag.Groups["value"].Value switch
            {
                "file" => Scaffold.File,
                "workflow-member" => Scaffold.WorkflowMember,
                "statements" => Scaffold.Statements,
                "test-member" => Scaffold.TestMember,
                "skip" => Scaffold.Skip,
                var other => throw new InvalidOperationException(
                    $"{docFile}:{line} — unknown scaffold '{other}'."),
            };

            var reason = ReasonTag.Match(info) is { Success: true } r ? r.Groups["value"].Value : null;
            if (scaffold == Scaffold.Skip && string.IsNullOrWhiteSpace(reason))
            {
                throw new InvalidOperationException(
                    $"{docFile}:{line} — scaffold=skip needs reason=\"why this cannot compile\".");
            }

            snippets.Add(new DocSnippet(docFile, index, line, scaffold, reason, match.Groups["code"].Value));
            index++;
        }

        return snippets;
    }

    /// <summary>
    /// Turns a snippet into a compilable source file.
    ///
    /// A snippet showing one step still refers to the others around it, so a filler supplies every
    /// step the docs name, minus the ones the snippet declares itself.
    ///
    /// The filler's steps go inside the class this scaffold opens, because
    /// <c>StepRegistryGenerator</c> reads the members of a single class declaration — a step sitting
    /// in a second <c>partial</c> block is absent from the generated <c>Steps</c> table. A snippet
    /// that declares the workflow class itself therefore has to name every step it references, which
    /// is a fair thing to ask of a complete example.
    /// </summary>
    public static string Scaffolded(DocSnippet snippet)
    {
        var body = new StringBuilder();
        body.AppendLine(Usings);

        switch (snippet.Scaffold)
        {
            case Scaffold.File:
                body.AppendLine(snippet.Code);
                break;

            case Scaffold.WorkflowMember:
                body.AppendLine("public partial class OrderFulfillmentWorkflow : Workflow<OrderState>");
                body.AppendLine("{");
                body.AppendLine(snippet.Code);
                body.AppendLine(FillerSteps(snippet.Code));
                body.AppendLine("}");
                break;

            case Scaffold.Statements:
                body.AppendLine("public sealed class DocStatements : DocStatementsAmbient");
                body.AppendLine("{");
                body.AppendLine("    public async Task Run()");
                body.AppendLine("    {");
                body.AppendLine(snippet.Code);
                body.AppendLine("        await Task.CompletedTask;");
                body.AppendLine("    }");
                body.AppendLine("}");
                break;

            case Scaffold.TestMember:
                body.AppendLine("public sealed class DocTests : DocStatementsAmbient");
                body.AppendLine("{");
                body.AppendLine(snippet.Code);
                body.AppendLine("}");
                break;

            case Scaffold.Skip:
                throw new InvalidOperationException("A skipped snippet is never scaffolded.");
        }

        if (snippet.Scaffold is Scaffold.Statements or Scaffold.TestMember)
        {
            body.AppendLine(Ambient);
        }

        body.AppendLine(FillerFor(snippet.Code, snippet.Scaffold));
        return body.ToString();
    }

    private const string Usings = """
        using System;
        using System.Collections.Generic;
        using System.Linq;
        using System.Threading;
        using System.Threading.Tasks;
        using Microsoft.Extensions.Time.Testing;
        using Akka.Hosting;
        using Akka.Cluster.Hosting;
        using Akka.Persistence.Hosting;
        using Akka.Remote.Hosting;
        using Microsoft.Extensions.DependencyInjection;
        using Sagant;
        using OpenTelemetry.Metrics;
        using OpenTelemetry.Trace;
        using Sagant.Clients;
        using Sagant.Descriptors;
        using Sagant.Effects;
        using Sagant.Protocol;
        using Sagant.Runtime.Akka;
        using Sagant.Runtime.Akka.Clustering;
        using Sagant.Runtime.Akka.Deadlines;
        using Sagant.Scheduling;
        using Sagant.Settings;
        using Sagant.Testing;
        using Sagant.Docs.Tests.Fixtures;
        using Xunit;
        """;

    /// <summary>Every step the documentation refers to by name, with the shape it refers to it in.</summary>
    private static readonly (string Name, string Declaration)[] KnownSteps =
    [
        ("ChargePaymentStep", "[WorkflowStep] public StepEffect<OrderState> ChargePaymentStep(StepContext<OrderState> ctx) => StepEffects.ThenComplete();"),
        ("RefundPaymentStep", "[WorkflowStep] public StepEffect<OrderState> RefundPaymentStep() => StepEffects.ThenComplete();"),
        ("FastTrackStep", "[WorkflowStep] public StepEffect<OrderState> FastTrackStep() => StepEffects.ThenComplete();"),
        ("EscalateStep", "[WorkflowStep] public StepEffect<OrderState> EscalateStep() => StepEffects.ThenComplete();"),
        ("NextStep", "[WorkflowStep] public StepEffect<OrderState> NextStep() => StepEffects.ThenComplete();"),
        ("SlowStep", "[WorkflowStep] public StepEffect<OrderState> SlowStep() => StepEffects.ThenComplete();"),
        ("WaitForPaymentWebhook", "[WorkflowStep] public StepEffect<OrderState> WaitForPaymentWebhook() => StepEffects.ThenComplete();"),
        ("StartLineItemWorkflows", "[WorkflowStep] public StepEffect<OrderState> StartLineItemWorkflows(StepContext<OrderState> ctx) => StepEffects.ThenComplete();"),
        ("OnLineItemsDone", "[WorkflowStep] public StepEffect<OrderState> OnLineItemsDone(ChildGroupResult result) => StepEffects.ThenComplete();"),
    ];

    /// <summary>Matches a snippet declaring some workflow class of its own, distinct from the
    /// canonical <c>OrderFulfillmentWorkflow</c> — a self-contained example (the README's minimum
    /// quickstart, say) that needs no filler for an example it never references.</summary>
    private static readonly Regex DeclaresAWorkflowClass = new(@"class\s+\w+\s*:\s*Workflow<", RegexOptions.Compiled);

    private static string FillerFor(string code, Scaffold scaffold)
    {
        var declaresWorkflow = code.Contains("class OrderFulfillmentWorkflow", StringComparison.Ordinal);

        if (!declaresWorkflow && DeclaresAWorkflowClass.IsMatch(code))
        {
            return GreetingWorkflowFillerIfReferenced(code);
        }

        // The generator reads one class declaration, so the steps have to sit in whichever
        // declaration is the workflow's real one. A workflow-member scaffold already opened it and
        // put them there; anything else leaves this filler as the only declaration there is.
        var ownsTheDeclaration = scaffold != Scaffold.WorkflowMember && !declaresWorkflow;

        var filler = new StringBuilder();
        filler.AppendLine(GreetingWorkflowFillerIfReferenced(code));
        filler.AppendLine(declaresWorkflow
            ? "public partial class OrderFulfillmentWorkflow"
            : "public partial class OrderFulfillmentWorkflow : Workflow<OrderState>");
        filler.AppendLine("{");

        // The docs use these without ever showing the constructor that supplies them.
        filler.AppendLine("    private readonly IPaymentService _payment = new RealPaymentService();");
        filler.AppendLine("    private readonly ICustomerService _customers = default!;");
        filler.AppendLine("    public OrderFulfillmentWorkflow() { }");
        filler.AppendLine("    public OrderFulfillmentWorkflow(IPaymentService payment) { _payment = payment; }");

        if (!code.Contains("EmptyState", StringComparison.Ordinal))
        {
            filler.AppendLine("    public override OrderState EmptyState() => OrderState.Empty();");
        }

        if (ownsTheDeclaration)
        {
            filler.AppendLine(FillerSteps(code));
        }

        filler.AppendLine("}");
        return filler.ToString();
    }

    private static string FillerSteps(string code)
    {
        var steps = new StringBuilder();
        foreach (var (name, declaration) in KnownSteps)
        {
            if (!Declares(code, name))
            {
                steps.AppendLine($"    {declaration}");
            }
        }

        return steps.ToString();
    }

    /// <summary>
    /// The README's minimum quickstart example, shown in full in the doc that defines it and
    /// referenced by name alone in the doc that registers/drives it — the same split
    /// <c>OrderFulfillmentWorkflow</c> gets, at a smaller scale. A snippet that only references
    /// <c>GreetingWorkflow</c> (register/drive) needs this filler; one that declares it itself
    /// (define) supplies its own and this returns nothing so the two never collide.
    /// </summary>
    private static string GreetingWorkflowFillerIfReferenced(string code) =>
        Regex.IsMatch(code, @"\bGreetingWorkflow\b") && !code.Contains("class GreetingWorkflow", StringComparison.Ordinal)
            ? """
              public sealed record GreetingState(string Name = "", string? Greeting = null);
              public sealed record Greet(string Name);
              public partial class GreetingWorkflow : Workflow<GreetingState>
              {
                  public override GreetingState EmptyState() => new();

                  [WorkflowCommandHandler]
                  public CommandEffect<GreetingState> Start(Greet cmd, CommandContext<GreetingState> ctx) =>
                      Effects.UpdateState(ctx.State with { Name = cmd.Name }).TransitionTo(Steps.SayHello);

                  [WorkflowStep]
                  public StepEffect<GreetingState> SayHello(StepContext<GreetingState> ctx) =>
                      StepEffects.UpdateState(ctx.State with { Greeting = $"Hello, {ctx.State.Name}!" }).ThenComplete();
              }
              """
            : string.Empty;

    /// <summary>
    /// The names a caller-side snippet writes without introducing. Emitted as fields, so a snippet
    /// opening with <c>var harness = ...</c> shadows one legally.
    /// </summary>
    private const string Ambient = """
        public abstract class DocStatementsAmbient
        {
            protected readonly IServiceCollection services = new ServiceCollection();
            protected readonly IServiceProvider sp = default!;
            protected readonly AkkaConfigurationBuilder builder = default!;
            protected readonly IWorkflowClient client = default!;
            protected readonly string orderId = "order-1";
            protected readonly IPaymentService fakePaymentService = new RealPaymentService();
            protected readonly IInventoryService fakeInventory = default!;
            protected readonly TaskCompletionSource gate = new();
            protected readonly IEnumerable<ChildStart> children = Array.Empty<ChildStart>();
            protected readonly WorkflowTestHarness<OrderFulfillmentWorkflow, OrderState> harness = default!;
        }
        """;

    private static bool Declares(string code, string memberName) =>
        Regex.IsMatch(code, $@"\b{Regex.Escape(memberName)}\s*\(");
}
