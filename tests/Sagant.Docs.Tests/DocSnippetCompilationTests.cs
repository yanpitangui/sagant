using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Sagant.SourceGenerators;

namespace Sagant.Docs.Tests;

/// <summary>
/// Compiles every `csharp` block in `README.md` and `docs/*.md`.
///
/// Documentation drifts silently: a rename in the public surface leaves the prose reading fine and
/// the code wrong, and nothing fails. This makes a doc snippet as breakable as a test, so a change
/// to the surface has to update whatever teaches it.
///
/// Snippets compile with the source generator running, because most of what the docs show depends on
/// generated output — a step is referenced as <c>Steps.ChargePaymentStep</c>, which only exists once
/// the generator has seen the class.
/// </summary>
public class DocSnippetCompilationTests
{
    private static readonly string DocsDirectory =
        Path.Combine(AppContext.BaseDirectory, "docs");

    public static TheoryData<DocSnippet> Snippets()
    {
        var data = new TheoryData<DocSnippet>();
        foreach (var snippet in AllSnippets())
        {
            data.Add(snippet);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Snippets))]
    public void SnippetCompiles(DocSnippet snippet)
    {
        if (snippet.Scaffold == Scaffold.Skip)
        {
            Assert.False(string.IsNullOrWhiteSpace(snippet.SkipReason));
            return;
        }

        var source = DocSnippetExtractor.Scaffolded(snippet);
        var errors = Compile(source)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToList();

        Assert.True(
            errors.Count == 0,
            $"""
             {snippet} does not compile.

             {string.Join(Environment.NewLine, errors.Select(e => "  " + e))}

             ── scaffolded source ──
             {Numbered(source)}
             """);
    }

    /// <summary>
    /// The docs have to be reachable from the test binary at all, which a wrong copy-to-output glob
    /// would silently turn into zero snippets and a green run.
    /// </summary>
    [Fact]
    public void EveryDocumentedFileIsCovered()
    {
        var snippets = AllSnippets();

        Assert.True(Directory.Exists(DocsDirectory), $"No docs copied to {DocsDirectory}.");
        Assert.NotEmpty(snippets);

        // A doc that loses its last snippet is fine; a doc that was never read is not.
        var filesWithSnippets = snippets.Select(s => s.DocFile).Distinct().ToList();
        Assert.Contains("README.md", filesWithSnippets);
    }

    /// <summary>Skipping is deliberate, so the reasons stay visible, out in the open where they can be counted.</summary>
    [Fact]
    public void SkippedSnippetsStayRare()
    {
        var snippets = AllSnippets();
        var skipped = snippets.Where(s => s.Scaffold == Scaffold.Skip).ToList();

        Assert.True(
            skipped.Count * 4 <= snippets.Count,
            $"""
             {skipped.Count} of {snippets.Count} snippets are skipped, which is more than a quarter.
             Skipping is for code that genuinely cannot compile, and at this rate the check stops
             meaning anything:

             {string.Join(Environment.NewLine, skipped.Select(s => $"  {s} — {s.SkipReason}"))}
             """);
    }

    private static IReadOnlyList<DocSnippet> AllSnippets()
    {
        if (!Directory.Exists(DocsDirectory))
        {
            return Array.Empty<DocSnippet>();
        }

        return Directory.EnumerateFiles(DocsDirectory, "*.md")
            .OrderBy(f => f)
            .SelectMany(f => DocSnippetExtractor.Extract(Path.GetFileName(f), File.ReadAllText(f)))
            .ToList();
    }

    private static ImmutableArray<Diagnostic> Compile(string source)
    {
        var compilation = CSharpCompilation.Create(
            "DocSnippetAssembly",
            [CSharpSyntaxTree.ParseText(source)],
            MetadataReferences.Value,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var driver = CSharpGeneratorDriver.Create(new StepRegistryGenerator());
        driver.RunGeneratorsAndUpdateCompilation(compilation, out var withGenerated, out var generatorDiagnostics);

        return generatorDiagnostics.AddRange(withGenerated.GetDiagnostics());
    }

    /// <summary>
    /// Every assembly this test project itself runs against. Taking the real set means a snippet is
    /// checked against the same Sagant and Akka.NET builds the repo ships.
    /// </summary>
    private static readonly Lazy<MetadataReference[]> MetadataReferences = new(() =>
    {
        var byName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var trusted = (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);

        foreach (var path in trusted)
        {
            byName[Path.GetFileNameWithoutExtension(path)] = path;
        }

        // The test binary's own directory wins, so a project reference beats anything the host
        // happened to load first.
        foreach (var path in Directory.EnumerateFiles(AppContext.BaseDirectory, "*.dll"))
        {
            byName[Path.GetFileNameWithoutExtension(path)] = path;
        }

        return byName.Values
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToArray();
    });

    private static string Numbered(string source) =>
        string.Join(
            Environment.NewLine,
            source.Split('\n').Select((line, i) => $"{i + 1,4}| {line.TrimEnd()}"));
}
