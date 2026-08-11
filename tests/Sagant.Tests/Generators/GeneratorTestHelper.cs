using System.Collections.Immutable;
using Sagant.SourceGenerators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Sagant.Tests.Generators;

internal static class GeneratorTestHelper
{
    public static (Compilation OutputCompilation, ImmutableArray<Diagnostic> Diagnostics, IReadOnlyList<string> GeneratedSources)
        RunGenerator(string source)
    {
        var references = Basic.Reference.Assemblies.Net100.References.All
            .Append(MetadataReference.CreateFromFile(typeof(Workflow<>).Assembly.Location))
            .ToArray();

        var compilation = CSharpCompilation.Create(
            "GeneratorTestAssembly",
            new[] { CSharpSyntaxTree.ParseText(source) },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new StepRegistryGenerator();
        var driver = CSharpGeneratorDriver.Create(generator);

        driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(
            compilation, out var outputCompilation, out var generatorDiagnostics);

        var runResult = driver.GetRunResult();
        var generatedSources = runResult.Results
            .SelectMany(r => r.GeneratedSources)
            .Select(s => s.SourceText.ToString())
            .ToList();

        var compileDiagnostics = outputCompilation.GetDiagnostics();

        return (outputCompilation, generatorDiagnostics.AddRange(compileDiagnostics), generatedSources);
    }
}
