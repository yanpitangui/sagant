; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
SAG001  | Sagant | Error | Workflow class with handler methods must be partial
SAG002  | Sagant | Warning | Workflow state should be immutable
SAG003  | Sagant | Info | Workflow state exposes a mutable collection
SAG004  | Sagant | Error | Query handler must return QueryEffect
SAG005  | Sagant | Error | Command handler must be synchronous
SAG006  | Sagant | Error | Child-result handler must return ChildResultEffect&lt;TState&gt;
SAG007  | Sagant | Error | A workflow declares at most one child-result handler
