# Benchmarks

Reproducible numbers for the allocation/CPU claims made about Sagant's core layer — the same kind of
claim a commit message or `docs/guarantees.md` makes in passing ("a fan-out's per-report cost stays
flat"), pinned to a benchmark anyone can run again, on this machine or any other.

`Sagant.Benchmarks` is a console app driven by [BenchmarkDotNet](https://benchmarkdotnet.org/) —
`dotnet test Sagant.slnx` already skips it, since it carries no test SDK reference for the test
runner to find. `IsPackable` is `false`, and it lives outside `src/`, so the release workflow's own
pack step never reaches it either. `dotnet build Sagant.slnx` still compiles it, which is what keeps
it honest as the core API changes underneath it.

## Running

```bash
cd benchmarks/Sagant.Benchmarks
dotnet run -c Release                              # runs every benchmark class
dotnet run -c Release -- --filter '*ChildFanOut*'   # one class
```

Always `-c Release` — BenchmarkDotNet refuses to run a Debug build. Each run takes several minutes;
how many iterations that takes is decided by BenchmarkDotNet's own statistical stopping rules.

## What's here

- **`ChildFanOutBenchmarks`** — what one child report costs a parent awaiting a group, at group sizes
  of 100/1,000/10,000. This is guarantee H5's claim as a runnable number: the read side
  (`ChildGroupPolicy.TallyGroup`) and the write side (`WorkflowEventFold.Apply` folding a
  `ChildMemberUpdated`) are benchmarked as separate methods, so each one's own regression shows up in
  its own row.
- **`TransitionPlannerBenchmarks`** — what `WorkflowTransitionPlanner.Plan` costs per transition, for
  a transition that writes state and one that leaves it alone.

Add a class here for a change whose whole justification is an allocation or throughput number — the
next person auditing that change gets a command to run, not a transcript to trust.
