using Sagant.Settings;
using Sagant.Descriptors;
using Sagant.Effects;
using Sagant.Protocol;
using Sagant.Runtime.Akka.Clustering;
using Akka.Actor;
using Akka.Delivery;
using Akka.TestKit;
using Akka.TestKit.Xunit2;

namespace Sagant.Runtime.Akka.Tests.Support;

/// <summary>
/// Shared fixture for <see cref="WorkflowEntityActor{TWorkflow, TState}"/> tests: a scriptable
/// workflow whose step/command behavior is fully test-controlled (per-instance, no static/shared
/// state), plus an actor-creation helper. Subclass with whatever HOCON config a given test suite
/// needs (e.g. <see cref="global::Akka.TestKit.TestScheduler"/> for deterministic timeout tests).
/// </summary>
public abstract class WorkflowActorTestKit : TestKit
{
    protected WorkflowActorTestKit(string config) : base(config)
    {
    }

    public sealed class TestState
    {
        public string Value { get; init; } = "initial";
    }

    public sealed record StartWorkflow(int Amount);

    /// <summary>
    /// A step reference for the scripted workflows below. They hand-implement the dispatcher
    /// interfaces, so there is no generated <c>Steps</c> table to name a step through — this is the
    /// deliberate way to build one from a name known only at runtime.
    /// </summary>
    protected static StepRef<ScriptableWorkflow, NoInput> Step(string name) => new(name);

    /// <inheritdoc cref="Step(string)"/>
    protected static StepRef<ScriptableWorkflow, TInput> Step<TInput>(string name) => new(name);

    /// <summary>
    /// A script for the shared workflow fixture. Tests state only the behavior relevant to their
    /// scenario; dispatcher table construction stays inside test support.
    /// </summary>
    protected sealed class WorkflowScript
    {
        private readonly Dictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> _steps = new();
        private readonly Dictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>> _cancellableSteps = new();
        private readonly Dictionary<Type, Func<TestState, object, CommandEffect<TestState>>> _commands = new();
        private readonly Dictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>> _queries = new();
        private Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? _childResult;

        internal IReadOnlyDictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> Steps => _steps;
        internal IReadOnlyDictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>> CancellableSteps => _cancellableSteps;
        internal IReadOnlyDictionary<Type, Func<TestState, object, CommandEffect<TestState>>> Commands => _commands;
        internal IReadOnlyDictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>> Queries => _queries;
        internal Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? ChildResult => _childResult;

        /// <summary>Registers a read-only query handler. Asynchronous, so a test can park one
        /// mid-flight and observe that the entity keeps handling other messages.</summary>
        public WorkflowScript Query<TQuery>(Func<TestState, TQuery, CancellationToken, Task<QueryEffect>> handler)
            where TQuery : notnull
        {
            _queries.Add(typeof(TQuery), (state, query, ct) => handler(state, (TQuery)query, ct));
            return this;
        }

        /// <summary>Synchronous form of <see cref="Query{TQuery}"/>.</summary>
        public WorkflowScript Query<TQuery>(Func<TestState, TQuery, QueryEffect> handler)
            where TQuery : notnull
        {
            _queries.Add(typeof(TQuery), (state, query, _) => Task.FromResult(handler(state, (TQuery)query)));
            return this;
        }

        public WorkflowScript Step(string name, Func<TestState, object?, Task<StepEffect<TestState>>> handler)
        {
            _steps.Add(name, handler);
            return this;
        }

        public WorkflowScript Command<TCommand>(Func<TestState, TCommand, CommandEffect<TestState>> handler)
            where TCommand : notnull
        {
            _commands.Add(typeof(TCommand), (state, command) => handler(state, (TCommand)command));
            return this;
        }

        /// <summary>Registers the handler a parent runs as each of its children settles.</summary>
        public WorkflowScript OnChildResult(Func<ChildResultContext<TestState>, ChildResultEffect<TestState>> handler)
        {
            _childResult = handler;
            return this;
        }

        public WorkflowScript CancellableStep(
            string name,
            Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>> handler)
        {
            _cancellableSteps.Add(name, handler);
            return this;
        }
    }

    protected static WorkflowScript Script() => new();

    /// <summary>
    /// Shared dispatcher plumbing for every scriptable workflow identity below. <c>TSelf</c> is a
    /// CRTP parameter (not a runtime-meaningful type), used only so the generated-style dispatch
    /// tables below can name a concrete workflow identity. A concrete sealed subclass supplies the
    /// public constructor and the one-line <see cref="IWorkflowTypeInfo"/> implementation — that's
    /// the entire cost of registering a second identity under <c>WorkflowHandleRegistry</c>, which
    /// keys by compile-time <c>TWorkflow</c> type, not by any runtime string.
    /// </summary>
    protected abstract class ScriptableWorkflowBase<TSelf> : Workflow<TestState>, IWorkflowStepDispatcher<TestState>, IWorkflowCommandDispatcher<TestState>, IWorkflowQueryDispatcher<TestState>, IWorkflowChildResultDispatcher<TestState>
        where TSelf : ScriptableWorkflowBase<TSelf>
    {
        private readonly IReadOnlyDictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> _steps;
        private readonly IReadOnlyDictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>? _ctSteps;
        private readonly IReadOnlyDictionary<Type, Func<TestState, object, CommandEffect<TestState>>> _commands;
        private readonly WorkflowSettings _settings;
        private readonly string? _workflowTypeName;

        private readonly IReadOnlyDictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>? _queries;

        protected ScriptableWorkflowBase(
            IReadOnlyDictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> steps,
            IReadOnlyDictionary<Type, Func<TestState, object, CommandEffect<TestState>>> commands,
            WorkflowSettings? settings,
            IReadOnlyDictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>? ctSteps,
            string? workflowTypeName,
            IReadOnlyDictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>? queries = null,
            Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? childResult = null)
        {
            _childResult = childResult;
            _steps = steps;
            _commands = commands;
            _settings = settings ?? WorkflowSettings.Default;
            _ctSteps = ctSteps;
            _workflowTypeName = workflowTypeName;
            _queries = queries;
        }

        private readonly Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? _childResult;

        bool IWorkflowChildResultDispatcher<TestState>.TryGetChildResultHandler(out ChildResultDescriptor<TestState> descriptor)
        {
            if (_childResult is null)
            {
                descriptor = default;
                return false;
            }

            descriptor = new ChildResultDescriptor<TestState>((_, ctx) => _childResult(ctx));
            return true;
        }

        bool IWorkflowQueryDispatcher<TestState>.TryGetQuery(Type queryType, out QueryDescriptor<TestState> descriptor)
        {
            if (_queries is not null && _queries.TryGetValue(queryType, out var fn))
            {
                descriptor = new QueryDescriptor<TestState>(
                    queryType, queryType.Name, (w, ctx, query) => fn(ctx.State, query, ctx.CancellationToken));
                return true;
            }

            descriptor = default;
            return false;
        }

        public override TestState EmptyState() => new();

        public override WorkflowSettings Settings() => _settings;

        // Every WorkflowEntityActor test shares this one hand-written stand-in class (not
        // generator-touched, so the base class's GetType().Name fallback would apply) — that makes
        // "ScriptableWorkflow" collide across every concurrently-running test file's span/metric
        // tags. Span assertions get away with it by scoping on workflow.persistence_id instead (see
        // WorkflowTracingTests), but the metrics WorkflowDiagnostics.RecordStatusChange records
        // deliberately carry no per-instance tag (see its own doc comment on why) — so a metrics
        // test needs a distinct WorkflowTypeName per test to disambiguate instead. Defaults to the
        // real class name for every test that doesn't care.
        public override string WorkflowTypeName => _workflowTypeName ?? base.WorkflowTypeName;

        bool IWorkflowStepDispatcher<TestState>.TryGetStep(string stepName, out StepDescriptor<TestState> descriptor)
        {
            // Most tests don't care about cancellation and just want the plain two-arg form; a
            // step registered in _ctSteps (see WorkflowStepCancellationTests) gets the real token
            // forwarded, same as a generated dispatcher would for a step method that declares one.
            if (_ctSteps is not null && _ctSteps.TryGetValue(stepName, out var ctFn))
            {
                descriptor = new StepDescriptor<TestState>(stepName, typeof(object), (w, ctx, input) => ctFn(ctx.State, input, ctx.CancellationToken));
                return true;
            }

            if (_steps.TryGetValue(stepName, out var fn))
            {
                descriptor = new StepDescriptor<TestState>(stepName, typeof(object), (w, ctx, input) => fn(ctx.State, input));
                return true;
            }

            descriptor = default;
            return false;
        }

        IReadOnlyCollection<string> IWorkflowStepDispatcher<TestState>.StepNames =>
            _ctSteps is null ? (IReadOnlyCollection<string>)_steps.Keys : new List<string>(_steps.Keys).Concat(_ctSteps.Keys).ToList();

        bool IWorkflowCommandDispatcher<TestState>.TryGetHandler(Type commandType, out CommandDescriptor<TestState> descriptor)
        {
            if (_commands.TryGetValue(commandType, out var fn))
            {
                descriptor = new CommandDescriptor<TestState>(commandType, commandType.Name, (w, ctx, cmd) => fn(ctx.State, cmd));
                return true;
            }

            descriptor = default;
            return false;
        }
    }

    protected sealed class ScriptableWorkflow : ScriptableWorkflowBase<ScriptableWorkflow>, IWorkflowTypeInfo
    {
        public ScriptableWorkflow(
            IReadOnlyDictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> steps,
            IReadOnlyDictionary<Type, Func<TestState, object, CommandEffect<TestState>>> commands,
            WorkflowSettings? settings = null,
            IReadOnlyDictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>? ctSteps = null,
            string? workflowTypeName = null,
            IReadOnlyDictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>? queries = null,
            Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? childResult = null)
            : base(steps, commands, settings, ctSteps, workflowTypeName, queries, childResult)
        {
        }

        static string IWorkflowTypeInfo.WorkflowTypeName => "ScriptableWorkflow";
    }

    /// <summary>
    /// A second scriptable workflow identity, distinct from <see cref="ScriptableWorkflow"/> only in
    /// its compile-time type and <see cref="IWorkflowTypeInfo.WorkflowTypeName"/>. Exists for tests
    /// that need two independently-addressable workflows in the same <c>WorkflowHandleRegistry</c>
    /// (e.g. a parent talking to a child over real <c>ShardingProducerController</c>/
    /// <c>ConsumerController</c> stand-ins) — <c>Register&lt;TWorkflow, TState&gt;</c> keys off
    /// <c>TWorkflow</c>, so the second party needs its own type, not just its own persistence id.
    /// </summary>
    protected sealed class AltScriptableWorkflow : ScriptableWorkflowBase<AltScriptableWorkflow>, IWorkflowTypeInfo
    {
        public AltScriptableWorkflow(
            IReadOnlyDictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> steps,
            IReadOnlyDictionary<Type, Func<TestState, object, CommandEffect<TestState>>> commands,
            WorkflowSettings? settings = null,
            IReadOnlyDictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>? ctSteps = null,
            string? workflowTypeName = null,
            IReadOnlyDictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>? queries = null,
            Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? childResult = null)
            : base(steps, commands, settings, ctSteps, workflowTypeName, queries, childResult)
        {
        }

        static string IWorkflowTypeInfo.WorkflowTypeName => "AltScriptableWorkflow";
    }

    /// <summary>
    /// A third scriptable workflow identity, distinct from both <see cref="ScriptableWorkflow"/> and
    /// <see cref="AltScriptableWorkflow"/> — for tests needing three independently-addressable
    /// workflows in the same <c>WorkflowHandleRegistry</c> at once (e.g. a grandparent/parent/child
    /// tree, where the middle actor needs its own type distinct from both its neighbors).
    /// </summary>
    protected sealed class Alt2ScriptableWorkflow : ScriptableWorkflowBase<Alt2ScriptableWorkflow>, IWorkflowTypeInfo
    {
        public Alt2ScriptableWorkflow(
            IReadOnlyDictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> steps,
            IReadOnlyDictionary<Type, Func<TestState, object, CommandEffect<TestState>>> commands,
            WorkflowSettings? settings = null,
            IReadOnlyDictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>? ctSteps = null,
            string? workflowTypeName = null,
            IReadOnlyDictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>? queries = null,
            Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? childResult = null)
            : base(steps, commands, settings, ctSteps, workflowTypeName, queries, childResult)
        {
        }

        static string IWorkflowTypeInfo.WorkflowTypeName => "Alt2ScriptableWorkflow";
    }

    protected IActorRef CreateActor(
        string persistenceId,
        Dictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>> steps,
        Dictionary<Type, Func<TestState, object, CommandEffect<TestState>>> commands,
        WorkflowSettings? settings = null,
        Dictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>? ctSteps = null,
        TimeProvider? timeProvider = null,
        IActorRef? consumerController = null,
        string? workflowTypeName = null,
        TimeSpan? gracefulShutdownGrace = null,
        int snapshotEveryNEvents = 10,
        Dictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>? queries = null,
        Func<ChildResultContext<TestState>, ChildResultEffect<TestState>>? childResult = null)
    {
        return Sys.ActorOf(Props.Create(() =>
            new WorkflowEntityActor<ScriptableWorkflow, TestState>(
                persistenceId, () => new ScriptableWorkflow(steps, commands, settings, ctSteps, workflowTypeName, queries, childResult),
                consumerController ?? ActorRefs.Nobody, timeoutScheduler: null, gracefulShutdownGrace, timeProvider,
                snapshotEveryNEvents)));
    }

    protected IActorRef CreateActor(
        string persistenceId,
        WorkflowScript script,
        WorkflowSettings? settings = null,
        TimeProvider? timeProvider = null,
        IActorRef? consumerController = null,
        string? workflowTypeName = null,
        TimeSpan? gracefulShutdownGrace = null,
        int snapshotEveryNEvents = 10) =>
        CreateActor(
            persistenceId,
            new Dictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>>(script.Steps),
            new Dictionary<Type, Func<TestState, object, CommandEffect<TestState>>>(script.Commands),
            settings,
            ctSteps: new Dictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>(script.CancellableSteps),
            timeProvider: timeProvider,
            consumerController: consumerController,
            workflowTypeName: workflowTypeName,
            gracefulShutdownGrace: gracefulShutdownGrace,
            snapshotEveryNEvents: snapshotEveryNEvents,
            queries: new Dictionary<Type, Func<TestState, object, CancellationToken, Task<QueryEffect>>>(script.Queries),
            childResult: script.ChildResult);

    /// <summary>
    /// Creates an actor under the <see cref="AltScriptableWorkflow"/> identity instead of
    /// <see cref="ScriptableWorkflow"/> — for the one side of a two-workflow-type test (see
    /// <see cref="AltScriptableWorkflow"/>'s own doc comment) that needs to be registered and
    /// resolved as a genuinely different <c>TWorkflow</c>.
    /// </summary>
    protected IActorRef CreateAltActor(
        string persistenceId,
        WorkflowScript script,
        WorkflowSettings? settings = null,
        TimeProvider? timeProvider = null,
        IActorRef? consumerController = null,
        string? workflowTypeName = null,
        TimeSpan? gracefulShutdownGrace = null)
    {
        var steps = new Dictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>>(script.Steps);
        var commands = new Dictionary<Type, Func<TestState, object, CommandEffect<TestState>>>(script.Commands);
        var ctSteps = new Dictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>(script.CancellableSteps);

        return Sys.ActorOf(Props.Create(() =>
            new WorkflowEntityActor<AltScriptableWorkflow, TestState>(
                persistenceId, () => new AltScriptableWorkflow(steps, commands, settings, ctSteps, workflowTypeName),
                consumerController ?? ActorRefs.Nobody, timeoutScheduler: null, gracefulShutdownGrace, timeProvider)));
    }

    /// <summary>
    /// Creates an actor under the <see cref="Alt2ScriptableWorkflow"/> identity — the third identity,
    /// for the one side of a three-workflow-type test (see <see cref="Alt2ScriptableWorkflow"/>'s own
    /// doc comment) that needs to be registered and resolved as a third distinct <c>TWorkflow</c>.
    /// </summary>
    protected IActorRef CreateAlt2Actor(
        string persistenceId,
        WorkflowScript script,
        WorkflowSettings? settings = null,
        TimeProvider? timeProvider = null,
        IActorRef? consumerController = null,
        string? workflowTypeName = null,
        TimeSpan? gracefulShutdownGrace = null)
    {
        var steps = new Dictionary<string, Func<TestState, object?, Task<StepEffect<TestState>>>>(script.Steps);
        var commands = new Dictionary<Type, Func<TestState, object, CommandEffect<TestState>>>(script.Commands);
        var ctSteps = new Dictionary<string, Func<TestState, object?, CancellationToken, Task<StepEffect<TestState>>>>(script.CancellableSteps);

        return Sys.ActorOf(Props.Create(() =>
            new WorkflowEntityActor<Alt2ScriptableWorkflow, TestState>(
                persistenceId, () => new Alt2ScriptableWorkflow(steps, commands, settings, ctSteps, workflowTypeName),
                consumerController ?? ActorRefs.Nobody, timeoutScheduler: null, gracefulShutdownGrace, timeProvider)));
    }

    /// <summary>
    /// Registers the standard scriptable child workflow with inert delivery endpoints and returns
    /// the producer endpoint so a test can assert the messages sent to a child. This keeps the
    /// registry plumbing out of lifecycle scenarios that do not care about it.
    /// </summary>
    protected TestProbe RegisterScriptableChild()
    {
        var producer = CreateTestProbe();
        WorkflowHandleRegistryProvider.Instance.Apply(Sys)
            .Register<ScriptableWorkflow, TestState>(ActorRefs.Nobody, producer.Ref);
        return producer;
    }

    /// <summary>
    /// Registers the <see cref="Alt2ScriptableWorkflow"/> identity with inert delivery endpoints,
    /// mirroring <see cref="RegisterScriptableChild"/> for the third workflow identity.
    /// </summary>
    protected TestProbe RegisterAlt2ScriptableChild()
    {
        var producer = CreateTestProbe();
        WorkflowHandleRegistryProvider.Instance.Apply(Sys)
            .Register<Alt2ScriptableWorkflow, TestState>(ActorRefs.Nobody, producer.Ref);
        return producer;
    }

    /// <summary>
    /// Reads the durable relationship created for a child. The relationship is the runtime's
    /// source of truth for its id and generation, so tests do not reproduce either convention.
    /// </summary>
    protected ChildWorkflowRelationship GetChild(IActorRef actor, string childWorkflowId)
    {
        actor.Tell(new GetDiagnostics<TestState>(), TestActor);
        var diagnostics = ExpectMsg<Diagnostics<TestState>>();
        return Assert.Single(diagnostics.Envelope.Children!, child => child.ChildWorkflowId == childWorkflowId);
    }

    /// <summary>
    /// Delivers a child lifecycle event using the persisted relationship rather than hand-built
    /// actor-protocol fields. The runtime owns relationship ids and generation values.
    /// </summary>
    protected void NotifyChild(
        IActorRef actor,
        ChildWorkflowRelationship relationship,
        ChildStatus status,
        object? result = null,
        WorkflowFailure? failure = null,
        string? resultTraceParent = null) =>
        actor.Tell(
            new ChildLifecycleNotification(
                relationship.RelationshipId,
                relationship.ChildWorkflowId,
                relationship.Generation,
                status,
                result,
                failure,
                resultTraceParent),
            TestActor);

    /// <summary>
    /// Stands in for a real <c>ShardingProducerController</c>/<c>ConsumerController</c> pair: forwards
    /// every <see cref="WorkflowProducerAdapter.Enqueue"/> straight to <paramref name="target"/> (given
    /// at construction) as a <see cref="ConsumerController.Delivery{T}"/> — the exact shape
    /// <c>WorkflowEntityActor.HandleDelivery</c> expects — enough to exercise the real send/receive/
    /// confirm code path between two (or more, chained) actual entity actors created directly in this
    /// test process. Shared infra: any test wiring more than one real
    /// <see cref="WorkflowEntityActor{TWorkflow, TState}"/> together uses this, not just one file's
    /// worth of scenarios.
    /// </summary>
    protected sealed class RelayProducerAdapter : ReceiveActor
    {
        private long _seqNr;

        public RelayProducerAdapter(IActorRef target)
        {
            Receive<WorkflowProducerAdapter.Enqueue>(msg =>
                target.Tell(new ConsumerController.Delivery<WorkflowEnvelope>(msg.Envelope, Self, "relay-producer", ++_seqNr)));
            Receive<ConsumerController.Confirmed>(_ => { });
        }
    }
}
