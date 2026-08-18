using MessagePack;
using MessagePack.Formatters;
using MessagePack.ImmutableCollection;
using MessagePack.Resolvers;

namespace Sagant.Runtime.Akka.Serialization;

/// <summary>
/// The byte-level codec <see cref="SagantSerializer"/> and <see cref="WorkflowRuntimeStateSerializer"/>
/// both delegate to — MessagePack. Each serializer keeps its own manifest scheme on top of this:
/// <see cref="SagantSerializer"/> writes its own short strings, <see cref="WorkflowRuntimeStateSerializer"/>
/// rides <c>IncludeManifest</c>'s CLR-name manifest.
/// </summary>
internal static class SagantCodec
{
    /// <summary>
    /// <see cref="ImmutableCollectionResolver"/> covers <c>ImmutableDictionary</c>/<c>ImmutableList</c>
    /// and the rest of <c>System.Collections.Immutable</c> directly, so <c>WorkflowRuntimeState.Children</c>
    /// serializes as the keyed map it is. <see cref="TypelessContractlessStandardResolver"/>
    /// handles a plain POCO or record with no attributes at all, which is every type this codebase
    /// persists — nothing here asks a workflow author to decorate their own <c>TState</c> or commands.
    /// <see cref="PolymorphicResolver"/> wraps both so a field declared as <c>object</c>/an abstract
    /// base carries its own concrete type on the wire.
    /// </summary>
    private static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
        .WithResolver(new PolymorphicResolver(CompositeResolver.Create(
            ImmutableCollectionResolver.Instance,
            TypelessContractlessStandardResolver.Instance)));

    public static byte[] ToBinary(object obj) =>
        MessagePackSerializer.Serialize(obj.GetType(), obj, Options);

    public static object FromBinary(byte[] bytes, Type type) =>
        MessagePackSerializer.Deserialize(type, bytes, Options)!;

    /// <summary>
    /// Wraps a declared type that can differ from an instance's runtime type — an <c>object</c>-typed
    /// field, an abstract base class — in <see cref="ForceTypelessFormatter{T}"/>, MessagePack's own
    /// formatter for exactly this: it embeds enough type information to reconstruct the concrete
    /// runtime type on the way back in, the same way a sealed, single-shape type never needs to.
    /// </summary>
    private sealed class PolymorphicResolver : IFormatterResolver
    {
        private readonly IFormatterResolver _inner;
        private readonly System.Collections.Concurrent.ConcurrentDictionary<Type, object> _cache = new();

        public PolymorphicResolver(IFormatterResolver inner) => _inner = inner;

        public IMessagePackFormatter<T>? GetFormatter<T>() =>
            (IMessagePackFormatter<T>)_cache.GetOrAdd(typeof(T), _ =>
                typeof(T).IsClass && (typeof(T).IsAbstract || !typeof(T).IsSealed)
#pragma warning disable CS8619 // A MessagePack-CSharp annotation mismatch between ForceTypelessFormatter<T> and IMessagePackFormatter<T>, harmless here.
                    ? (IMessagePackFormatter<T>)new ForceTypelessFormatter<T>()
#pragma warning restore CS8619
                    : _inner.GetFormatterWithVerify<T>());
    }
}
