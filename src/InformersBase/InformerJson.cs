using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     The JSON type metadata the library uses to serialize, clone, diff and compare objects: a chain of <see cref="IJsonTypeInfoResolver" />s,
    ///     never reflection-based serialization unless the app allows it. Apps add a source-generated <c>JsonSerializerContext</c> listing the
    ///     types they watch (<see cref="AddResolver" />); libraries register theirs as defaults, which apps' resolvers take precedence over.
    /// </summary>
    /// <remarks>
    ///     When reflection-based serialization is enabled (the default, unless the app is trimmed or published as NativeAOT), types no resolver
    ///     knows fall back to reflection. In a trimmed or AOT app every serialized type must come from a resolver.
    /// </remarks>
    public static class InformerJson
    {
        private static readonly object Lock = new object();
        private static readonly List<IJsonTypeInfoResolver> AppResolvers = new List<IJsonTypeInfoResolver>();
        private static readonly List<IJsonTypeInfoResolver> DefaultResolvers = new List<IJsonTypeInfoResolver>();
        private static Func<JsonSerializerOptions> _baseOptions = () => new JsonSerializerOptions(JsonSerializerDefaults.Web);
        private static volatile JsonSerializerOptions _options;

        /// <summary>The options every serialization of the library goes through. Rebuilt (a new instance) when resolvers are added.</summary>
        public static JsonSerializerOptions Options
        {
            get
            {
                var options = _options;
                if (options != null)
                    return options;
                lock (Lock)
                    return _options ??= Build(JsonSerializer.IsReflectionEnabledByDefault);
            }
        }

        /// <summary>
        ///     Adds an app's resolver (typically a source-generated <c>JsonSerializerContext</c>). It takes precedence over resolvers added before
        ///     it and over the library defaults.
        /// </summary>
        public static void AddResolver(IJsonTypeInfoResolver resolver)
        {
            if (resolver == null)
                throw new ArgumentNullException(nameof(resolver));
            lock (Lock)
            {
                if (AppResolvers.Contains(resolver))
                    return;
                AppResolvers.Insert(0, resolver);
                _options = null;
            }
        }

        /// <summary>
        ///     Registers a library's resolver, used after every app resolver, and optionally the options the chain starts from (naming policy,
        ///     converters). For libraries building on this one.
        /// </summary>
        public static void AddDefaultResolver(IJsonTypeInfoResolver resolver, Func<JsonSerializerOptions> baseOptions = null)
        {
            if (resolver == null)
                throw new ArgumentNullException(nameof(resolver));
            lock (Lock)
            {
                if (!DefaultResolvers.Contains(resolver))
                    DefaultResolvers.Add(resolver);
                if (baseOptions != null)
                    _baseOptions = baseOptions;
                _options = null;
            }
        }

        /// <summary>The metadata of <typeparamref name="T" />.</summary>
        /// <exception cref="NotSupportedException">No resolver knows the type (and reflection is disabled)</exception>
        public static JsonTypeInfo<T> GetTypeInfo<T>() => (JsonTypeInfo<T>)GetTypeInfo(typeof(T));

        /// <summary>The metadata of <paramref name="type" />.</summary>
        /// <exception cref="NotSupportedException">No resolver knows the type (and reflection is disabled)</exception>
        public static JsonTypeInfo GetTypeInfo(Type type)
        {
            if (TryGetTypeInfo(type, out var info))
                return info;
            throw new NotSupportedException(
                $"No JSON metadata for {type}. Add it to a source-generated JsonSerializerContext ([JsonSerializable(typeof({type.Name}))]) and " +
                $"register it with {nameof(InformerJson)}.{nameof(AddResolver)} (or the operator's Json option).");
        }

        /// <summary>The metadata of <paramref name="type" />, if a resolver (or reflection, when enabled) provides it.</summary>
        public static bool TryGetTypeInfo(Type type, out JsonTypeInfo info)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            return Options.TryGetTypeInfo(type, out info);
        }

        /// <summary>Serializes through <see cref="Options" />, by the object's runtime type.</summary>
        public static byte[] SerializeToUtf8Bytes(object value) =>
            value == null ? "null"u8.ToArray() : JsonSerializer.SerializeToUtf8Bytes(value, GetTypeInfo(value.GetType()));

        /// <summary>Serializes through <see cref="Options" />, by the object's runtime type.</summary>
        public static string Serialize(object value) =>
            value == null ? "null" : JsonSerializer.Serialize(value, GetTypeInfo(value.GetType()));

        /// <summary>Deserializes through <see cref="Options" />.</summary>
        public static T Deserialize<T>(string json) => JsonSerializer.Deserialize(json, GetTypeInfo<T>());

        /// <summary>Deserializes through <see cref="Options" />.</summary>
        public static T Deserialize<T>(JsonElement element) => element.Deserialize(GetTypeInfo<T>());

        /// <summary>Deserializes through <see cref="Options" />.</summary>
        public static T Deserialize<T>(ReadOnlySpan<byte> utf8Json) => JsonSerializer.Deserialize(utf8Json, GetTypeInfo<T>());

        /// <summary>A deep copy through a serialization round-trip of the object's runtime type.</summary>
        public static T Clone<T>(T value) where T : class
        {
            if (value == null)
                return null;
            var info = GetTypeInfo(value.GetType());
            return (T)JsonSerializer.Deserialize(JsonSerializer.SerializeToUtf8Bytes(value, info), info);
        }

        /// <summary>Builds the options from the registered resolvers; without the reflection fallback when <paramref name="allowReflection" /> is false.</summary>
        internal static JsonSerializerOptions Build(bool allowReflection)
        {
            JsonSerializerOptions options;
            List<IJsonTypeInfoResolver> chain;
            lock (Lock)
            {
                options = _baseOptions();
                chain = new List<IJsonTypeInfoResolver>(AppResolvers);
                chain.AddRange(DefaultResolvers);
            }

            if (allowReflection && JsonSerializer.IsReflectionEnabledByDefault)
                chain.Add(ReflectionResolver());
            options.TypeInfoResolver = JsonTypeInfoResolver.Combine(chain.ToArray());
            options.MakeReadOnly();
            return options;
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Only used when reflection-based serialization is enabled, i.e. not in trimmed or AOT apps")]
        [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Only used when reflection-based serialization is enabled, i.e. not in trimmed or AOT apps")]
        private static IJsonTypeInfoResolver ReflectionResolver() => new DefaultJsonTypeInfoResolver();
    }
}
