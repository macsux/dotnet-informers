using System;
using System.Collections.Concurrent;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Identifies a Kubernetes resource by API group, version and plural resource name, e.g. <c>apps/v1/deployments</c> or
    ///     <c>cert-manager.io/v1/certificates</c>. The core group is the empty string (<c>v1/pods</c>).
    /// </summary>
    public sealed record GroupVersionResource(string Group, string Version, string Resource)
    {
        private static readonly ConcurrentDictionary<Type, GroupVersionResource> ByType = new ConcurrentDictionary<Type, GroupVersionResource>();

        /// <summary>The group and version as written in <c>apiVersion</c>, e.g. <c>apps/v1</c> or <c>v1</c>.</summary>
        public string ApiVersion => string.IsNullOrEmpty(Group) ? Version : $"{Group}/{Version}";

        /// <summary>The resource of a model type, from its <see cref="KubernetesEntityAttribute" />.</summary>
        public static GroupVersionResource For<T>() => For(typeof(T));

        /// <summary>The resource of a model type, from its <see cref="KubernetesEntityAttribute" />.</summary>
        public static GroupVersionResource For(Type type) => ByType.GetOrAdd(type, t =>
        {
            var attribute = t.GetKubernetesTypeMetadata();
            return new GroupVersionResource(attribute.Group ?? string.Empty, attribute.ApiVersion, attribute.PluralName);
        });

        /// <summary>Parses <c>group/version/resource</c> (or <c>version/resource</c> for the core group).</summary>
        public static GroupVersionResource Parse(string value)
        {
            var parts = (value ?? throw new ArgumentNullException(nameof(value))).Split('/');
            return parts.Length switch
            {
                2 => new GroupVersionResource(string.Empty, parts[0], parts[1]),
                3 => new GroupVersionResource(parts[0], parts[1], parts[2]),
                _ => throw new FormatException($"'{value}' is not group/version/resource"),
            };
        }

        public override string ToString() => $"{ApiVersion}/{Resource}";
    }

    /// <summary>What an informer receives for each object.</summary>
    public enum PayloadShape
    {
        /// <summary>The complete typed object.</summary>
        Full,

        /// <summary><c>apiVersion</c>, <c>kind</c> and <c>metadata</c> only (<see cref="PartialObjectMetadata" />), requested from the API server as such.</summary>
        MetadataOnly,

        /// <summary>Typed metadata plus the raw JSON object (<see cref="UntypedKubernetesObject" />), for resources without a C# type.</summary>
        Untyped,
    }
}
