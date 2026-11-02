using System;

namespace Steeltoe.Informers.Entities
{
    /// <summary>Leaves the property (or type) out of the schema; on a class, leaves it out of CRD generation.</summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property)]
    public class IgnoreAttribute : Attribute;

    /// <summary>The property holds a whole Kubernetes object (<c>x-kubernetes-embedded-resource</c>).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class EmbeddedResourceAttribute : Attribute;

    /// <summary>Keeps fields the schema doesn't declare (<c>x-kubernetes-preserve-unknown-fields</c>).</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class)]
    public class PreserveUnknownFieldsAttribute : Attribute;

    /// <summary>Short names for <c>kubectl</c> (e.g. <c>as</c> for <c>appsets</c>).</summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class KubernetesEntityShortNamesAttribute(params string[] shortNames) : Attribute
    {
        public string[] ShortNames => shortNames;
    }

    /// <summary>The scale subresource, for <c>kubectl scale</c> and autoscalers.</summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class ScaleSubresourceAttribute(string specReplicasPath, string statusReplicasPath, string labelSelectorPath = null) : Attribute
    {
        public string SpecReplicasPath => specReplicasPath;
        public string StatusReplicasPath => statusReplicasPath;
        public string LabelSelectorPath => labelSelectorPath;
    }

    /// <summary>Of several versions of a kind, the one stored in etcd (exactly one).</summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class StorageVersionAttribute : Attribute;

    /// <summary>How server-side apply merges a list.</summary>
    public enum XListType
    {
        Atomic,
        Set,
        Map,
    }

    /// <summary>How server-side apply merges a list (<c>x-kubernetes-list-type</c>).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class XListTypeAttribute(XListType listType) : Attribute
    {
        public XListType ListType => listType;
    }

    /// <summary>
    ///     The keys of a <c>map</c> list (<c>x-kubernetes-list-map-keys</c>), together with <c>[XListType(XListType.Map)]</c>.
    /// </summary>
    /// <remarks>
    ///     Named differently from KubeOps' <c>XListMapKeysAttribute</c>: its Transpiler checks map keys against its own <c>XListType</c> enum,
    ///     which it can't recognize in ours, and would reject them. The generator applies this attribute itself.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property)]
    public class ListMapKeysAttribute(params string[] keys) : Attribute
    {
        public string[] Keys => keys;
    }

    /// <summary>How server-side apply merges a map.</summary>
    public enum XMapType
    {
        Granular,
        Atomic,
    }

    /// <summary>How server-side apply merges a map (<c>x-kubernetes-map-type</c>).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class XMapTypeAttribute(XMapType mapType) : Attribute
    {
        public XMapType MapType => mapType;
    }
}
