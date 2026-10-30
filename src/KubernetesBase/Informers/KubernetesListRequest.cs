namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>Query parameters and representation of a list or watch request.</summary>
    internal sealed class KubernetesListRequest
    {
        public string LabelSelector { get; set; }
        public string FieldSelector { get; set; }
        public int? Limit { get; set; }
        public string Continue { get; set; }
        public string ResourceVersion { get; set; }
        public bool Watch { get; set; }
        public bool? AllowWatchBookmarks { get; set; }
        public int? TimeoutSeconds { get; set; }

        /// <summary>The <c>Accept</c> header, e.g. the metadata-only representation.</summary>
        public string Accept { get; set; }

        public const string MetadataListAccept = "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json";
        public const string MetadataAccept = "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json";
    }
}
