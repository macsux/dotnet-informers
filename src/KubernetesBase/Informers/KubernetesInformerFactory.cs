using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using RetryPolicy = Steeltoe.Informers.InformersBase.FaultTolerance.RetryPolicy;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Hands out shared Kubernetes informers by resource and payload shape. A C# type alone doesn't identify an informer: the same resource
    ///     can be watched as full objects, metadata only, or untyped.
    /// </summary>
    public interface IKubernetesInformerFactory
    {
        /// <summary>The informer of <typeparamref name="T" /> as full typed objects.</summary>
        IKubernetesInformer<T> Get<T>() where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>The metadata-only informer of the resource of <typeparamref name="T" /> (e.g. Secrets without their data).</summary>
        IKubernetesInformer<PartialObjectMetadata> MetadataOnly<T>() where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>The metadata-only informer of a resource.</summary>
        IKubernetesInformer<PartialObjectMetadata> MetadataOnly(GroupVersionResource resource);

        /// <summary>The untyped informer of a resource, for resources without a C# type (e.g. CRDs picked at runtime).</summary>
        IKubernetesInformer<UntypedKubernetesObject> Dynamic(GroupVersionResource resource);
    }

    /// <inheritdoc />
    public sealed class KubernetesInformerFactory : IKubernetesInformerFactory
    {
        private readonly IKubernetesResourceClient _client;
        private readonly ILoggerFactory _loggerFactory;
        private readonly RetryPolicy _retryPolicy;
        private readonly Dictionary<(GroupVersionResource, PayloadShape), KubernetesInformerRegistration> _registrations;
        private readonly ConcurrentDictionary<(GroupVersionResource, PayloadShape), object> _informers = new ConcurrentDictionary<(GroupVersionResource, PayloadShape), object>();

        /// <param name="kubernetes">The client whose connection settings and credentials the informers use</param>
        /// <param name="registrations">Setup (transforms, indexes) per resource and shape</param>
        /// <param name="loggerFactory">Optional logging</param>
        public KubernetesInformerFactory(k8s.Kubernetes kubernetes, IEnumerable<KubernetesInformerRegistration> registrations = null, ILoggerFactory loggerFactory = null)
            : this(new KubernetesGenericClient(kubernetes), registrations, loggerFactory)
        {
        }

        internal KubernetesInformerFactory(IKubernetesResourceClient client, IEnumerable<KubernetesInformerRegistration> registrations = null, ILoggerFactory loggerFactory = null, RetryPolicy retryPolicy = null)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _loggerFactory = loggerFactory;
            _retryPolicy = retryPolicy;
            _registrations = (registrations ?? Enumerable.Empty<KubernetesInformerRegistration>())
                .GroupBy(x => (x.Resource, x.Shape))
                .ToDictionary(x => x.Key, x => x.Last());
        }

        public IKubernetesInformer<T> Get<T>() where T : IKubernetesObject<V1ObjectMeta>
        {
            var resource = GroupVersionResource.For<T>();
            return (IKubernetesInformer<T>)_informers.GetOrAdd((resource, PayloadShape.Full), key =>
                new KubernetesInformer<T>(_client, resource, PayloadShape.Full, KubernetesInformer<T>.TypedDeserializer, Registration<T>(key), _retryPolicy, Logger(key)));
        }

        public IKubernetesInformer<PartialObjectMetadata> MetadataOnly<T>() where T : IKubernetesObject<V1ObjectMeta> => MetadataOnly(GroupVersionResource.For<T>());

        public IKubernetesInformer<PartialObjectMetadata> MetadataOnly(GroupVersionResource resource) =>
            (IKubernetesInformer<PartialObjectMetadata>)_informers.GetOrAdd((resource ?? throw new ArgumentNullException(nameof(resource)), PayloadShape.MetadataOnly), key =>
                new KubernetesInformer<PartialObjectMetadata>(_client, resource, PayloadShape.MetadataOnly, KubernetesInformer<PartialObjectMetadata>.TypedDeserializer, Registration<PartialObjectMetadata>(key), _retryPolicy, Logger(key)));

        public IKubernetesInformer<UntypedKubernetesObject> Dynamic(GroupVersionResource resource) =>
            (IKubernetesInformer<UntypedKubernetesObject>)_informers.GetOrAdd((resource ?? throw new ArgumentNullException(nameof(resource)), PayloadShape.Untyped), key =>
                new KubernetesInformer<UntypedKubernetesObject>(_client, resource, PayloadShape.Untyped, UntypedKubernetesObject.FromJson, Registration<UntypedKubernetesObject>(key), _retryPolicy, Logger(key)));

        private KubernetesInformerRegistration<T> Registration<T>((GroupVersionResource, PayloadShape) key) where T : IKubernetesObject<V1ObjectMeta> =>
            _registrations.TryGetValue(key, out var registration) ? registration as KubernetesInformerRegistration<T> : null;

        private ILogger Logger((GroupVersionResource Resource, PayloadShape Shape) key) =>
            _loggerFactory?.CreateLogger($"Steeltoe.Informers.KubernetesInformer[{key.Resource}, {key.Shape}]");
    }
}
