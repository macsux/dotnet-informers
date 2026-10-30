using System;
using System.Collections.Generic;
using k8s;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     How the informer of one resource and payload shape is set up. The registration is part of the informer's identity: informers of the
    ///     same resource with different registrations never share a producer.
    /// </summary>
    public abstract class KubernetesInformerRegistration
    {
        internal KubernetesInformerRegistration(GroupVersionResource resource, PayloadShape shape)
        {
            Resource = resource;
            Shape = shape;
        }

        public GroupVersionResource Resource { get; }
        public PayloadShape Shape { get; }
        internal bool MutationDetection { get; set; }
    }

    /// <summary>Setup of the informer of <typeparamref name="T" />: a transform applied before caching, indexes, and the mutation detector.</summary>
    public sealed class KubernetesInformerRegistration<T> : KubernetesInformerRegistration where T : IKubernetesObject<V1ObjectMeta>
    {
        private readonly List<Func<T, T>> _transforms = new List<Func<T, T>>();

        public KubernetesInformerRegistration() : base(GroupVersionResource.For<T>(), PayloadShape.Full)
        {
        }

        internal KubernetesInformerRegistration(GroupVersionResource resource, PayloadShape shape) : base(resource, shape)
        {
        }

        internal Dictionary<string, Func<T, IEnumerable<string>>> Indexes { get; } = new Dictionary<string, Func<T, IEnumerable<string>>>();

        internal Func<T, T> TransformFunction => _transforms.Count == 0 ? null : obj =>
        {
            foreach (var transform in _transforms)
                obj = transform(obj);
            return obj;
        };

        /// <summary>
        ///     Runs in the producer on every freshly deserialized object before it's cached and shared (the one place mutating is allowed),
        ///     e.g. to drop <c>metadata.managedFields</c>. Every consumer of this informer sees the transformed objects.
        /// </summary>
        public KubernetesInformerRegistration<T> Transform(Func<T, T> transform)
        {
            _transforms.Add(transform ?? throw new ArgumentNullException(nameof(transform)));
            return this;
        }

        /// <summary>Drops <c>metadata.managedFields</c> and kubectl's <c>last-applied-configuration</c> annotation before caching.</summary>
        public KubernetesInformerRegistration<T> StripManagedFields() => Transform(obj =>
        {
            if (obj.Metadata != null)
            {
                obj.Metadata.ManagedFields = null;
                obj.Metadata.Annotations?.Remove("kubectl.kubernetes.io/last-applied-configuration");
            }

            return obj;
        });

        /// <summary>Maintains a named index on the shared cache.</summary>
        public KubernetesInformerRegistration<T> AddIndex(string name, Func<T, IEnumerable<string>> selector)
        {
            Indexes[name ?? throw new ArgumentNullException(nameof(name))] = selector ?? throw new ArgumentNullException(nameof(selector));
            return this;
        }

        /// <summary>
        ///     Fails the informer when a cached object is modified after it was handed out (objects are shared by reference and must not be
        ///     modified). Costs a serialization per object; meant for tests and debug builds.
        /// </summary>
        public KubernetesInformerRegistration<T> DetectMutations(bool enabled = true)
        {
            MutationDetection = enabled;
            return this;
        }
    }
}
