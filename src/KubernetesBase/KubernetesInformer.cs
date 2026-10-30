using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.InformersBase;
using RetryPolicy = Steeltoe.Informers.InformersBase.FaultTolerance.RetryPolicy;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     A Kubernetes informer: a <see cref="SharedInformer{TKey,TResource}" /> fed by a <see cref="KubernetesProducer{T}" /> per distinct
    ///     server filter (namespace, label and field selectors).
    /// </summary>
    /// <typeparam name="TResource">The type of Kubernetes resource</typeparam>
    internal sealed class KubernetesInformer<TResource> : IKubernetesInformer<TResource>, ISharedSource<string, TResource>
        where TResource : IKubernetesObject<V1ObjectMeta>
    {
        private readonly SharedInformer<string, TResource> _informer;
        private readonly KubernetesProducer<TResource> _producer;
        private readonly IKubernetesResourceClient _client;

        public KubernetesInformer(
            IKubernetesResourceClient client,
            GroupVersionResource resource,
            PayloadShape shape,
            Func<JsonElement, TResource> deserialize,
            KubernetesInformerRegistration<TResource> registration = null,
            RetryPolicy retryPolicy = null,
            ILogger logger = null)
        {
            KubernetesConflictDetection.EnsureRegistered();
            _client = client;
            Resource = resource;
            Shape = shape;
            _producer = new KubernetesProducer<TResource>(client, resource, shape, deserialize, retryPolicy, logger);
            var options = new SharedInformerOptions<string, TResource>
            {
                KeySelector = x => x.ObjectKey(),
                KeyComparer = StringComparer.Ordinal,
                ChangeComparer = ResourceVersionComparer<TResource>.Instance,
                FilterTranslator = new KubernetesFilterTranslator<TResource>(),
                Transform = registration?.TransformFunction,
                Name = resource.ToString(),
            };
            if (registration != null)
            {
                foreach (var index in registration.Indexes)
                    options.Indexes[index.Key] = index.Value;
                if (registration.MutationDetection)
                    options.MutationFingerprint = x => KubernetesJson.Serialize(x);
            }

            _informer = new SharedInformer<string, TResource>(_producer.RunAsync, options, ListOnce);
        }

        /// <summary>Creates a typed informer for <typeparamref name="TResource" /> (full payload).</summary>
        internal KubernetesInformer(IKubernetesResourceClient client, RetryPolicy retryPolicy = null, ILogger logger = null, KubernetesInformerRegistration<TResource> registration = null)
            : this(client, GroupVersionResource.For<TResource>(), PayloadShape.Full, TypedDeserializer, registration, retryPolicy, logger)
        {
        }

        internal static TResource TypedDeserializer(JsonElement element) => KubernetesJson.Deserialize<TResource>(element.GetRawText());

        public GroupVersionResource Resource { get; }
        public PayloadShape Shape { get; }

        internal KubernetesProducer<TResource> Producer => _producer;

        /// <summary>Number of list/watch connections currently open for this informer (one per distinct server filter in use).</summary>
        public int ActiveProducers => _informer.ActiveProducers;

        public bool IsSynced => _informer.IsSynced;

        public Task WaitForSyncAsync(CancellationToken cancellationToken = default) => _informer.WaitForSyncAsync(cancellationToken);

        public IInformer<string, TResource> Where(Expression<Func<TResource, bool>> predicate) => _informer.Where(predicate);

        public IInformable<string, TResource> ListWatch() => _informer;

        public IInformer<string, TResource> ListWatch(KubernetesInformerOptions options) => _informer.WithServerFilter(ToServerFilter(options));

        public IAsyncEnumerable<TResource> List(CancellationToken cancellationToken = default) => _informer.List(cancellationToken);

        public IAsyncEnumerable<TResource> List(KubernetesInformerOptions options, CancellationToken cancellationToken = default) =>
            ListOnce(ToServerFilter(options), cancellationToken);

        public IAsyncEnumerator<ResourceEvent<string, TResource>> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            _informer.GetAsyncEnumerator(cancellationToken);

        SharedSubscription<string, TResource> ISharedSource<string, TResource>.Subscribe(CancellationToken cancellationToken) =>
            ((ISharedSource<string, TResource>)_informer).Subscribe(cancellationToken);

        private async IAsyncEnumerable<TResource> ListOnce(ServerFilter filter, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var selection = await KubernetesSelection.ResolveAsync(_client, Resource, filter, cancellationToken).ConfigureAwait(false);
            var (items, _) = await _producer.ListAsync(selection, cancellationToken).ConfigureAwait(false);
            foreach (var item in items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }

        /// <summary>Explicit options are the same server filter a predicate would push down, so they share producers with predicates.</summary>
        internal static ServerFilter ToServerFilter(KubernetesInformerOptions options)
        {
            var terms = new List<ServerFilterTerm>();
            if (!string.IsNullOrEmpty(options?.Namespace))
                terms.Add(new ServerFilterTerm(KubernetesFilterTranslator.Namespace, options.Namespace));
            if (!string.IsNullOrEmpty(options?.LabelSelector))
                terms.AddRange(SplitSelector(options.LabelSelector).Select(x => new ServerFilterTerm(KubernetesFilterTranslator.Label, x)));
            return ServerFilter.Create(terms);
        }

        /// <summary>Splits a label selector into its requirements (commas inside <c>in (...)</c> sets don't split).</summary>
        internal static IEnumerable<string> SplitSelector(string selector)
        {
            var depth = 0;
            var start = 0;
            for (var i = 0; i < selector.Length; i++)
            {
                switch (selector[i])
                {
                    case '(':
                        depth++;
                        break;
                    case ')':
                        depth--;
                        break;
                    case ',' when depth == 0:
                        var part = selector.Substring(start, i - start).Trim();
                        if (part.Length > 0)
                            yield return part;
                        start = i + 1;
                        break;
                }
            }

            var last = selector.Substring(start).Trim();
            if (last.Length > 0)
                yield return last;
        }
    }

    /// <summary>The Kubernetes no-op check: two versions of an object with the same <c>resourceVersion</c> are the same content.</summary>
    internal sealed class ResourceVersionComparer<T> : IEqualityComparer<T> where T : IMetadata<V1ObjectMeta>
    {
        public static ResourceVersionComparer<T> Instance { get; } = new ResourceVersionComparer<T>();

        public bool Equals(T x, T y)
        {
            var a = x?.Metadata?.ResourceVersion;
            var b = y?.Metadata?.ResourceVersion;
            return a != null && b != null && a == b && x.Metadata.Uid == y.Metadata.Uid;
        }

        public int GetHashCode(T obj) => obj?.Metadata?.ResourceVersion?.GetHashCode() ?? 0;
    }
}
