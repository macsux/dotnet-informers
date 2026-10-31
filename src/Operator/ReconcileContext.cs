using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    /// <summary>
    ///     One reconcile of one object: a snapshot of the latest state when it starts, built from the controller's caches (nothing in it is
    ///     fetched from the API server). Objects are copy-on-write handles: read <c>Value</c>, change <c>Edit()</c>, persist with
    ///     <see cref="SaveAsync" />; edits not saved when the reconcile returns are discarded (and logged).
    /// </summary>
    public class ReconcileContext<T> where T : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly List<RelatedSet> _related;
        private readonly Expectations _expectations;

        internal ReconcileContext(
            string key,
            ReconcileTrigger trigger,
            T obj,
            IEnumerable<RelatedSet> related,
            IKubernetesGenericClient client,
            Expectations expectations,
            IEventRecorder events,
            ILogger logger)
        {
            Key = key;
            Trigger = trigger;
            Object = new Tracked<T>(obj);
            _related = related as List<RelatedSet> ?? related.ToList();
            Client = client;
            _expectations = expectations;
            Events = (events ?? NullEventRecorder.Instance).For(obj);
            Logger = logger;
        }

        /// <summary>The object's key, <c>namespace/name</c> (<c>name</c> when cluster-scoped).</summary>
        public string Key { get; }

        /// <summary>Why this reconcile runs.</summary>
        public ReconcileTrigger Trigger { get; }

        /// <summary>The primary object.</summary>
        public Tracked<T> Object { get; }

        /// <summary>Whether expectations of owned creates and deletes for this object are met (or timed out).</summary>
        public bool ExpectationsSatisfied => _expectations.IsSatisfied(Key);

        /// <summary>Events about the primary object.</summary>
        public IObjectEventRecorder Events { get; }

        public ILogger Logger { get; }

        /// <summary>The client every write of this context goes through.</summary>
        public IKubernetesGenericClient Client { get; }

        /// <summary>Every current child of the object declared with <c>Owns&lt;TChild&gt;()</c>, including ones being deleted.</summary>
        public IReadOnlyList<Tracked<TChild>> Owned<TChild>() where TChild : class, IKubernetesObject<V1ObjectMeta>
        {
            var set = _related.OfType<RelatedSet<TChild>>().FirstOrDefault(x => x.Kind == RelationshipKind.Owns)
                      ?? throw new InvalidOperationException($"The controller doesn't declare Owns<{typeof(TChild).Name}>()");
            return set.Items;
        }

        /// <summary>
        ///     The existing objects of <c>References&lt;TOther&gt;</c> and <c>Watches&lt;TOther&gt;</c> relationships (only those named
        ///     <paramref name="name" />, if given). A referenced object that doesn't exist is simply absent.
        /// </summary>
        public IReadOnlyList<Tracked<TOther>> Related<TOther>(string name = null) where TOther : class, IKubernetesObject<V1ObjectMeta>
        {
            var sets = _related.OfType<RelatedSet<TOther>>().Where(x => x.Kind != RelationshipKind.Owns && (name == null || x.Name == name)).ToList();
            if (sets.Count == 0)
                throw new InvalidOperationException($"The controller doesn't declare References<{typeof(TOther).Name}>() or Watches<{typeof(TOther).Name}>(){(name == null ? "" : $" named {name}")}");
            return sets.Count == 1 ? sets[0].Items : sets.SelectMany(x => x.Items).Distinct().ToList();
        }

        /// <summary>
        ///     Creates <paramref name="child" /> owned by the object (controller owner reference) and records it in the expectations, so the next
        ///     reconcile waits until the informer has seen it. Returns the child as stored.
        /// </summary>
        public Task<TChild> CreateOwnedAsync<TChild>(TChild child, CancellationToken cancellationToken) where TChild : class, IKubernetesObject<V1ObjectMeta> =>
            Client.CreateOwnedAsync(Object.Original, child, _expectations, cancellationToken: cancellationToken);

        /// <summary>Deletes an owned child (guarded by its UID) and records it in the expectations. False when it was already gone.</summary>
        public Task<bool> DeleteOwnedAsync<TChild>(Tracked<TChild> child, CancellationToken cancellationToken) where TChild : class, IKubernetesObject<V1ObjectMeta> =>
            Client.DeleteOwnedAsync(Object.Original, (child ?? throw new ArgumentNullException(nameof(child))).Original, _expectations, cancellationToken: cancellationToken);

        /// <summary>
        ///     Saves every edited handle (the object and related objects): sends only the difference of each, status to <c>/status</c>, guarded by
        ///     its <c>resourceVersion</c>. A conflict (409) is retried by the controller with the latest state.
        /// </summary>
        public async Task SaveAsync(CancellationToken cancellationToken)
        {
            if (Object.IsModified)
                await Client.SaveAsync(Object, cancellationToken: cancellationToken).ConfigureAwait(false);
            foreach (var set in _related)
                await set.SaveAsync(Client, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Handles with edits that would change something but weren't saved.</summary>
        internal int UnsavedChanges() => (Object.HasChanges() ? 1 : 0) + _related.Sum(x => x.UnsavedChanges());
    }
}
