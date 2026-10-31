using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     The <c>resourceVersion</c>s a controller's own writes returned, per object key, so their echoes can be told apart from other writers'
    ///     changes (<see cref="OwnWritesExtensions.IgnoreWritesFrom{TResource}" />). Entries go when their echo is seen, the key is deleted, or
    ///     they expire, so the table stays bounded by writes in flight. Thread-safe.
    /// </summary>
    public sealed class OwnWrites
    {
        private readonly object _lock = new object();
        private readonly Dictionary<string, List<Entry>> _entries = new Dictionary<string, List<Entry>>(StringComparer.Ordinal);
        private readonly TimeSpan _timeout;
        private readonly Func<DateTime> _clock;
        private DateTime _nextPurge;

        private readonly record struct Entry(string ResourceVersion, DateTime ExpiresAt);

        /// <param name="timeout">How long an entry waits for its echo (default one minute)</param>
        public OwnWrites(TimeSpan? timeout = null) : this(timeout ?? TimeSpan.FromMinutes(1), () => DateTime.UtcNow)
        {
        }

        internal OwnWrites(TimeSpan timeout, Func<DateTime> clock)
        {
            _timeout = timeout;
            _clock = clock;
            _nextPurge = clock() + timeout;
        }

        /// <summary>Entries waiting for their echo.</summary>
        public int Count
        {
            get
            {
                lock (_lock)
                    return _entries.Values.Sum(x => x.Count);
            }
        }

        /// <summary>Records a write by the object the API server returned for it.</summary>
        public void Record(IKubernetesObject<V1ObjectMeta> written)
        {
            if (written?.Metadata?.ResourceVersion != null)
                Record(written.ObjectKey(), written.Metadata.ResourceVersion);
        }

        /// <summary>Records a write of the object with key <paramref name="key" /> that produced <paramref name="resourceVersion" />.</summary>
        public void Record(string key, string resourceVersion)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (resourceVersion == null)
                throw new ArgumentNullException(nameof(resourceVersion));
            var now = _clock();
            lock (_lock)
            {
                if (now >= _nextPurge)
                    PurgeUnsafe(now);
                if (!_entries.TryGetValue(key, out var list))
                    _entries[key] = list = new List<Entry>(1);
                list.Add(new Entry(resourceVersion, now + _timeout));
            }
        }

        /// <summary>Whether this version of the object is the echo of one of our writes; a match is consumed.</summary>
        public bool IsOwnWrite(string key, string resourceVersion)
        {
            if (key == null || resourceVersion == null)
                return false;
            var now = _clock();
            lock (_lock)
            {
                if (!_entries.TryGetValue(key, out var list))
                    return false;
                list.RemoveAll(x => x.ExpiresAt <= now);
                var index = list.FindIndex(x => x.ResourceVersion == resourceVersion);
                if (index >= 0)
                    list.RemoveAt(index);
                if (list.Count == 0)
                    _entries.Remove(key);
                return index >= 0;
            }
        }

        /// <summary>Forgets every entry of a key (e.g. it was deleted).</summary>
        public void Forget(string key)
        {
            lock (_lock)
                _entries.Remove(key);
        }

        private void PurgeUnsafe(DateTime now)
        {
            foreach (var key in _entries.Keys.ToList())
            {
                var list = _entries[key];
                list.RemoveAll(x => x.ExpiresAt <= now);
                if (list.Count == 0)
                    _entries.Remove(key);
            }

            _nextPurge = now + _timeout;
        }
    }

    public static class OwnWritesExtensions
    {
        /// <summary>
        ///     Marks a modification <see cref="EventTypeFlags.Quiet" /> when it's the echo of a write recorded in <paramref name="writes" />: it
        ///     still updates the key's latest value, but doesn't start a reconcile. Any other version triggers as usual; if someone else wrote after
        ///     us, the merged change carries their version, so it isn't mistaken for ours. Combined with <c>TriggerOn</c>, a change is quiet if
        ///     either says so.
        /// </summary>
        public static IInformable<string, TResource> IgnoreWritesFrom<TResource>(this IInformable<string, TResource> source, OwnWrites writes)
            where TResource : IKubernetesObject<V1ObjectMeta>
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (writes == null)
                throw new ArgumentNullException(nameof(writes));
            return source.Select(e =>
            {
                var flags = e.EventFlags;
                if (flags.HasFlag(EventTypeFlags.Delete))
                {
                    writes.Forget(e.Key);
                    return e;
                }

                if (flags.HasFlag(EventTypeFlags.Modify) && !flags.HasFlag(EventTypeFlags.Sync) && !flags.IsQuiet() &&
                    writes.IsOwnWrite(e.Key, e.Value?.Metadata?.ResourceVersion))
                    return new ResourceEvent<string, TResource>(flags | EventTypeFlags.Quiet, e.Key, e.Value);
                return e;
            }).AsInformable();
        }

        /// <summary>A client that records the result of every write it makes (create, replace, patch, apply) in <paramref name="writes" />.</summary>
        public static IKubernetesGenericClient RecordingWritesTo(this IKubernetesGenericClient client, OwnWrites writes) =>
            new RecordingClient(client ?? throw new ArgumentNullException(nameof(client)), writes ?? throw new ArgumentNullException(nameof(writes)));

        private sealed class RecordingClient : IKubernetesGenericClient
        {
            private readonly IKubernetesGenericClient _inner;
            private readonly OwnWrites _writes;

            public RecordingClient(IKubernetesGenericClient inner, OwnWrites writes)
            {
                _inner = inner;
                _writes = writes;
            }

            private T Recorded<T>(T written) where T : IKubernetesObject<V1ObjectMeta>
            {
                _writes.Record(written);
                return written;
            }

            public Task<T> GetAsync<T>(string name, string @namespace = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.GetAsync<T>(name, @namespace, cancellationToken);

            public Task<IReadOnlyList<T>> ListAsync<T>(string @namespace = null, string labelSelector = null, string fieldSelector = null, CancellationToken cancellationToken = default)
                where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.ListAsync<T>(@namespace, labelSelector, fieldSelector, cancellationToken);

            public async Task<T> CreateAsync<T>(T obj, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                Recorded(await _inner.CreateAsync(obj, options, cancellationToken).ConfigureAwait(false));

            public async Task<T> ReplaceAsync<T>(T obj, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
                where T : IKubernetesObject<V1ObjectMeta> =>
                Recorded(await _inner.ReplaceAsync(obj, status, options, cancellationToken).ConfigureAwait(false));

            public async Task<T> PatchAsync<T>(T obj, V1Patch patch, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
                where T : IKubernetesObject<V1ObjectMeta> =>
                Recorded(await _inner.PatchAsync(obj, patch, status, options, cancellationToken).ConfigureAwait(false));

            public async Task<T> ApplyAsync<T>(T desired, string fieldManager, bool force = false, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                Recorded(await _inner.ApplyAsync(desired, fieldManager, force, cancellationToken).ConfigureAwait(false));

            public Task<bool> DeleteAsync<T>(T obj, V1DeleteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.DeleteAsync(obj, options, cancellationToken);

            public Task<KubernetesResourceInfo> GetResourceInfoAsync(IKubernetesObject<V1ObjectMeta> obj, CancellationToken cancellationToken = default) =>
                _inner.GetResourceInfoAsync(obj, cancellationToken);

            public Task<KubernetesResourceInfo> GetResourceInfoAsync(GroupVersionResource resource, CancellationToken cancellationToken = default) =>
                _inner.GetResourceInfoAsync(resource, cancellationToken);
        }
    }
}
