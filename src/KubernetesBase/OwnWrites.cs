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
    ///     changes (<see cref="OwnWritesExtensions.IgnoreWritesFrom{TResource}" />). Entries go when the key is deleted or they expire, so the
    ///     table stays bounded by recent writes. Thread-safe.
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

        /// <summary>
        ///     Whether this version of the object is the echo of one of our writes. Entries aren't consumed by a match: several pipelines may watch
        ///     the same object (e.g. two relationships over one type), and each must recognize the echo; they expire instead.
        /// </summary>
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
                if (list.Count == 0)
                {
                    _entries.Remove(key);
                    return false;
                }

                return list.Exists(x => x.ResourceVersion == resourceVersion);
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

        /// <summary>
        ///     A client that records, in <paramref name="writes" />, the versions its writes produced: replaces and patches guarded by the
        ///     <c>resourceVersion</c> they were based on, when the server returned a new one. Unguarded writes (server-side apply, a patch without
        ///     the precondition) aren't recorded: when they change nothing the server returns the current version, which may be another writer's.
        /// </summary>
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

            /// <summary>Records <paramref name="written" /> when the write was guarded by <paramref name="baseVersion" /> and produced a new version.</summary>
            private T Recorded<T>(T written, string baseVersion) where T : IKubernetesObject<V1ObjectMeta>
            {
                if (baseVersion != null && written?.Metadata?.ResourceVersion != null && written.Metadata.ResourceVersion != baseVersion)
                    _writes.Record(written);
                return written;
            }

            /// <summary>The <c>metadata.resourceVersion</c> precondition a merge patch carries, if any.</summary>
            private static string Precondition(V1Patch patch)
            {
                if (patch.Type != V1Patch.PatchType.MergePatch || patch.Content is not string json)
                    return null;
                try
                {
                    using var document = System.Text.Json.JsonDocument.Parse(json);
                    return document.RootElement.TryGetProperty("metadata", out var metadata) && metadata.ValueKind == System.Text.Json.JsonValueKind.Object &&
                           metadata.TryGetProperty("resourceVersion", out var version) && version.ValueKind == System.Text.Json.JsonValueKind.String
                        ? version.GetString()
                        : null;
                }
                catch (System.Text.Json.JsonException)
                {
                    return null;
                }
            }

            public Task<T> GetAsync<T>(string name, string @namespace = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.GetAsync<T>(name, @namespace, cancellationToken);

            public Task<IReadOnlyList<T>> ListAsync<T>(string @namespace = null, string labelSelector = null, string fieldSelector = null, CancellationToken cancellationToken = default)
                where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.ListAsync<T>(@namespace, labelSelector, fieldSelector, cancellationToken);

            // a create's echo is an add, which always triggers
            public Task<T> CreateAsync<T>(T obj, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.CreateAsync(obj, options, cancellationToken);

            public async Task<T> ReplaceAsync<T>(T obj, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
                where T : IKubernetesObject<V1ObjectMeta> =>
                Recorded(await _inner.ReplaceAsync(obj, status, options, cancellationToken).ConfigureAwait(false), obj?.Metadata?.ResourceVersion);

            public async Task<T> PatchAsync<T>(T obj, V1Patch patch, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
                where T : IKubernetesObject<V1ObjectMeta> =>
                Recorded(await _inner.PatchAsync(obj, patch, status, options, cancellationToken).ConfigureAwait(false), Precondition(patch));

            public Task<T> ApplyAsync<T>(T desired, string fieldManager, bool force = false, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.ApplyAsync(desired, fieldManager, force, cancellationToken);

            public Task<bool> DeleteAsync<T>(T obj, V1DeleteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
                _inner.DeleteAsync(obj, options, cancellationToken);

            public Task<KubernetesResourceInfo> GetResourceInfoAsync(IKubernetesObject<V1ObjectMeta> obj, CancellationToken cancellationToken = default) =>
                _inner.GetResourceInfoAsync(obj, cancellationToken);

            public Task<KubernetesResourceInfo> GetResourceInfoAsync(GroupVersionResource resource, CancellationToken cancellationToken = default) =>
                _inner.GetResourceInfoAsync(resource, cancellationToken);
        }
    }
}
