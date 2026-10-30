using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase.Diagnostics;

namespace Steeltoe.Informers.InformersBase
{
    internal readonly struct CacheEntry<TValue>
    {
        public CacheEntry(TValue value, long revision)
        {
            Value = value;
            Revision = revision;
        }

        public TValue Value { get; }

        /// <summary>Taken from one counter per cache, so a key's revision never repeats, even across a delete and re-add.</summary>
        public long Revision { get; }
    }

    /// <summary>
    ///     The latest object per key, plus the cursors reading from it. One lock (<see cref="Lock" />) guards the items, the indexes, the cursor
    ///     list and every cursor's dirty set, so "apply + mark key dirty" and "register cursor + take its snapshot" are atomic with respect to
    ///     each other. Exactly one producer writes to a cache.
    /// </summary>
    internal sealed class SharedCache<TKey, TValue> : IInformerSink<TValue>
    {
        internal readonly object Lock = new object();
        private readonly Func<TValue, TKey> _keySelector;
        private readonly IEqualityComparer<TKey> _keyComparer;
        private readonly Func<TValue, TValue> _transform;
        private readonly Func<TValue, string> _fingerprint;
        private readonly FaultMode _fault;
        private readonly KeyValuePair<string, object> _nameTag;
        private Dictionary<TKey, CacheEntry<TValue>> _items;
        private readonly Dictionary<TKey, string> _fingerprints;
        private readonly Dictionary<TKey, long> _perKeyRevisions; // FaultMode.PerKeyRevision only
        private readonly List<Cursor<TKey, TValue>> _cursors = new List<Cursor<TKey, TValue>>();
        private readonly List<ICacheIndex<TKey, TValue>> _indexes = new List<ICacheIndex<TKey, TValue>>();
        private readonly Dictionary<string, CacheIndex<TKey, TValue, string>> _namedIndexes = new Dictionary<string, CacheIndex<TKey, TValue, string>>();
        private readonly TaskCompletionSource<bool> _syncedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _revision;
        private volatile bool _synced;
        private bool _completed;
        private Exception _error;

        public SharedCache(SharedInformerOptions<TKey, TValue> options, FaultMode fault = FaultMode.None)
        {
            _keySelector = options.KeySelector ?? throw new ArgumentException("KeySelector is required", nameof(options));
            _keyComparer = options.KeyComparer ?? EqualityComparer<TKey>.Default;
            ChangeComparer = options.ChangeComparer;
            _transform = options.Transform;
            _fingerprint = options.MutationFingerprint;
            _fault = fault;
            _nameTag = InformerDiagnostics.Tag("informer", options.Name ?? typeof(TValue).Name);
            _items = new Dictionary<TKey, CacheEntry<TValue>>(_keyComparer);
            if (_fingerprint != null)
                _fingerprints = new Dictionary<TKey, string>(_keyComparer);
            if (fault == FaultMode.PerKeyRevision)
                _perKeyRevisions = new Dictionary<TKey, long>(_keyComparer);
            foreach (var index in options.Indexes)
            {
                var cacheIndex = new CacheIndex<TKey, TValue, string>(this, index.Value, null);
                _namedIndexes.Add(index.Key, cacheIndex);
                _indexes.Add(cacheIndex);
            }
        }

        internal IEqualityComparer<TValue> ChangeComparer { get; }
        internal IEqualityComparer<TKey> KeyComparer => _keyComparer;
        internal bool IsSynced => _synced;
        internal Task SyncedTask => _syncedTcs.Task;
        internal bool IsCompletedUnsafe => _completed;
        internal Exception ErrorUnsafe => _error;
        internal Dictionary<TKey, CacheEntry<TValue>> ItemsUnsafe => _items;
        internal IReadOnlyList<Cursor<TKey, TValue>> CursorsUnsafe => _cursors;

        public int CursorCount
        {
            get
            {
                lock (Lock)
                    return _cursors.Count;
            }
        }

        /// <summary>A copy of the content (tests and List()).</summary>
        public Dictionary<TKey, TValue> Snapshot()
        {
            lock (Lock)
            {
                var copy = new Dictionary<TKey, TValue>(_items.Count, _keyComparer);
                foreach (var pair in _items)
                    copy.Add(pair.Key, pair.Value.Value);
                return copy;
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (Lock)
            {
                if (_items.TryGetValue(key, out var entry))
                {
                    value = entry.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }

        internal CacheIndex<TKey, TValue, string> GetNamedIndex(string name)
        {
            lock (Lock)
                return _namedIndexes.TryGetValue(name, out var index) ? index : null;
        }

        /// <summary>
        ///     Adds an index over the items already in the cache and every later change. Thread-safe. Dispose the returned index to remove it.
        /// </summary>
        public CacheIndex<TKey, TValue, TIndexKey> AddIndex<TIndexKey>(Func<TValue, IEnumerable<TIndexKey>> selector, IEqualityComparer<TIndexKey> comparer = null)
        {
            var index = new CacheIndex<TKey, TValue, TIndexKey>(this, selector, comparer);
            lock (Lock)
            {
                foreach (var pair in _items)
                    index.UpdateUnsafe(pair.Key, pair.Value.Value, true);
                _indexes.Add(index);
            }

            return index;
        }

        internal void RemoveIndex(ICacheIndex<TKey, TValue> index)
        {
            lock (Lock)
                _indexes.Remove(index);
        }

        /// <summary>
        ///     Registers a new cursor. Under the lock the cursor joins the list the producer marks, with a reset pending; the reset's snapshot is
        ///     taken (under the same lock) on its first pull.
        /// </summary>
        public Cursor<TKey, TValue> Register(Action<Cursor<TKey, TValue>> onDispose = null)
        {
            var cursor = new Cursor<TKey, TValue>(this, onDispose);
            if (_fault == FaultMode.NonAtomicRegistration)
            {
                List<KeyValuePair<TKey, CacheEntry<TValue>>> snapshot;
                bool synced;
                lock (Lock)
                {
                    snapshot = SnapshotEntriesUnsafe();
                    synced = _synced;
                }

                Thread.Yield();
                lock (Lock)
                {
                    cursor.PreloadResetUnsafe(snapshot, synced);
                    _cursors.Add(cursor);
                }

                return cursor;
            }

            lock (Lock)
                _cursors.Add(cursor);
            return cursor;
        }

        internal void Unregister(Cursor<TKey, TValue> cursor)
        {
            lock (Lock)
                _cursors.Remove(cursor);
        }

        internal List<KeyValuePair<TKey, CacheEntry<TValue>>> SnapshotEntriesUnsafe()
        {
            var list = new List<KeyValuePair<TKey, CacheEntry<TValue>>>(_items.Count);
            foreach (var pair in _items)
                list.Add(pair);
            return list;
        }

        public void Replace(IEnumerable<TValue> items)
        {
            // Only the producer writes, so reading _items outside the lock is safe here; the new dictionary is swapped in atomically.
            var fresh = new Dictionary<TKey, CacheEntry<TValue>>(_keyComparer);
            var wasSynced = _synced;
            if (wasSynced)
                VerifyAllUnchanged();
            var changed = new List<TKey>();
            foreach (var raw in items)
            {
                var item = _transform != null ? _transform(raw) : raw;
                var key = _keySelector(item);
                if (wasSynced && _items.TryGetValue(key, out var existing) && ChangeComparer != null && ChangeComparer.Equals(existing.Value, item))
                {
                    fresh[key] = existing;
                    continue;
                }

                fresh[key] = new CacheEntry<TValue>(item, NextRevision(key));
                if (wasSynced)
                    changed.Add(key);
            }

            lock (Lock)
            {
                var old = _items;
                _items = fresh;
                if (!wasSynced)
                {
                    foreach (var index in _indexes)
                        index.ClearUnsafe();
                    foreach (var pair in fresh)
                    {
                        Fingerprint(pair.Key, pair.Value.Value);
                        foreach (var index in _indexes)
                            index.UpdateUnsafe(pair.Key, pair.Value.Value, true);
                    }

                    _synced = true;
                    _syncedTcs.TrySetResult(true);
                    // Cursors that joined before the first list are still waiting with a reset pending; wake them up to take it.
                    foreach (var cursor in _cursors)
                        cursor.WakeUnsafe();
                    return;
                }

                InformerDiagnostics.Relists.Add(1, _nameTag);
                foreach (var key in changed)
                {
                    Fingerprint(key, fresh[key].Value);
                    foreach (var index in _indexes)
                        index.UpdateUnsafe(key, fresh[key].Value, true);
                    MarkUnsafe(key, true);
                }

                foreach (var pair in old)
                {
                    if (fresh.ContainsKey(pair.Key))
                        continue;
                    _fingerprints?.Remove(pair.Key);
                    _perKeyRevisions?.Remove(pair.Key);
                    foreach (var index in _indexes)
                        index.UpdateUnsafe(pair.Key, default, false);
                    MarkUnsafe(pair.Key, true);
                }
            }
        }

        public void Upsert(TValue item)
        {
            if (_transform != null)
                item = _transform(item);
            var key = _keySelector(item);
            if (_items.TryGetValue(key, out var existing))
            {
                VerifyUnchanged(key, existing.Value);
                if (ChangeComparer != null && ChangeComparer.Equals(existing.Value, item))
                    return;
            }

            InformerDiagnostics.EventsApplied.Add(1, _nameTag);
            Apply(key, item, false);
        }

        public void Delete(TValue item)
        {
            if (_transform != null)
                item = _transform(item);
            var key = _keySelector(item);
            if (!_items.TryGetValue(key, out var existing))
                return;
            VerifyUnchanged(key, existing.Value);
            InformerDiagnostics.EventsApplied.Add(1, _nameTag);
            Apply(key, item, true);
        }

        private void Apply(TKey key, TValue item, bool isDelete)
        {
            switch (_fault)
            {
                case FaultMode.MarkAfterApplySeparateLock:
                    lock (Lock)
                        ApplyUnsafe(key, item, isDelete);
                    Thread.Yield();
                    lock (Lock)
                        MarkUnsafe(key, false);
                    return;
                case FaultMode.MarkBeforeApply:
                    lock (Lock)
                        MarkUnsafe(key, false);
                    Thread.Yield();
                    lock (Lock)
                        ApplyUnsafe(key, item, isDelete);
                    return;
                default:
                    lock (Lock)
                    {
                        ApplyUnsafe(key, item, isDelete);
                        MarkUnsafe(key, false);
                    }

                    return;
            }
        }

        private void ApplyUnsafe(TKey key, TValue item, bool isDelete)
        {
            if (isDelete)
            {
                _items.Remove(key);
                _fingerprints?.Remove(key);
                _perKeyRevisions?.Remove(key);
                foreach (var index in _indexes)
                    index.UpdateUnsafe(key, default, false);
            }
            else
            {
                _items[key] = new CacheEntry<TValue>(item, NextRevision(key));
                Fingerprint(key, item);
                foreach (var index in _indexes)
                    index.UpdateUnsafe(key, item, true);
            }
        }

        private void MarkUnsafe(TKey key, bool computed)
        {
            foreach (var cursor in _cursors)
                cursor.MarkDirtyUnsafe(key, computed);
        }

        private long NextRevision(TKey key)
        {
            if (_perKeyRevisions == null)
                return Interlocked.Increment(ref _revision);
            _perKeyRevisions.TryGetValue(key, out var revision);
            _perKeyRevisions[key] = ++revision;
            return revision;
        }

        private void Fingerprint(TKey key, TValue value)
        {
            if (_fingerprints != null)
                _fingerprints[key] = _fingerprint(value);
        }

        private void VerifyUnchanged(TKey key, TValue value)
        {
            if (_fingerprints == null)
                return;
            string expected;
            lock (Lock)
            {
                if (!_fingerprints.TryGetValue(key, out expected))
                    return;
            }

            if (expected != _fingerprint(value))
                throw new InvalidOperationException($"Cached object '{key}' was modified after the informer handed it out. Objects shared by an informer must not be mutated; clone them first.");
        }

        private void VerifyAllUnchanged()
        {
            if (_fingerprints == null)
                return;
            foreach (var pair in _items)
                VerifyUnchanged(pair.Key, pair.Value.Value);
        }

        /// <summary>The producer ended normally: cursors drain what they have, then end.</summary>
        public void Complete()
        {
            lock (Lock)
            {
                _completed = true;
                foreach (var cursor in _cursors)
                    cursor.WakeUnsafe();
            }
        }

        /// <summary>The producer failed: every cursor's next pull throws.</summary>
        public void SetError(Exception error)
        {
            lock (Lock)
            {
                _error = error;
                _syncedTcs.TrySetException(error);
                foreach (var cursor in _cursors)
                    cursor.WakeUnsafe();
            }
        }
    }
}
