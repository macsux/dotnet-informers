using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     One consumer's view of a <see cref="SharedCache{TKey,TValue}" />. It never buffers events: it holds a set of dirty keys (marked by the
    ///     producer under the cache lock) and, per key, the revision and value it last sent. Each pull reads the key's <em>current</em> entry and
    ///     works out Add/Modify/Delete, or nothing. Memory is bounded by the number of distinct keys, and the producer never waits for it.
    /// </summary>
    /// <remarks>
    ///     States: reset pending (until the first pull after the cache is synced), resetting (serving the snapshot taken at that pull), watching.
    ///     A relist never sends a cursor back to reset pending: it marks the changed keys dirty and the cursor emits computed changes for them.
    /// </remarks>
    internal sealed class Cursor<TKey, TValue> : IAsyncEnumerator<ResourceEvent<TKey, TValue>>
    {
        private readonly SharedCache<TKey, TValue> _cache;
        private readonly Action<Cursor<TKey, TValue>> _onDispose;

        // Guarded by _cache.Lock (touched by the producer).
        private readonly HashQueue<TKey> _dirty;
        private readonly HashSet<TKey> _computed;
        private bool _resetPending = true;
        private TaskCompletionSource<bool> _waiter;
        private bool _disposed;

        // Consumer-private: only touched inside MoveNextAsync, which is never called concurrently.
        private readonly Dictionary<TKey, Sent> _sent;
        private List<KeyValuePair<TKey, CacheEntry<TValue>>> _resetBuffer;
        private int _resetIndex;
        private ResourceEvent<TKey, TValue> _current;

        private readonly struct Sent
        {
            public Sent(long revision, TValue value)
            {
                Revision = revision;
                Value = value;
            }

            public long Revision { get; }
            public TValue Value { get; }
        }

        internal Cursor(SharedCache<TKey, TValue> cache, Action<Cursor<TKey, TValue>> onDispose)
        {
            _cache = cache;
            _onDispose = onDispose;
            _dirty = new HashQueue<TKey>(cache.KeyComparer);
            _computed = new HashSet<TKey>(cache.KeyComparer);
            _sent = new Dictionary<TKey, Sent>(cache.KeyComparer);
        }

        public ResourceEvent<TKey, TValue> Current => _current;

        internal SharedCache<TKey, TValue> Cache => _cache;

        /// <summary>Test hook: an object attached by the consumer.</summary>
        internal object Tag { get; set; }

        /// <summary>Keys waiting to be looked at (bounded by the number of distinct keys).</summary>
        internal int DirtyCount
        {
            get
            {
                lock (_cache.Lock)
                    return _dirty.Count;
            }
        }

        /// <summary>Keys this cursor has sent and still tracks (bounded by the number of distinct keys).</summary>
        internal int TrackedCount => _sent.Count;

        /// <summary>True while the consumer is parked with nothing left to deliver. Call under the cache lock.</summary>
        internal bool IsIdleUnsafe => _waiter != null && _dirty.Count == 0 && (!_resetPending || !_cache.IsSynced);

        internal void MarkDirtyUnsafe(TKey key, bool computed)
        {
            if (_resetPending)
                return; // the reset's snapshot (taken later, under the lock) will include this change
            if (computed)
                _computed.Add(key);
            else
                _computed.Remove(key);
            if (_dirty.Enqueue(key))
                WakeUnsafe();
        }

        internal void PreloadResetUnsafe(List<KeyValuePair<TKey, CacheEntry<TValue>>> snapshot, bool synced)
        {
            if (!synced)
                return;
            _resetPending = false;
            _resetBuffer = snapshot;
            _resetIndex = 0;
        }

        internal void WakeUnsafe()
        {
            var waiter = _waiter;
            _waiter = null;
            waiter?.TrySetResult(true);
        }

        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(CancellationToken.None);

        public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_resetBuffer != null)
                {
                    if (_resetBuffer.Count == 0)
                    {
                        _resetBuffer = null;
                        _current = ResourceEvent<TKey, TValue>.EmptyReset;
                        return true;
                    }

                    EmitResetItem();
                    return true;
                }

                switch (TakeNextStep(out var key, out var found, out var entry, out var computed, out var waiter))
                {
                    case Step.End:
                        return false;
                    case Step.StartReset:
                        continue;
                    case Step.Delta:
                        if (TryComputeDelta(key, found, entry, computed))
                            return true;
                        continue;
                    case Step.Wait:
                        if (cancellationToken.CanBeCanceled)
                        {
                            using (cancellationToken.Register(static s => ((TaskCompletionSource<bool>)s).TrySetCanceled(), waiter))
                                await waiter.Task.ConfigureAwait(false);
                        }
                        else
                        {
                            await waiter.Task.ConfigureAwait(false);
                        }

                        continue;
                }
            }
        }

        private enum Step
        {
            End,
            StartReset,
            Delta,
            Wait,
        }

        /// <summary>The only part of a pull that takes the cache lock: decides what to do next and reads what it needs.</summary>
        private Step TakeNextStep(out TKey key, out bool found, out CacheEntry<TValue> entry, out bool computed, out TaskCompletionSource<bool> waiter)
        {
            key = default;
            found = false;
            entry = default;
            computed = false;
            waiter = null;
            lock (_cache.Lock)
            {
                if (_disposed)
                    return Step.End;
                if (_cache.ErrorUnsafe != null)
                    throw new InformerException("The informer's producer failed.", _cache.ErrorUnsafe);

                if (_resetPending && _cache.IsSynced)
                {
                    // Taking the snapshot and clearing the dirty set happen in one critical section: every change after this point marks its
                    // key dirty again, every change before it is in the snapshot.
                    _resetBuffer = _cache.SnapshotEntriesUnsafe();
                    _resetIndex = 0;
                    _resetPending = false;
                    _dirty.Clear();
                    _computed.Clear();
                    _sent.Clear();
                    return Step.StartReset;
                }

                if (!_resetPending && _dirty.Count > 0)
                {
                    key = _dirty.Dequeue();
                    computed = _computed.Remove(key);
                    found = _cache.ItemsUnsafe.TryGetValue(key, out entry);
                    return Step.Delta;
                }

                if (_cache.IsCompletedUnsafe)
                    return Step.End;

                waiter = _waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                return Step.Wait;
            }
        }

        private void EmitResetItem()
        {
            var item = _resetBuffer[_resetIndex];
            var flags = EventTypeFlags.Reset;
            if (_resetIndex == 0)
                flags |= EventTypeFlags.ResetStart;
            if (_resetIndex == _resetBuffer.Count - 1)
                flags |= EventTypeFlags.ResetEnd;
            _resetIndex++;
            if (_resetIndex == _resetBuffer.Count)
                _resetBuffer = null;

            _sent[item.Key] = new Sent(item.Value.Revision, item.Value.Value);
            _current = new ResourceEvent<TKey, TValue>(flags, item.Key, item.Value.Value);
        }

        private bool TryComputeDelta(TKey key, bool found, CacheEntry<TValue> entry, bool computed)
        {
            var computedFlag = computed ? EventTypeFlags.Computed : 0;
            if (!found)
            {
                if (!_sent.Remove(key, out var last))
                    return false; // added and deleted before we ever sent it
                _current = new ResourceEvent<TKey, TValue>(EventTypeFlags.Delete | computedFlag, key, last.Value);
                return true;
            }

            EventTypeFlags flags;
            if (_sent.TryGetValue(key, out var sent))
            {
                if (sent.Revision == entry.Revision)
                    return false;
                var comparer = _cache.ChangeComparer;
                if (comparer != null && comparer.Equals(sent.Value, entry.Value))
                {
                    _sent[key] = new Sent(entry.Revision, entry.Value);
                    return false;
                }

                flags = EventTypeFlags.Modify;
            }
            else
            {
                flags = EventTypeFlags.Add;
            }

            _sent[key] = new Sent(entry.Revision, entry.Value);
            _current = new ResourceEvent<TKey, TValue>(flags | computedFlag, key, entry.Value);
            return true;
        }

        public ValueTask DisposeAsync()
        {
            lock (_cache.Lock)
            {
                if (_disposed)
                    return default;
                _disposed = true;
                _dirty.Clear();
                WakeUnsafe();
            }

            _cache.Unregister(this);
            _onDispose?.Invoke(this);
            return default;
        }
    }
}
