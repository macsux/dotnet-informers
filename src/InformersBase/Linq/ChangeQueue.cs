using System.Collections.Generic;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    /// <summary>
    ///     Pending changes merged per key, for operators that pull their source eagerly (<c>Resync</c>, <c>RunAsync</c>). It holds, per key,
    ///     the latest pending state and what the consumer currently has, and works out the outgoing flags when a key is taken: Add then
    ///     Modify is an Add, Add then Delete is nothing, Delete then Add is a Modify. Memory is bounded by the number of keys. Not thread-safe:
    ///     callers hold their own lock.
    /// </summary>
    internal sealed class ChangeQueue<TKey, TValue>
    {
        private readonly Dictionary<TKey, Pending> _pending = new Dictionary<TKey, Pending>();
        private readonly HashQueue<TKey> _ready = new HashQueue<TKey>();
        private readonly HashSet<TKey> _blocked = new HashSet<TKey>();
        private readonly Dictionary<TKey, TValue> _known = new Dictionary<TKey, TValue>();

        private struct Pending
        {
            public bool Exists;
            public TValue Value;
            public bool Computed;
            public bool FromReset;
            public bool SyncOnly;
            public EventTypeFlags RetryFlags; // non-zero: redeliver these flags (a retry with nothing newer pending)
        }

        /// <summary>Keys ready to be taken.</summary>
        public int ReadyCount => _ready.Count;

        /// <summary>Keys with a pending change, including blocked ones.</summary>
        public int PendingCount => _pending.Count;

        /// <summary>The keys the consumer currently has.</summary>
        public IReadOnlyDictionary<TKey, TValue> Known => _known;

        public bool HasPending(TKey key) => _pending.ContainsKey(key);

        /// <summary>Records a change from the source.</summary>
        public void Enqueue(ResourceEvent<TKey, TValue> e, bool fromReset = false)
        {
            var exists = !e.EventFlags.HasFlag(EventTypeFlags.Delete);
            var hadPending = _pending.TryGetValue(e.Key, out var pending);
            // a Sync from upstream stays a Sync unless a real change is pending (then that change covers it)
            var syncOnly = e.EventFlags.HasFlag(EventTypeFlags.Sync) && exists && (!hadPending || pending.SyncOnly);
            if (e.EventFlags.HasFlag(EventTypeFlags.Sync) && hadPending && !pending.SyncOnly)
            {
                MakeReady(e.Key);
                return;
            }

            pending.Exists = exists;
            if (exists || e.Value != null)
                pending.Value = e.Value;
            pending.Computed = e.EventFlags.HasFlag(EventTypeFlags.Computed);
            pending.FromReset = fromReset;
            pending.SyncOnly = syncOnly;
            pending.RetryFlags = 0;
            _pending[e.Key] = pending;
            MakeReady(e.Key);
        }

        /// <summary>A complete state from the source: keys the consumer has that aren't in it become computed deletes.</summary>
        public void EnqueueReset(IReadOnlyCollection<ResourceEvent<TKey, TValue>> items)
        {
            var present = new HashSet<TKey>();
            foreach (var item in items)
                present.Add(item.Key);
            foreach (var known in _known.ToList())
            {
                if (!present.Contains(known.Key))
                    Enqueue(new ResourceEvent<TKey, TValue>(EventTypeFlags.Delete | EventTypeFlags.Computed, known.Key, known.Value));
            }

            foreach (var key in _pending.Keys.ToList())
            {
                if (!present.Contains(key) && !_known.ContainsKey(key))
                    _pending.Remove(key);
            }

            foreach (var item in items)
                Enqueue(item, fromReset: true);
        }

        /// <summary>Asks for a key the consumer has to be delivered again (flagged Sync), unless a change for it is already pending.</summary>
        public void RequestSync(TKey key)
        {
            if (_pending.ContainsKey(key) || !_known.TryGetValue(key, out var value))
                return;
            _pending[key] = new Pending { Exists = true, Value = value, SyncOnly = true };
            MakeReady(key);
        }

        /// <summary>
        ///     Redelivers <paramref name="failed" /> with the key's latest state, unless a newer change is already pending (which then runs
        ///     instead).
        /// </summary>
        /// <returns>False when there is nothing to retry (the key is gone and its delete was handled, or it came back since)</returns>
        public bool Retry(ResourceEvent<TKey, TValue> failed)
        {
            var key = failed.Key;
            if (_pending.ContainsKey(key))
            {
                MakeReady(key);
                return true;
            }

            var exists = _known.TryGetValue(key, out var value);
            var failedDelete = failed.EventFlags.HasFlag(EventTypeFlags.Delete);
            if (exists == failedDelete)
                return false; // gone and already reported, or re-added since the failed delete (that add ran already)
            _pending[key] = new Pending
            {
                Exists = exists,
                Value = exists ? value : failed.Value,
                RetryFlags = failed.EventFlags & ~(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd | EventTypeFlags.ResetEmpty),
            };
            MakeReady(key);
            return true;
        }

        /// <summary>Holds a key back (e.g. while a handler runs for it): changes are merged but it isn't ready until unblocked.</summary>
        public void Block(TKey key) => _blocked.Add(key);

        public void Unblock(TKey key)
        {
            if (_blocked.Remove(key) && _pending.ContainsKey(key))
                _ready.Enqueue(key);
        }

        /// <summary>Takes the next ready key and its outgoing event; keys whose changes cancel out are skipped.</summary>
        public bool TryDequeue(out ResourceEvent<TKey, TValue> result)
        {
            while (_ready.Count > 0)
            {
                var key = _ready.Dequeue();
                if (_blocked.Contains(key) || !_pending.Remove(key, out var pending))
                    continue;
                var computed = pending.Computed ? EventTypeFlags.Computed : 0;
                var known = _known.TryGetValue(key, out var knownValue);
                if (pending.RetryFlags != 0)
                {
                    result = new ResourceEvent<TKey, TValue>(pending.RetryFlags, key, pending.Value);
                    return true;
                }

                if (pending.Exists)
                {
                    _known[key] = pending.Value;
                    EventTypeFlags flags;
                    if (pending.FromReset)
                        flags = EventTypeFlags.Reset;
                    else if (!known)
                        flags = EventTypeFlags.Add;
                    else if (pending.SyncOnly)
                        flags = EventTypeFlags.Modify | EventTypeFlags.Sync;
                    else
                        flags = EventTypeFlags.Modify;
                    result = new ResourceEvent<TKey, TValue>(flags | computed, key, pending.Value);
                    return true;
                }

                if (known)
                {
                    _known.Remove(key);
                    result = new ResourceEvent<TKey, TValue>(EventTypeFlags.Delete | computed, key, pending.Value ?? knownValue);
                    return true;
                }
            }

            result = default;
            return false;
        }

        /// <summary>Replaces what the consumer has (it was just sent a reset with these items) and drops everything pending.</summary>
        public void ResetKnown(IEnumerable<ResourceEvent<TKey, TValue>> items)
        {
            _pending.Clear();
            _ready.Clear();
            _known.Clear();
            foreach (var item in items)
                _known[item.Key] = item.Value;
        }

        private void MakeReady(TKey key)
        {
            if (!_blocked.Contains(key))
                _ready.Enqueue(key);
        }
    }
}
