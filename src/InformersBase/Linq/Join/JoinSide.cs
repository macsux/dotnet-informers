using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    /// <summary>
    ///     One input of a join: its event stream plus a way to look objects up by join key. Over a source informer it looks matches up through
    ///     an index on the producer's shared cache; over any other informable it keeps its own index (references only).
    /// </summary>
    internal abstract class JoinSide<TKey, TValue, TJoinKey> : IAsyncDisposable
    {
        private static readonly TJoinKey[] NoKeys = Array.Empty<TJoinKey>();
        private readonly Func<TValue, IEnumerable<TJoinKey>> _keySelector;
        private readonly ResetCollector<TKey, TValue> _resets = new ResetCollector<TKey, TValue>();
        private readonly Dictionary<TKey, TJoinKey[]> _lastKeys = new Dictionary<TKey, TJoinKey[]>(); // join keys as this join last saw them
        private Task<bool> _pending;

        protected JoinSide(Func<TValue, IEnumerable<TJoinKey>> keySelector, IAsyncEnumerator<ResourceEvent<TKey, TValue>> events)
        {
            _keySelector = keySelector;
            Events = events;
        }

        public IAsyncEnumerator<ResourceEvent<TKey, TValue>> Events { get; }

        /// <summary>The side's first reset has been applied.</summary>
        public bool Synced { get; private set; }

        public bool Completed { get; private set; }

        /// <summary>Keys this join has seen alive.</summary>
        public IEnumerable<TKey> KnownKeys => _lastKeys.Keys;

        public TJoinKey[] JoinKeysOf(TValue value) => CacheIndex<TKey, TValue, TJoinKey>.SelectKeys(_keySelector, value);

        public TJoinKey[] LastJoinKeys(TKey key) => _lastKeys.TryGetValue(key, out var keys) ? keys : NoKeys;

        /// <summary>The current objects under a join key.</summary>
        public abstract IEnumerable<KeyValuePair<TKey, TValue>> Lookup(TJoinKey joinKey);

        /// <summary>The current object for a key, if it exists (and passes any client-side filter).</summary>
        public abstract bool TryGet(TKey key, out TValue value);

        protected abstract void ApplyUpsert(TKey key, TValue value, TJoinKey[] oldKeys, TJoinKey[] newKeys);

        protected abstract void ApplyDelete(TKey key, TJoinKey[] oldKeys);

        /// <summary>Starts (or returns) the pending pull of the next event.</summary>
        public Task<bool> PendingMove => _pending ??= Completed ? Task.FromResult(false) : Events.MoveNextAsync().AsTask();

        /// <summary>
        ///     Consumes the completed pull. Returns the changes it caused: (key, old join keys, new join keys, computed), or nothing while a reset
        ///     is still being collected.
        /// </summary>
        public List<SideChange> TakeCompleted()
        {
            var task = _pending;
            _pending = null;
            var changes = new List<SideChange>();
            if (!task.GetAwaiter().GetResult())
            {
                Completed = true;
                return changes;
            }

            var e = Events.Current;
            if (_resets.TryCollect(e, out var reset))
            {
                if (reset == null)
                    return changes;
                var present = new HashSet<TKey>();
                foreach (var item in reset)
                {
                    present.Add(item.Key);
                    changes.Add(Upsert(item.Key, item.Value, Synced));
                }

                foreach (var key in new List<TKey>(_lastKeys.Keys))
                {
                    if (!present.Contains(key))
                        changes.Add(Delete(key, true));
                }

                Synced = true;
                return changes;
            }

            var computed = e.EventFlags.HasFlag(EventTypeFlags.Computed);
            changes.Add(e.EventFlags.HasFlag(EventTypeFlags.Delete) ? Delete(e.Key, computed) : Upsert(e.Key, e.Value, computed));
            return changes;
        }

        private SideChange Upsert(TKey key, TValue value, bool computed)
        {
            var oldKeys = LastJoinKeys(key);
            var newKeys = JoinKeysOf(value);
            _lastKeys[key] = newKeys;
            ApplyUpsert(key, value, oldKeys, newKeys);
            return new SideChange(key, oldKeys, newKeys, computed);
        }

        private SideChange Delete(TKey key, bool computed)
        {
            var oldKeys = LastJoinKeys(key);
            _lastKeys.Remove(key);
            ApplyDelete(key, oldKeys);
            return new SideChange(key, oldKeys, NoKeys, computed);
        }

        public virtual async ValueTask DisposeAsync()
        {
            if (_pending != null)
            {
                try
                {
                    await _pending.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // cancelled together with the join
                }
            }

            await Events.DisposeAsync().ConfigureAwait(false);
        }

        internal readonly struct SideChange
        {
            public SideChange(TKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys, bool computed)
            {
                Key = key;
                OldKeys = oldKeys;
                NewKeys = newKeys;
                Computed = computed;
            }

            public TKey Key { get; }
            public TJoinKey[] OldKeys { get; }
            public TJoinKey[] NewKeys { get; }
            public bool Computed { get; }
        }

        public static JoinSide<TKey, TValue, TJoinKey> Open(IInformable<TKey, TValue> source, Func<TValue, IEnumerable<TJoinKey>> keySelector, CancellationToken cancellationToken)
        {
            if (source is ISharedSource<TKey, TValue> shared)
                return new SharedJoinSide(shared.Subscribe(cancellationToken), keySelector);
            return new PrivateJoinSide(source.GetAsyncEnumerator(cancellationToken), keySelector);
        }

        /// <summary>Looks matches up through an index on the producer's shared cache, applying any client-side remainder.</summary>
        private sealed class SharedJoinSide : JoinSide<TKey, TValue, TJoinKey>
        {
            private readonly SharedSubscription<TKey, TValue> _subscription;
            private readonly CacheIndex<TKey, TValue, TJoinKey> _index;

            public SharedJoinSide(SharedSubscription<TKey, TValue> subscription, Func<TValue, IEnumerable<TJoinKey>> keySelector)
                : base(keySelector, subscription.Events)
            {
                _subscription = subscription;
                _index = subscription.Cache.AddIndex(keySelector);
            }

            public override IEnumerable<KeyValuePair<TKey, TValue>> Lookup(TJoinKey joinKey)
            {
                var matches = _index.Lookup(joinKey);
                if (_subscription.Remainder != null)
                    matches.RemoveAll(x => !_subscription.Remainder(x.Value));
                return matches;
            }

            public override bool TryGet(TKey key, out TValue value) =>
                _subscription.Cache.TryGetValue(key, out value) && (_subscription.Remainder == null || _subscription.Remainder(value));

            protected override void ApplyUpsert(TKey key, TValue value, TJoinKey[] oldKeys, TJoinKey[] newKeys)
            {
            }

            protected override void ApplyDelete(TKey key, TJoinKey[] oldKeys)
            {
            }

            public override async ValueTask DisposeAsync()
            {
                await base.DisposeAsync().ConfigureAwait(false);
                _index.Dispose();
            }
        }

        /// <summary>Keeps its own index over the input's events (references only).</summary>
        private sealed class PrivateJoinSide : JoinSide<TKey, TValue, TJoinKey>
        {
            private readonly Dictionary<TKey, TValue> _values = new Dictionary<TKey, TValue>();
            private readonly Dictionary<TJoinKey, HashSet<TKey>> _index = new Dictionary<TJoinKey, HashSet<TKey>>();

            public PrivateJoinSide(IAsyncEnumerator<ResourceEvent<TKey, TValue>> events, Func<TValue, IEnumerable<TJoinKey>> keySelector)
                : base(keySelector, events)
            {
            }

            public override IEnumerable<KeyValuePair<TKey, TValue>> Lookup(TJoinKey joinKey)
            {
                if (!_index.TryGetValue(joinKey, out var keys))
                    return Array.Empty<KeyValuePair<TKey, TValue>>();
                var result = new List<KeyValuePair<TKey, TValue>>(keys.Count);
                foreach (var key in keys)
                    result.Add(new KeyValuePair<TKey, TValue>(key, _values[key]));
                return result;
            }

            public override bool TryGet(TKey key, out TValue value) => _values.TryGetValue(key, out value);

            protected override void ApplyUpsert(TKey key, TValue value, TJoinKey[] oldKeys, TJoinKey[] newKeys)
            {
                RemoveFromIndex(key, oldKeys);
                _values[key] = value;
                foreach (var joinKey in newKeys)
                {
                    if (!_index.TryGetValue(joinKey, out var set))
                        _index[joinKey] = set = new HashSet<TKey>();
                    set.Add(key);
                }
            }

            protected override void ApplyDelete(TKey key, TJoinKey[] oldKeys)
            {
                RemoveFromIndex(key, oldKeys);
                _values.Remove(key);
            }

            private void RemoveFromIndex(TKey key, TJoinKey[] joinKeys)
            {
                foreach (var joinKey in joinKeys)
                {
                    if (_index.TryGetValue(joinKey, out var set))
                    {
                        set.Remove(key);
                        if (set.Count == 0)
                            _index.Remove(joinKey);
                    }
                }
            }
        }
    }
}
