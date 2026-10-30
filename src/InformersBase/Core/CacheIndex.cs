using System;
using System.Collections.Generic;
using System.Linq;

namespace Steeltoe.Informers.InformersBase
{
    internal interface ICacheIndex<in TKey, in TValue>
    {
        void UpdateUnsafe(TKey key, TValue value, bool exists);
        void ClearUnsafe();
    }

    /// <summary>
    ///     A multi-valued index over a <see cref="SharedCache{TKey,TValue}" />: one object can be indexed under several keys. Maintained under
    ///     the cache lock by every apply. A selector that throws or returns null indexes the object under no key.
    /// </summary>
    internal sealed class CacheIndex<TKey, TValue, TIndexKey> : ICacheIndex<TKey, TValue>, IDisposable
    {
        private static readonly TIndexKey[] NoKeys = Array.Empty<TIndexKey>();
        private readonly SharedCache<TKey, TValue> _cache;
        private readonly Func<TValue, IEnumerable<TIndexKey>> _selector;
        private readonly Dictionary<TIndexKey, HashSet<TKey>> _byIndexKey;
        private readonly Dictionary<TKey, TIndexKey[]> _byKey;

        public CacheIndex(SharedCache<TKey, TValue> cache, Func<TValue, IEnumerable<TIndexKey>> selector, IEqualityComparer<TIndexKey> comparer)
        {
            _cache = cache;
            _selector = selector;
            _byIndexKey = new Dictionary<TIndexKey, HashSet<TKey>>(comparer ?? EqualityComparer<TIndexKey>.Default);
            _byKey = new Dictionary<TKey, TIndexKey[]>(cache.KeyComparer);
        }

        internal static TIndexKey[] SelectKeys(Func<TValue, IEnumerable<TIndexKey>> selector, TValue value)
        {
            try
            {
                var keys = selector(value);
                return keys == null ? NoKeys : keys.Where(x => x != null).Distinct().ToArray();
            }
            catch (Exception)
            {
                return NoKeys;
            }
        }

        public void UpdateUnsafe(TKey key, TValue value, bool exists)
        {
            if (_byKey.Remove(key, out var oldKeys))
            {
                foreach (var oldKey in oldKeys)
                {
                    if (_byIndexKey.TryGetValue(oldKey, out var set))
                    {
                        set.Remove(key);
                        if (set.Count == 0)
                            _byIndexKey.Remove(oldKey);
                    }
                }
            }

            if (!exists)
                return;
            var newKeys = SelectKeys(_selector, value);
            if (newKeys.Length == 0)
                return;
            _byKey[key] = newKeys;
            foreach (var newKey in newKeys)
            {
                if (!_byIndexKey.TryGetValue(newKey, out var set))
                    _byIndexKey[newKey] = set = new HashSet<TKey>(_cache.KeyComparer);
                set.Add(key);
            }
        }

        public void ClearUnsafe()
        {
            _byIndexKey.Clear();
            _byKey.Clear();
        }

        /// <summary>The current objects indexed under <paramref name="indexKey" />.</summary>
        public List<KeyValuePair<TKey, TValue>> Lookup(TIndexKey indexKey)
        {
            var result = new List<KeyValuePair<TKey, TValue>>();
            if (indexKey == null)
                return result;
            lock (_cache.Lock)
            {
                if (!_byIndexKey.TryGetValue(indexKey, out var keys))
                    return result;
                foreach (var key in keys)
                    result.Add(new KeyValuePair<TKey, TValue>(key, _cache.ItemsUnsafe[key].Value));
            }

            return result;
        }

        public void Dispose() => _cache.RemoveIndex(this);
    }
}
