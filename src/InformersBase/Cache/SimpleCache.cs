using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase.Cache
{
    public class SimpleCache<TKey, TResource> : ICache<TKey, TResource>
    {
        private Dictionary<string, Func<TResource, List<string>>> _indexers = new Dictionary<string, Func<TResource, List<string>>>();
        private Dictionary<string, Dictionary<string, HashSet<TKey>>> _indices = new Dictionary<string, Dictionary<string, HashSet<TKey>>>();

        private readonly IDictionary<TKey, TResource> _items;
        protected readonly object _syncRoot = new object();
        private readonly TaskCompletionSource<bool> _synchronized = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenSource _feedCancellation;

        /// <summary>Completes when the cache received its first complete state (a reset); faults if the source fails before that.</summary>
        public Task Synchronized => _synchronized.Task;

        /// <summary>Creates a cache kept up to date from <paramref name="source" /> until <paramref name="cancellationToken" /> is cancelled or the cache is disposed.</summary>
        public SimpleCache(IInformable<TKey, TResource> source, CancellationToken cancellationToken) : this()
        {
            _feedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _ = FeedAsync(source, _feedCancellation.Token);
        }

        private async Task FeedAsync(IInformable<TKey, TResource> source, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var _ in source.Into(this).WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _synchronized.TrySetCanceled(cancellationToken);
            }
            catch (Exception e)
            {
                _synchronized.TrySetException(e);
            }
        }

        public SimpleCache()
        {
            _items = new Dictionary<TKey, TResource>();
        }

        public SimpleCache(IDictionary<TKey, TResource> items, long version)
        {
            Version = version;
            _items = new Dictionary<TKey, TResource>(items);
        }
        public List<TResource> ByIndex(string indexName, string indexKey) 
        {
            lock (_syncRoot)
            {
                if(!_indexers.ContainsKey(indexName))
                    throw new InvalidOperationException($"Index {indexName} does not exist");
                var index = _indices[indexName];
                if(!index.TryGetValue(indexKey, out var set))
                    return new List<TResource>();
                var items = set.Select(x => _items[x]).ToList();
                return items;
            }
        }
        public void AddIndex(string indexName, Func<TResource, List<string>> indexFunc) 
        {
            _indices[indexName] = new Dictionary<string, HashSet<TKey>>();
            _indexers[indexName] = indexFunc;
        }
        private void UpdateIndices(TResource oldObj, TResource newObj, TKey key) 
        {
            // if we got an old object, we need to remove it before we can add
            // it again.
            if (oldObj != null) 
            {
                DeleteFromIndices(oldObj, key);
            }
            foreach (var indexEntry in _indexers) 
            {
                var indexName = indexEntry.Key;
                var indexFunc = indexEntry.Value;
                var indexValues = indexFunc(newObj);
                if (!indexValues.Any()) {
                    continue;
                }

                if (!_indices.TryGetValue(indexName, out var index))
                {
                    index = new Dictionary<string, HashSet<TKey>>();
                    _indices[indexName] = index;
                }
                foreach (var indexValue in indexValues) 
                {
                    if (!index.TryGetValue(indexValue, out var indexSet))
                    {
                        indexSet = new HashSet<TKey>();
                        index[indexValue] = indexSet;
                    }
                    indexSet.Add(key);
                }
            }
        }
        private void DeleteFromIndices(TResource oldObj, TKey key) 
        {
            foreach (var indexEntry in _indexers)
            {
                var indexFunc = indexEntry.Value;
                var indexValues = indexFunc(oldObj);
                if (!indexValues.Any()) 
                {
                    continue;
                }

                if (!_indices.TryGetValue(indexEntry.Key, out var index))
                    continue;
                foreach (var indexValue in indexValues) 
                {
                    if(index.TryGetValue(indexValue, out var indexSet))
                    {
                        indexSet.Remove(key);
                    }
                }
            }
        }
        public void Reset(IDictionary<TKey, TResource> newValues)
        {
            lock (_syncRoot)
            {
                _items.Clear();
                foreach (var index in _indices.Values)
                    index.Clear();
                foreach (var item in newValues)
                {
                    _items.Add(item.Key, item.Value);
                    UpdateIndices(default, item.Value, item.Key);
                }

                _synchronized.TrySetResult(true);
            }
        }

        public ICacheSnapshot<TKey, TResource> Snapshot()
        {
            lock (_syncRoot)
            {
                return new SimpleCacheSnapshot(this, Version);
            }
        }

        public IEnumerator<KeyValuePair<TKey, TResource>> GetEnumerator()
        {
            lock (_syncRoot)
            {
                return _items.ToList().GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            lock (_syncRoot)
            {
                return _items.ToList().GetEnumerator();
            }
        }

        public void Add(KeyValuePair<TKey, TResource> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            lock (_syncRoot)
            {
                _items.Clear();
                foreach (var index in _indices.Values)
                {
                    index.Clear();
                }
            }
        }

        public bool Contains(KeyValuePair<TKey, TResource> item)
        {
            lock (_syncRoot)
            {
                return _items.Contains(item);
            }
        }

        public void CopyTo(KeyValuePair<TKey, TResource>[] array, int arrayIndex)
        {
            lock (_syncRoot)
            {
                _items.CopyTo(array, arrayIndex);
            }
        }

        public bool Remove(KeyValuePair<TKey, TResource> item)
        {
            lock (_syncRoot)
            {
                if (_items.TryGetValue(item.Key, out var existing))
                {
                    DeleteFromIndices(existing, item.Key);
                }

                return _items.Remove(item.Key);
            }
        }

        public int Count
        {
            get
            {
                lock (_syncRoot)
                {
                    return _items.Count;
                }
            }
        }

        public bool IsReadOnly => false;

        public void Add(TKey key, TResource value)
        {
            lock (_syncRoot)
            {
                _items.Add(key, value);
                UpdateIndices(default, value, key);
            }
        }

        public bool ContainsKey(TKey key)
        {
            lock (_syncRoot)
            {
                return _items.ContainsKey(key);
            }
        }

        public bool Remove(TKey key)
        {
            lock (_syncRoot)
            {
                if (!_items.Remove(key, out var existing))
                {
                    return false;
                }
                DeleteFromIndices(existing, key);
                return true;
            }
        }

        public bool TryGetValue(TKey key, out TResource value)
        {
            lock (_syncRoot)
            {
                return _items.TryGetValue(key, out value);
            }
        }

        public TResource this[TKey key]
        {
            get
            {
                lock (_syncRoot)
                {
                    return _items[key];
                }
            }
            set
            {
                lock (_syncRoot)
                {
                    _items.TryGetValue(key, out var oldValue);
                    _items[key] = value;
                    UpdateIndices(oldValue, value, key);
                }
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                lock (_syncRoot)
                {
                    return _items.Keys.ToList();
                }
            }
        }

        public ICollection<TResource> Values
        {
            get
            {
                lock (_syncRoot)
                {
                    return _items.Values.ToList();
                }
            }
        }

        public virtual void Dispose()
        {
            _feedCancellation?.Cancel();
            _feedCancellation?.Dispose();
        }

        public long Version { get; set; }

        internal sealed class SimpleCacheSnapshot : ICacheSnapshot<TKey, TResource>
        {
            private readonly Dictionary<TKey, TResource> _items;
            public SimpleCacheSnapshot(IDictionary<TKey, TResource> cache, long version)
            {
                _items = new Dictionary<TKey, TResource>(cache);
                Version = version;
            }
            public IEnumerator<KeyValuePair<TKey, TResource>> GetEnumerator() => _items.GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
            public int Count => _items.Count;
            public bool ContainsKey(TKey key) => _items.ContainsKey(key);
            public bool TryGetValue(TKey key, out TResource value) => _items.TryGetValue(key, out value);
            public TResource this[TKey key] => _items[key];
            public IEnumerable<TKey> Keys => _items.Keys;
            public IEnumerable<TResource> Values => _items.Values;
            public long Version { get; }
        }
    }
}
