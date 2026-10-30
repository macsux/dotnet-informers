using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase.Diagnostics;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     An informer over any list/watch source. A producer (supplied by the source) fills a shared cache; every enumeration attaches a
    ///     cursor to it. Producers are ref-counted per server filter: the first consumer of a filter starts one, the last one to leave stops it.
    /// </summary>
    /// <typeparam name="TKey">The type of key</typeparam>
    /// <typeparam name="TResource">The type of resource</typeparam>
    public class SharedInformer<TKey, TResource> : IInformer<TKey, TResource>, ISharedSource<TKey, TResource>
    {
        private readonly Registry _registry;
        private readonly ServerFilter _filter;
        private readonly Func<TResource, bool> _remainder;

        /// <param name="producer">Fills the shared cache for one server filter until cancelled</param>
        /// <param name="options">Keying, change detection, filter translation and transform</param>
        /// <param name="list">Optional one-off list for <see cref="List" />; without it, <see cref="List" /> reads one reset from the informer</param>
        public SharedInformer(
            InformerProducer<TResource> producer,
            SharedInformerOptions<TKey, TResource> options,
            Func<ServerFilter, CancellationToken, IAsyncEnumerable<TResource>> list = null)
            : this(new Registry(producer ?? throw new ArgumentNullException(nameof(producer)), options ?? throw new ArgumentNullException(nameof(options)), list), ServerFilter.Empty, null)
        {
        }

        internal SharedInformer(InformerProducer<TResource> producer, SharedInformerOptions<TKey, TResource> options, FaultMode fault)
            : this(new Registry(producer, options, null, fault), ServerFilter.Empty, null)
        {
        }

        private SharedInformer(Registry registry, ServerFilter filter, Func<TResource, bool> remainder)
        {
            _registry = registry;
            _filter = filter;
            _remainder = remainder;
        }

        /// <summary>The server filter this informer's producer is subscribed with.</summary>
        public ServerFilter ServerFilter => _filter;

        /// <summary>Whether part of this informer's predicates is evaluated client-side.</summary>
        public bool HasClientFilter => _remainder != null;

        /// <summary>Number of producers currently running across all server filters of this informer.</summary>
        public int ActiveProducers => _registry.ActiveProducers;

        public bool IsSynced => _registry.IsSynced(_filter);

        public Task WaitForSyncAsync(CancellationToken cancellationToken = default) => _registry.WaitForSyncAsync(_filter, cancellationToken);

        public IInformer<TKey, TResource> Where(Expression<Func<TResource, bool>> predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));
            var (filter, remainder) = FilterSplitter.Split(predicate, _registry.Options.FilterTranslator, _filter);
            return new SharedInformer<TKey, TResource>(_registry, filter, FilterSplitter.And(_remainder, remainder?.Compile()));
        }

        public IInformable<TKey, TResource> ListWatch() => this;

        /// <summary>A view of this informer with additional server filter terms (e.g. from explicit options rather than a predicate).</summary>
        internal SharedInformer<TKey, TResource> WithServerFilter(ServerFilter filter) =>
            new SharedInformer<TKey, TResource>(_registry, _filter.With(filter.Terms), _remainder);

        public IAsyncEnumerable<TResource> List(CancellationToken cancellationToken = default)
        {
            if (_registry.List != null)
            {
                var items = _registry.List(_filter, cancellationToken);
                return _remainder == null ? items : items.Where(_remainder);
            }

            return ListFromReset(cancellationToken);
        }

        private async IAsyncEnumerable<TResource> ListFromReset([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var enumerator = GetAsyncEnumerator(cancellationToken);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var e = enumerator.Current;
                if (e.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                    yield break;
                yield return e.Value;
                if (e.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
                    yield break;
            }
        }

        public IAsyncEnumerator<ResourceEvent<TKey, TResource>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var cursor = new CancellableCursor(_registry.Acquire(_filter), cancellationToken);
            if (_remainder == null)
                return cursor;
            return Informable.Create(_ => cursor).Where(_remainder).GetAsyncEnumerator(cancellationToken);
        }

        SharedSubscription<TKey, TResource> ISharedSource<TKey, TResource>.Subscribe(CancellationToken cancellationToken)
        {
            var cursor = _registry.Acquire(_filter);
            var cancellable = new CancellableCursor(cursor, cancellationToken);
            var events = _remainder == null ? (IAsyncEnumerator<ResourceEvent<TKey, TResource>>)cancellable : Informable.Create(_ => cancellable).Where(_remainder).GetAsyncEnumerator(cancellationToken);
            return new SharedSubscription<TKey, TResource>(cursor.Cache, _remainder, events);
        }

        internal SharedCache<TKey, TResource> TryGetCache() => _registry.TryGetCache(_filter);

        private sealed class CancellableCursor : IAsyncEnumerator<ResourceEvent<TKey, TResource>>
        {
            private readonly Cursor<TKey, TResource> _cursor;
            private readonly CancellationToken _cancellationToken;

            public CancellableCursor(Cursor<TKey, TResource> cursor, CancellationToken cancellationToken)
            {
                _cursor = cursor;
                _cancellationToken = cancellationToken;
            }

            public ResourceEvent<TKey, TResource> Current => _cursor.Current;
            public ValueTask<bool> MoveNextAsync() => _cursor.MoveNextAsync(_cancellationToken);
            public ValueTask DisposeAsync() => _cursor.DisposeAsync();
        }

        /// <summary>Hands out one producer and cache per server filter, ref-counted by attached cursors.</summary>
        private sealed class Registry
        {
            private readonly object _lock = new object();
            private readonly Dictionary<ServerFilter, Shared> _shared = new Dictionary<ServerFilter, Shared>();
            private readonly Dictionary<ServerFilter, Task> _lastProducer = new Dictionary<ServerFilter, Task>(); // may still be stopping
            private readonly Dictionary<ServerFilter, TaskCompletionSource<bool>> _syncWaiters = new Dictionary<ServerFilter, TaskCompletionSource<bool>>();
            private readonly InformerProducer<TResource> _producer;
            private readonly FaultMode _fault;
            private readonly KeyValuePair<string, object> _nameTag;

            public Registry(InformerProducer<TResource> producer, SharedInformerOptions<TKey, TResource> options, Func<ServerFilter, CancellationToken, IAsyncEnumerable<TResource>> list, FaultMode fault = FaultMode.None)
            {
                if (options.KeySelector == null)
                    throw new ArgumentException("KeySelector is required", nameof(options));
                _producer = producer;
                Options = options;
                List = list;
                _fault = fault;
                _nameTag = InformerDiagnostics.Tag("informer", options.Name ?? typeof(TResource).Name);
            }

            public SharedInformerOptions<TKey, TResource> Options { get; }
            public Func<ServerFilter, CancellationToken, IAsyncEnumerable<TResource>> List { get; }

            public int ActiveProducers
            {
                get
                {
                    lock (_lock)
                        return _shared.Count;
                }
            }

            public SharedCache<TKey, TResource> TryGetCache(ServerFilter filter)
            {
                lock (_lock)
                    return _shared.TryGetValue(filter, out var shared) ? shared.Cache : null;
            }

            public bool IsSynced(ServerFilter filter)
            {
                lock (_lock)
                    return _shared.TryGetValue(filter, out var shared) && shared.Cache.IsSynced;
            }

            public Task WaitForSyncAsync(ServerFilter filter, CancellationToken cancellationToken)
            {
                TaskCompletionSource<bool> waiter;
                lock (_lock)
                {
                    if (_shared.TryGetValue(filter, out var shared) && shared.Cache.IsSynced)
                        return Task.CompletedTask;
                    if (!_syncWaiters.TryGetValue(filter, out waiter))
                        _syncWaiters[filter] = waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                return waiter.Task.WaitAsync(cancellationToken);
            }

            public Cursor<TKey, TResource> Acquire(ServerFilter filter)
            {
                lock (_lock)
                {
                    if (!_shared.TryGetValue(filter, out var shared))
                    {
                        shared = new Shared(new SharedCache<TKey, TResource>(Options, _fault));
                        _shared.Add(filter, shared);
                        // A producer for this filter may still be unwinding after its last consumer left; start only once it has stopped, so a
                        // filter never has two list/watch connections open at once.
                        _lastProducer.TryGetValue(filter, out var previous);
                        var task = shared.ProducerTask = Task.Run(() => RunProducerAsync(previous, filter, shared));
                        _lastProducer[filter] = task;
                        _ = task.ContinueWith(t =>
                        {
                            lock (_lock)
                            {
                                if (_lastProducer.TryGetValue(filter, out var last) && last == t)
                                    _lastProducer.Remove(filter);
                            }
                        }, TaskScheduler.Default);
                        _ = shared.Cache.SyncedTask.ContinueWith(t => OnSynced(filter, shared, t), TaskScheduler.Default);
                    }

                    shared.RefCount++;
                    // Registering inside the registry lock means a cursor never attaches to a producer that's being torn down.
                    return shared.Cache.Register(_ => Release(filter, shared));
                }
            }

            private void OnSynced(ServerFilter filter, Shared shared, Task synced)
            {
                TaskCompletionSource<bool> waiter;
                lock (_lock)
                {
                    if (!_shared.TryGetValue(filter, out var current) || current != shared || !_syncWaiters.Remove(filter, out waiter))
                        return;
                }

                if (synced.IsFaulted)
                    waiter.TrySetException(new InformerException("The informer's producer failed.", synced.Exception!.GetBaseException()));
                else
                    waiter.TrySetResult(true);
            }

            private async Task RunProducerAsync(Task previous, ServerFilter filter, Shared shared)
            {
                try
                {
                    if (previous != null)
                        await previous.ConfigureAwait(false); // never faults: RunProducerAsync catches everything
                    shared.Cts.Token.ThrowIfCancellationRequested();
                    InformerDiagnostics.ActiveProducers.Add(1, _nameTag);
                    try
                    {
                        await _producer(shared.Cache, filter, shared.Cts.Token).ConfigureAwait(false);
                    }
                    finally
                    {
                        InformerDiagnostics.ActiveProducers.Add(-1, _nameTag);
                    }

                    shared.Cache.Complete();
                }
                catch (OperationCanceledException) when (shared.Cts.IsCancellationRequested)
                {
                    // the last consumer left
                }
                catch (Exception e)
                {
                    shared.Cache.SetError(e);
                }
            }

            private void Release(ServerFilter filter, Shared shared)
            {
                lock (_lock)
                {
                    if (--shared.RefCount > 0)
                        return;
                    _shared.Remove(filter);
                }

                shared.Cts.Cancel();
            }

            private sealed class Shared
            {
                public Shared(SharedCache<TKey, TResource> cache) => Cache = cache;
                public SharedCache<TKey, TResource> Cache { get; }
                public CancellationTokenSource Cts { get; } = new CancellationTokenSource();
                public int RefCount;
                public Task ProducerTask;
            }
        }
    }

    /// <summary>Implemented by informers backed by a shared cache, so joins can look matches up through its indexes.</summary>
    internal interface ISharedSource<TKey, TResource>
    {
        SharedSubscription<TKey, TResource> Subscribe(CancellationToken cancellationToken);
    }

    /// <summary>A consumer attached to a shared cache: its events (with any client-side remainder applied) plus the cache for lookups.</summary>
    internal sealed class SharedSubscription<TKey, TResource>
    {
        public SharedSubscription(SharedCache<TKey, TResource> cache, Func<TResource, bool> remainder, IAsyncEnumerator<ResourceEvent<TKey, TResource>> events)
        {
            Cache = cache;
            Remainder = remainder;
            Events = events;
        }

        public SharedCache<TKey, TResource> Cache { get; }
        public Func<TResource, bool> Remainder { get; }
        public IAsyncEnumerator<ResourceEvent<TKey, TResource>> Events { get; }
    }
}
