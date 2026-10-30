using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Every <paramref name="period" />, re-emits every key the consumer has as <c>Modify | Sync</c> with its current value, so consumers
        ///     re-reconcile without upstream changes. A key that already has a change pending isn't synced again; the pending change covers it.
        ///     Chains anywhere: before <c>RunAsync</c>, or inside a projection.
        /// </summary>
        public static IInformable<TKey, TResource> Resync<TKey, TResource>(this IInformable<TKey, TResource> source, TimeSpan period)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            if (period <= TimeSpan.Zero)
                throw Error.ArgumentOutOfRange(nameof(period));
            return Create<TKey, TResource>(cancellationToken => new ResyncEnumerator<TKey, TResource>(source, period, cancellationToken));
        }

        /// <summary>
        ///     Pulls its source eagerly into a <see cref="ChangeQueue{TKey,TValue}" /> (so pending changes are visible when a sync is due) and serves
        ///     the consumer from it. Memory stays bounded by the number of keys.
        /// </summary>
        private sealed class ResyncEnumerator<TKey, TResource> : IAsyncEnumerator<ResourceEvent<TKey, TResource>>
        {
            private readonly object _lock = new object();
            private readonly IInformable<TKey, TResource> _source;
            private readonly TimeSpan _period;
            private readonly CancellationToken _cancellationToken;
            private readonly CancellationTokenSource _stop;
            private readonly ChangeQueue<TKey, TResource> _queue = new ChangeQueue<TKey, TResource>();
            private readonly Queue<ResourceEvent<TKey, TResource>> _resets = new Queue<ResourceEvent<TKey, TResource>>();
            private TaskCompletionSource<bool> _waiter;
            private Task _pump;
            private Task _timer;
            private Exception _error;
            private bool _completed;

            public ResyncEnumerator(IInformable<TKey, TResource> source, TimeSpan period, CancellationToken cancellationToken)
            {
                _source = source;
                _period = period;
                _cancellationToken = cancellationToken;
                _stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            }

            public ResourceEvent<TKey, TResource> Current { get; private set; }

            public async ValueTask<bool> MoveNextAsync()
            {
                if (_pump == null)
                {
                    _pump = Task.Run(PumpAsync);
                    _timer = Task.Run(TimerAsync);
                }

                while (true)
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                    TaskCompletionSource<bool> waiter;
                    lock (_lock)
                    {
                        if (_resets.Count > 0)
                        {
                            Current = _resets.Dequeue();
                            return true;
                        }

                        if (_queue.TryDequeue(out var next))
                        {
                            Current = next;
                            return true;
                        }

                        if (_error != null)
                            throw _error;
                        if (_completed)
                            return false;
                        waiter = _waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }

                    await waiter.Task.WaitAsync(_cancellationToken).ConfigureAwait(false);
                }
            }

            private async Task PumpAsync()
            {
                try
                {
                    var resets = new ResetCollector<TKey, TResource>();
                    await foreach (var e in _source.WithCancellation(_stop.Token).ConfigureAwait(false))
                    {
                        lock (_lock)
                        {
                            if (resets.TryCollect(e, out var reset))
                            {
                                if (reset == null)
                                    continue;
                                // a reset is a complete state: it replaces whatever the consumer has, and whatever was pending
                                _resets.Clear();
                                _queue.ResetKnown(reset);
                                if (reset.Count == 0)
                                    _resets.Enqueue(ResourceEvent<TKey, TResource>.EmptyReset);
                                foreach (var item in reset)
                                    _resets.Enqueue(item);
                            }
                            else
                            {
                                _queue.Enqueue(e);
                            }

                            WakeUnsafe();
                        }
                    }

                    lock (_lock)
                    {
                        _completed = true;
                        WakeUnsafe();
                    }
                }
                catch (OperationCanceledException) when (_stop.IsCancellationRequested)
                {
                }
                catch (Exception e)
                {
                    lock (_lock)
                    {
                        _error = e;
                        WakeUnsafe();
                    }
                }
            }

            private async Task TimerAsync()
            {
                try
                {
                    using var timer = new PeriodicTimer(_period);
                    while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
                    {
                        lock (_lock)
                        {
                            foreach (var key in _queue.Known.Keys.ToList())
                                _queue.RequestSync(key);
                            WakeUnsafe();
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
            }

            private void WakeUnsafe()
            {
                var waiter = _waiter;
                _waiter = null;
                waiter?.TrySetResult(true);
            }

            public async ValueTask DisposeAsync()
            {
                _stop.Cancel();
                if (_pump != null)
                    await Task.WhenAll(_pump, _timer).ConfigureAwait(false);
                _stop.Dispose();
            }
        }
    }
}
