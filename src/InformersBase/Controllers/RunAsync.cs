using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Diagnostics;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Runs <paramref name="handler" /> for every change, controller style: per-key serialization (two changes to one key never run in
        ///     parallel), up to <see cref="RunOptions.MaxConcurrency" /> keys at once, and retries with per-key backoff. A retry always runs with the
        ///     key's latest state, and a new change to a key supersedes its pending retry. Completes when the source completes and every handler
        ///     finished, or when <paramref name="cancellationToken" /> is cancelled: it then stops starting handlers and waits for running ones
        ///     (which receive the token) before returning normally. A source failure is thrown once running handlers finished.
        /// </summary>
        /// <param name="source">The informable to reconcile</param>
        /// <param name="handler">Reconciles one key; a thrown exception counts as <see cref="ReconcileResult.Requeue" /></param>
        /// <param name="options">Concurrency, backoff, rate limit and leader election</param>
        /// <param name="cancellationToken">Stops the loop</param>
        public static Task RunAsync<TKey, TResource>(
            this IInformable<TKey, TResource> source,
            Func<ResourceEvent<TKey, TResource>, CancellationToken, Task<ReconcileResult>> handler,
            RunOptions options = null,
            CancellationToken cancellationToken = default)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            if (handler == null)
                throw Error.ArgumentNull(nameof(handler));
            return new ReconcileLoop<TKey, TResource>(source, handler, options ?? new RunOptions()).RunAsync(cancellationToken);
        }

        /// <inheritdoc cref="RunAsync{TKey,TResource}(IInformable{TKey,TResource},Func{ResourceEvent{TKey,TResource},CancellationToken,Task{ReconcileResult}},RunOptions,CancellationToken)" />
        public static Task RunAsync<TKey, TResource>(
            this IInformable<TKey, TResource> source,
            Func<ResourceEvent<TKey, TResource>, CancellationToken, Task> handler,
            RunOptions options = null,
            CancellationToken cancellationToken = default)
        {
            if (handler == null)
                throw Error.ArgumentNull(nameof(handler));
            return source.RunAsync(async (e, ct) =>
            {
                await handler(e, ct).ConfigureAwait(false);
                return ReconcileResult.Done;
            }, options, cancellationToken);
        }

        private sealed class ReconcileLoop<TKey, TResource>
        {
            private static readonly TimeSpan ConflictDelayCap = TimeSpan.FromSeconds(1);
            private readonly object _lock = new object();
            private readonly IInformable<TKey, TResource> _source;
            private readonly Func<ResourceEvent<TKey, TResource>, CancellationToken, Task<ReconcileResult>> _handler;
            private readonly RunOptions _options;
            private readonly Backoff _backoff;
            private readonly KeyValuePair<string, object> _nameTag;
            private readonly ChangeQueue<TKey, TResource> _queue = new ChangeQueue<TKey, TResource>();
            private readonly Dictionary<TKey, int> _attempts = new Dictionary<TKey, int>();
            private readonly Dictionary<TKey, int> _conflicts = new Dictionary<TKey, int>();
            private readonly Dictionary<TKey, long> _scheduled = new Dictionary<TKey, long>(); // key -> token of its pending timed retry
            private long _nextToken;
            private readonly HashSet<Task> _running = new HashSet<Task>();
            private readonly ResetCollector<TKey, TResource> _resets = new ResetCollector<TKey, TResource>();
            private readonly CancellationTokenSource _timers = new CancellationTokenSource();
            private TaskCompletionSource<bool> _signal = NewSignal();
            private int _scheduledRetries;
            private int _inFlight;
            private bool _sourceCompleted;
            private Exception _sourceError;
            private long _reportedDepth;

            public ReconcileLoop(IInformable<TKey, TResource> source, Func<ResourceEvent<TKey, TResource>, CancellationToken, Task<ReconcileResult>> handler, RunOptions options)
            {
                if (options.MaxConcurrency < 1)
                    throw new ArgumentOutOfRangeException(nameof(options), "MaxConcurrency must be at least 1");
                _source = source;
                _handler = handler;
                _options = options;
                _backoff = options.Backoff ?? Backoff.Default;
                _nameTag = InformerDiagnostics.Tag("controller", options.Name ?? typeof(TResource).Name);
            }

            private static TaskCompletionSource<bool> NewSignal() => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            public async Task RunAsync(CancellationToken cancellationToken)
            {
                using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var pump = Task.Run(() => PumpAsync(stop.Token), CancellationToken.None);
                try
                {
                    await DispatchAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    stop.Cancel();
                    await pump.ConfigureAwait(false);
                    Task[] running;
                    lock (_lock)
                        running = _running.ToArray();
                    await Task.WhenAll(running).ConfigureAwait(false);
                    _timers.Cancel(); // pending retries die with the loop
                    ReportDepth(0);
                }

                lock (_lock)
                {
                    if (_sourceError != null && !cancellationToken.IsCancellationRequested)
                        throw new InformerException("The source of the reconcile loop failed.", _sourceError);
                }
            }

            private async Task PumpAsync(CancellationToken cancellationToken)
            {
                try
                {
                    await foreach (var e in _source.WithCancellation(cancellationToken).ConfigureAwait(false))
                    {
                        lock (_lock)
                        {
                            if (_resets.TryCollect(e, out var reset))
                            {
                                if (reset == null)
                                    continue;
                                foreach (var item in reset)
                                    SupersedeRetry(item.Key);
                                _queue.EnqueueReset(reset);
                            }
                            else
                            {
                                SupersedeRetry(e.Key);
                                _queue.Enqueue(e);
                            }

                            SignalUnsafe();
                        }
                    }

                    lock (_lock)
                    {
                        _sourceCompleted = true;
                        SignalUnsafe();
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                }
                catch (Exception e)
                {
                    lock (_lock)
                    {
                        _sourceError = e;
                        SignalUnsafe();
                    }
                }
            }

            private async Task DispatchAsync(CancellationToken cancellationToken)
            {
                // not disposed: handlers release their slot after the dispatcher may have returned
                var slots = new SemaphoreSlim(_options.MaxConcurrency);
                while (!cancellationToken.IsCancellationRequested)
                {
                    var leadership = CancellationToken.None;
                    if (_options.LeaderElection != null)
                    {
                        try
                        {
                            leadership = await WaitForLeadershipAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        if (leadership == default)
                            return; // the source ended or failed while waiting
                    }

                    using var leading = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, leadership);
                    try
                    {
                        while (true)
                        {
                            await slots.WaitAsync(leading.Token).ConfigureAwait(false);
                            ResourceEvent<TKey, TResource> next;
                            try
                            {
                                if (_options.RateLimit != null)
                                    await _options.RateLimit.WaitAsync(leading.Token).ConfigureAwait(false);
                                var taken = await TakeNextAsync(leading.Token).ConfigureAwait(false);
                                if (taken == null)
                                {
                                    slots.Release();
                                    // the source ended or failed; running handlers keep their (still linked) token until they finish
                                    Task[] remaining;
                                    lock (_lock)
                                        remaining = _running.ToArray();
                                    await Task.WhenAll(remaining).ConfigureAwait(false);
                                    return;
                                }

                                next = taken.Value;
                            }
                            catch
                            {
                                slots.Release();
                                throw;
                            }

                            var task = RunHandlerAsync(next, leading.Token, slots);
                            lock (_lock)
                                _running.Add(task);
                            _ = task.ContinueWith(t =>
                            {
                                lock (_lock)
                                    _running.Remove(t);
                            }, TaskScheduler.Default);
                        }
                    }
                    catch (OperationCanceledException) when (leading.IsCancellationRequested)
                    {
                        // shutdown or lost leadership: running handlers see the same token; wait for them before acting again
                        Task[] running;
                        lock (_lock)
                            running = _running.ToArray();
                        await Task.WhenAll(running).ConfigureAwait(false);
                    }
                }
            }

            private async Task<CancellationToken> WaitForLeadershipAsync(CancellationToken cancellationToken)
            {
                var leadership = _options.LeaderElection.WaitForLeadershipAsync(cancellationToken);
                while (true)
                {
                    Task signal;
                    lock (_lock)
                    {
                        if (_sourceError != null || (_sourceCompleted && _queue.PendingCount == 0 && _scheduledRetries == 0))
                            return default;
                        signal = _signal.Task;
                    }

                    if (await Task.WhenAny(leadership, signal).ConfigureAwait(false) == leadership)
                        return await leadership.ConfigureAwait(false);
                }
            }

            /// <summary>Waits for a ready key; null when the source ended or failed and nothing is left to do.</summary>
            private async Task<ResourceEvent<TKey, TResource>?> TakeNextAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    Task signal;
                    lock (_lock)
                    {
                        if (_sourceError != null)
                            return null;
                        if (_queue.TryDequeue(out var next))
                        {
                            _queue.Block(next.Key);
                            _inFlight++;
                            ReportDepthUnsafe();
                            return next;
                        }

                        if (_sourceCompleted && _inFlight == 0 && _scheduledRetries == 0 && _queue.PendingCount == 0)
                            return null;
                        signal = _signal.Task;
                    }

                    await signal.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            private async Task RunHandlerAsync(ResourceEvent<TKey, TResource> e, CancellationToken cancellationToken, SemaphoreSlim slots)
            {
                await Task.Yield();
                var started = Stopwatch.GetTimestamp();
                using var activity = InformerDiagnostics.ActivitySource.StartActivity("reconcile", ActivityKind.Internal);
                activity?.SetTag("informers.controller", _nameTag.Value);
                activity?.SetTag("informers.key", e.Key?.ToString());
                activity?.SetTag("informers.event", e.EventFlags.ToString());
                string outcome;
                try
                {
                    var result = await _handler(e, cancellationToken).ConfigureAwait(false);
                    outcome = OnResult(e, result);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // shutdown or lost leadership is not a failure: the key runs again when work resumes
                    outcome = "canceled";
                    lock (_lock)
                    {
                        if (!_queue.HasPending(e.Key))
                            RetryUnsafe(e);
                    }
                }
                catch (Exception ex) when (_options.IsConflictException(ex))
                {
                    outcome = "conflict";
                    int conflicts;
                    lock (_lock)
                    {
                        _conflicts.TryGetValue(e.Key, out conflicts);
                        _conflicts[e.Key] = ++conflicts;
                    }

                    // promptly, with the latest state; doesn't count toward the key's backoff
                    var delay = conflicts <= 1 ? TimeSpan.Zero : TimeSpan.FromTicks(Math.Min(ConflictDelayCap.Ticks, TimeSpan.FromMilliseconds(10).Ticks << Math.Min(conflicts - 2, 20)));
                    ScheduleRetry(e, delay);
                    _options.Logger?.LogDebug(ex, "Conflict reconciling {Key}; retrying with the latest state", e.Key);
                }
                catch (Exception ex)
                {
                    outcome = "error";
                    InformerDiagnostics.ReconcileErrors.Add(1, _nameTag);
                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    var delay = NextBackoff(e.Key);
                    _options.Logger?.LogError(ex, "Reconciling {Key} failed; retrying in {Delay}", e.Key, delay);
                    ScheduleRetry(e, delay);
                }
                finally
                {
                    lock (_lock)
                    {
                        _queue.Unblock(e.Key);
                        _inFlight--;
                        SignalUnsafe();
                    }

                    slots.Release();
                }

                InformerDiagnostics.Reconciles.Add(1, _nameTag, InformerDiagnostics.Tag("result", outcome));
                InformerDiagnostics.ReconcileDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, _nameTag);
                activity?.SetTag("informers.result", outcome);
            }

            private string OnResult(ResourceEvent<TKey, TResource> e, ReconcileResult result)
            {
                lock (_lock)
                    _conflicts.Remove(e.Key);
                if (result.IsDone)
                {
                    lock (_lock)
                    {
                        _attempts.Remove(e.Key);
                        _scheduled.Remove(e.Key);
                    }

                    return "done";
                }

                if (result.IsRequeueAfter)
                {
                    ScheduleRetry(e, result.Delay);
                    return "requeue_after";
                }

                ScheduleRetry(e, NextBackoff(e.Key));
                return "requeue";
            }

            private TimeSpan NextBackoff(TKey key)
            {
                lock (_lock)
                {
                    _attempts.TryGetValue(key, out var attempts);
                    _attempts[key] = ++attempts;
                    return _backoff.GetDelay(attempts);
                }
            }

            private void ScheduleRetry(ResourceEvent<TKey, TResource> e, TimeSpan delay)
            {
                long token;
                lock (_lock)
                {
                    // a change that arrived while the handler ran supersedes the retry: it runs next, with the latest state
                    if (_queue.HasPending(e.Key))
                        return;
                    InformerDiagnostics.Requeues.Add(1, _nameTag);
                    if (delay <= TimeSpan.Zero)
                    {
                        RetryUnsafe(e);
                        return;
                    }

                    token = ++_nextToken;
                    _scheduled[e.Key] = token;
                    _scheduledRetries++;
                }

                _ = Task.Delay(delay, _timers.Token).ContinueWith(t =>
                {
                    if (t.IsCanceled)
                        return;
                    lock (_lock)
                    {
                        _scheduledRetries--;
                        if (_scheduled.TryGetValue(e.Key, out var current) && current == token)
                        {
                            _scheduled.Remove(e.Key);
                            RetryUnsafe(e);
                        }

                        ReportDepthUnsafe();
                        SignalUnsafe();
                    }
                }, TaskScheduler.Default);
            }

            private void RetryUnsafe(ResourceEvent<TKey, TResource> e)
            {
                if (!_queue.Retry(e))
                {
                    // the key is gone and its delete was already handled: forget its backoff
                    _attempts.Remove(e.Key);
                    _conflicts.Remove(e.Key);
                }
            }

            /// <summary>A new change to a key drops its pending retry (the change runs as soon as a worker is free).</summary>
            private void SupersedeRetry(TKey key) => _scheduled.Remove(key);

            private void SignalUnsafe()
            {
                ReportDepthUnsafe();
                var signal = _signal;
                _signal = NewSignal();
                signal.TrySetResult(true);
            }

            private void ReportDepthUnsafe() => ReportDepth(_queue.ReadyCount);

            private void ReportDepth(long depth)
            {
                var delta = depth - Interlocked.Exchange(ref _reportedDepth, depth);
                if (delta != 0)
                    InformerDiagnostics.QueueDepth.Add(delta, _nameTag);
            }
        }
    }
}
