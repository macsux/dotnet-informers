using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class RunAsyncTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static KeyValuePair<string, int> P(string key, int value) => new KeyValuePair<string, int>(key, value);

        private static ResourceEvent<string, int> E(EventTypeFlags flags, string key, int value) => new ResourceEvent<string, int>(flags, key, value);

        private static RunOptions Fast(int concurrency = 1) => new RunOptions { MaxConcurrency = concurrency, Backoff = Backoff.Constant(TimeSpan.FromMilliseconds(20)) };

        [Fact]
        public async Task CompletesWhenFiniteSourceIsDone_HandlesEveryKey()
        {
            var seen = new ConcurrentBag<string>();
            await Informable.Create(new[] { 1, 2, 3 }, x => x.ToString())
                .RunAsync((e, ct) =>
                {
                    seen.Add(e.Key);
                    return Task.CompletedTask;
                }, Fast(), Ct)
                .TimeoutIfNotDebugging();
            seen.Should().BeEquivalentTo("1", "2", "3");
        }

        [Fact]
        public async Task SameKeyNeverRunsConcurrently_OtherKeysDo()
        {
            var source = new TestSource<string, int>();
            source.SendReset(P("a", 0), P("b", 0));
            var running = new ConcurrentDictionary<string, int>();
            var maxPerKey = 0;
            var maxTotal = 0;
            var total = 0;
            var handled = 0;
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.RunAsync(async (e, ct) =>
            {
                var perKey = running.AddOrUpdate(e.Key, 1, (_, n) => n + 1);
                var now = Interlocked.Increment(ref total);
                InterlockedMax(ref maxPerKey, perKey);
                InterlockedMax(ref maxTotal, now);
                await Task.Delay(30, ct);
                running.AddOrUpdate(e.Key, 0, (_, n) => n - 1);
                Interlocked.Decrement(ref total);
                Interlocked.Increment(ref handled);
            }, Fast(4), cts.Token);
            for (var i = 1; i <= 10; i++)
                source.Send(E(EventTypeFlags.Modify, "a", i), E(EventTypeFlags.Modify, "b", i));
            await Task.Delay(400, Ct);
            cts.Cancel();
            await run.TimeoutIfNotDebugging();
            maxPerKey.Should().Be(1);
            maxTotal.Should().Be(2);
            handled.Should().BeLessThan(22, "changes to a busy key merge into one");
        }

        [Fact]
        public async Task ThrowingHandler_RetriesWithLatestState_OtherKeysKeepFlowing()
        {
            var source = new TestSource<string, int>();
            source.SendReset(P("bad", 1), P("good", 1));
            var badValues = new ConcurrentQueue<int>();
            var goodHandled = new TaskCompletionSource<bool>();
            var badSucceeded = new TaskCompletionSource<int>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.RunAsync((e, ct) =>
            {
                if (e.Key == "good")
                {
                    goodHandled.TrySetResult(true);
                    return Task.CompletedTask;
                }

                badValues.Enqueue(e.Value);
                if (e.Value < 3)
                {
                    if (badValues.Count == 1)
                        source.Send(E(EventTypeFlags.Modify, "bad", 3)); // a newer state arrives while it fails
                    throw new InvalidOperationException("boom");
                }

                badSucceeded.TrySetResult(e.Value);
                return Task.CompletedTask;
            }, new RunOptions { MaxConcurrency = 2, Backoff = Backoff.Constant(TimeSpan.FromSeconds(5)) }, cts.Token);
            await goodHandled.Task.TimeoutIfNotDebugging();
            // the newer change supersedes the 5 s backoff and runs as soon as a worker is free
            (await badSucceeded.Task.TimeoutIfNotDebugging()).Should().Be(3);
            badValues.Should().Equal(1, 3);
            cts.Cancel();
            await run.TimeoutIfNotDebugging();
        }

        [Fact]
        public async Task Requeue_UsesBackoff_RequeueAfter_UsesExactDelay_DoneResetsBackoff()
        {
            var calls = new ConcurrentQueue<(DateTime At, int Attempt)>();
            var attempt = 0;
            var done = new TaskCompletionSource<bool>();
            var backoff = new RecordingBackoff();
            await Task.WhenAny(Informable.Create(new[] { 1 }, x => "k")
                .RunAsync(async (e, ct) =>
                {
                    var n = Interlocked.Increment(ref attempt);
                    calls.Enqueue((DateTime.UtcNow, n));
                    await Task.Yield();
                    if (n <= 2)
                        return ReconcileResult.Requeue();
                    if (n == 3)
                        return ReconcileResult.RequeueAfter(TimeSpan.FromMilliseconds(150));
                    done.TrySetResult(true);
                    return ReconcileResult.Done;
                }, new RunOptions { Backoff = backoff }, Ct), done.Task).TimeoutIfNotDebugging();
            await done.Task.TimeoutIfNotDebugging();
            backoff.Attempts.Should().Equal(1, 2);
            var c = calls.ToArray();
            (c[3].At - c[2].At).Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(140));
        }

        private sealed class RecordingBackoff : Backoff
        {
            public readonly ConcurrentQueue<int> Queue = new ConcurrentQueue<int>();
            public int[] Attempts => Queue.ToArray();

            public override TimeSpan GetDelay(int attempt)
            {
                Queue.Enqueue(attempt);
                return TimeSpan.FromMilliseconds(10);
            }
        }

        [Fact]
        public async Task Conflict_RetriesPromptly_WithoutCountingTowardBackoff()
        {
            var backoff = new RecordingBackoff();
            var attempt = 0;
            var done = new TaskCompletionSource<bool>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = Informable.Create(new[] { 1 }, x => "k").RunAsync((e, ct) =>
            {
                if (Interlocked.Increment(ref attempt) <= 3)
                    throw new ConflictException();
                done.TrySetResult(true);
                return Task.CompletedTask;
            }, new RunOptions { Backoff = backoff, IsConflict = ex => ex is ConflictException }, cts.Token);
            await done.Task.TimeoutIfNotDebugging();
            backoff.Attempts.Should().BeEmpty();
            await run.TimeoutIfNotDebugging();
        }

        private sealed class ConflictException : Exception
        {
        }

        [Fact]
        public async Task Cancellation_StopsStartingWork_WaitsForRunningHandlers_ReturnsNormally()
        {
            var source = new TestSource<string, int>();
            source.SendReset(P("a", 1));
            var started = new TaskCompletionSource<bool>();
            var finished = false;
            using var cts = new CancellationTokenSource();
            var run = source.RunAsync(async (e, ct) =>
            {
                started.TrySetResult(true);
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    await Task.Delay(100, CancellationToken.None); // cleanup that must finish before RunAsync returns
                    finished = true;
                    throw;
                }
            }, Fast(), cts.Token);
            await started.Task.TimeoutIfNotDebugging();
            cts.Cancel();
            await run.TimeoutIfNotDebugging();
            finished.Should().BeTrue();
        }

        [Fact]
        public async Task SourceFailure_IsThrown()
        {
            var source = new TestSource<string, int>();
            source.SendReset(P("a", 1));
            var run = Informable.Create<string, int>(ct => Fail(ct)).RunAsync((e, ct) => Task.CompletedTask, Fast(), Ct);
            var act = async () => await run.TimeoutIfNotDebugging();
            (await act.Should().ThrowAsync<InformerException>()).WithInnerException<TimeoutException>();

            static async IAsyncEnumerator<ResourceEvent<string, int>> Fail(CancellationToken ct)
            {
                yield return new ResourceEvent<string, int>(EventTypeFlags.ResetEmpty, null);
                await Task.Delay(10, ct);
                throw new TimeoutException();
            }
        }

        [Fact]
        public async Task SyncEvents_ReachTheHandlerFlaggedSync()
        {
            var source = new TestSource<string, int>();
            source.SendReset(P("a", 1));
            var synced = new TaskCompletionSource<ResourceEvent<string, int>>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.Resync(TimeSpan.FromMilliseconds(100)).RunAsync((e, ct) =>
            {
                if (e.EventFlags.HasFlag(EventTypeFlags.Sync))
                    synced.TrySetResult(e);
                return Task.CompletedTask;
            }, Fast(), cts.Token);
            var e = await synced.Task.TimeoutIfNotDebugging();
            e.EventFlags.Should().HaveFlag(EventTypeFlags.Modify);
            cts.Cancel();
            await run;
        }

        [Fact]
        public async Task Deletes_ReachTheHandler_WithLastValue()
        {
            var source = new TestSource<string, int>();
            source.SendReset(P("a", 7));
            var deleted = new TaskCompletionSource<ResourceEvent<string, int>>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.RunAsync((e, ct) =>
            {
                if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                    deleted.TrySetResult(e);
                return Task.CompletedTask;
            }, Fast(), cts.Token);
            await Task.Delay(50, Ct);
            source.Send(E(EventTypeFlags.Delete | EventTypeFlags.Computed, "a", 7));
            var e = await deleted.Task.TimeoutIfNotDebugging();
            e.Value.Should().Be(7);
            e.EventFlags.Should().HaveFlag(EventTypeFlags.Computed);
            cts.Cancel();
            await run;
        }

        [Fact]
        public async Task LeaderElection_HandlersOnlyRunWhileLeading_LosingLeadershipCancelsThem()
        {
            var election = new ManualLeaderElection();
            var source = new TestSource<string, int>();
            source.SendReset(P("a", 1));
            var runs = 0;
            var cancelled = new TaskCompletionSource<bool>();
            var secondRun = new TaskCompletionSource<bool>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.RunAsync(async (e, ct) =>
            {
                if (Interlocked.Increment(ref runs) == 1)
                {
                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        cancelled.TrySetResult(true);
                        throw;
                    }
                }

                secondRun.TrySetResult(true);
            }, new RunOptions { LeaderElection = election }, cts.Token);

            await Task.Delay(100, Ct);
            runs.Should().Be(0, "not the leader yet");
            election.Elect();
            await WaitUntil(() => Volatile.Read(ref runs) == 1);
            election.Depose();
            await cancelled.Task.TimeoutIfNotDebugging();
            await Task.Delay(100, Ct);
            runs.Should().Be(1, "nothing runs while not leading");
            election.Elect();
            await secondRun.Task.TimeoutIfNotDebugging(); // the interrupted key runs again once leading
            cts.Cancel();
            await run.TimeoutIfNotDebugging();
        }

        private sealed class ManualLeaderElection : ILeaderElection
        {
            private readonly object _lock = new object();
            private TaskCompletionSource<CancellationToken> _leading = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
            private CancellationTokenSource _term;

            public void Elect()
            {
                lock (_lock)
                {
                    _term = new CancellationTokenSource();
                    _leading.TrySetResult(_term.Token);
                }
            }

            public void Depose()
            {
                lock (_lock)
                {
                    _leading = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _term.Cancel();
                }
            }

            public Task<CancellationToken> WaitForLeadershipAsync(CancellationToken cancellationToken)
            {
                lock (_lock)
                    return _leading.Task.WaitAsync(cancellationToken);
            }
        }

        private static void InterlockedMax(ref int target, int value)
        {
            int current;
            while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current)
            {
            }
        }

        private static async Task WaitUntil(Func<bool> condition)
        {
            for (var i = 0; i < 500 && !condition(); i++)
                await Task.Delay(10);
            condition().Should().BeTrue();
        }
    }
}
