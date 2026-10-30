using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    internal sealed class StressConfig
    {
        public int Seed { get; set; }
        public int Keys { get; set; } = 200;
        public int Operations { get; set; } = 20_000;
        public double DeleteProbability { get; set; } = 0.25;
        public double RelistProbability { get; set; } = 0.0005;
        public int CheckpointEvery { get; set; } = 2_500;
        public int MaxConsumers { get; set; } = 12;
        public FaultMode Fault { get; set; } = FaultMode.None;
        public TimeSpan CheckpointTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }

    internal sealed class StressResult
    {
        public int Joins;
        public int Leaves;
        public int LeftMidReset;
        public int JoinedBeforeSync;
        public int Relists;
        public int Checkpoints;
        public int ViewsVerified;
        public long EventsDelivered;
        public long ResetsDelivered;
        public int ConsumersCompleted;
        public readonly ConcurrentQueue<string> Failures = new ConcurrentQueue<string>();
        public TimeSpan Elapsed;

        public override string ToString() =>
            $"joins={Joins} leaves={Leaves} leftMidReset={LeftMidReset} joinedBeforeSync={JoinedBeforeSync} relists={Relists} " +
            $"checkpoints={Checkpoints} viewsVerified={ViewsVerified} events={EventsDelivered} resets={ResetsDelivered} " +
            $"completed={ConsumersCompleted} failures={Failures.Count} elapsed={Elapsed.TotalMilliseconds:F0}ms";
    }

    /// <summary>
    /// One randomized run: a producer thread applies upserts, deletes and occasional relists to a shared cache while consumers
    /// join and leave at random with different pacing. Every <see cref="StressConfig.CheckpointEvery"/> operations the producer
    /// pauses until every attached consumer is parked with nothing left to read, then each consumer's view is compared with
    /// the cache. At the end the producer completes and every remaining consumer's final view is compared with the cache.
    /// </summary>
    internal static class StressHarness
    {
        public static async Task<StressResult> RunAsync(StressConfig config)
        {
            var result = new StressResult();
            var cache = CoreTestHelpers.NewCache(fault: config.Fault);
            var consumers = new ConcurrentDictionary<int, Task>();
            var producerDone = 0;
            var pauseSpawning = 0;
            var sw = Stopwatch.StartNew();

            void Checkpoint()
            {
                Volatile.Write(ref pauseSpawning, 1);
                var deadline = Stopwatch.StartNew();
                var spin = new SpinWait();
                while (true)
                {
                    lock (cache.Lock)
                    {
                        if (cache.CursorsUnsafe.All(c => c.IsIdleUnsafe))
                        {
                            var expected = cache.ItemsUnsafe.ToDictionary(p => p.Key, p => p.Value.Value.Value);
                            foreach (var cursor in cache.CursorsUnsafe)
                            {
                                var view = (ConsumerView)cursor.Tag;
                                var diff = view.Diff(expected);
                                if (diff != null)
                                    result.Failures.Enqueue($"checkpoint {result.Checkpoints}: {diff}");
                                result.ViewsVerified++;
                            }

                            result.Checkpoints++;
                            break;
                        }
                    }

                    if (deadline.Elapsed > config.CheckpointTimeout)
                    {
                        result.Failures.Enqueue($"checkpoint {result.Checkpoints}: consumers never went idle");
                        break;
                    }

                    if (spin.NextSpinWillYield)
                        Thread.Sleep(1);
                    else
                        spin.SpinOnce();
                }

                Volatile.Write(ref pauseSpawning, 0);
            }

            void Produce()
            {
                var rng = new Random(config.Seed);
                long value = 0;
                var model = new Dictionary<int, long>();

                // Some consumers join before the first list: they must wait for it rather than see an empty reset.
                Thread.Sleep(rng.Next(0, 3));
                Relist();
                for (var i = 1; i <= config.Operations; i++)
                {
                    var r = rng.NextDouble();
                    var key = rng.Next(config.Keys);
                    if (r < config.RelistProbability)
                    {
                        Relist();
                        result.Relists++;
                    }
                    else if (r < config.RelistProbability + config.DeleteProbability && model.Remove(key))
                    {
                        cache.Delete(key);
                    }
                    else
                    {
                        model[key] = ++value;
                        cache.Upsert(new Item(key, value));
                    }

                    if (i % config.CheckpointEvery == 0)
                        Checkpoint();
                    else if (rng.Next(32) == 0)
                        Thread.Yield();
                }

                void Relist()
                {
                    model = new Dictionary<int, long>();
                    for (var k = 0; k < config.Keys; k++)
                    {
                        if (rng.Next(10) < 7)
                            model[k] = ++value;
                    }

                    cache.Replace(model.Select(p => new Item(p.Key, p.Value)).ToList());
                }
            }

            async Task Consume(int id, int seed)
            {
                var rng = new Random(seed);
                var view = new ConsumerView(id);
                var pacing = rng.Next(4); // 0 fast, 1 yield, 2 slow, 3 bursty stalls
                var leaveAfter = rng.Next(2) == 0 ? rng.Next(0, 3 * config.Keys) : int.MaxValue;
                var joinedBeforeSync = !cache.IsSynced;
                var cursor = cache.Register();
                cursor.Tag = view;
                Interlocked.Increment(ref result.Joins);
                if (joinedBeforeSync)
                    Interlocked.Increment(ref result.JoinedBeforeSync);
                var completed = false;
                try
                {
                    while (true)
                    {
                        if (view.Events >= leaveAfter)
                        {
                            Interlocked.Increment(ref result.Leaves);
                            if (view.InReset)
                                Interlocked.Increment(ref result.LeftMidReset);
                            break;
                        }

                        if (!await cursor.MoveNextAsync())
                        {
                            completed = true;
                            break;
                        }

                        view.Apply(cursor.Current);
                        switch (pacing)
                        {
                            case 1:
                                await Task.Yield();
                                break;
                            case 2 when rng.Next(50) == 0:
                                await Task.Delay(1);
                                break;
                            case 3 when rng.Next(500) == 0:
                                await Task.Delay(rng.Next(5, 30));
                                break;
                        }
                    }
                }
                catch (Exception e)
                {
                    result.Failures.Enqueue($"consumer {id} threw {e}");
                }
                finally
                {
                    await cursor.DisposeAsync();
                }

                foreach (var error in view.Errors)
                    result.Failures.Enqueue(error);
                Interlocked.Add(ref result.EventsDelivered, view.Events);
                Interlocked.Add(ref result.ResetsDelivered, view.Resets);
                if (completed)
                {
                    var diff = view.Diff(cache.Snapshot().ToDictionary(p => p.Key, p => p.Value.Value));
                    if (diff != null)
                        result.Failures.Enqueue($"final: {diff}");
                    Interlocked.Increment(ref result.ConsumersCompleted);
                }
            }

            async Task Spawn()
            {
                var rng = new Random(config.Seed * 7919 + 1);
                var nextId = 0;
                while (Volatile.Read(ref producerDone) == 0)
                {
                    foreach (var done in consumers.Where(p => p.Value.IsCompleted).ToList())
                        consumers.TryRemove(done.Key, out _);

                    if (Volatile.Read(ref pauseSpawning) == 0 && consumers.Count < config.MaxConsumers)
                    {
                        var id = nextId++;
                        var seed = rng.Next();
                        consumers[id] = Task.Run(() => Consume(id, seed));
                    }

                    if (rng.Next(4) == 0)
                        await Task.Delay(1);
                    else
                        await Task.Yield();
                }
            }

            var spawner = Task.Run(Spawn);
            await Task.Run(Produce);
            cache.Complete();
            Volatile.Write(ref producerDone, 1);
            await spawner;
            await Task.WhenAll(consumers.Values);
            result.Elapsed = sw.Elapsed;
            return result;
        }
    }
}
