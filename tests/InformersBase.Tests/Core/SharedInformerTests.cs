using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using static Steeltoe.Informers.InformersBase.Tests.Core.CoreTestHelpers;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    public class SharedInformerTests
    {
        /// <summary>A producer that lists keys 0-5 (filtered by "even"/"odd" server terms), then modifies its first key every millisecond until cancelled.</summary>
        private sealed class CountingProducer
        {
            public int Started;
            public int Running;
            public int MaxRunningPerFilter;
            public readonly ConcurrentBag<ServerFilter> Filters = new ConcurrentBag<ServerFilter>();
            private readonly ConcurrentDictionary<ServerFilter, int> _runningPerFilter = new ConcurrentDictionary<ServerFilter, int>();

            public async Task Produce(IInformerSink<Item> sink, ServerFilter filter, CancellationToken ct)
            {
                Interlocked.Increment(ref Started);
                Interlocked.Increment(ref Running);
                Filters.Add(filter);
                InterlockedMax(ref MaxRunningPerFilter, _runningPerFilter.AddOrUpdate(filter, 1, (_, n) => n + 1));
                try
                {
                    var parity = filter.ValuesOf("parity").SingleOrDefault();
                    bool Matches(int k) => parity == null || (parity == "even") == (k % 2 == 0);
                    sink.Replace(Enumerable.Range(0, 6).Where(Matches).Select(i => I(i, i)));
                    var updated = Matches(0) ? 0 : 1;
                    long v = 100;
                    while (true)
                    {
                        await Task.Delay(1, ct);
                        sink.Upsert(I(updated, ++v));
                    }
                }
                finally
                {
                    _runningPerFilter.AddOrUpdate(filter, 0, (_, n) => n - 1);
                    Interlocked.Decrement(ref Running);
                }
            }

            private static void InterlockedMax(ref int target, int value)
            {
                int current;
                while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current)
                {
                }
            }
        }

        /// <summary>Translates <c>x.Key % 2 == 0</c> / <c>!= 0</c> into a "parity" server term.</summary>
        private sealed class ParityTranslator : IFilterTranslator<Item>
        {
            public bool TryTranslate(Expression term, ParameterExpression parameter, ServerFilter current, out ServerFilter result)
            {
                result = null;
                if (term is BinaryExpression { Left: BinaryExpression { NodeType: ExpressionType.Modulo } modulo, Right: ConstantExpression { Value: 0 } } binary
                    && modulo.Right is ConstantExpression { Value: 2 }
                    && modulo.Left is MemberExpression { Member.Name: nameof(Item.Key) } member && member.Expression == parameter
                    && current.ValuesOf("parity").FirstOrDefault() == null)
                {
                    if (binary.NodeType == ExpressionType.Equal)
                        result = current.With(new ServerFilterTerm("parity", "even"));
                    else if (binary.NodeType == ExpressionType.NotEqual)
                        result = current.With(new ServerFilterTerm("parity", "odd"));
                }

                return result != null;
            }
        }

        private static SharedInformer<int, Item> NewInformer(CountingProducer producer, bool translate = true)
        {
            var options = Options();
            if (translate)
                options.FilterTranslator = new ParityTranslator();
            return new SharedInformer<int, Item>(producer.Produce, options);
        }

        [Fact]
        public async Task OneProducerPerServerFilter_StoppedAfterLastConsumer()
        {
            var producer = new CountingProducer();
            var informer = NewInformer(producer);

            var a = informer.GetAsyncEnumerator();
            var b = informer.GetAsyncEnumerator();
            Assert.True(await a.MoveNextAsync());
            Assert.True(await b.MoveNextAsync());
            Assert.Equal(1, producer.Started);
            Assert.True(a.Current.EventFlags.HasFlag(EventTypeFlags.ResetStart));
            Assert.True(informer.IsSynced);

            await a.DisposeAsync();
            Assert.Equal(1, informer.ActiveProducers);
            await b.DisposeAsync();
            Assert.Equal(0, informer.ActiveProducers);
            Assert.False(informer.IsSynced);
            await WaitUntil(() => Volatile.Read(ref producer.Running) == 0);

            // A consumer after the last one left starts a fresh producer and gets a fresh reset.
            var c = informer.GetAsyncEnumerator();
            Assert.True(await c.MoveNextAsync());
            Assert.True(c.Current.EventFlags.HasFlag(EventTypeFlags.ResetStart));
            Assert.Equal(2, producer.Started);
            await c.DisposeAsync();
        }

        [Fact]
        public async Task EquivalentPredicates_ShareOneProducer_EachAppliesItsOwnRemainder()
        {
            var producer = new CountingProducer();
            var informer = NewInformer(producer);

            var small = informer.Where(x => x.Key % 2 == 0 && x.Key < 3);
            var large = informer.Where(x => x.Key > 3 && x.Key % 2 == 0);
            var odd = informer.Where(x => x.Key % 2 != 0);
            Assert.Equal(((SharedInformer<int, Item>)small).ServerFilter, ((SharedInformer<int, Item>)large).ServerFilter);

            await using var e1 = small.GetAsyncEnumerator();
            await using var e2 = large.GetAsyncEnumerator();
            await using var e3 = odd.GetAsyncEnumerator();
            var r1 = await ReadReset(e1);
            var r2 = await ReadReset(e2);
            var r3 = await ReadReset(e3);
            Assert.Equal(new[] { 0, 2 }, r1);
            Assert.Equal(new[] { 4 }, r2);
            Assert.Equal(new[] { 1, 3, 5 }, r3);
            Assert.Equal(2, informer.ActiveProducers);
            Assert.Equal(2, producer.Started);
            Assert.Contains(producer.Filters, f => f.ValuesOf("parity").Single() == "even");
            Assert.Contains(producer.Filters, f => f.ValuesOf("parity").Single() == "odd");
        }

        [Fact]
        public async Task OrMixingTranslatableTerms_StaysClientSide()
        {
            var producer = new CountingProducer();
            var informer = NewInformer(producer);
            var mixed = (SharedInformer<int, Item>)informer.Where(x => x.Key % 2 == 0 || x.Key == 3);
            Assert.True(mixed.ServerFilter.IsEmpty);
            Assert.True(mixed.HasClientFilter);
            await using var e = mixed.GetAsyncEnumerator();
            Assert.Equal(new[] { 0, 2, 3, 4 }, await ReadReset(e));
        }

        [Fact]
        public async Task PushdownGivesSameResultsAsClientSideEvaluation()
        {
            Expression<Func<Item, bool>>[] predicates =
            {
                x => x.Key % 2 == 0,
                x => x.Key % 2 != 0 && x.Key > 1,
                x => x.Key > 1 && x.Key % 2 == 0 && x.Key < 5,
                x => x.Key % 2 == 0 || x.Key == 1,
            };
            foreach (var predicate in predicates)
            {
                var pushed = NewInformer(new CountingProducer()).Where(predicate);
                var client = NewInformer(new CountingProducer(), translate: false).Where(predicate);
                await using var a = pushed.GetAsyncEnumerator();
                await using var b = client.GetAsyncEnumerator();
                Assert.Equal(await ReadReset(b), await ReadReset(a));
            }
        }

        [Fact]
        public async Task WaitForSync_CompletesWhenAConsumerAttachesAndTheListArrives()
        {
            var producer = new CountingProducer();
            var informer = NewInformer(producer);
            var waiting = informer.WaitForSyncAsync();
            await Task.Delay(50);
            Assert.False(waiting.IsCompleted);
            await using var e = informer.GetAsyncEnumerator();
            var first = e.MoveNextAsync();
            await waiting.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(informer.IsSynced);
            await first;
            await informer.WaitForSyncAsync().WaitAsync(TimeSpan.FromSeconds(5)); // already synced
        }

        [Fact]
        public async Task ConcurrentAcquireRelease_NeverLeaksProducers_NeverTwoPerFilter()
        {
            var producer = new CountingProducer();
            var informer = NewInformer(producer);
            var views = new IInformable<int, Item>[] { informer, informer.Where(x => x.Key % 2 == 0), informer.Where(x => x.Key % 2 != 0) };
            var tasks = Enumerable.Range(0, 64).Select(i => Task.Run(async () =>
            {
                var rng = new Random(i);
                for (var n = 0; n < 50; n++)
                {
                    var e = views[rng.Next(views.Length)].GetAsyncEnumerator();
                    var reads = rng.Next(1, 6);
                    for (var r = 0; r < reads && await e.MoveNextAsync(); r++)
                    {
                    }

                    await e.DisposeAsync();
                }
            }));
            await Task.WhenAll(tasks);
            Assert.Equal(0, informer.ActiveProducers);
            await WaitUntil(() => Volatile.Read(ref producer.Running) == 0);
            // A new producer for a filter starts only after the previous one (cancelled when its last consumer left) has stopped.
            Assert.Equal(1, producer.MaxRunningPerFilter);
        }

        [Fact]
        public async Task ProducerFailure_ReachesConsumersAndSyncWaiters()
        {
            var informer = new SharedInformer<int, Item>((sink, filter, ct) => throw new TimeoutException("list failed"), Options());
            var waiting = informer.WaitForSyncAsync();
            var e = informer.GetAsyncEnumerator();
            var ex = await Assert.ThrowsAsync<InformerException>(async () => await e.MoveNextAsync());
            Assert.IsType<TimeoutException>(ex.InnerException);
            await Assert.ThrowsAsync<InformerException>(() => waiting.WaitAsync(TimeSpan.FromSeconds(5)));
            await e.DisposeAsync();
        }

        [Fact]
        public async Task List_WithoutListFunction_ReadsOneReset()
        {
            var informer = NewInformer(new CountingProducer());
            var items = await informer.Where(x => x.Key % 2 == 0).List().ToListAsync();
            Assert.Equal(new[] { 0, 2, 4 }, items.Select(x => x.Key).OrderBy(x => x));
            await WaitUntil(() => informer.ActiveProducers == 0);
        }

        private static async Task<int[]> ReadReset(IAsyncEnumerator<ResourceEvent<int, Item>> e)
        {
            var keys = new List<int>();
            while (await e.MoveNextAsync())
            {
                if (e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                    break;
                keys.Add(e.Current.Key);
                if (e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
                    break;
            }

            return keys.OrderBy(x => x).ToArray();
        }
    }
}
