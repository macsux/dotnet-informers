using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static Steeltoe.Informers.InformersBase.Tests.Core.CoreTestHelpers;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    public class SlowConsumerTests
    {
        private readonly ITestOutputHelper _output;

        public SlowConsumerTests(ITestOutputHelper output) => _output = output;

        [Fact]
        public async Task StalledConsumer_DoesNotBlockProducerOrFastConsumer_AndStaysBoundedByKeys()
        {
            const int keys = 1_000;
            const int events = 500_000;
            var cache = NewCache();
            cache.Replace(Enumerable.Range(0, keys).Select(k => I(k, 0)));

            // Stalled: reads two reset items, then never pulls again (but stays attached).
            var stalledView = new ConsumerView(0);
            var stalled = cache.Register();
            for (var i = 0; i < 2; i++)
            {
                Assert.True(await stalled.MoveNextAsync());
                stalledView.Apply(stalled.Current);
            }

            var fastView = new ConsumerView(1);
            var fast = cache.Register();
            var fastTask = Task.Run(async () =>
            {
                while (await fast.MoveNextAsync())
                    fastView.Apply(fast.Current);
            });

            var maxDirty = 0;
            var produce = Task.Run(() =>
            {
                var rng = new Random(1);
                for (long i = 1; i <= events; i++)
                {
                    cache.Upsert(I(rng.Next(keys), i));
                    if (i % 100_000 == 0)
                        maxDirty = Math.Max(maxDirty, stalled.DirtyCount);
                }
            });

            var finished = await Task.WhenAny(produce, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.Same(produce, finished); // the producer finished while the stalled consumer never pulled
            cache.Complete();
            await fastTask;

            var expected = cache.Snapshot().ToDictionary(p => p.Key, p => p.Value.Value);
            Assert.Empty(fastView.Errors);
            Assert.Null(fastView.Diff(expected));
            Assert.InRange(maxDirty, 0, keys);
            Assert.InRange(stalled.DirtyCount, 0, keys);

            // When it finally resumes, it converges on the latest state with at most one event per key after its reset.
            var resumedEvents = 0;
            while (await stalled.MoveNextAsync())
            {
                stalledView.Apply(stalled.Current);
                resumedEvents++;
            }

            Assert.Empty(stalledView.Errors);
            Assert.Null(stalledView.Diff(expected));
            Assert.True(resumedEvents <= 2 * keys, $"resumed with {resumedEvents} events");
            _output.WriteLine($"fast consumer received {fastView.Events} events for {events} produced; stalled dirty max {maxDirty}; resumed with {resumedEvents}");
        }
    }
}
