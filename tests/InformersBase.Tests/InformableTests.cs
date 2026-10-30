using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Cache;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class InformableTests
    {
        [Fact]
        public async Task Select()
        {
            var sut = await Informable
                .Create(new[]
                {
                    new TestResource("1", value: "hello"),
                    new TestResource("2", value: "there")
                }, x => x.Key)
                .Select(x => x.Value.ToUpper())
                .ToList();
            
            sut.Should().BeEquivalentTo("HELLO", "THERE");
        }
        [Fact]
        public async Task Cast()
        {
            var sut = await Informable
                .Create(new[]
                {
                    new TestResource("1", value: "hello"),
                }, x => x.Key)
                .Cast<string, TestResource, object>()
                .ToList();
            sut.Should().BeEquivalentTo(new[] { new TestResource("1", value: "hello") });
        }
        [Fact]
        public async Task Do_Action()
        {
            var called = false;
            var sut = await Informable
                .Create(new[]
                {
                    new TestResource("1", value: "hello"),
                }, x => x.Key)
                .Do(e => called = true)
                .ToList();
            
            called.Should().BeTrue();
        }
        [Fact]
        public async Task Do_ActionWithOldValue()
        {
            string oldValue = null;
            int doCount = 0;
            await foreach (var _ in Informable
                .Create(new[] { new TestResource("1", value: "hello") }, 
                    x => x.Key, 
                    new []{ new TestResource("1", value: "there").ToResourceEvent(EventTypeFlags.Modify) })
                .Do((e, existing) =>
                {
                    doCount++;
                    oldValue = existing?.Value;
                }))
            {
            }

            doCount.Should().Be(2);
            oldValue.Should().Be("hello");
        }
        
        [Fact]
        public async Task Do_Task()
        {
            var items = new List<int>();
            int doCount = 0;
            HashSet<int> completed = new HashSet<int>();
            await Informable
                .Create(new[] { 1,2 }, x => x)
                .Do(async x =>
                {
                    var i = doCount++;
                    if (doCount == 1)
                        await Task.Delay(1000);
                    else
                        await Task.Delay(100);
                    completed.Add(i);
                })
                .Do(x =>
                {
                    completed.Should().NotContain(x.Value);
                    items.Add(x.Value);
                })
                .ToList();
            items.Should().BeEquivalentTo(new[] { 1, 2 });
        }

        [Fact]
        public async Task Into()
        {
            var cache = new SimpleCache<int, int>();
            var source = new TestSource<int, int>();
            source.SendReset(new KeyValuePair<int, int>(1, 1), new KeyValuePair<int, int>(2, 2), new KeyValuePair<int, int>(3, 3));
            var sut = source.Into(cache).GetAsyncEnumerator(TestContext.Current.CancellationToken);
            for (var i = 0; i < 3; i++)
                await sut.MoveNextAsync().TimeoutIfNotDebugging();
            await cache.Synchronized.TimeoutIfNotDebugging();
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 1, 2, 3 });

            source.Send(1.ToResourceEvent(EventTypeFlags.Delete));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 2, 3 });

            source.Send(4.ToResourceEvent(EventTypeFlags.Add));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 2, 3, 4 });

            source.Send(1.ToResourceEvent(EventTypeFlags.Modify, 2));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 1, 3, 4 });
            cache.Keys.ToList().Should().BeEquivalentTo(new[] { 2, 3, 4 });
        }

        [Fact]
        public async Task SimpleCache_FedFromSource_IsSynchronizedAfterFirstReset()
        {
            var source = new TestSource<int, int>();
            using var cache = new SimpleCache<int, int>(source, TestContext.Current.CancellationToken);
            cache.Synchronized.IsCompleted.Should().BeFalse();
            source.SendReset(new KeyValuePair<int, int>(1, 1));
            await cache.Synchronized.TimeoutIfNotDebugging();
            cache.Values.Should().BeEquivalentTo(new[] { 1 });
        }

        [Fact]
        public async Task Where()
        {
            var source = new TestSource<int, int>();
            source.SendReset(new[] { 1, 2, 3, 4, 0 }.Select(x => new KeyValuePair<int, int>(x, x)).ToArray());
            var sut = source.Where(x => x > 2).GetAsyncEnumerator(TestContext.Current.CancellationToken);

            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.Should().Be(3.ToResourceEvent(EventTypeFlags.ResetStart));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.Should().Be(4.ToResourceEvent(EventTypeFlags.ResetEnd));

            // not in the filtered view: nothing
            source.Send(1.ToResourceEvent(EventTypeFlags.Delete));
            var next = sut.MoveNextAsync();
            await Task.Delay(50, TestContext.Current.CancellationToken);
            next.IsCompleted.Should().BeFalse();

            source.Send(3.ToResourceEvent(EventTypeFlags.Delete));
            (await next.TimeoutIfNotDebugging()).Should().BeTrue();
            sut.Current.Should().Be(3.ToResourceEvent(EventTypeFlags.Delete));

            source.Send(5.ToResourceEvent(EventTypeFlags.Add));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.Should().Be(5.ToResourceEvent(EventTypeFlags.Add));
        }

        [Fact]
        public async Task Where_ObjectsLeavingAndEnteringTheFilter_AreDeletesAndAdds()
        {
            var source = new TestSource<string, TestResource>();
            source.SendReset(new KeyValuePair<string, TestResource>("a", new TestResource("a", value: "in")));
            var sut = source.Where(x => x.Value == "in").GetAsyncEnumerator(TestContext.Current.CancellationToken);
            await sut.MoveNextAsync().TimeoutIfNotDebugging();

            source.Send(new TestResource("a", value: "out").ToResourceEvent(EventTypeFlags.Modify));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.EventFlags.Should().HaveFlag(EventTypeFlags.Delete);

            source.Send(new TestResource("a", value: "in").ToResourceEvent(EventTypeFlags.Modify | EventTypeFlags.Computed));
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.EventFlags.Should().Be(EventTypeFlags.Add | EventTypeFlags.Computed);
        }

        [Fact]
        public async Task Where_CanBeEnumeratedMoreThanOnce()
        {
            var where = Informable.Create(new[] { 1, 2, 3 }, x => x).Where(x => x > 1);
            (await where.ToList()).Should().BeEquivalentTo(new[] { 2, 3 });
            (await where.ToList()).Should().BeEquivalentTo(new[] { 2, 3 });
        }

        [Fact]
        public async Task ToLookup_UsesKeySelector()
        {
            var lookup = await Informable.Create(new[] { 1, 2, 3, 4 }, x => x).ToLookup(x => x % 2 == 0 ? "even" : "odd");
            lookup["even"].Should().BeEquivalentTo(new[] { 2, 4 });
            lookup["odd"].Should().BeEquivalentTo(new[] { 1, 3 });
        }

        [Fact]
        public async Task ToList_HonoursCancellation()
        {
            var source = new TestSource<int, int>();
            using var cts = new System.Threading.CancellationTokenSource(50);
            var act = async () => await source.ToList(cts.Token);
            await act.Should().ThrowAsync<System.OperationCanceledException>();
        }

        [Fact]
        public async Task ComputeMissedEventsBetweenResets_LaterResetsBecomeComputedChanges_IntoKeepsThem()
        {
            var cache = new SimpleCache<int, int>();
            var source = new TestSource<int, int>();
            source.SendReset(new KeyValuePair<int, int>(1, 1), new KeyValuePair<int, int>(2, 2));
            source.SendReset(new KeyValuePair<int, int>(2, 2), new KeyValuePair<int, int>(3, 3)); // relist: 1 gone, 3 new
            source.Complete();
            var events = await source.ComputeMissedEventsBetweenResets(EqualityComparer<int>.Default).Into(cache).ToListAsync(TestContext.Current.CancellationToken);
            events.Skip(2).Select(x => (x.EventFlags, x.Key)).Should().Equal(
                (EventTypeFlags.Delete | EventTypeFlags.Computed, 1),
                (EventTypeFlags.Add | EventTypeFlags.Computed, 3));
            cache.Keys.Should().BeEquivalentTo(new[] { 2, 3 });
        }
    }
    
}