using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
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
            Informable
                .Create(new[] { new TestResource("1", value: "hello") }, 
                    x => x.Key, 
                    new []{ new TestResource("1", value: "there").ToResourceEvent(EventTypeFlags.Modify) })
                .Do((e, existing) =>
                {
                    doCount++;
                    oldValue = existing?.Value;
                })
                .Subscribe();

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
            var source = new[] {1, 2, 3};
            var subject = new Subject<ResourceEvent<int,int>>();
            
            Informable.Create(source, x => x, subject.ToAsyncEnumerable())
                .Into(cache)
                .Subscribe();
            await cache.Synchronized.TimeoutIfNotDebugging();
            cache.Values.ToList().Should().BeEquivalentTo(source);
            subject.OnNext(1.ToResourceEvent(EventTypeFlags.Delete));
            await Task.Delay(100);
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 2,3 });
            
            subject.OnNext(4.ToResourceEvent(EventTypeFlags.Add));
            await Task.Delay(100);
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 2,3,4 });
            
            subject.OnNext(1.ToResourceEvent(EventTypeFlags.Modify,2));
            await Task.Delay(100);
            cache.Values.ToList().Should().BeEquivalentTo(new[] { 1,3,4 });
            cache.Keys.ToList().Should().BeEquivalentTo(new[] { 2,3,4 });
        }

        [Fact]
        public async Task Where()
        {
            var source = new[] {1, 2, 3, 4, 0 };
            var subject = new Subject<ResourceEvent<int,int>>();
            
            var sut = Informable.Create(source, x => x, subject.ToAsyncEnumerable())
                .Where(x => x > 2).GetAsyncEnumerator();

            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.Should().Be(3.ToResourceEvent(EventTypeFlags.ResetStart));
            
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.Should().Be(4.ToResourceEvent(EventTypeFlags.Reset));
            
            await sut.MoveNextAsync().TimeoutIfNotDebugging();
            sut.Current.Should().Be(ResourceEvent.Create<int,int>(EventTypeFlags.ResetEnd, 0));

            subject.OnNext(1.ToResourceEvent(EventTypeFlags.Delete));
            var next = sut.MoveNextAsync();
            next.IsCompleted.Should().BeFalse();
            
            subject.OnNext(3.ToResourceEvent(EventTypeFlags.Delete));
            await next.TimeoutIfNotDebugging();
            next.IsCompleted.Should().BeTrue();
            sut.Current.Should().BeEquivalentTo(3.ToResourceEvent(EventTypeFlags.Delete));
            
            subject.OnNext(5.ToResourceEvent(EventTypeFlags.Add));
            next = sut.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            sut.Current.Should().Be(5.ToResourceEvent(EventTypeFlags.Add));
        }
    }
    
}