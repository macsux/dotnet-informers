using System.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class ObservableToInformerTests
    {
        [Fact]
        public void EmptyEnumerator_MoveNext_UnfinishedTask()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var enumerator = sut.GetAsyncEnumerator();
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeFalse();
        }
        
        [Fact]
        public void EmptyReset_MoveNext_EmptyResetEvent()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            subject.OnNext(ResourceEvent<string, TestResource>.EmptyReset);
            var enumerator = sut.GetAsyncEnumerator();
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(ResourceEvent<string, TestResource>.EmptyReset);
            
            next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeFalse();
        }
        
        [Fact]
        public void MultiItemReset_DelayedEnumeration()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                ResourceEvent.Create(EventTypeFlags.ResetStart, "1", new TestResource("1")),
                ResourceEvent.Create(EventTypeFlags.ResetEnd, "2", new TestResource("2")),
            };
            subject.OnNext(events[0]);
            subject.OnNext(events[1]);
            
            var enumerator = sut.GetAsyncEnumerator();
            
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(events[0]);
            
            next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(events[1]);
            
            next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeFalse();
        }
        
        [Fact]
        public void UpstreamResetWithModify_BeforeDownstreamReset()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1", 1).ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("2", 1).ToResourceEvent(EventTypeFlags.ResetEnd),
                new TestResource("2", 2).ToResourceEvent(EventTypeFlags.Modify),
            };
            subject.OnNext(events[0]);
            subject.OnNext(events[1]);
            subject.OnNext(events[2]);
            
            var enumerator = sut.GetAsyncEnumerator();
            
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(events[0]);
            
            next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(new TestResource("2", 2).ToResourceEvent(EventTypeFlags.ResetEnd));
            
            next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeFalse();
        }
        
        [Fact]
        public void Delete()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1", 1).ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("2", 1).ToResourceEvent(EventTypeFlags.ResetEnd),
                new TestResource("1", 2).ToResourceEvent(EventTypeFlags.Delete),
            };
            subject.OnNext(events[0]);
            subject.OnNext(events[1]);
            
            var enumerator = sut.GetAsyncEnumerator();
            
            enumerator.MoveNextAsync();
            enumerator.MoveNextAsync();
            subject.OnNext(events[2]);
            var next = enumerator.MoveNextAsync();
            
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(events[2]);
        }
        
        [Fact]
        public void Add()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1" ).ToResourceEvent(EventTypeFlags.ResetEmpty),
                new TestResource("2" ).ToResourceEvent(EventTypeFlags.Add),
            };
            subject.OnNext(events[0]);
            var enumerator = sut.GetAsyncEnumerator();
            enumerator.MoveNextAsync();
            
            subject.OnNext(events[1]);
            enumerator.MoveNextAsync();
            
            enumerator.Current.Should().Be(events[1]);
        }
        [Fact]
        public void Modify()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1" ).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd),
                new TestResource("1", 2).ToResourceEvent(EventTypeFlags.Modify),
            };
            subject.OnNext(events[0]);
            var enumerator = sut.GetAsyncEnumerator();
            enumerator.MoveNextAsync();
            
            subject.OnNext(events[1]);
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            next.Result.Should().BeTrue();
            enumerator.Current.Should().Be(events[1]);
        }
        [Fact]
        public void ModifySameValue()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1" ).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd),
                new TestResource("1").ToResourceEvent(EventTypeFlags.Modify),
            };
            subject.OnNext(events[0]);
            var enumerator = sut.GetAsyncEnumerator();
            enumerator.MoveNextAsync();
            
            subject.OnNext(events[1]);
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeFalse();
        }
        
        [Fact]
        public void Delete_BeforeSentDownstream_DownstreamNotNotified()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1" ).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd),
                new TestResource("1").ToResourceEvent(EventTypeFlags.Delete),
            };
            var enumerator = sut.GetAsyncEnumerator();
            subject.OnNext(events[0]);
            subject.OnNext(events[1]);
            enumerator.MoveNextAsync().IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(ResourceEvent<string, TestResource>.EmptyReset);
            enumerator.MoveNextAsync().IsCompleted.Should().BeFalse();

        }

        [Fact]
        public void OnComplete_Empty_FinishedSequence()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            subject.OnCompleted();
            var enumerator = sut.GetAsyncEnumerator();
            
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            next.Result.Should().BeFalse();
        }
        [Fact]
        public async Task OnComplete_PendingItems()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var events = new[]
            {
                new TestResource("1", 1).ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("2", 1).ToResourceEvent(EventTypeFlags.ResetEnd),
            };
            var sut = subject.ToInformable();
            subject.OnNext(events[0]);
            subject.OnNext(events[1]);
            subject.OnCompleted();
            
            var result = await sut.ToEventList();
            result.Should().BeEquivalentTo(events);
        }
        [Fact]
        public async Task MoveNext_WhenDisposed_ReturnFalse()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            
            var sut = subject.ToInformable();
            var enumerator = sut.GetAsyncEnumerator();
            await enumerator.DisposeAsync();
            subject.OnNext(ResourceEvent<string, TestResource>.EmptyReset);
            var next = await enumerator.MoveNextAsync();
            next.Should().BeFalse();
        }

        
        [Fact]
        public void InterruptedReset1()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1").ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("2").ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("3").ToResourceEvent(EventTypeFlags.ResetEnd),
            };
            subject.OnNext(events[0]);
            var enumerator = sut.GetAsyncEnumerator();
            enumerator.MoveNextAsync();
            subject.OnNext(events[1]);
            subject.OnNext(events[2]);
            var next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(events[1]);
            next = enumerator.MoveNextAsync();
            next.IsCompleted.Should().BeTrue();
            enumerator.Current.Should().Be(events[2]);
        }
        
        [Fact]
        public void InterruptedReset2()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1").ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("2").ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("3").ToResourceEvent(EventTypeFlags.ResetEnd),
            };
            var enumerator = sut.GetAsyncEnumerator();

            for (int i = 0; i < 3; i++)
            {
                subject.OnNext(events[i]);
                enumerator.MoveNextAsync();
                enumerator.Current.Should().Be(events[i]);
            }
        }
        [Fact]
        public void InterruptedReset3()
        {
            var subject = new ReplaySubject<ResourceEvent<string, TestResource>>();
            var sut = subject.ToInformable();
            var events = new[]
            {
                new TestResource("1").ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("2").ToResourceEvent(EventTypeFlags.ResetStart),
                new TestResource("3").ToResourceEvent(EventTypeFlags.ResetEnd),
            };
            
            for (int i = 0; i < 3; i++)
            {
                subject.OnNext(events[i]);
            }
            
            var enumerator = sut.GetAsyncEnumerator();
            subject.OnCompleted();
            
            sut.Take(2).ToEventList().Result.Should().BeEquivalentTo(new[] { events[1],events[2] });
        }
    }
}