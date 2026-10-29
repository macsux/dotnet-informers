using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Steeltoe.Informers.InformersBase.Cache;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class ResetExtractorTests
    {
        
        [Fact]
        public async Task ResetEmpty()
        {
            var sut = new Informable.ResetExtractor<int, int>();
            
            var result = sut.ApplyEvent(ResourceEvent<int, int>.EmptyReset, out var reset);
            result.Should().BeTrue();
            reset.Should().BeEquivalentTo(new[] { ResourceEvent<int, int>.EmptyReset });
            sut.IsResetting.Should().BeFalse();
        }

        [Fact]
        public async Task ResetSingleItem()
        {
            var sut = new Informable.ResetExtractor<int,int>();
            var item = ResourceEvent.Create(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, 1, 1);
            var result = sut.ApplyEvent(item, out var reset);
            result.Should().BeTrue();
            reset.Should().BeEquivalentTo(new[] { item });
            sut.IsResetting.Should().BeFalse();
        }
        [Fact]
        public async Task Extract()
        {
            var sut = new Informable.ResetExtractor<int,int>();
            var enumerator = Informable.Create(new[]{1,2}, x => x, new[]{3.ToResourceEvent(EventTypeFlags.Add)}).GetAsyncEnumerator();
            var reset = await sut.Extract(enumerator);
            reset.Should().BeEquivalentTo(new[] { 1.ToResourceEvent(EventTypeFlags.ResetStart), 2.ToResourceEvent(EventTypeFlags.ResetEnd) });
            await enumerator.MoveNextAsync().TimeoutIfNotDebugging();
            enumerator.Current.Should().Be(3.ToResourceEvent(EventTypeFlags.Add));
        }
        [Fact]
        public async Task ResetMultiple()
        {
            var sut = new Informable.ResetExtractor<int,int>();
            var items = new[]
            {
                ResourceEvent.Create(EventTypeFlags.ResetStart, 1, 1),
                ResourceEvent.Create(EventTypeFlags.ResetEnd, 2, 2)
            };
            var result = sut.ApplyEvent(items[0], out var reset);
            result.Should().BeFalse();
            reset.Should().BeNull();
            sut.IsResetting.Should().BeTrue();
            
            result = sut.ApplyEvent(items[1], out reset);
            result.Should().BeTrue();
            reset.Should().BeEquivalentTo(items);
            sut.IsResetting.Should().BeFalse();
        }
        [Fact]
        public async Task NonResetItem()
        {
            var sut = new Informable.ResetExtractor<int,int>();
            var items = new[]
            {
                ResourceEvent.Create(EventTypeFlags.Modify, 1, 1),
            };
            var result = sut.ApplyEvent(items[0], out var reset);
            result.Should().BeFalse();
            reset.Should().BeNull();
            sut.IsResetting.Should().BeFalse();
            
        }
        [Fact]
        public async Task ResetImplicit()
        {
            var sut = new Informable.ResetExtractor<int,int>();
            var items = new[]
            {
                ResourceEvent.Create(EventTypeFlags.ResetStart, 1, 1),
                ResourceEvent.Create(EventTypeFlags.Modify, 2, 2)
            };
            var result = sut.ApplyEvent(items[0], out var reset);
            result.Should().BeFalse();
            reset.Should().BeNull();
            sut.IsResetting.Should().BeTrue();
            
            result = sut.ApplyEvent(items[1], out reset);
            result.Should().BeTrue();
            reset.Should().BeEquivalentTo(new[] { items[0] });
            sut.IsResetting.Should().BeFalse();
        }
    }
}