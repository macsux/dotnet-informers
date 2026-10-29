using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Cache;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class CacheTests
    {
        [Fact]
        public void GetByIndex_WithIndex_Correct()
        {
            var sut = new SimpleCache<string, TestEntity>();
            sut.AddIndex("label", x => x.Labels.Keys.ToList());
            sut.AddIndex("labelValue", x => x.Labels.Select(x => $"{x.Key}:{x.Value}").ToList());
            sut.AddIndex("namespace", x => new List<string>() {x.Namespace});
            
            var a = new TestEntity { Key = "1", Namespace = "a", Labels = {{ "app","hello"}, {"blah", "test" }}};
            var b = new TestEntity { Key = "2", Namespace = "c", Labels = {{ "app","b"}, {"blah", "test" }}};
            sut[a.Key] = a;
            sut[b.Key] = b;
            sut.ByIndex("namespace", "a").Should().BeEquivalentTo(new[] { a });
            sut.ByIndex("label", "app").Should().BeEquivalentTo(new[] { a,b });
            sut.ByIndex("labelValue", "app:b").Should().BeEquivalentTo(new[] { b });
        }

        private class TestEntity
        {
          
            public string Key { get; set; }
            public string Namespace { get; set; }
            public Dictionary<string, string> Labels { get; set; } = new Dictionary<string, string>();
        }
    }
}