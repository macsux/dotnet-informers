using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase.Cache;
using Steeltoe.Informers.KubernetesBase.Cache;
using Xunit;
using static Steeltoe.Informers.InformersBase.Tests.CacheTests;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesCacheTests
    {
        [Fact]
        public void GetByIndex_WithIndex_Correct()
        {
            var sut = new KubernetesCache<V1Pod>();
            
            var a1 = new V1Pod { Metadata = new V1ObjectMeta { Name = "a1", Labels = new Dictionary<string, string> { {"app", "a"} }, NamespaceProperty = "default"}};
            var a2 = new V1Pod { Metadata = new V1ObjectMeta { Name = "a2", Labels = new Dictionary<string, string> { {"app", "a"} }, NamespaceProperty = "default"}};
            var b1 = new V1Pod { Metadata = new V1ObjectMeta { Name = "b1", Labels = new Dictionary<string, string> { {"app", "b"} }}};
            sut.Set(a1);
            sut.Set(a2);
            sut.Set(b1);
            sut.ByNamespace("default").Should().BeEquivalentTo(new[] { a1, a2 });
            sut.ByLabel("app").Should().BeEquivalentTo(new[] { a1,a2, b1 });
            sut.ByLabel("app", "a").Should().BeEquivalentTo(new[] { a1, a2 });
            sut.ByLabel("app", "b").Should().BeEquivalentTo(new[] { b1 });
        }
    }
}