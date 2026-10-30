using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesFilterTranslatorTests
    {
        private static readonly GroupVersionResource Pods = GroupVersionResource.For<V1Pod>();

        private static (ServerFilter Filter, bool HasRemainder) Split(Expression<Func<V1Pod, bool>> predicate)
        {
            var (filter, remainder) = FilterSplitter.Split(predicate, new KubernetesFilterTranslator<V1Pod>(), ServerFilter.Empty);
            return (filter, remainder != null);
        }

        private static string Terms(ServerFilter filter) => string.Join(" ", filter.Terms.Select(x => x.ToString()));

        public static IEnumerable<object[]> Translations()
        {
            var ns = "payments";
            var values = new[] { "b", "a" };
            object[] Case(Expression<Func<V1Pod, bool>> predicate, string terms, bool remainder = false) => new object[] { predicate, terms, remainder };
            yield return Case(p => p.Metadata.NamespaceProperty == "payments", "namespace:payments");
            yield return Case(p => p.Namespace() == ns, "namespace:payments");
            yield return Case(p => "payments" == p.Metadata.NamespaceProperty, "namespace:payments");
            yield return Case(p => p.Metadata.NamespaceProperty != "kube-system", "field:metadata.namespace!=kube-system");
            yield return Case(p => p.Metadata.Name == "web", "field:metadata.name=web");
            yield return Case(p => p.Name() != "web", "field:metadata.name!=web");
            yield return Case(p => p.GetLabel("app") == "web", "label:app=web");
            yield return Case(p => p.GetLabel("app") != "web", "label:app!=web");
            yield return Case(p => !(p.GetLabel("app") == "web"), "label:app!=web");
            yield return Case(p => p.GetLabel("app") != null, "label:app");
            yield return Case(p => p.GetLabel("app") == null, "label:!app");
            yield return Case(p => p.Metadata.Labels["app"] == "web", "label:app=web");
            yield return Case(p => p.Metadata.Labels["app"] != "web", "", remainder: true); // would throw client-side without the label
            yield return Case(p => p.Metadata.Labels.ContainsKey("app"), "label:app");
            yield return Case(p => !p.Metadata.Labels.ContainsKey("app"), "label:!app");
            yield return Case(p => values.Contains(p.GetLabel("tier")), "label:tier in (a,b)");
            yield return Case(p => !values.Contains(p.GetLabel("tier")), "label:tier notin (a,b)");
            yield return Case(p => p.Spec.NodeName == "n1", "field:spec.nodeName=n1");
            yield return Case(p => p.Status.Phase != "Running", "field:status.phase!=Running");
            yield return Case(p => p.Spec.NodeName == null, "", remainder: true);
            yield return Case(p => p.Spec.Containers.Count > 1, "", remainder: true);
            yield return Case(p => p.GetLabel("app") == "web" || p.Metadata.NamespaceProperty == "x", "", remainder: true);
            yield return Case(p => p.Metadata.NamespaceProperty == "a" && p.Metadata.NamespaceProperty == "b", "namespace:a", remainder: true);
            yield return Case(p => p.Metadata.NamespaceProperty == "a" && p.Metadata.NamespaceProperty == "a", "namespace:a");
            yield return Case(p => p.GetLabel("Bad Key!") == "x", "", remainder: true);
            yield return Case(p => p.GetLabel("app") == "not a valid value", "", remainder: true);
            yield return Case(p => p.GetLabel("app") == "web" && p.Spec.Containers.Count > 1 && p.Spec.NodeName == "n1", "field:spec.nodeName=n1 label:app=web", remainder: true);
        }

        [Theory]
        [MemberData(nameof(Translations))]
        public void Translates(Expression<Func<V1Pod, bool>> predicate, string terms, bool remainder)
        {
            var (filter, hasRemainder) = Split(predicate);
            Terms(filter).Should().Be(terms);
            hasRemainder.Should().Be(remainder);
        }

        [Fact]
        public void EquivalentPredicates_GiveEqualFilters()
        {
            Split(p => p.GetLabel("b") == "2" && p.GetLabel("a") == "1").Filter
                .Should().Be(Split(p => p.GetLabel("a") == "1" && p.GetLabel("b") == "2").Filter);
        }

        [Fact]
        public void FieldSelectorsOnlyForTypesThatSupportThem()
        {
            var (filter, remainder) = FilterSplitter.Split<V1Deployment>(d => d.Spec.Template.Spec.NodeName == "n1", new KubernetesFilterTranslator<V1Deployment>(), ServerFilter.Empty);
            filter.IsEmpty.Should().BeTrue();
            remainder.Should().NotBeNull();
        }

        /// <summary>
        ///     Random pods and random predicates: the informer's result with pushdown (selectors evaluated by the fake API server with API server
        ///     semantics, remainder client-side) must equal evaluating the whole predicate client-side over every pod.
        /// </summary>
        [Fact]
        public async Task Pushdown_GivesIdenticalResultsToClientSideEvaluation()
        {
            var rng = new Random(42);
            string Pick(params string[] options) => options[rng.Next(options.Length)];
            var api = new FakeApiServer();
            var pods = new List<V1Pod>();
            for (var i = 0; i < 200; i++)
            {
                var labels = new Dictionary<string, string>();
                if (rng.Next(3) > 0)
                    labels["app"] = Pick("web", "db", "cache");
                if (rng.Next(2) > 0)
                    labels["tier"] = Pick("a", "b", "c");
                var pod = new V1Pod
                {
                    ApiVersion = "v1",
                    Kind = "Pod",
                    Metadata = new V1ObjectMeta { NamespaceProperty = Pick("ns1", "ns2", "ns3"), Name = $"p{i}", Labels = labels.Count > 0 ? labels : null },
                    Spec = new V1PodSpec { NodeName = rng.Next(4) == 0 ? null : Pick("n1", "n2"), Containers = Enumerable.Range(0, rng.Next(1, 4)).Select(c => new V1Container { Name = $"c{c}" }).ToList() },
                    Status = new V1PodStatus { Phase = Pick("Running", "Pending", "Failed") },
                };
                pods.Add(pod);
                api.Put(Pods, pod);
            }

            var tiers = new[] { "a", "c" };
            var predicates = new Expression<Func<V1Pod, bool>>[]
            {
                p => p.Metadata.NamespaceProperty == "ns1",
                p => p.Namespace() != "ns2" && p.GetLabel("app") == "web",
                p => p.GetLabel("app") != "web",
                p => p.GetLabel("tier") == null && p.Spec.NodeName == "n1",
                p => tiers.Contains(p.GetLabel("tier")) && p.Status.Phase == "Running",
                p => !tiers.Contains(p.GetLabel("tier")) && p.Spec.Containers.Count > 1,
                p => p.Metadata.Labels != null && p.Metadata.Labels.ContainsKey("app") && p.Spec.NodeName != "n2",
                p => p.Status.Phase != "Failed" || p.GetLabel("app") == "db",
                p => p.Metadata.Name == "p7" || p.Metadata.Name == "p8",
                p => p.Metadata.NamespaceProperty == "ns3" && p.GetLabel("app") != null && p.Metadata.Labels["app"] == "cache",
            };
            var factory = new KubernetesInformerFactory(api, retryPolicy: RetryPolicy.None);
            var informer = factory.Get<V1Pod>();
            foreach (var predicate in predicates)
            {
                var expected = pods.Where(predicate.Compile()).Select(x => x.ObjectKey()).OrderBy(x => x).ToList();
                await using var e = informer.Where(predicate).GetAsyncEnumerator(TestContext.Current.CancellationToken);
                var actual = new List<string>();
                while (await e.MoveNextAsync())
                {
                    if (!e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                        actual.Add(e.Current.Key);
                    if (e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
                        break;
                }

                actual.OrderBy(x => x).Should().Equal(expected, predicate.ToString());
            }

            api.Requests.Where(x => !x.Request.Watch).Should().Contain(x => x.Request.LabelSelector != null || x.Request.FieldSelector != null || x.Namespace != null);
        }
    }
}
