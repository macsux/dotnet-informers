using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Microsoft.Reactive.Testing;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesInformerExtensionsTests
    {
        public static IEnumerable<object[]> GetTestScenarios()
        {
            var pod1 = new V1Pod {Metadata = new V1ObjectMeta {Name = "pod1", OwnerReferences = new List<V1OwnerReference> {new V1OwnerReference {Kind = "Service", Name = "svc1"}}}};
            var pod1NoOwner = new V1Pod {Metadata = new V1ObjectMeta {Name = "pod1"}};
            var pod2 = new V1Pod {Metadata = new V1ObjectMeta {Name = "pod2", OwnerReferences = new List<V1OwnerReference> {new V1OwnerReference {Kind = "Service", Name = "svc1"}}}};
            var svc1 = new V1Service() {Metadata = new V1ObjectMeta {Name = "svc1"}};
            yield return new object[]{
                "Initial pods & service", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0)
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0)
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name))
                }
            };
            yield return new object[]{
                "Initial pods & service + add pod", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                    pod2.ToResourceEvent(EventTypeFlags.Add).ScheduleFiring(100)
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0)
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                    Tuple.Create(pod2, svc1).ToResourceEvent(EventTypeFlags.Add, Tuple.Create(pod2.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Initial pods & service + delete pod", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                    pod1.ToResourceEvent(EventTypeFlags.Delete).ScheduleFiring(100)
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0)
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.Delete, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Initial pods & service + update pod", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                    pod1.ToResourceEvent(EventTypeFlags.Modify).ScheduleFiring(100)
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0)
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.Modify, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Initial pods & service + update service", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                    svc1.ToResourceEvent(EventTypeFlags.Modify).ScheduleFiring(100),
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.Modify, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Initial pods & service + delete service", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                    svc1.ToResourceEvent(EventTypeFlags.Delete).ScheduleFiring(100)
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.Delete, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Initial pod + add service", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                },
                new [] // services stream
                {
                    ResourceEvent<string, V1Service>.EmptyReset.ScheduleFiring(0),
                    svc1.ToResourceEvent(EventTypeFlags.Add).ScheduleFiring(100)
                },
                new [] // expected
                {
                    ResourceEvent<Tuple<string,string>,Tuple<V1Pod, V1Service>>.EmptyReset,
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.Add, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Initial pods & service + Modify Pod remove owner", // description
                new [] // pods stream
                {
                    pod1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0),
                    pod1NoOwner.ToResourceEvent(EventTypeFlags.Modify).ScheduleFiring(100),
                },
                new [] // services stream
                {
                    svc1.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd).ScheduleFiring(0)
                },
                new [] // expected
                {
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                    Tuple.Create(pod1NoOwner, svc1).ToResourceEvent(EventTypeFlags.Delete, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
            yield return new object[]{
                "Empty + Add pod & service", // description
                new [] // pods stream
                {
                    ResourceEvent<string, V1Pod>.EmptyReset.ScheduleFiring(0),
                    pod1.ToResourceEvent(EventTypeFlags.Add).ScheduleFiring(100),
                },
                new [] // services stream
                {
                    ResourceEvent<string, V1Service>.EmptyReset.ScheduleFiring(0),
                    svc1.ToResourceEvent(EventTypeFlags.Add).ScheduleFiring(200)
                },
                new [] // expected
                {
                    ResourceEvent<Tuple<string,string>,Tuple<V1Pod, V1Service>>.EmptyReset,
                    Tuple.Create(pod1, svc1).ToResourceEvent(EventTypeFlags.Add, Tuple.Create(pod1.Metadata.Name, svc1.Metadata.Name)),
                }
            };
        }
        
        // [Theory]
        // [MemberData(nameof(GetTestScenarios))]
        // public async Task JoinOwner(string description, ScheduledEvent<V1Pod>[] podsScenario, ScheduledEvent<V1Service>[] servicesScenario, ResourceEvent<Tuple<string,string>, Tuple<V1Pod, V1Service>>[] expected)
        // {
        //     var scheduler = new TestScheduler();
        //     var pods = podsScenario.ToTestObservable(scheduler, false);
        //     var services = servicesScenario.ToTestObservable(scheduler, false);
        //     
        //     var result = Task.Run(async () => await pods.JoinOwner(services, Tuple.Create).TimeoutIfNotDebugging().ToList());
        //     await Task.Delay(100); // allow subscriptions to be established in statement above
        //     scheduler.Start();
        //     await result;
        //     result.Result.Should().BeEquivalentTo(expected);
        // }
    }
}