using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Reactive.Testing;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Steeltoe.Informers.InformersBase.Tests.Utils
{
    public static class TestExtensions
    {
        private static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

        public static ScheduledEvent<T> ScheduleFiring<T>(this ResourceEvent<string, T> obj, long fireAt)
        {
            return new ScheduledEvent<T> { Event = obj, ScheduledAt = fireAt };
        }

        public static List<ResourceEvent<string, T>> ToResourceEventList<T>(this ScheduledEvent<T>[] obj) 
            => obj.OrderBy(x => x.ScheduledAt).Select(x => x.Event).ToList();
        public static IObservable<T> TimeoutIfNotDebugging<T>(this IObservable<T> source) =>
            source.TimeoutIfNotDebugging(DefaultTimeout);

        public static IObservable<T> TimeoutIfNotDebugging<T>(this IObservable<T> source, TimeSpan timeout) =>
            Debugger.IsAttached ? source : source.Timeout(timeout);

        public static async Task TimeoutIfNotDebugging(this Task task) => await task.TimeoutIfNotDebugging(DefaultTimeout);
        public static async Task TimeoutIfNotDebugging(this Task task, TimeSpan timeout)
        {
            async Task<bool> Wrapper()
            {
                await task;
                return true;
            }
            await Wrapper().TimeoutIfNotDebugging(timeout);
        }
        
        public static async Task TimeoutIfNotDebugging(this ValueTask task) => await task.AsTask().TimeoutIfNotDebugging(DefaultTimeout);
        public static async Task TimeoutIfNotDebugging(this ValueTask task, TimeSpan timeout) => await task.AsTask().TimeoutIfNotDebugging(timeout);
        public static async Task<T> TimeoutIfNotDebugging<T>(this ValueTask<T> task) => await task.AsTask().TimeoutIfNotDebugging(DefaultTimeout);
        public static async Task<T> TimeoutIfNotDebugging<T>(this ValueTask<T> task, TimeSpan timeout) => await task.AsTask().TimeoutIfNotDebugging(timeout);

        public static async Task<T> TimeoutIfNotDebugging<T>(this Task<T> task) => await task.TimeoutIfNotDebugging(DefaultTimeout);

        public static async Task<T> TimeoutIfNotDebugging<T>(this Task<T> task, TimeSpan timeout)
        {
            if (Debugger.IsAttached)
            {
                return await task;
            }

            using var timeoutCancellationTokenSource = new CancellationTokenSource();
            var completedTask = await Task.WhenAny(task, Task.Delay(timeout, timeoutCancellationTokenSource.Token));
            if (completedTask == task)
            {
                timeoutCancellationTokenSource.Cancel();
                return await task; // Very important in order to propagate exceptions
            }

            throw new TimeoutException("The operation has timed out.");
        }

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };

        public static string ToJson(this object obj)
        {
            return JsonSerializer.Serialize(obj, JsonOptions);
        }

       

        public static IObservable<ResourceEvent<string, T>> ToTestObservable<T>(this ICollection<ScheduledEvent<T>> source, TestScheduler testScheduler = null, bool startOnSubscribe = true, ILogger logger = null, bool realTime = false, int timeMultiplier = 1)
        {
            if (testScheduler == null)
            {
                testScheduler = new TestScheduler();
            }

            if (realTime)
            {
                return Observable.Create<ResourceEvent<string, T>>(async o =>
                {
                    long lastTrigger = 0;
                    foreach (var e in source)
                    {
                        var sleepDelay = e.ScheduledAt * timeMultiplier - lastTrigger;
                        await Task.Delay(TimeSpan.FromMilliseconds(sleepDelay));
                        o.OnNext(e.Event);
                        lastTrigger = e.ScheduledAt * timeMultiplier;
                    }

                    await Task.Delay(10);
                    o.OnCompleted();
                });
            }
            var subject = new Subject<ResourceEvent<string, T>>();
            var closeAt = source.Max(x => x.ScheduledAt);
            foreach (var e in source)
            {
                testScheduler.ScheduleAbsolute(e.ScheduledAt, () => subject.OnNext(e.Event));
            }
            testScheduler.ScheduleAbsolute(closeAt, async () =>
            {
                logger?.LogTrace("Test sequence is complete");
                await Task.Delay(10);
                subject.OnCompleted();
            });
            return Observable.Create<ResourceEvent<string, T>>(o =>
            {
                var subscription = subject.Subscribe(o);
                if (startOnSubscribe && !testScheduler.IsEnabled)
                {
                    
                    testScheduler.Start();
                }

                return subscription;
            });
        }
        public static ResourceEvent<string, TResource> ToResourceEvent<TResource>(this TResource obj, EventTypeFlags typeFlags)
        where TResource : TestResource
        {
            return new ResourceEvent<string, TResource>(typeFlags, obj.Key, obj);
        }
        public static ResourceEvent<string, TestResource>[] ToBasicExpected(this IEnumerable<ScheduledEvent<TestResource>> events)
        {
            var lastKnown = new Dictionary<string, TestResource>();
            var retval = new List<ResourceEvent<string, TestResource>>();
            foreach (var e in events.Select(x => x.Event))
            {
                var item = e;
                if (e.EventFlags.HasFlag(EventTypeFlags.Modify))
                {
                    item = new ResourceEvent<string, TestResource>(e.EventFlags, e.Key, e.Value);
                }
                if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                {
                    lastKnown.Remove(e.Value.Key);
                }
                else
                {
                    if (e.Value != null)
                    {
                        lastKnown[e.Value.Key] = e.Value;
                    }
                }

                retval.Add(item);
            }

            return retval.ToArray();
        }
    }
}

