using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Cache;

namespace System.Linq
{
    public static partial class Informable
    {
        public static IInformable<TKey, TResource> Into<TKey, TResource>(
            this IInformable<TKey, TResource> source,
            ICache<TKey, TResource> cache,
            Func<TResource, TResource, TResource> mapper = null)        
        {
            if (mapper == null)
            {
                mapper = (oldResource, newResource) => newResource;
            }

          
            async IAsyncEnumerator<ResourceEvent<TKey, TResource>> SourceEnumerator(CancellationToken token)
            {
                long msgNum = 0;
                var synchronizedSource = source.WithResets(events =>
                {
                    var reset = events
                        .Where(x => x.Key != null && x.Value != null)
                        .ToDictionary(x => x.Key, x => x.Value);

                    cache.Reset(reset);
                    cache.Version += events.Count;
                    return events;
                }, notification =>
                {
                    if (notification.EventFlags.HasFlag(EventTypeFlags.Reset))
                    {
                    }
                    else if (!notification.EventFlags.HasFlag(EventTypeFlags.Delete))
                    {
                        cache.Version++;
                        if (notification.Value != null)
                        {
                            var key = notification.Key;
                            if (cache.TryGetValue(key, out var existing))
                            {
                                notification = new ResourceEvent<TKey, TResource>(notification.EventFlags, notification.Key, mapper(existing, notification.Value));
                            }
                            cache[notification.Key] = notification.Value;
                        }
                    }
                    else
                    {
                        cache.Remove(notification.Key);
                    }

                    return notification;
                    // return new CacheSynchronized<ResourceEvent<TKey, TResource>>(msgNum, cache.Version, notification);
                });
                await foreach (var item in synchronizedSource.WithCancellation(token))
                {
                    yield return item;
                }
            }
            return Create<TKey, TResource>(SourceEnumerator);
        }
    }
    
    
}