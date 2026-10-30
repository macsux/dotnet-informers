using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Cache;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Mirrors the informable into <paramref name="cache" /> as it's enumerated, and passes every event on after the cache reflects it.
        ///     A reset replaces the cache content (and completes <see cref="SimpleCache{TKey,TResource}.Synchronized" />).
        /// </summary>
        /// <param name="source">The source informable</param>
        /// <param name="cache">The cache to keep up to date</param>
        /// <param name="mapper">Optional: combines the cached value with an update into the value to store</param>
        public static IInformable<TKey, TResource> Into<TKey, TResource>(
            this IInformable<TKey, TResource> source,
            ICache<TKey, TResource> cache,
            Func<TResource, TResource, TResource> mapper = null)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            if (cache == null)
                throw Error.ArgumentNull(nameof(cache));
            return Create<TKey, TResource>(cancellationToken => IntoIterator(source, cache, mapper, cancellationToken));
        }

        private static async IAsyncEnumerator<ResourceEvent<TKey, TResource>> IntoIterator<TKey, TResource>(
            IInformable<TKey, TResource> source,
            ICache<TKey, TResource> cache,
            Func<TResource, TResource, TResource> mapper,
            CancellationToken cancellationToken)
        {
            var resets = new ResetCollector<TKey, TResource>();
            await foreach (var e in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (resets.TryCollect(e, out var reset))
                {
                    if (reset == null)
                        continue;
                    var items = new Dictionary<TKey, TResource>();
                    foreach (var item in reset)
                        items[item.Key] = item.Value;
                    cache.Reset(items);
                    cache.Version += reset.Count;
                    if (reset.Count == 0)
                    {
                        yield return ResourceEvent<TKey, TResource>.EmptyReset;
                        continue;
                    }

                    foreach (var item in reset)
                        yield return item;
                    continue;
                }

                var notification = e;
                if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                {
                    cache.Remove(e.Key);
                }
                else
                {
                    if (mapper != null && cache.TryGetValue(e.Key, out var existing))
                        notification = new ResourceEvent<TKey, TResource>(e.EventFlags, e.Key, mapper(existing, e.Value));
                    cache[e.Key] = notification.Value;
                }

                cache.Version++;
                yield return notification;
            }
        }
    }
}
