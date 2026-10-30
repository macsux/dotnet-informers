using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Filters an informable client-side. The result keeps the informable contract: an object that stops matching arrives as a
        ///     <c>Delete</c>, one that starts matching as an <c>Add</c>, and a reset only holds the matching objects.
        /// </summary>
        public static IInformable<TKey, TResource> Where<TKey, TResource>(this IInformable<TKey, TResource> source, Func<TResource, bool> predicate)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            if (predicate == null)
                throw Error.ArgumentNull(nameof(predicate));
            return Create<TKey, TResource>(cancellationToken => WhereIterator(source, predicate, cancellationToken));
        }

        private static async IAsyncEnumerator<ResourceEvent<TKey, TResource>> WhereIterator<TKey, TResource>(
            IInformable<TKey, TResource> source,
            Func<TResource, bool> predicate,
            CancellationToken cancellationToken)
        {
            var downstream = new HashSet<TKey>(); // keys the consumer currently has
            var resets = new ResetCollector<TKey, TResource>();
            await foreach (var e in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (resets.TryCollect(e, out var reset))
                {
                    if (reset == null)
                        continue;
                    downstream.Clear();
                    var matching = new List<KeyValuePair<TKey, TResource>>();
                    foreach (var item in reset)
                    {
                        if (predicate(item.Value))
                            matching.Add(new KeyValuePair<TKey, TResource>(item.Key, item.Value));
                    }

                    foreach (var item in ResetCollector<TKey, TResource>.ToResetBlock(matching))
                    {
                        if (!item.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                            downstream.Add(item.Key);
                        yield return item;
                    }

                    continue;
                }

                var computed = e.EventFlags & EventTypeFlags.Computed;
                if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                {
                    if (downstream.Remove(e.Key))
                        yield return e;
                    continue;
                }

                if (predicate(e.Value))
                {
                    if (downstream.Add(e.Key))
                        yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.Add | computed, e.Key, e.Value);
                    else if (e.EventFlags.HasFlag(EventTypeFlags.Add))
                        yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.Modify | computed, e.Key, e.Value);
                    else
                        yield return e;
                }
                else if (downstream.Remove(e.Key))
                {
                    // the object no longer matches: for this consumer it's gone
                    yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.Delete | computed, e.Key, e.Value);
                }
            }
        }
    }
}
