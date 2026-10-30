using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     For sources that send a complete reset after reconnecting: the first reset passes through, and every later reset is turned into
        ///     computed <c>Delete</c>/<c>Add</c>/<c>Modify</c> events for the differences from what the consumer already has, flagged
        ///     <see cref="EventTypeFlags.Computed" />. With a <paramref name="comparer" />, unchanged objects send nothing; without one, every
        ///     surviving object arrives as a computed <c>Modify</c>.
        /// </summary>
        public static IInformable<TKey, TResource> ComputeMissedEventsBetweenResets<TKey, TResource>(this IInformable<TKey, TResource> source, IEqualityComparer<TResource> comparer = null)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            return Create<TKey, TResource>(cancellationToken => ComputeMissedIterator(source, comparer, cancellationToken));
        }

        private static async IAsyncEnumerator<ResourceEvent<TKey, TResource>> ComputeMissedIterator<TKey, TResource>(
            IInformable<TKey, TResource> source,
            IEqualityComparer<TResource> comparer,
            CancellationToken cancellationToken)
        {
            Dictionary<TKey, TResource> known = null; // null until the first reset went through
            var resets = new ResetCollector<TKey, TResource>();
            await foreach (var e in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (resets.TryCollect(e, out var reset))
                {
                    if (reset == null)
                        continue;
                    if (known == null)
                    {
                        known = new Dictionary<TKey, TResource>();
                        foreach (var item in reset)
                            known[item.Key] = item.Value;
                        if (reset.Count == 0)
                            yield return ResourceEvent<TKey, TResource>.EmptyReset;
                        foreach (var item in reset)
                            yield return item;
                        continue;
                    }

                    var fresh = new Dictionary<TKey, TResource>();
                    foreach (var item in reset)
                        fresh[item.Key] = item.Value;
                    foreach (var old in known.ToList())
                    {
                        if (!fresh.ContainsKey(old.Key))
                        {
                            known.Remove(old.Key);
                            yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.Delete | EventTypeFlags.Computed, old.Key, old.Value);
                        }
                    }

                    foreach (var item in fresh)
                    {
                        if (!known.TryGetValue(item.Key, out var existing))
                        {
                            known[item.Key] = item.Value;
                            yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.Add | EventTypeFlags.Computed, item.Key, item.Value);
                        }
                        else if (comparer == null || !comparer.Equals(existing, item.Value))
                        {
                            known[item.Key] = item.Value;
                            yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.Modify | EventTypeFlags.Computed, item.Key, item.Value);
                        }
                    }

                    continue;
                }

                if (known != null)
                {
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                        known.Remove(e.Key);
                    else
                        known[e.Key] = e.Value;
                }

                yield return e;
            }
        }
    }
}
