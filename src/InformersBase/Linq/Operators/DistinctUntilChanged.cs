using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Drops a <c>Modify</c> when <paramref name="selector" /> returns the same value as for the object this consumer last received for
        ///     that key, e.g. <c>x =&gt; x.Metadata.Generation</c> to ignore status-only updates (including the controller's own status writes).
        ///     Adds, deletes, resets and <c>Sync</c> events always pass. It's per consumer: other consumers of the same informer are unaffected.
        /// </summary>
        public static IInformable<TKey, TResource> DistinctUntilChanged<TKey, TResource, TValue>(
            this IInformable<TKey, TResource> source,
            Func<TResource, TValue> selector,
            IEqualityComparer<TValue> comparer = null)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            if (selector == null)
                throw Error.ArgumentNull(nameof(selector));
            return Create<TKey, TResource>(cancellationToken => DistinctUntilChangedIterator(source, selector, comparer ?? EqualityComparer<TValue>.Default, cancellationToken));
        }

        private static async IAsyncEnumerator<ResourceEvent<TKey, TResource>> DistinctUntilChangedIterator<TKey, TResource, TValue>(
            IInformable<TKey, TResource> source,
            Func<TResource, TValue> selector,
            IEqualityComparer<TValue> comparer,
            CancellationToken cancellationToken)
        {
            var last = new Dictionary<TKey, TValue>();
            await foreach (var e in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var flags = e.EventFlags;
                if (flags.HasFlag(EventTypeFlags.ResetStart) || flags.HasFlag(EventTypeFlags.ResetEmpty))
                    last.Clear();
                if (flags.HasFlag(EventTypeFlags.ResetEmpty))
                {
                    yield return e;
                    continue;
                }

                if (flags.HasFlag(EventTypeFlags.Delete))
                {
                    last.Remove(e.Key);
                    yield return e;
                    continue;
                }

                var value = selector(e.Value);
                if (flags.HasFlag(EventTypeFlags.Modify) && !flags.HasFlag(EventTypeFlags.Sync) && last.TryGetValue(e.Key, out var previous) && comparer.Equals(previous, value))
                    continue;
                last[e.Key] = value;
                yield return e;
            }
        }
    }
}
