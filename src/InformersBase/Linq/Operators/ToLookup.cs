using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public partial class Informable
    {
        /// <summary>Groups the informable's current state (its first reset) by <paramref name="keySelector" />.</summary>
        public static async Task<ILookup<TLookupKey, TSource>> ToLookup<TKey, TSource, TLookupKey>(
            this IInformable<TKey, TSource> source,
            Func<TSource, TLookupKey> keySelector,
            CancellationToken cancellationToken = default)
        {
            if (keySelector == null)
                throw Error.ArgumentNull(nameof(keySelector));
            var result = await source.ToEventList(cancellationToken).ConfigureAwait(false);
            return result.Select(x => x.Value).ToLookup(keySelector);
        }
    }
}
