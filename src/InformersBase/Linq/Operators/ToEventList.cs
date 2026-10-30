using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public partial class Informable
    {
        /// <summary>Reads the informable's first reset: the complete state at the time the consumer attached.</summary>
        internal static async Task<IList<ResourceEvent<TKey, TSource>>> ToEventList<TKey, TSource>(this IInformable<TKey, TSource> source, CancellationToken cancellationToken = default)
        {
            var resets = new ResetCollector<TKey, TSource>();
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (resets.TryCollect(item, out var reset) && reset != null)
                    return reset;
            }

            return new List<ResourceEvent<TKey, TSource>>();
        }
    }
}
