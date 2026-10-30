using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Cache;

namespace System.Linq
{
    public partial class Informable
    {
        public static async Task<IList<TSource>> ToList<TKey, TSource>(this IInformable<TKey, TSource> source, CancellationToken cancellationToken = default)
        {
            var result = await source.ToEventList(cancellationToken).ConfigureAwait(false);
            return result.Select(x => x.Value).ToList();
        }
    }
}