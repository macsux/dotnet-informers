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
        public static IInformable<TKey, TResource> Do<TKey, TResource>(this IInformable<TKey, TResource> source, Action<ResourceEvent<TKey,TResource>> onChange)
        {
            var result = source.Select(x =>
            {
                onChange(x);
                return x;
            }).AsInformable();
            return result;
        }
        
        public static IInformable<TKey, TResource> Do<TKey, TResource>(this IInformable<TKey, TResource> source, Action<ResourceEvent<TKey,TResource>, TResource> onChange)
        {
            var cache = new Dictionary<TKey,TResource>();
            var result = source
                .Select(x =>
                {
                    cache.TryGetValue(x.Key, out var existing);
                    onChange(x, existing);
                    if(x.EventFlags.HasFlag(EventTypeFlags.ResetStart))
                        cache.Clear();
                    if (x.EventFlags.HasFlag(EventTypeFlags.Delete))
                        cache.Remove(x.Key);
                    else if(!x.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                        cache[x.Key] = x.Value;
                    return x;
                })
                .AsInformable();
            return result;
        }
        public static IInformable<TKey, TResource> Do<TKey, TResource>(this IInformable<TKey, TResource> source, Func<ResourceEvent<TKey,TResource>,Task> onChange)
        {
            var result = source.Select(async (ResourceEvent<TKey, TResource> x, CancellationToken _) =>
            {
                await onChange(x);
                return x;
            }).AsInformable();
            return result;
        }
    }
}