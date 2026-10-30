using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    /// <summary>A keyed value. Values come from a global counter in the stress tests, so a key's value only ever grows.</summary>
    public readonly record struct Item(int Key, long Value);

    internal static class CoreTestHelpers
    {
        public static readonly TimeSpan Short = TimeSpan.FromMilliseconds(100);

        public static SharedInformerOptions<int, Item> Options(IEqualityComparer<Item> changeComparer = null) =>
            new SharedInformerOptions<int, Item> { KeySelector = x => x.Key, ChangeComparer = changeComparer };

        public static SharedCache<int, Item> NewCache(IEqualityComparer<Item> changeComparer = null, FaultMode fault = FaultMode.None) =>
            new SharedCache<int, Item>(Options(changeComparer), fault);

        public static Item I(int key, long value) => new Item(key, value);

        public static void Delete(this SharedCache<int, Item> cache, int key) => cache.Delete(new Item(key, 0));

        /// <summary>Reads until nothing more arrives within a short time, for a cursor (supports cancelling the wait).</summary>
        internal static async Task<List<ResourceEvent<int, Item>>> Drain(this Cursor<int, Item> cursor)
        {
            var list = new List<ResourceEvent<int, Item>>();
            while (true)
            {
                using var cts = new CancellationTokenSource(Short);
                try
                {
                    if (!await cursor.MoveNextAsync(cts.Token))
                        return list;
                }
                catch (OperationCanceledException)
                {
                    return list;
                }

                list.Add(cursor.Current);
            }
        }

        public static string Fmt(IEnumerable<ResourceEvent<int, Item>> events) =>
            string.Join(" ", events.Select(e => $"{e.EventFlags & ~(EventTypeFlags.Reset | EventTypeFlags.Current)}:{e.Key}={e.Value.Value}"));

        public static async Task WaitUntil(Func<bool> condition, int timeoutMs = 5000)
        {
            for (var i = 0; i < timeoutMs / 10 && !condition(); i++)
                await Task.Delay(10);
            if (!condition())
                throw new TimeoutException("condition not met");
        }
    }
}
