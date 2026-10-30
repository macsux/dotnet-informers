using System.Collections.Generic;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    /// <summary>Collects a reset block (<c>ResetStart</c> … <c>ResetEnd</c>, or <c>ResetEmpty</c>) from a stream of events.</summary>
    internal sealed class ResetCollector<TKey, TResource>
    {
        private List<ResourceEvent<TKey, TResource>> _items;

        /// <summary>
        ///     Feeds one event. Returns <see langword="true" /> when the event belongs to a reset; <paramref name="complete" /> is set to the
        ///     whole block (without the <c>ResetEmpty</c> marker) once its last event arrived.
        /// </summary>
        public bool TryCollect(ResourceEvent<TKey, TResource> e, out List<ResourceEvent<TKey, TResource>> complete)
        {
            complete = null;
            var flags = e.EventFlags;
            if (!flags.HasFlag(EventTypeFlags.Reset))
                return false;
            if (flags.HasFlag(EventTypeFlags.ResetStart) || _items == null)
                _items = new List<ResourceEvent<TKey, TResource>>();
            if (!flags.HasFlag(EventTypeFlags.ResetEmpty))
                _items.Add(e);
            if (flags.HasFlag(EventTypeFlags.ResetEnd))
            {
                complete = _items;
                _items = null;
            }

            return true;
        }

        /// <summary>Turns a list of key/value pairs into a reset block, or a single <c>ResetEmpty</c> if empty.</summary>
        public static IEnumerable<ResourceEvent<TKey, TResource>> ToResetBlock(IReadOnlyList<KeyValuePair<TKey, TResource>> items)
        {
            if (items.Count == 0)
            {
                yield return ResourceEvent<TKey, TResource>.EmptyReset;
                yield break;
            }

            for (var i = 0; i < items.Count; i++)
            {
                var flags = EventTypeFlags.Reset;
                if (i == 0)
                    flags |= EventTypeFlags.ResetStart;
                if (i == items.Count - 1)
                    flags |= EventTypeFlags.ResetEnd;
                yield return new ResourceEvent<TKey, TResource>(flags, items[i].Key, items[i].Value);
            }
        }
    }
}
