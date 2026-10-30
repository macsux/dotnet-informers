using System.Collections.Generic;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>FIFO queue that holds each item at most once.</summary>
    internal sealed class HashQueue<T>
    {
        private readonly Queue<T> _queue = new Queue<T>();
        private readonly HashSet<T> _set;

        public HashQueue(IEqualityComparer<T> comparer = null) => _set = new HashSet<T>(comparer);

        public int Count => _queue.Count;

        public bool Contains(T item) => _set.Contains(item);

        public bool Enqueue(T item)
        {
            if (!_set.Add(item))
                return false;
            _queue.Enqueue(item);
            return true;
        }

        public T Dequeue()
        {
            var item = _queue.Dequeue();
            _set.Remove(item);
            return item;
        }

        public void Clear()
        {
            _queue.Clear();
            _set.Clear();
            _queue.TrimExcess();
            _set.TrimExcess();
        }
    }
}
