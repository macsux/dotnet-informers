using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;

namespace Steeltoe.Informers.InformersBase.Tests.Utils
{
    /// <summary>A hand-fed informable for operator tests: write events to it, the enumeration reads them in order.</summary>
    public sealed class TestSource<TKey, TValue> : IInformable<TKey, TValue>
    {
        private readonly Channel<ResourceEvent<TKey, TValue>> _channel = Channel.CreateUnbounded<ResourceEvent<TKey, TValue>>();

        public void Send(params ResourceEvent<TKey, TValue>[] events)
        {
            foreach (var e in events)
                _channel.Writer.TryWrite(e);
        }

        public void SendReset(params KeyValuePair<TKey, TValue>[] items) => Send(items.ToReset(x => x.Key, x => x.Value, true).ToArray());

        public void Complete() => _channel.Writer.TryComplete();

        public IAsyncEnumerator<ResourceEvent<TKey, TValue>> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            _channel.Reader.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }
}
