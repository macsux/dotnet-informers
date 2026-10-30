using System.Collections.Concurrent;
using System.Threading;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>Counters the integration tests assert on.</summary>
    public sealed class ControllerStats
    {
        private readonly ConcurrentDictionary<string, long> _counters = new ConcurrentDictionary<string, long>();

        public long this[string name] => _counters.TryGetValue(name, out var value) ? value : 0;

        public void Increment(string name, long by = 1) => _counters.AddOrUpdate(name, by, (_, v) => v + by);

        public override string ToString() => string.Join(", ", _counters);
    }
}
