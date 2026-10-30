using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>An overall limit on how fast <c>RunAsync</c> starts handlers, across all keys.</summary>
    public abstract class RateLimit
    {
        /// <summary>Waits until the next handler may start.</summary>
        public abstract ValueTask WaitAsync(CancellationToken cancellationToken);

        /// <summary>A token bucket: <paramref name="perSecond" /> handler starts per second on average, bursts of up to <paramref name="burst" />.</summary>
        public static RateLimit TokenBucket(double perSecond, int burst) => new TokenBucketRateLimit(perSecond, burst);

        private sealed class TokenBucketRateLimit : RateLimit
        {
            private readonly object _lock = new object();
            private readonly double _perSecond;
            private readonly int _burst;
            private readonly Stopwatch _clock = Stopwatch.StartNew();
            private double _tokens;
            private double _lastRefill;

            public TokenBucketRateLimit(double perSecond, int burst)
            {
                if (perSecond <= 0)
                    throw new ArgumentOutOfRangeException(nameof(perSecond));
                if (burst < 1)
                    throw new ArgumentOutOfRangeException(nameof(burst));
                _perSecond = perSecond;
                _burst = burst;
                _tokens = burst;
            }

            public override async ValueTask WaitAsync(CancellationToken cancellationToken)
            {
                TimeSpan wait;
                lock (_lock)
                {
                    var now = _clock.Elapsed.TotalSeconds;
                    _tokens = Math.Min(_burst, _tokens + (now - _lastRefill) * _perSecond);
                    _lastRefill = now;
                    _tokens -= 1; // reserve a token; a negative balance is the queue of waiters
                    if (_tokens >= 0)
                        return;
                    wait = TimeSpan.FromSeconds(-_tokens / _perSecond);
                }

                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
