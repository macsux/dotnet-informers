using System;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>The delay before retry number <c>attempt</c> (1-based) of a key.</summary>
    public abstract class Backoff
    {
        public abstract TimeSpan GetDelay(int attempt);

        /// <summary><paramref name="initial" /> doubled (or multiplied by <paramref name="factor" />) per attempt, capped at <paramref name="max" />.</summary>
        public static Backoff Exponential(TimeSpan initial, TimeSpan max, double factor = 2) => new ExponentialBackoff(initial, max, factor);

        /// <summary>The same delay for every attempt.</summary>
        public static Backoff Constant(TimeSpan delay) => new ExponentialBackoff(delay, delay, 1);

        /// <summary>The default: 5 ms doubling up to 15 minutes, like client-go's workqueue.</summary>
        public static Backoff Default { get; } = Exponential(TimeSpan.FromMilliseconds(5), TimeSpan.FromMinutes(15));

        private sealed class ExponentialBackoff : Backoff
        {
            private readonly TimeSpan _initial;
            private readonly TimeSpan _max;
            private readonly double _factor;

            public ExponentialBackoff(TimeSpan initial, TimeSpan max, double factor)
            {
                if (initial < TimeSpan.Zero || max < initial || factor < 1)
                    throw new ArgumentOutOfRangeException(nameof(initial));
                _initial = initial;
                _max = max;
                _factor = factor;
            }

            public override TimeSpan GetDelay(int attempt)
            {
                var ticks = _initial.Ticks * Math.Pow(_factor, Math.Max(0, attempt - 1));
                return ticks >= _max.Ticks || double.IsInfinity(ticks) ? _max : TimeSpan.FromTicks((long)ticks);
            }
        }
    }
}
