using System;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>What a <c>RunAsync</c> handler wants done with the key it just handled.</summary>
    public readonly struct ReconcileResult : IEquatable<ReconcileResult>
    {
        private readonly int _kind; // 0 done, 1 requeue with backoff, 2 requeue after delay

        private ReconcileResult(int kind, TimeSpan delay)
        {
            _kind = kind;
            Delay = delay;
        }

        /// <summary>Success: resets the key's backoff.</summary>
        public static ReconcileResult Done => default;

        /// <summary>Not done: try again with the key's exponential backoff (what a thrown exception does too).</summary>
        public static ReconcileResult Requeue() => new ReconcileResult(1, TimeSpan.Zero);

        /// <summary>Try again after exactly <paramref name="delay" />, e.g. a dependency isn't ready yet or a planned re-check.</summary>
        public static ReconcileResult RequeueAfter(TimeSpan delay) => new ReconcileResult(2, delay < TimeSpan.Zero ? TimeSpan.Zero : delay);

        public bool IsDone => _kind == 0;

        /// <summary>The handler asked for a retry with backoff.</summary>
        public bool IsRequeue => _kind == 1;

        /// <summary>The handler asked for a retry after <see cref="Delay" />.</summary>
        public bool IsRequeueAfter => _kind == 2;

        /// <summary>The delay of <see cref="RequeueAfter" />.</summary>
        public TimeSpan Delay { get; }

        public bool Equals(ReconcileResult other) => _kind == other._kind && Delay == other.Delay;
        public override bool Equals(object obj) => obj is ReconcileResult other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(_kind, Delay);
        public override string ToString() => _kind switch { 0 => "Done", 1 => "Requeue", _ => $"RequeueAfter({Delay})" };
    }
}
