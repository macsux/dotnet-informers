using System.Threading;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>Why <c>RunAsync</c> runs a handler.</summary>
    public enum ReconcileTrigger
    {
        /// <summary>A change of the key (or the initial state).</summary>
        Change,

        /// <summary>A retry the handler asked for (requeue, an exception or a conflict).</summary>
        Retry,

        /// <summary>A periodic re-check (<c>Resync</c>).</summary>
        Resync,
    }

    /// <summary>What a <c>RunAsync</c> handler can learn about its run.</summary>
    public static class RunContext
    {
        internal static readonly AsyncLocal<ReconcileTrigger> TriggerValue = new AsyncLocal<ReconcileTrigger>();

        /// <summary>Inside a <c>RunAsync</c> handler: why it runs.</summary>
        public static ReconcileTrigger Trigger => TriggerValue.Value;
    }
}
