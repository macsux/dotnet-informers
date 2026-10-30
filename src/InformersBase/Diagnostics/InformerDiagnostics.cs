using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Steeltoe.Informers.InformersBase.Diagnostics
{
    /// <summary>
    ///     The names of the <see cref="Meter" /> and <see cref="ActivitySource" /> the library reports through. Subscribe to them with any
    ///     OpenTelemetry or <c>System.Diagnostics</c> listener; no exporter is included.
    /// </summary>
    public static class InformerDiagnostics
    {
        /// <summary>Name of the meter and activity source.</summary>
        public const string Name = "Steeltoe.Informers";

        internal static readonly Meter Meter = new Meter(Name);
        internal static readonly ActivitySource ActivitySource = new ActivitySource(Name);

        // producer / watch health
        internal static readonly Counter<long> EventsApplied = Meter.CreateCounter<long>("informers.events", description: "Watch events applied to a shared cache");
        internal static readonly Counter<long> Relists = Meter.CreateCounter<long>("informers.relists", description: "Complete relists applied after the initial list");
        internal static readonly Counter<long> WatchRestarts = Meter.CreateCounter<long>("informers.watch.restarts", description: "Watch connections re-established");
        internal static readonly Counter<long> ProducerErrors = Meter.CreateCounter<long>("informers.producer.errors", description: "Transient producer errors that were retried");
        internal static readonly UpDownCounter<long> ActiveProducers = Meter.CreateUpDownCounter<long>("informers.producers.active", description: "Producers (list/watch connections) running");

        // reconcile loop
        internal static readonly Counter<long> Reconciles = Meter.CreateCounter<long>("informers.reconcile.count", description: "Handler invocations by RunAsync, by result");
        internal static readonly Histogram<double> ReconcileDuration = Meter.CreateHistogram<double>("informers.reconcile.duration", unit: "s", description: "Handler duration");
        internal static readonly Counter<long> ReconcileErrors = Meter.CreateCounter<long>("informers.reconcile.errors", description: "Handler invocations that threw");
        internal static readonly Counter<long> Requeues = Meter.CreateCounter<long>("informers.reconcile.requeues", description: "Keys scheduled for retry");
        internal static readonly UpDownCounter<long> QueueDepth = Meter.CreateUpDownCounter<long>("informers.reconcile.queue.depth", description: "Keys waiting for a worker");

        internal static KeyValuePair<string, object> Tag(string name, object value) => new KeyValuePair<string, object>(name, value);
    }
}
