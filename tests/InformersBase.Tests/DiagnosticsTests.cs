using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Diagnostics;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class DiagnosticsTests
    {
        [Fact]
        public async Task RunAsync_ReportsReconcileMetricsAndActivities()
        {
            var name = $"metrics-{Guid.NewGuid():N}";
            var measurements = new ConcurrentQueue<(string Instrument, double Value, string Result)>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == InformerDiagnostics.Name)
                    l.EnableMeasurementEvents(instrument);
            };
            void Record(Instrument instrument, double value, ReadOnlySpan<System.Collections.Generic.KeyValuePair<string, object>> tags)
            {
                string controller = null, result = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == "controller")
                        controller = tag.Value as string;
                    if (tag.Key == "result")
                        result = tag.Value as string;
                }

                if (controller == name)
                    measurements.Enqueue((instrument.Name, value, result));
            }

            listener.SetMeasurementEventCallback<long>((i, v, t, _) => Record(i, v, t));
            listener.SetMeasurementEventCallback<double>((i, v, t, _) => Record(i, v, t));
            listener.Start();

            var activities = new ConcurrentQueue<Activity>();
            using var activityListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == InformerDiagnostics.Name,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = a =>
                {
                    if ((string)a.GetTagItem("informers.controller") == name)
                        activities.Enqueue(a);
                },
            };
            ActivitySource.AddActivityListener(activityListener);

            var failed = 0;
            await Informable.Create(new[] { 1, 2, 3 }, x => x.ToString()).RunAsync((e, ct) =>
            {
                if (e.Key == "2" && failed++ == 0)
                    throw new InvalidOperationException("once");
                return Task.CompletedTask;
            }, new RunOptions { Name = name, Backoff = Backoff.Constant(TimeSpan.FromMilliseconds(5)) }, TestContext.Current.CancellationToken);

            measurements.Where(x => x.Instrument == "informers.reconcile.count" && x.Result == "done").Sum(x => x.Value).Should().Be(3);
            measurements.Where(x => x.Instrument == "informers.reconcile.errors").Sum(x => x.Value).Should().Be(1);
            measurements.Where(x => x.Instrument == "informers.reconcile.requeues").Sum(x => x.Value).Should().Be(1);
            measurements.Count(x => x.Instrument == "informers.reconcile.duration").Should().Be(4);
            activities.Should().HaveCount(4);
        }
    }
}
