using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase.Diagnostics;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>Which Event API a <see cref="KubernetesEventRecorder" /> writes.</summary>
    public enum EventApi
    {
        /// <summary><c>events.k8s.io/v1</c> (the current API)</summary>
        EventsV1,

        /// <summary><c>v1</c> core events (older clusters and tools)</summary>
        CoreV1,
    }

    public sealed class KubernetesEventRecorderOptions
    {
        public EventApi Api { get; set; } = EventApi.EventsV1;

        /// <summary>The controller emitting the events, e.g. <c>example.com/appset-controller</c>.</summary>
        public string ReportingController { get; set; } = "steeltoe.io/informers";

        /// <summary>The emitting instance (default: the <c>POD_NAME</c> environment variable, or the machine name).</summary>
        public string ReportingInstance { get; set; } = Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName;

        /// <summary>Repeats of the same event (object, type, reason, action, message) within this window update one Event's count.</summary>
        public TimeSpan AggregationWindow { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>Events waiting to be written; when full, new events are dropped (and counted).</summary>
        public int QueueCapacity { get; set; } = 1000;

        /// <summary>Events per involved object that can be written at once; refilled by one every <see cref="RefillInterval" />.</summary>
        public int BurstPerObject { get; set; } = 25;

        public TimeSpan RefillInterval { get; set; } = TimeSpan.FromMinutes(5);
    }

    /// <summary>
    ///     Writes Kubernetes Events through a bounded background queue: recording never blocks or fails the caller, and API errors are logged,
    ///     counted and dropped. Repeats within <see cref="KubernetesEventRecorderOptions.AggregationWindow" /> update one Event's series count
    ///     instead of creating new objects, and each involved object is rate limited (like client-go's recorder).
    /// </summary>
    public sealed class KubernetesEventRecorder : IEventRecorder, IAsyncDisposable
    {
        private readonly IKubernetesGenericClient _client;
        private readonly KubernetesEventRecorderOptions _options;
        private readonly ILogger _logger;
        private readonly Func<DateTime> _clock;
        private readonly Channel<Request> _queue;
        private readonly Dictionary<string, Series> _series = new Dictionary<string, Series>(StringComparer.Ordinal);
        private readonly Dictionary<string, Bucket> _buckets = new Dictionary<string, Bucket>(StringComparer.Ordinal);
        private readonly Task _worker;
        private int _pending;
        private long _lastNameTicks;
        private TaskCompletionSource<bool> _idle = Completed();

        private sealed record Request(V1ObjectReference Involved, V1ObjectReference Related, string Type, string Reason, string Message, string Action, DateTime At);

        private sealed class Series
        {
            public string Name;
            public string Namespace;
            public int Count;
            public DateTime First;
        }

        private sealed class Bucket
        {
            public double Tokens;
            public DateTime Updated;
        }

        public KubernetesEventRecorder(IKubernetesGenericClient client, KubernetesEventRecorderOptions options = null, ILogger logger = null)
            : this(client, options, logger, () => DateTime.UtcNow)
        {
        }

        internal KubernetesEventRecorder(IKubernetesGenericClient client, KubernetesEventRecorderOptions options, ILogger logger, Func<DateTime> clock)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _options = options ?? new KubernetesEventRecorderOptions();
            _logger = logger ?? NullLogger.Instance;
            _clock = clock;
            _queue = Channel.CreateBounded<Request>(new BoundedChannelOptions(_options.QueueCapacity) { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true });
            _worker = Task.Run(RunAsync);
        }

        /// <inheritdoc />
        public void Record(IKubernetesObject<V1ObjectMeta> involved, string type, string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null)
        {
            if (involved?.Metadata?.Name == null || string.IsNullOrEmpty(reason))
                return;
            lock (_series)
            {
                if (_pending++ == 0)
                    _idle = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            var request = new Request(Reference(involved), related == null ? null : Reference(related), type ?? EventTypes.Normal, reason, message ?? string.Empty, action, _clock());
            if (!_queue.Writer.TryWrite(request))
            {
                Done();
                InformerDiagnostics.KubernetesEvents.Add(1, InformerDiagnostics.Tag("result", "dropped"));
                _logger.LogDebug("Event queue full; dropped {Reason} for {Object}", reason, involved.ObjectKey());
            }
        }

        /// <summary>Completes when every event recorded so far has been written (or dropped).</summary>
        public Task WhenIdleAsync()
        {
            lock (_series)
                return _idle.Task;
        }

        public async ValueTask DisposeAsync()
        {
            _queue.Writer.TryComplete();
            await _worker.ConfigureAwait(false);
        }

        private async Task RunAsync()
        {
            await foreach (var request in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    var result = await WriteAsync(request).ConfigureAwait(false);
                    InformerDiagnostics.KubernetesEvents.Add(1, InformerDiagnostics.Tag("result", result));
                }
                catch (Exception ex)
                {
                    InformerDiagnostics.KubernetesEvents.Add(1, InformerDiagnostics.Tag("result", "failed"));
                    _logger.LogWarning(ex, "Writing event {Reason} for {Kind} {Name} failed; dropped", request.Reason, request.Involved.Kind, request.Involved.Name);
                }
                finally
                {
                    Done();
                }
            }
        }

        private void Done()
        {
            lock (_series)
            {
                if (--_pending == 0)
                    _idle.TrySetResult(true);
            }
        }

        private async Task<string> WriteAsync(Request request)
        {
            var objectKey = $"{request.Involved.Kind}|{request.Involved.NamespaceProperty}|{request.Involved.Name}|{request.Involved.Uid}";
            if (!TakeToken(objectKey, request.At))
                return "rate_limited";
            var seriesKey = $"{objectKey}|{request.Type}|{request.Reason}|{request.Action}|{request.Message}";
            Purge(request.At);
            if (_series.TryGetValue(seriesKey, out var series))
            {
                series.Count++;
                try
                {
                    await UpdateAsync(series, request).ConfigureAwait(false);
                    return "updated";
                }
                catch (Exception ex) when (ex.IsNotFound())
                {
                    // the Event expired (the API server keeps them about an hour): start a new one
                    _series.Remove(seriesKey);
                }
            }

            var created = await CreateAsync(request).ConfigureAwait(false);
            _series[seriesKey] = new Series { Name = created.Name, Namespace = created.Namespace, Count = 1, First = request.At };
            return "created";
        }

        private bool TakeToken(string objectKey, DateTime now)
        {
            if (!_buckets.TryGetValue(objectKey, out var bucket))
                _buckets[objectKey] = bucket = new Bucket { Tokens = _options.BurstPerObject, Updated = now };
            var refill = _options.RefillInterval > TimeSpan.Zero ? (now - bucket.Updated) / _options.RefillInterval : double.PositiveInfinity;
            bucket.Tokens = Math.Min(_options.BurstPerObject, bucket.Tokens + refill);
            bucket.Updated = now;
            if (bucket.Tokens < 1)
                return false;
            bucket.Tokens--;
            return true;
        }

        private void Purge(DateTime now)
        {
            foreach (var key in _series.Where(x => now - x.Value.First > _options.AggregationWindow).Select(x => x.Key).ToList())
                _series.Remove(key);
            foreach (var key in _buckets.Where(x => x.Value.Tokens >= _options.BurstPerObject - 1 && now - x.Value.Updated > _options.RefillInterval * _options.BurstPerObject).Select(x => x.Key).ToList())
                _buckets.Remove(key);
        }

        private async Task<(string Name, string Namespace)> CreateAsync(Request request)
        {
            var ns = string.IsNullOrEmpty(request.Involved.NamespaceProperty) ? "default" : request.Involved.NamespaceProperty;
            var name = EventName(request.Involved.Name, request.At);
            if (_options.Api == EventApi.CoreV1)
            {
                await _client.CreateAsync(new Corev1Event
                {
                    ApiVersion = "v1",
                    Kind = "Event",
                    Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = ns },
                    InvolvedObject = request.Involved,
                    Related = request.Related,
                    Type = request.Type,
                    Reason = request.Reason,
                    Message = request.Message,
                    Action = request.Action,
                    Count = 1,
                    FirstTimestamp = request.At,
                    LastTimestamp = request.At,
                    EventTime = request.At,
                    Source = new V1EventSource { Component = _options.ReportingController, Host = _options.ReportingInstance },
                    ReportingComponent = _options.ReportingController,
                    ReportingInstance = _options.ReportingInstance,
                }).ConfigureAwait(false);
            }
            else
            {
                await _client.CreateAsync(new Eventsv1Event
                {
                    ApiVersion = "events.k8s.io/v1",
                    Kind = "Event",
                    Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = ns },
                    Regarding = request.Involved,
                    Related = request.Related,
                    Type = request.Type,
                    Reason = request.Reason,
                    Note = Truncate(request.Message, 1024),
                    Action = request.Action ?? request.Reason,
                    EventTime = request.At,
                    ReportingController = _options.ReportingController,
                    ReportingInstance = _options.ReportingInstance,
                }).ConfigureAwait(false);
            }

            return (name, ns);
        }

        private Task UpdateAsync(Series series, Request request)
        {
            var at = request.At.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", System.Globalization.CultureInfo.InvariantCulture);
            if (_options.Api == EventApi.CoreV1)
            {
                return _client.PatchAsync(
                    new Corev1Event { ApiVersion = "v1", Kind = "Event", Metadata = new V1ObjectMeta { Name = series.Name, NamespaceProperty = series.Namespace } },
                    new V1Patch($"{{\"count\":{series.Count},\"lastTimestamp\":\"{at[..19]}Z\"}}", V1Patch.PatchType.MergePatch));
            }

            return _client.PatchAsync(
                new Eventsv1Event { ApiVersion = "events.k8s.io/v1", Kind = "Event", Metadata = new V1ObjectMeta { Name = series.Name, NamespaceProperty = series.Namespace } },
                new V1Patch($"{{\"series\":{{\"count\":{series.Count},\"lastObservedTime\":\"{at}\"}}}}", V1Patch.PatchType.MergePatch));
        }

        private static V1ObjectReference Reference(IKubernetesObject<V1ObjectMeta> obj)
        {
            var (apiVersion, kind) = obj.TypeMeta();
            return new V1ObjectReference
            {
                ApiVersion = apiVersion,
                Kind = kind,
                Name = obj.Metadata.Name,
                NamespaceProperty = obj.Metadata.NamespaceProperty,
                Uid = obj.Metadata.Uid,
                ResourceVersion = obj.Metadata.ResourceVersion,
            };
        }

        private string EventName(string objectName, DateTime at)
        {
            // like client-go (name.<unix nanos in hex>), kept unique when two events share a tick
            _lastNameTicks = Math.Max(at.ToUniversalTime().Ticks, _lastNameTicks + 1);
            var suffix = "." + ((_lastNameTicks - DateTime.UnixEpoch.Ticks) * 100).ToString("x");
            return Truncate(objectName, 253 - suffix.Length) + suffix;
        }

        private static string Truncate(string value, int length) => value.Length <= length ? value : value[..length];

        private static TaskCompletionSource<bool> Completed()
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs.TrySetResult(true);
            return tcs;
        }
    }
}
