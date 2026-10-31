using System;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Records Kubernetes Events: the human-readable trail <c>kubectl describe</c> shows ("Created pod demo-x7k2p"). Recording never blocks or
    ///     fails the caller.
    /// </summary>
    public interface IEventRecorder
    {
        /// <param name="involved">The object the event is about</param>
        /// <param name="type"><c>Normal</c> or <c>Warning</c></param>
        /// <param name="reason">CamelCase reason, e.g. <c>SuccessfulCreate</c></param>
        /// <param name="message">Human-readable details</param>
        /// <param name="action">What was done or failed, e.g. <c>Create</c> (optional)</param>
        /// <param name="related">A second object involved, e.g. the created pod (optional)</param>
        void Record(IKubernetesObject<V1ObjectMeta> involved, string type, string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null);
    }

    /// <summary>An <see cref="IEventRecorder" /> bound to one object.</summary>
    public interface IObjectEventRecorder
    {
        /// <summary>Records a <c>Normal</c> event about the bound object.</summary>
        void Normal(string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null);

        /// <summary>Records a <c>Warning</c> event about the bound object.</summary>
        void Warning(string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null);
    }

    public static class EventRecorderExtensions
    {
        /// <summary>Records a <c>Normal</c> event about <paramref name="involved" />.</summary>
        public static void Normal(this IEventRecorder recorder, IKubernetesObject<V1ObjectMeta> involved, string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null) =>
            recorder.Record(involved, EventTypes.Normal, reason, message, action, related);

        /// <summary>Records a <c>Warning</c> event about <paramref name="involved" />.</summary>
        public static void Warning(this IEventRecorder recorder, IKubernetesObject<V1ObjectMeta> involved, string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null) =>
            recorder.Record(involved, EventTypes.Warning, reason, message, action, related);

        /// <summary>A recorder bound to <paramref name="involved" />.</summary>
        public static IObjectEventRecorder For(this IEventRecorder recorder, IKubernetesObject<V1ObjectMeta> involved) => new BoundRecorder(recorder, involved);

        private sealed class BoundRecorder : IObjectEventRecorder
        {
            private readonly IEventRecorder _recorder;
            private readonly IKubernetesObject<V1ObjectMeta> _involved;

            public BoundRecorder(IEventRecorder recorder, IKubernetesObject<V1ObjectMeta> involved)
            {
                _recorder = recorder ?? throw new ArgumentNullException(nameof(recorder));
                _involved = involved ?? throw new ArgumentNullException(nameof(involved));
            }

            public void Normal(string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null) =>
                _recorder.Record(_involved, EventTypes.Normal, reason, message, action, related);

            public void Warning(string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null) =>
                _recorder.Record(_involved, EventTypes.Warning, reason, message, action, related);
        }
    }

    /// <summary>The event types Kubernetes defines.</summary>
    public static class EventTypes
    {
        public const string Normal = "Normal";
        public const string Warning = "Warning";
    }

    /// <summary>Discards every event (for control planes without the <c>Event</c> resource, or tests).</summary>
    public sealed class NullEventRecorder : IEventRecorder
    {
        public static NullEventRecorder Instance { get; } = new NullEventRecorder();

        public void Record(IKubernetesObject<V1ObjectMeta> involved, string type, string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null)
        {
        }
    }

    /// <summary>Writes events to a logger instead of the API server.</summary>
    public sealed class LoggingEventRecorder : IEventRecorder
    {
        private readonly ILogger _logger;

        public LoggingEventRecorder(ILogger logger) => _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        public void Record(IKubernetesObject<V1ObjectMeta> involved, string type, string reason, string message, string action = null, IKubernetesObject<V1ObjectMeta> related = null) =>
            _logger.Log(type == EventTypes.Warning ? LogLevel.Warning : LogLevel.Information, "{Kind} {Object}: {Reason} {Message}", involved?.TypeMeta().Kind, involved?.ObjectKey(), reason, message);
    }
}
