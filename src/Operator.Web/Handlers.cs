using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;

namespace Steeltoe.Informers.Operator.Web
{
    /// <summary>What Kubernetes tells an admission webhook about a request.</summary>
    public sealed class AdmissionContext
    {
        /// <summary><c>CREATE</c>, <c>UPDATE</c> or <c>DELETE</c>.</summary>
        public string Operation { get; init; }

        public string Namespace { get; init; }
        public string Name { get; init; }

        /// <summary>The subresource written, e.g. <c>status</c>, or null.</summary>
        public string SubResource { get; init; }

        public UserInfo UserInfo { get; init; }

        /// <summary>The request won't be persisted: side effects must be skipped.</summary>
        public bool DryRun { get; init; }
    }

    /// <summary>The verdict of a validating webhook.</summary>
    public sealed class AdmissionResult
    {
        private readonly List<string> _warnings = new List<string>();

        private AdmissionResult(bool allowed, string message, int code)
        {
            Allowed = allowed;
            Message = message;
            StatusCode = code;
        }

        public bool Allowed { get; }
        public string Message { get; }
        public int StatusCode { get; }

        /// <summary>Warnings shown to the client (e.g. <c>kubectl</c>), allowed or not.</summary>
        public IReadOnlyList<string> Warnings => _warnings;

        public static AdmissionResult Allow() => new AdmissionResult(true, null, 200);

        /// <summary>Rejects the request with <paramref name="message" /> (HTTP <paramref name="statusCode" />, default 403 Forbidden).</summary>
        public static AdmissionResult Deny(string message, int statusCode = 403) => new AdmissionResult(false, message, statusCode);

        public AdmissionResult WithWarning(string warning)
        {
            _warnings.Add(warning ?? throw new ArgumentNullException(nameof(warning)));
            return this;
        }
    }

    /// <summary>A webhook class registered with <c>AddValidator</c>, <c>AddMutator</c> or <c>AddConverter</c>.</summary>
    public interface IWebhook
    {
        internal WebhookDescriptor Describe(Type implementationType);
    }

    /// <summary>
    ///     Validates objects of <typeparamref name="T" /> before they're persisted. Override the operations to check; the others allow. Served at
    ///     <c>/validate/{group}/{version}/{plural}</c>.
    /// </summary>
    public interface IValidator<T> : IWebhook where T : class, IKubernetesObject<V1ObjectMeta>
    {
        /// <summary>The operations Kubernetes sends to this webhook (default <c>CREATE</c> and <c>UPDATE</c>; add <c>DELETE</c> to validate deletes).</summary>
        IReadOnlyList<string> Operations => new[] { "CREATE", "UPDATE" };

        ValueTask<AdmissionResult> CreateAsync(T obj, AdmissionContext context, CancellationToken cancellationToken) => new(AdmissionResult.Allow());

        ValueTask<AdmissionResult> UpdateAsync(T oldObj, T obj, AdmissionContext context, CancellationToken cancellationToken) => new(AdmissionResult.Allow());

        ValueTask<AdmissionResult> DeleteAsync(T obj, AdmissionContext context, CancellationToken cancellationToken) => new(AdmissionResult.Allow());

        WebhookDescriptor IWebhook.Describe(Type implementationType) => WebhookDescriptor.Validating<T>(implementationType, Operations);
    }

    /// <summary>
    ///     Defaults or otherwise changes objects of <typeparamref name="T" /> before they're validated and persisted: return the changed object,
    ///     and the JSON patch is computed for you. Served at <c>/mutate/{group}/{version}/{plural}</c>.
    /// </summary>
    public interface IMutator<T> : IWebhook where T : class, IKubernetesObject<V1ObjectMeta>
    {
        /// <summary>The operations Kubernetes sends to this webhook (default <c>CREATE</c> and <c>UPDATE</c>).</summary>
        IReadOnlyList<string> Operations => new[] { "CREATE", "UPDATE" };

        /// <summary>Returns the object to persist (<paramref name="obj" /> is a private copy: change it freely).</summary>
        ValueTask<T> CreateAsync(T obj, AdmissionContext context, CancellationToken cancellationToken) => new(obj);

        /// <summary>Returns the object to persist (<paramref name="obj" /> is a private copy: change it freely).</summary>
        ValueTask<T> UpdateAsync(T oldObj, T obj, AdmissionContext context, CancellationToken cancellationToken) => new(obj);

        WebhookDescriptor IWebhook.Describe(Type implementationType) => WebhookDescriptor.Mutating<T>(implementationType, Operations);
    }

    /// <summary>
    ///     Hub-and-spoke conversion between versions of a custom resource: <typeparamref name="THub" /> is the storage version, and each other
    ///     version converts to and from it. Served at <c>/convert/{group}/{plural}</c>, one endpoint per resource for all its converters.
    /// </summary>
    public interface IConverter<TSpoke, THub> : IWebhook
        where TSpoke : class, IKubernetesObject<V1ObjectMeta>
        where THub : class, IKubernetesObject<V1ObjectMeta>
    {
        THub ToHub(TSpoke spoke);

        TSpoke FromHub(THub hub);

        WebhookDescriptor IWebhook.Describe(Type implementationType) => WebhookDescriptor.Converting<TSpoke, THub>(implementationType);
    }
}
