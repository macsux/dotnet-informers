using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Steeltoe.Informers.Operator.Web
{
    /// <summary>How Kubernetes reaches the webhooks, for the generated webhook configurations.</summary>
    public sealed class WebhookOptions
    {
        /// <summary>The Service in front of the operator's pods.</summary>
        public string ServiceName { get; set; } = "operator";

        public string ServiceNamespace { get; set; } = "default";

        public int Port { get; set; } = 443;

        /// <summary><c>Fail</c> (default) or <c>Ignore</c> when the webhook can't be reached.</summary>
        public string FailurePolicy { get; set; } = "Fail";

        public int TimeoutSeconds { get; set; } = 10;

        /// <summary>The CA bundle (base64 PEM) to put in the configurations; usually injected by cert-manager instead.</summary>
        public string CaBundle { get; set; }

        /// <summary>The prefix of the configuration names (default: the service name).</summary>
        public string Name { get; set; }
    }

    internal sealed class WebhookRegistration
    {
        public WebhookRegistration(Type implementationType) => ImplementationType = implementationType;

        public Type ImplementationType { get; }

        public WebhookDescriptor Describe(IServiceProvider services) => ((IWebhook)services.GetRequiredService(ImplementationType)).Describe(ImplementationType);
    }

    public static class WebhookExtensions
    {
        /// <summary>Registers a validating webhook (<see cref="IValidator{T}" />), resolved per request.</summary>
        public static OperatorBuilder AddValidator<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TValidator>(this OperatorBuilder builder)
            where TValidator : class, IWebhook => builder.AddWebhook<TValidator>();

        /// <summary>Registers a mutating webhook (<see cref="IMutator{T}" />), resolved per request.</summary>
        public static OperatorBuilder AddMutator<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMutator>(this OperatorBuilder builder)
            where TMutator : class, IWebhook => builder.AddWebhook<TMutator>();

        /// <summary>Registers a conversion webhook (<see cref="IConverter{TSpoke,THub}" />), resolved per request.</summary>
        public static OperatorBuilder AddConverter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConverter>(this OperatorBuilder builder)
            where TConverter : class, IWebhook => builder.AddWebhook<TConverter>();

        /// <summary>Configures how Kubernetes reaches the webhooks (for the generated <c>ValidatingWebhookConfiguration</c>, <c>MutatingWebhookConfiguration</c> and CRD conversion).</summary>
        public static OperatorBuilder Webhooks(this OperatorBuilder builder, Action<WebhookOptions> configure)
        {
            var options = new WebhookOptions();
            configure?.Invoke(options);
            builder.Services.RemoveAll<WebhookOptions>();
            builder.Services.AddSingleton(options);
            return builder;
        }

        private static OperatorBuilder AddWebhook<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(this OperatorBuilder builder) where T : class, IWebhook
        {
            builder.Services.TryAddScoped<T>();
            builder.Services.AddSingleton(new WebhookRegistration(typeof(T)));
            builder.Services.TryAddSingleton(new WebhookOptions());
            builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IOperatorManifest, WebhookManifest>());
            return builder;
        }

        /// <summary>
        ///     Maps every registered webhook: <c>/validate/{group}/{version}/{plural}</c>, <c>/mutate/...</c> and <c>/convert/{group}/{plural}</c>.
        ///     TLS is the deployment's job (cert-manager or a mounted secret).
        /// </summary>
        public static IEndpointRouteBuilder MapKubernetesWebhooks(this IEndpointRouteBuilder endpoints)
        {
            var services = endpoints.ServiceProvider;
            var logger = services.GetService<ILoggerFactory>()?.CreateLogger("Steeltoe.Informers.Webhooks") ?? NullLogger.Instance;
            foreach (var descriptor in Descriptors(services).Where(x => x.Kind != WebhookKind.Converting))
                endpoints.MapPost(descriptor.Path, context => AdmitAsync(context, descriptor, logger));
            foreach (var group in Descriptors(services).Where(x => x.Kind == WebhookKind.Converting).GroupBy(x => x.Path))
            {
                var converters = group.ToList();
                endpoints.MapPost(group.Key, context => ConvertAsync(context, converters, logger));
            }

            return endpoints;
        }

        internal static IReadOnlyList<WebhookDescriptor> Descriptors(IServiceProvider services)
        {
            using var scope = services.CreateScope();
            return services.GetServices<WebhookRegistration>().Select(x => x.Describe(scope.ServiceProvider)).ToList();
        }

        internal static async Task AdmitAsync(HttpContext context, WebhookDescriptor descriptor, ILogger logger)
        {
            var review = await JsonSerializer.DeserializeAsync(context.Request.Body, WebhookJsonContext.Default.AdmissionReview, context.RequestAborted).ConfigureAwait(false);
            if (review?.Request == null)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            AdmissionResponse response;
            try
            {
                var instance = context.RequestServices.GetRequiredService(descriptor.ImplementationType);
                response = await descriptor.Admit(instance, review.Request, context.RequestAborted).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Webhook {Path} failed for {Operation} {Namespace}/{Name}", descriptor.Path, review.Request.Operation, review.Request.Namespace, review.Request.Name);
                response = new AdmissionResponse { Uid = review.Request.Uid, Allowed = false, Status = new ReviewStatus { Status = "Failure", Code = 500, Message = ex.Message } };
            }

            context.Response.ContentType = "application/json";
            await JsonSerializer.SerializeAsync(context.Response.Body, new AdmissionReview { Response = response }, WebhookJsonContext.Default.AdmissionReview, context.RequestAborted).ConfigureAwait(false);
        }

        internal static async Task ConvertAsync(HttpContext context, IReadOnlyList<WebhookDescriptor> converters, ILogger logger)
        {
            var review = await JsonSerializer.DeserializeAsync(context.Request.Body, WebhookJsonContext.Default.ConversionReview, context.RequestAborted).ConfigureAwait(false);
            if (review?.Request == null)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var response = new ConversionResponse { Uid = review.Request.Uid, ConvertedObjects = new List<JsonElement>() };
            try
            {
                var hubVersion = converters[0].HubApiVersion;
                foreach (var obj in review.Request.Objects ?? new List<JsonElement>())
                {
                    var version = obj.TryGetProperty("apiVersion", out var v) ? v.GetString() : null;
                    object hub;
                    if (version == hubVersion)
                    {
                        hub = converters[0].ReadHub(obj);
                    }
                    else
                    {
                        var spoke = converters.FirstOrDefault(x => x.SpokeApiVersion == version) ?? throw new InvalidOperationException($"No converter from {version}");
                        hub = spoke.ToHub(context.RequestServices.GetRequiredService(spoke.ImplementationType), obj);
                    }

                    JsonObject converted;
                    if (review.Request.DesiredApiVersion == hubVersion)
                    {
                        converted = converters[0].WriteHub(hub);
                    }
                    else
                    {
                        var target = converters.FirstOrDefault(x => x.SpokeApiVersion == review.Request.DesiredApiVersion)
                                     ?? throw new InvalidOperationException($"No converter to {review.Request.DesiredApiVersion}");
                        converted = target.FromHub(context.RequestServices.GetRequiredService(target.ImplementationType), hub);
                    }

                    using var document = JsonDocument.Parse(converted.ToJsonString());
                    response.ConvertedObjects.Add(document.RootElement.Clone());
                }

                response.Result = new ReviewStatus { Status = "Success" };
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Conversion to {Version} failed", review.Request.DesiredApiVersion);
                response.ConvertedObjects = null;
                response.Result = new ReviewStatus { Status = "Failure", Message = ex.Message };
            }

            context.Response.ContentType = "application/json";
            await JsonSerializer.SerializeAsync(context.Response.Body, new ConversionReview { Response = response }, WebhookJsonContext.Default.ConversionReview, context.RequestAborted).ConfigureAwait(false);
        }
    }
}
