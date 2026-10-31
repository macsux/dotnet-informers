using System;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;

namespace Steeltoe.Informers.KubernetesBase
{
    public static class Extensions
    {
        /// <summary>
        ///     Checks if the type of exception is the one that is temporary and will resolve itself over time.
        /// </summary>
        /// <param name="exception">Exception to check.</param>
        /// <returns>Return <see langword="true"/> if exception is transient, or <see langword="false"/> if it's not.</returns>
        public static bool IsTransient(this Exception exception)
        {
            if (exception is HttpOperationException { Response: not null } statusException)
            {
                return statusException.Response.StatusCode >= HttpStatusCode.ServiceUnavailable || statusException.Response.StatusCode == HttpStatusCode.RequestTimeout;
            }

            if (exception is HttpRequestException || exception is KubernetesException || exception is System.IO.IOException || exception is TimeoutException)
            {
                return true;
            }

            if (exception is TaskCanceledException { InnerException: TimeoutException })
            {
                return true; // HttpClient timeout
            }

            return false;
        }
        /// <summary>
        ///     Checks if the exception indicates that the requested resource version is too old and the list needs to be fetched from scratch
        /// </summary>
        internal static bool IsResourceVersionGone(this Exception exception) => exception.IsGone();

        /// <summary>
        ///     Checks if the exception is an optimistic-concurrency conflict (HTTP 409 other than <c>AlreadyExists</c>): the object changed since it
        ///     was read.
        /// </summary>
        public static bool IsConflict(this Exception exception) => StatusCode(exception) == HttpStatusCode.Conflict && Reason(exception) != "AlreadyExists";

        /// <summary>Checks if a create failed because an object of that name exists (HTTP 409, reason <c>AlreadyExists</c>).</summary>
        public static bool IsAlreadyExists(this Exception exception) => StatusCode(exception) == HttpStatusCode.Conflict && Reason(exception) == "AlreadyExists";

        /// <summary>Checks if the object doesn't exist (HTTP 404).</summary>
        public static bool IsNotFound(this Exception exception) => StatusCode(exception) == HttpStatusCode.NotFound;

        /// <summary>Checks if the requested resource version is gone (HTTP 410), e.g. a watch after compaction.</summary>
        public static bool IsGone(this Exception exception) => StatusCode(exception) == HttpStatusCode.Gone;

        private static HttpStatusCode? StatusCode(Exception exception) => exception switch
        {
            HttpOperationException { Response: not null } http => http.Response.StatusCode,
            KubernetesException { Status.Code: not null } k8s => (HttpStatusCode)k8s.Status.Code.Value,
            _ => null,
        };

        /// <summary>The <c>reason</c> of the <c>Status</c> the API server returned with an error.</summary>
        private static string Reason(Exception exception)
        {
            if (exception is KubernetesException k8s)
                return k8s.Status?.Reason;
            var content = (exception as HttpOperationException)?.Response?.Content;
            if (string.IsNullOrEmpty(content))
                return null;
            try
            {
                using var document = JsonDocument.Parse(content);
                return document.RootElement.ValueKind == JsonValueKind.Object && document.RootElement.TryGetProperty("reason", out var reason) && reason.ValueKind == JsonValueKind.String
                    ? reason.GetString()
                    : null;
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }
}
