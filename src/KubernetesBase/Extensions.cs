using System;
using System.Net;
using System.Net.Http;
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
        internal static bool IsResourceVersionGone(this Exception exception) => exception switch
        {
            HttpOperationException { Response.StatusCode: HttpStatusCode.Gone } => true,
            KubernetesException { Status.Code: (int)HttpStatusCode.Gone } => true,
            _ => false,
        };

        /// <summary>
        ///     Checks if the exception is an optimistic-concurrency conflict (HTTP 409): the object changed since it was read
        /// </summary>
        public static bool IsConflict(this Exception exception) => exception switch
        {
            HttpOperationException { Response.StatusCode: HttpStatusCode.Conflict } => true,
            KubernetesException { Status.Code: (int)HttpStatusCode.Conflict } => true,
            _ => false,
        };

        internal static void EnsureSuccessStatusCode(this HttpOperationResponse response)
        {
            if (!response.Response.IsSuccessStatusCode)
            {
                throw new HttpOperationException($"Web server replied with error code {response.Response.StatusCode}")
                {
                    Request = response.Request != null ? new HttpRequestMessageWrapper(response.Request, null) : null,
                    Response = new HttpResponseMessageWrapper(response.Response, null),
                };
            }
        }

        internal static string ToCamelCase(this string value)
        {
            // If there are 0 or 1 characters, just return the string.
            if (value == null || value.Length < 2)
            {
                return value;
            }

            // Split the string into words.
            var words = value.Split(
                new char[0],
                StringSplitOptions.RemoveEmptyEntries);

            // Combine the words.
            var result = words[0].ToLower();
            for (var i = 1; i < words.Length; i++)
            {
                result +=
                    words[i].Substring(0, 1).ToUpper() +
                    words[i].Substring(1);
            }

            return result;
        }
    }
}
