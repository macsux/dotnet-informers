using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using k8s;
using k8s.Autorest;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public static class KubernetesTestExtensions
    {
        public static string ToKubernetesJson(this object obj) => KubernetesJson.Serialize(obj);

        public static Watcher<T>.WatchEvent ToWatchEvent<T>(this T obj, WatchEventType eventType)
        {
            return new Watcher<T>.WatchEvent { Type = eventType, Object = obj };
        }

        public static HttpOperationResponse<KubernetesList<TV>> ToHttpOperationResponse<TL, TV>(this TL obj) where TL : IItems<TV> where TV : IKubernetesObject
        {
            var json = obj.ToKubernetesJson();
            return new HttpOperationResponse<KubernetesList<TV>>()
            {
                Body = KubernetesJson.Deserialize<KubernetesList<TV>>(json),
                Response = new HttpResponseMessage()
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(json, Encoding.UTF8, "application/json")
                }
            };
        }

        public static HttpOperationResponse<KubernetesList<T>> ToHttpOperationResponse<T>(this Watcher<T>.WatchEvent obj) where T : IKubernetesObject
        {
            return new[] { obj }.ToHttpOperationResponse();
        }

        public static HttpOperationResponse<KubernetesList<T>> ToHttpOperationResponse<T>(this IEnumerable<Watcher<T>.WatchEvent> obj) where T : IKubernetesObject
        {
            // watch responses are newline delimited JSON events
            return new HttpOperationResponse<KubernetesList<T>>()
            {
                Response = new HttpResponseMessage()
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(string.Join("\n", obj.Select(x => x.ToKubernetesJson())), Encoding.UTF8, "application/json")
                }
            };
        }
    }
}
