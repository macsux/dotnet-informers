using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using k8s.Autorest;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Cache;

namespace Steeltoe.Informers.KubernetesBase
{
    public static class KubernetesExtensions
    {
        public static KubernetesEntityAttribute GetKubernetesTypeMetadata<T>(this T obj) where T : IKubernetesObject
        {
            return obj.GetType().GetKubernetesTypeMetadata();
        }

        public static KubernetesEntityAttribute GetKubernetesTypeMetadata(this Type currentType)
        {
            var attr = currentType.GetCustomAttribute<KubernetesEntityAttribute>();
            if (attr == null)
            {
                throw new InvalidOperationException($"Custom resource must have {nameof(KubernetesEntityAttribute)} applied to it");
            }

            return attr;
        }

        public static T Initialize<T>(this T obj) where T : IKubernetesObject
        {
            var metadata = obj.GetKubernetesTypeMetadata();

            obj.ApiVersion = !string.IsNullOrEmpty(metadata.Group) ? $"{metadata.Group}/{metadata.ApiVersion}" : metadata.ApiVersion;
            obj.Kind = metadata.Kind ?? obj.GetType().Name;
            if (obj is IMetadata<V1ObjectMeta> withMetadata && withMetadata.Metadata == null)
            {
                withMetadata.Metadata = new V1ObjectMeta();
            }

            return obj;
        }

        /// <summary>
        ///     Reads a watch response (newline delimited JSON watch events) as an observable stream of events.
        ///     A watch event of type <see cref="WatchEventType.Error" /> is surfaced as <see cref="KubernetesException" /> carrying the server <see cref="V1Status" />
        /// </summary>
        public static IObservable<Watcher<T>.WatchEvent> Watch<T>(this Task<HttpOperationResponse<KubernetesList<T>>> responseTask) where T : IKubernetesObject
        {
            return Observable.Create<Watcher<T>.WatchEvent>(async (observer, cancellationToken) =>
            {
                using var response = await responseTask.ConfigureAwait(false);
                await using var stream = await response.Response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                using var reader = new StreamReader(stream);
                string line;
                while ((line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false)) != null)
                {
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    var watchEvent = KubernetesJson.Deserialize<Watcher<T>.WatchEvent>(line);
                    if (watchEvent.Type == WatchEventType.Error)
                    {
                        var status = KubernetesJson.Deserialize<Watcher<V1Status>.WatchEvent>(line).Object;
                        throw new KubernetesException(status);
                    }

                    observer.OnNext(watchEvent);
                }

                observer.OnCompleted();
            });
        }

        public static ResourceEvent<string, TResource> ToResourceEvent<TResource>(this TResource obj, EventTypeFlags typeFlags)
            where TResource : IKubernetesObject<V1ObjectMeta>
        {
            return new ResourceEvent<string, TResource>(typeFlags, obj.Metadata.Name, obj);
        }

        internal static bool IsValidKubernetesName(this string value)
        {
            return !Regex.IsMatch(value, "^[a-z0-9-]+$");
        }
    }
}