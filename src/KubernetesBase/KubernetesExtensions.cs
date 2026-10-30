using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        public static ResourceEvent<string, TResource> ToResourceEvent<TResource>(this TResource obj, EventTypeFlags typeFlags)
            where TResource : IKubernetesObject<V1ObjectMeta>
        {
            return new ResourceEvent<string, TResource>(typeFlags, obj.ObjectKey(), obj);
        }

        internal static bool IsValidKubernetesName(this string value)
        {
            return !Regex.IsMatch(value, "^[a-z0-9-]+$");
        }
    }
}