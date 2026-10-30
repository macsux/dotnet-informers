using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>
    ///     Keeps a ConfigMap <c>widget-&lt;name&gt;</c> with the <c>spec</c> of every Widget, a custom resource with no C# type: an untyped
    ///     informer (<c>informers.Dynamic(gvr)</c>) reads it as typed metadata plus raw JSON.
    /// </summary>
    public sealed class WidgetController
    {
        public static readonly GroupVersionResource Widgets = new GroupVersionResource(SampleResources.Group, "v1", "widgets");

        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;

        public WidgetController(IKubernetesInformerFactory informers, IKubernetes kubernetes)
        {
            _informers = informers;
            _kubernetes = kubernetes;
        }

        public ControllerStats Stats { get; } = new ControllerStats();

        public Task RunAsync(string ns, CancellationToken cancellationToken) =>
            _informers.Dynamic(Widgets)
                .Where(w => w.Metadata.NamespaceProperty == ns)
                .RunAsync(async (e, ct) =>
                {
                    Stats.Increment("reconciles");
                    var name = $"widget-{e.Value.Metadata.Name}";
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                    {
                        try
                        {
                            await _kubernetes.CoreV1.DeleteNamespacedConfigMapAsync(name, ns, cancellationToken: ct).ConfigureAwait(false);
                        }
                        catch (HttpOperationException ex) when (ex.Response.StatusCode == HttpStatusCode.NotFound)
                        {
                        }

                        return;
                    }

                    var data = new Dictionary<string, string>();
                    if (e.Value.Get("spec") is { ValueKind: JsonValueKind.Object } spec)
                    {
                        foreach (var property in spec.EnumerateObject())
                            data[property.Name] = property.Value.ValueKind == JsonValueKind.String ? property.Value.GetString() : property.Value.GetRawText();
                    }

                    var configMap = new V1ConfigMap
                    {
                        Metadata = new V1ObjectMeta
                        {
                            Name = name,
                            NamespaceProperty = ns,
                            OwnerReferences = new List<V1OwnerReference> { new V1OwnerReference { ApiVersion = e.Value.ApiVersion, Kind = e.Value.Kind, Name = e.Value.Metadata.Name, Uid = e.Value.Metadata.Uid } },
                        },
                        Data = data,
                    };
                    try
                    {
                        await _kubernetes.CoreV1.ReplaceNamespacedConfigMapAsync(configMap, name, ns, cancellationToken: ct).ConfigureAwait(false);
                    }
                    catch (HttpOperationException ex) when (ex.Response.StatusCode == HttpStatusCode.NotFound)
                    {
                        await _kubernetes.CoreV1.CreateNamespacedConfigMapAsync(configMap, ns, cancellationToken: ct).ConfigureAwait(false);
                    }

                    Stats.Increment("writes");
                }, new RunOptions { Name = nameof(WidgetController) }, cancellationToken);
    }
}
