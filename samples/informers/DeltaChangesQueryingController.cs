using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using k8s.Models;
using KellermanSoftware.CompareNetObjects;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace informers
{
    // this sample demos the informer in a basic controller context:
    // try creating and deleting some pods in the "default" namespace and watch the output
    public class DeltaChangesQueryingController : IController
    {
        private readonly IKubernetesInformer<V1Pod> _podInformer;
        private readonly ILogger _informerLogger;
        private readonly CompareLogic _objectCompare = new CompareLogic();
        private readonly ConcurrentDictionary<string, V1Pod> _lastSeen = new ConcurrentDictionary<string, V1Pod>();

        public DeltaChangesQueryingController(IKubernetesInformer<V1Pod> podInformer, ILoggerFactory loggerFactory)
        {
            _podInformer = podInformer;
            _informerLogger = loggerFactory.CreateLogger("Informer");
            _objectCompare.Config.MaxDifferences = 100;
        }

        public Task Initialize(CancellationToken cancellationToken) =>
            _podInformer
                .Where(pod => pod.Metadata.NamespaceProperty == "default") // pushed down to the API server
                .RunAsync((e, ct) =>
                {
                    PrintChanges(e);
                    return Task.CompletedTask;
                }, new RunOptions { Name = nameof(DeltaChangesQueryingController) }, cancellationToken);

        private void PrintChanges(ResourceEvent<string, V1Pod> changes)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"==={changes.EventFlags}===");
            sb.AppendLine($"Name: {changes.Value.Metadata.Name}");
            sb.AppendLine($"Version: {changes.Value.Metadata.ResourceVersion}");
            if (changes.EventFlags.HasFlag(EventTypeFlags.Modify) && _lastSeen.TryGetValue(changes.Key, out var previousValue))
            {
                var updateDelta = _objectCompare.Compare(previousValue, changes.Value);
                foreach (var difference in updateDelta.Differences)
                {
                    sb.AppendLine($"{difference.PropertyName}: {difference.Object1} -> {difference.Object2}");
                }
            }

            if (changes.EventFlags.HasFlag(EventTypeFlags.Delete))
                _lastSeen.TryRemove(changes.Key, out _);
            else
                _lastSeen[changes.Key] = changes.Value;
            _informerLogger.LogInformation(sb.ToString());
        }
    }
}
