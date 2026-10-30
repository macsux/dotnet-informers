# kubernetes
Experimental resources for working with Kubernetes in .NET

## Informers

An informer is a **level-triggered replica** of a collection of objects (e.g. every pod in a namespace) that each consumer reads at its own pace, as an `IAsyncEnumerable<ResourceEvent<TKey, TResource>>`.

- `Steeltoe.Informers.InformersBase`: the shared informer, LINQ operators, joins and the `RunAsync` reconcile loop. Nothing in it assumes Kubernetes.
- `Steeltoe.Informers.KubernetesBase`: the Kubernetes list/watch producer, filter pushdown, metadata-only and untyped informers, controller helpers.
- `Steeltoe.Informers.KubernetesCore`: dependency injection registration.

### The contract

- **Converges, doesn't replay.** Every consumer eventually sees the latest state of every key; intermediate versions may be skipped.
- **Merged per key.** Changes a consumer hasn't pulled yet arrive as one event: Add then Modify is an Add, Modify then Modify one Modify, Add then Delete nothing.
- **Starts with a reset.** A consumer's first events are `ResetStart` … `ResetEnd` (or `ResetEmpty`) holding the complete state, then deltas. A consumer that joins late misses nothing.
- **A relist sends computed changes, never a reset.** When the watch expires (410 Gone), each consumer receives `Add`/`Modify`/`Delete` flagged `Computed` for the differences only; objects with the same `resourceVersion` send nothing. A computed `Delete` carries the last known object (client-go's `DeletedFinalStateUnknown`).
- **Shared, not duplicated.** Consumers of the same informer whose `Where` predicates push down to the same server filter share one list/watch connection. Producers start with the first consumer and stop after the last one leaves.
- **Slow consumers don't hold anyone up**, and their memory is bounded by the number of keys.
- **Errors reach the consumer.** A fatal producer error is thrown from `MoveNextAsync` as `InformerException`. Transient errors are retried by the producer.
- **Keys** are `namespace/name` (`name` for cluster-scoped objects).

### Objects are shared by reference

Every consumer, cache and join result receives the same instances. **Never modify an object handed out by an informer**; clone it first (`obj.DeepClone()`). A mutation would corrupt the cache for every consumer. In tests, `registration.DetectMutations()` fails the informer when a cached object changes.

### Getting started

```csharp
services.AddKubernetesClient(KubernetesClientConfiguration.BuildDefaultConfig);
services.AddKubernetesInformer<V1Pod>(pods => pods
    .StripManagedFields()                                    // a transform runs before caching; every consumer sees its result
    .AddIndex("node", pod => new[] { pod.Spec.NodeName }));

// inject IKubernetesInformer<V1Pod>, or IKubernetesInformerFactory:
//   informers.Get<V1Pod>(), informers.MetadataOnly<V1Secret>(), informers.MetadataOnly(gvr), informers.Dynamic(gvr)

await foreach (var e in pods.Where(p => p.Metadata.NamespaceProperty == "payments").WithCancellation(ct))
{
    if (e.EventFlags.HasFlag(EventTypeFlags.Delete)) Forget(e.Key);
    else Remember(e.Value);
}
```

### Filtering on the server

`Where` on an informer takes an expression. The top-level `&&` terms the Kubernetes translator understands become the namespace, label selector and field selector of the list/watch request. The rest is evaluated client-side:

| Term | Pushed down as |
|---|---|
| `p.Metadata.NamespaceProperty == "ns"`, `p.Namespace() == ns` | the request namespace (`!=` becomes a field selector) |
| `p.Metadata.Name == "n"`, `p.Name() != "n"` | `metadata.name` field selector |
| `p.GetLabel("app") == "web"`, `!= "web"`, `== null`, `!= null` | label selector `app=web`, `app!=web`, `!app`, `app` |
| `p.Metadata.Labels["app"] == "web"` (only `==`), `p.Metadata.Labels.ContainsKey("app")` | label selector |
| `values.Contains(p.GetLabel("tier"))`, negated | `tier in (…)`, `tier notin (…)` |
| pod `p.Spec.NodeName`, `p.Status.Phase` (and other supported fields) | field selector |

- Compared values must be constants or captured variables.
- An `||` mixing translatable and untranslatable terms stays client-side.
- An object that stops matching arrives as a `Delete`, and one that starts matching as an `Add`.
- `Where` on an informer builds an expression tree, so constructs expression trees don't support (such as `?.`) don't compile there. Use `GetLabel`, or filter an informable that isn't the informer itself (e.g. after `.Select(x => x)`).

### Joins

```csharp
deployments.Where(d => d.Metadata.NamespaceProperty == "payments")
    .GroupJoin(pods.Where(p => p.Metadata.NamespaceProperty == "payments"),
        d => d.Spec.Selector.MatchLabels["app"],
        p => p.Metadata.Labels["app"],
        (d, ps) => new Readiness(d.Metadata.Name, ps.Count(p => p.Status.Phase == "Running")))
    .RunAsync((e, ct) => PatchAnnotation(e.Value, ct), new RunOptions { MaxConcurrency = 4 }, ct);
```

- `GroupJoin` gives one result per outer object; `Join` gives one per matching pair, keyed by both keys. Key selectors may return several keys (`p => p.OwnerUids()`).
- A result is recomputed when either side changes. It's emitted only if it differs from the last one sent (records compare by value; pass a comparer for results holding collections).
- The output starts with a reset once both sides are synced.
- A key selector that throws or returns null matches nothing.
- Joins compose: a join over a join works. Join owners by **UID** (`OwnerUids()`, `OwnedBy(owner)`), never by name: a recreated owner gets a new UID.

### Controllers

`RunAsync(handler, options, ct)` is a reconcile loop:

- It never runs one key twice at once, and runs up to `MaxConcurrency` keys in parallel.
- A retry runs with the key's latest state.
- On cancellation it stops starting handlers, waits for running ones, and returns.

| Handler outcome | Effect |
|---|---|
| `ReconcileResult.Done` | resets the key's backoff |
| `ReconcileResult.Requeue()`, or a thrown exception | retry with per-key exponential backoff (`RunOptions.Backoff`) |
| `ReconcileResult.RequeueAfter(delay)` | retry after exactly `delay` |
| 409 Conflict | prompt retry with the latest state, not counted as a failure |

A new change to a key replaces its pending retry. `RunOptions.RateLimit` limits handler starts overall.

- **Leader election.** Set `RunOptions.LeaderElection = new KubernetesLeaderElection(client, ns, "my-controller", podName)`. Every replica keeps its informers warm; only the leader runs handlers. Losing leadership cancels running handlers.
- **Duplicate creates.** `Expectations` records the creates and deletes a reconcile issued. Wrap the watched children with `expectations.Observe(pods, p => p.OwnerKeys("AppSet"))`, so observations happen before the join sees them, and skip acting until `IsSatisfied(key)` (see `samples/Controllers/AppSetController.cs`).
- **Ignoring your own status writes.** Use `DistinctUntilChanged(x => x.Metadata.Generation)`, or a signature of what you act on, to drop `Modify` events that don't change it. It's per consumer.
  - Place it **after** a join, so the join still sees the latest object.
  - A dropped event also never reaches `RunAsync`: write against the object's current `resourceVersion` (re-read it before the write) rather than the possibly older copy in the event.
- **Periodic re-checks.** Use `Resync(period)` to re-emit every key as `Modify | Sync` (keys with a pending change are skipped). Use `RequeueAfter` for per-key re-checks.
- **Finalizers.** A deletion first arrives as a `Modify` with `deletionTimestamp` set (`obj.IsBeingDeleted()`). The `Delete` follows once the last finalizer is removed.
- **Reading another informer.** Before a handler consults a cache filled from another informer (`Into(cache)`), wait for `informer.WaitForSyncAsync()` and `cache.Synchronized`. Otherwise an empty cache makes every object look orphaned.

### Metadata-only and untyped informers

- `informers.MetadataOnly<V1Secret>()` requests the API server's `PartialObjectMetadata` representation: labels, annotations, owner references and finalizers, but no data, `spec` or `status`.
- `informers.Dynamic(new GroupVersionResource("example.com", "v1", "widgets"))` watches a resource without a C# type. Each `UntypedKubernetesObject` has typed `Metadata` plus the raw JSON (`Raw`, `Get("spec")`).

Both keep keys, owner helpers, metadata pushdown, joins and `RunAsync`.

### Other sources

`SharedInformer<TKey, TResource>` works for any list/watch source:

- **Producer.** It fills an `IInformerSink<TResource>` (`Replace` for a complete list, `Upsert`, `Delete`) until cancelled.
- **Options.** `SharedInformerOptions` set the key selector, an optional no-op check (`ChangeComparer`), a filter translator (`IFilterTranslator<TResource>`), a transform and indexes.

### Diagnostics

The library reports through the meter and activity source named `Steeltoe.Informers` (`InformerDiagnostics.Name`):

- **Reconcile metrics:** count by result, duration, errors, requeues, queue depth.
- **Producer metrics:** events, relists, watch restarts, transient errors, active producers.
- **Tracing:** an activity around every reconcile.

### Samples and real-cluster tests

`samples/Controllers` holds controllers built on informers:

- deployment readiness (GroupJoin + Resync)
- an `AppSet` CRD replica controller (expectations, owner UIDs, finalizer, status, requeues, conflicts)
- metadata-only Secret usage and orphan cleanup
- an untyped `Widget` CRD
- node pod counts with field selectors

`samples/informers` runs the readiness controller with leader election (`--Controller=readiness --Namespace=demo --LeaderElection:Identity=a`).

`tests/Integration.Tests` runs them against a real cluster and asserts the end state. It also covers filter transitions, connection sharing, dropped connections, watch expiry after etcd compaction and an API server restart, and leader failover. Without a cluster the tests are skipped. To run them on kind:

```sh
kind create cluster --name informers --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes: [{role: control-plane}, {role: worker}]
EOF
kind get kubeconfig --name informers > /tmp/informers.kubeconfig
dotnet build Steeltoe.Informers.sln -c Release
INFORMERS_INTEGRATION_KUBECONFIG=/tmp/informers.kubeconfig INFORMERS_INTEGRATION_KIND_CLUSTER=informers \
  dotnet test --project tests/Integration.Tests -c Release --no-build
```
