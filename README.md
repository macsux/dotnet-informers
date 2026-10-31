# kubernetes
Experimental resources for working with Kubernetes in .NET

## Informers

An informer is a **level-triggered replica** of a collection of objects (e.g. every pod in a namespace) that each consumer reads at its own pace, as an `IAsyncEnumerable<ResourceEvent<TKey, TResource>>`.

- `Steeltoe.Informers.InformersBase`: the shared informer, LINQ operators, joins and the `RunAsync` reconcile loop. Nothing in it assumes Kubernetes.
- `Steeltoe.Informers.KubernetesBase`: the Kubernetes list/watch producer, filter pushdown, metadata-only and untyped informers, controller helpers.
- `Steeltoe.Informers.KubernetesCore`: dependency injection registration.
- `Steeltoe.Informers.Operator`: controller classes and their hosting, on top of the above.

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
- **Ignoring your own status writes.** `TriggerOn` decides which changes start a reconcile without dropping any: a change that doesn't alter what it selects arrives flagged `Quiet`. It still carries the latest object (joins, caches and retries stay current), but `RunAsync` doesn't run for it. When changes merge per key, quiet and non-quiet make non-quiet.
  - `TriggerOn(x => x.Spec, x => x.Metadata.Labels)`: values compare structurally (primitives by value, collections entry by entry, other objects by their JSON through `InformerJson`), so fresh objects from every watch event with the same content compare equal.
  - Presets: `Trigger.Generation`, `Trigger.AllButStatus` (generation, labels, annotations, finalizers, owner references), `Trigger.Any`; combinable with a field list (`TriggerOn(Trigger.Generation, x => x.Metadata.Labels)`) or with each other (`Trigger.Combine`). `TriggerWhen((previous, current) => …)` for anything else.
  - Adds, deletes, resets and syncs always trigger, and over Kubernetes objects so does `deletionTimestamp` being set, so finalizers run whatever the selection.
  - Apply it to a source before a join (the result triggers if any contributing change does) or after the join on the combined value. Unlike `Where`, it never removes an object.
  - `IgnoreWritesFrom(writes)` quiets only the echoes of your own writes: give the controller a client made with `client.RecordingWritesTo(writes)` (an `OwnWrites` table of the `resourceVersion`s your writes returned). Use it when the trigger includes what you write yourself.
- **Periodic re-checks.** Use `Resync(period)` to re-emit every key as `Modify | Sync` (keys with a pending change are skipped). Use `RequeueAfter` for per-key re-checks.
- **Finalizers.** A deletion first arrives as a `Modify` with `deletionTimestamp` set (`obj.IsBeingDeleted()`). The `Delete` follows once the last finalizer is removed. `Finalizers.Handle(client, x => x.Set, ReconcileAsync, ("example.com/pods", CleanupAsync))` wraps a handler: it adds the finalizers before reconciling, and once deletion starts runs each cleanup still pending in order instead (`Done` removes that finalizer; anything else retries). `obj.HasFinalizer(name)`, `client.AddFinalizersAsync` and `RemoveFinalizerAsync` (a 404 after removing the last one counts as success) are the pieces.
- **Reading another informer.** Before a handler consults a cache filled from another informer (`Into(cache)`), wait for `informer.WaitForSyncAsync()` and `cache.Synchronized`. Otherwise an empty cache makes every object look orphaned.

### Writing objects

There is one way to change an object: edit a copy, and the difference is sent as a JSON merge patch.

```csharp
await client.PatchAsync(set, s => s.Status.ReadyReplicas = ready, ct);          // → PATCH .../appsets/demo/status
await client.PatchAsync(pod, p => p.Metadata.Labels["tier"] = "web", ct);        // → PATCH .../pods/demo-x7k2p
```

- The object passed in is never modified (it's cloned first), so objects from an informer can be passed directly. Nothing is sent when the edit changes nothing.
- The patch carries the original's `resourceVersion`, so it fails with 409 Conflict if the object changed meanwhile, and `RunAsync` retries with the latest object. A merge patch replaces lists (finalizers, conditions) whole, which is only safe with this precondition; `new PatchOptions { RequireCurrentVersion = false }` opts out.
- With a status subresource (from discovery), `status` changes go to `/status` and the rest to the object: two requests when both changed.
- `Tracked<T>` is a copy-on-write handle: `Original` (shared, never modified), `Edit()` (clones on first call), `Value`, `IsModified`, `HasChanges()`. `client.SaveAsync(tracked)` sends the difference and makes the stored object the new original.
- `child.WithOwnerReference(owner)` adds a controller owner reference (rejecting owners without a UID, owners in another namespace and a second controller); `owner.ToOwnerReference()` builds one. `client.CreateOwnedAsync(owner, child, expectations)` and `DeleteOwnedAsync` set the owner reference and record the create or delete in `Expectations`.
- `status.Conditions.Set("Ready", true, reason: "PodsReady", observedGeneration: gen)` updates `lastTransitionTime` only when the status value changes; also `Get`, `IsTrue`, `Remove`.
- `client.ApplyAsync(desired, fieldManager)` declares a child's full desired state with server-side apply.

### Kubernetes Events

`KubernetesEventRecorder` (`IEventRecorder`) writes the Events `kubectl describe` shows:

```csharp
events.Warning(set, "FailedCreate", ex.Message, action: "Create", related: pod);
events.For(set).Normal("SuccessfulCreate", $"Created pod {pod.Name()}");
```

- Recording never blocks or fails the caller: events go through a bounded queue to a background writer; API errors are logged, counted (`informers.kubernetes_events`) and dropped.
- `events.k8s.io/v1` by default, `core/v1` with `Api = EventApi.CoreV1`; `ReportingController` and `ReportingInstance` (default: `POD_NAME`, or the machine name) are set.
- Repeats of the same event within `AggregationWindow` update one Event's series count; each involved object is rate limited (`BurstPerObject`, refilled every `RefillInterval`).
- `LoggingEventRecorder` and `NullEventRecorder` replace it for control planes without the `Event` resource.

### Controller classes

`Steeltoe.Informers.Operator` packages the pieces above. A controller declares its relationships once and reconciles one object at a time; it compiles to shared informers, joins, `Expectations` and `RunAsync`.

```csharp
builder.Services
    .AddKubernetesClient(KubernetesClientConfiguration.BuildDefaultConfig)
    .AddKubernetesOperator(o => o
        .Json(AppJsonContext.Default)                     // source-generated JSON for custom resources
        .LeaderElection("appset-operator", "operators")
        .Namespace("demo"))                               // default scope of every controller
    .AddController<AppSetController>()
    .AddController<V1Widget>(c => c.Reconcile(async (ctx, ct) => ReconcileResult.Done));

public sealed class AppSetController(ILogger<AppSetController> logger) : Controller<AppSet>
{
    public override void Configure(ControllerBuilder<AppSet> c) => c
        .TriggerOn(Trigger.AllButStatus)
        .Owns<V1Pod>()
        .References<V1Secret>(set => [set.Spec.SecretRef])
        .Finalizer("example.com/pods", CleanupPodsAsync)
        .MaxConcurrency(4);

    public override async Task<ReconcileResult> ReconcileAsync(ReconcileContext<AppSet> ctx, CancellationToken ct)
    {
        var pods = ctx.Owned<V1Pod>();                         // Tracked<V1Pod> handles, from the cache
        await ctx.CreateOwnedAsync(newPod, ct);                // owner reference + expectations
        ctx.Object.Edit().Status.ReadyReplicas = ready;        // copy-on-write
        await ctx.SaveAsync(ct);                               // PATCH /status, only if it changed
        return ReconcileResult.Done;
    }
}
```

- **Relationships**, each a `GroupJoin` on shared informers scoped to the controller's namespace:
  - `Owns<TChild>(trigger?)`: children with a controller owner reference to the primary (matched by UID). `ctx.Owned<TChild>()` lists them, including ones being deleted. Creates and deletes through `ctx.CreateOwnedAsync`/`DeleteOwnedAsync` are tracked: the reconcile isn't called until they're observed or time out.
  - `References<TOther>(primary => names, name?, trigger?)`: objects the primary names (`name` in its namespace, `namespace/name`, or a cluster-scoped name). `ctx.Related<TOther>(name)` holds the ones that exist; one appearing, changing or disappearing re-runs every primary that names it.
  - `Watches<TOther>(other => primaryKeys, name?, trigger?)`: objects that know which primaries they affect (a label, a field).
- **Triggers**: every change triggers unless `TriggerOn(...)` narrows it (`Trigger.AllButStatus`, `Trigger.Generation`, a field list); `IgnoreOwnWrites()` skips only the echoes of the controller's own writes.
- **Finalizers**: `Finalizer(name, cleanup)` adds it before the first reconcile; once deletion starts the cleanups run instead (in declaration order), `Done` removes each.
- **The context** is a snapshot from the caches: `Key`, `Trigger` (change, retry, resync), `Object` and related objects as `Tracked<T>` handles, `Events` bound to the object, `Logger`, `Client`, `CreateOwnedAsync`, `DeleteOwnedAsync`, `SaveAsync`. Edits not saved when the reconcile returns are discarded and logged.
- **Options**: `Namespace`/`AllNamespaces`, `Where` (pushed down), `MaxConcurrency`, `Backoff`, `RateLimit`, `Resync`, `ExpectationsTimeout`; `DeletedAsync(key, lastKnown)` runs once an object is gone.
- **Hosting**: one hosted service runs every controller. Startup waits (bounded by `WaitForSync`) for their informers to sync; a failing controller restarts with backoff without stopping the others; shutdown waits for running reconciles. Controllers are resolved with a scope per reconcile. `OperatorStatus` reports sync state and the resources and verbs each controller needs. A reconcile error records a `ReconcileError` warning event (`AutomaticEvents(false)` turns that off); `DisableEvents()` logs events instead.
- **Tests**: `ReconcileContext.ForTest(obj).WithOwned(...).WithRelated(...)` runs a reconcile without informers or an API server and records its `Writes`.

### Metadata-only and untyped informers

- `informers.MetadataOnly<V1Secret>()` requests the API server's `PartialObjectMetadata` representation: labels, annotations, owner references and finalizers, but no data, `spec` or `status`.
- `informers.Dynamic(new GroupVersionResource("example.com", "v1", "widgets"))` watches a resource without a C# type. Each `UntypedKubernetesObject` has typed `Metadata` plus the raw JSON (`Raw`, `Get("spec")`).

Both keep keys, owner helpers, metadata pushdown, joins and `RunAsync`.

### Other sources

`SharedInformer<TKey, TResource>` works for any list/watch source:

- **Producer.** It fills an `IInformerSink<TResource>` (`Replace` for a complete list, `Upsert`, `Delete`) until cancelled.
- **Options.** `SharedInformerOptions` set the key selector, an optional no-op check (`ChangeComparer`), a filter translator (`IFilterTranslator<TResource>`), a transform and indexes.

### JSON, trimming and NativeAOT

Every package is trim- and AOT-compatible (`IsTrimmable`, `IsAotCompatible`, analyzers clean). The library never serializes by reflection on its own: reading, writing, cloning and comparing objects go through `InformerJson`, a chain of `IJsonTypeInfoResolver`s:

1. your source-generated contexts, registered with `InformerJson.AddResolver(...)`,
2. the library's own context (`PartialObjectMetadata`, `UntypedKubernetesObject`, …) and KubernetesClient's source-generated `k8s.SourceGenerationContext`, which covers every built-in model,
3. reflection, only while it's enabled (not in trimmed or AOT apps).

So built-in types work everywhere; a custom resource needs a context in a trimmed or AOT app:

```csharp
[JsonSerializable(typeof(AppSet))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal partial class AppJsonContext : JsonSerializerContext;

InformerJson.AddResolver(AppJsonContext.Default);
```

`KubernetesGenericClient` (`IKubernetesGenericClient`) reads and writes any resource through the same metadata: `GetAsync`, `ListAsync`, `CreateAsync`, `ReplaceAsync` (optionally `/status`), `PatchAsync`, `ApplyAsync` (server-side apply), `DeleteAsync` (with a UID precondition) and discovery (`GetResourceInfoAsync`: scope, status and scale subresources). Errors are `HttpOperationException`s: check them with `IsNotFound()`, `IsConflict()`, `IsAlreadyExists()` and `IsGone()`.

`samples/AotSmoke` is published as NativeAOT in CI and run against a kind cluster. KubernetesClient 21 itself isn't trim-annotated: it reports trim warnings, and loading a kubeconfig file fails under NativeAOT (its YAML parsing uses reflection), so AOT apps use the in-cluster configuration or set `KubernetesClientConfiguration` properties directly.

### Diagnostics

The library reports through the meter and activity source named `Steeltoe.Informers` (`InformerDiagnostics.Name`):

- **Reconcile metrics:** count by result, duration, errors, requeues, queue depth.
- **Producer metrics:** events, relists, watch restarts, transient errors, active producers.
- **Tracing:** an activity around every reconcile.

### Samples and real-cluster tests

`samples/Controllers` holds controllers built on informers:

- deployment readiness (GroupJoin + Resync)
- an `AppSet` CRD replica controller as a `Controller<AppSet>` (owned pods, finalizer, status, requeues, conflicts)
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
