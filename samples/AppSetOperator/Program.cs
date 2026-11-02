using k8s;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Steeltoe.Informers.KubernetesCore;
using Steeltoe.Informers.Operator;
using Steeltoe.Informers.Samples.Controllers;

// dotnet run -- --Namespace=demo   runs the AppSet controller (apply samples/Controllers/Crds/appsets.yaml first)
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ControllerStats>();
builder.Services
    .AddKubernetesClient(KubernetesClientConfiguration.BuildDefaultConfig)
    .AddKubernetesOperator(o => o
        .Json(SampleJsonContext.Default)
        .Namespace(builder.Configuration["Namespace"])
        .RbacRole("appset-operator")
        .Events(e => e.ReportingController = "samples.steeltoe.io/appset-controller"))
    .AddController<AppSetController>();
await builder.Build().RunAsync();
