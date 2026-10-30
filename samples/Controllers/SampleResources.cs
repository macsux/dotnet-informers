using System.IO;
using System.Reflection;

namespace Steeltoe.Informers.Samples.Controllers
{
    public static class SampleResources
    {
        public const string Group = "samples.steeltoe.io";

        /// <summary>The CRD manifests the sample controllers need (<c>appsets.yaml</c>, <c>widgets.yaml</c>).</summary>
        public static string Crd(string name)
        {
            using var stream = typeof(SampleResources).Assembly.GetManifestResourceStream(name) ?? throw new FileNotFoundException(name);
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }
    }
}
