using System;
using System.Collections.Generic;
using System.IO;

namespace Steeltoe.Informers.Crds
{
    /// <summary>
    ///     <c>Steeltoe.Informers.Crds --assembly &lt;path&gt; --output &lt;dir&gt; [--reference &lt;dll&gt;]...</c>: writes a CRD YAML per custom resource of the
    ///     assembly. Run by the <c>InformersGenerateManifests</c> MSBuild target after build.
    /// </summary>
    public static class Program
    {
        public static int Main(string[] args)
        {
            string assembly = null, output = null;
            var references = new List<string>();
            for (var i = 0; i < args.Length - 1; i++)
            {
                switch (args[i])
                {
                    case "--assembly":
                        assembly = args[++i];
                        break;
                    case "--output":
                        output = args[++i];
                        break;
                    case "--reference":
                        references.Add(args[++i]);
                        break;
                }
            }

            if (assembly == null || output == null)
            {
                Console.Error.WriteLine("usage: Steeltoe.Informers.Crds --assembly <path> --output <dir> [--reference <dll>]...");
                return 2;
            }

            try
            {
                var files = CrdGenerator.Write(CrdGenerator.Generate(assembly, references), output);
                foreach (var file in files)
                    Console.WriteLine($"Steeltoe.Informers.Crds: {Path.GetRelativePath(Environment.CurrentDirectory, file)}");
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Steeltoe.Informers.Crds: error: {ex.Message}");
                return 1;
            }
        }
    }
}
