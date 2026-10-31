using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using k8s;
using k8s.Models;
using KubeOps.Transpiler;

namespace Steeltoe.Informers.Crds
{
    /// <summary>
    ///     Generates CustomResourceDefinitions from the entity types of a compiled assembly: KubeOps' Transpiler reads it through a
    ///     <see cref="MetadataLoadContext" /> (no user code runs), then the attributes whose enum arguments the Transpiler can only recognize as
    ///     its own types (<c>EntityScope</c>, <c>XListType</c>, <c>XMapType</c>, <c>PrinterColumnPriority</c>) are applied here by name, as is
    ///     <c>ListMapKeys</c>, which the Transpiler would reject next to a list type it can't read.
    /// </summary>
    public static class CrdGenerator
    {
        /// <summary>The CRDs of every type in <paramref name="assemblyPath" /> annotated with <c>[KubernetesEntity]</c> (and not <c>[Ignore]</c>).</summary>
        public static IReadOnlyList<V1CustomResourceDefinition> Generate(string assemblyPath, IEnumerable<string> referencePaths = null)
        {
            var paths = new List<string>();
            paths.AddRange(Directory.GetFiles(RuntimeEnvironment.GetRuntimeDirectory(), "*.dll"));
            paths.AddRange(Directory.GetFiles(Path.GetDirectoryName(Path.GetFullPath(assemblyPath))!, "*.dll"));
            paths.AddRange(Directory.GetFiles(AppContext.BaseDirectory, "*.dll"));
            if (referencePaths != null)
                paths.AddRange(referencePaths);
            using var context = new MetadataLoadContext(new PathAssemblyResolver(paths.Distinct()), typeof(object).Assembly.GetName().Name);
            var assembly = context.LoadFromAssemblyPath(Path.GetFullPath(assemblyPath));
            var types = assembly.GetTypes().Where(t => t.IsClass && !t.IsAbstract && HasAttribute(t, "KubernetesEntityAttribute") && !HasAttribute(t, "IgnoreAttribute")).ToList();
            var crds = context.Transpile(types).OrderBy(x => x.Metadata.Name, StringComparer.Ordinal).ToList();
            foreach (var crd in crds)
            {
                var versions = types.Where(t => Matches(t, crd)).ToList();
                foreach (var type in versions)
                {
                    if (Attribute(type, "EntityScopeAttribute") is { } scope && scope.ConstructorArguments.Count > 0 && Convert.ToInt32(scope.ConstructorArguments[0].Value) == 1)
                        crd.Spec.Scope = "Cluster";
                    var version = crd.Spec.Versions.First(v => v.Name == ApiVersion(type));
                    ApplyPropertyAttributes(type, version.Schema.OpenAPIV3Schema, 0);
                    ApplyPrinterColumnPriorities(type, version);
                }
            }

            return crds;
        }

        /// <summary>Writes each CRD as <c>&lt;plural&gt;.yaml</c> into <paramref name="outputDirectory" />; returns the files written.</summary>
        public static IReadOnlyList<string> Write(IEnumerable<V1CustomResourceDefinition> crds, string outputDirectory)
        {
            Directory.CreateDirectory(outputDirectory);
            var files = new List<string>();
            foreach (var crd in crds)
            {
                var file = Path.Combine(outputDirectory, $"{crd.Spec.Names.Plural}.yaml");
                var yaml = "# generated from C# entity types by Steeltoe.Informers.Crds; don't edit\n" + KubernetesYaml.Serialize(crd);
                if (!File.Exists(file) || File.ReadAllText(file) != yaml)
                    File.WriteAllText(file, yaml);
                files.Add(file);
            }

            return files;
        }

        private static bool Matches(Type type, V1CustomResourceDefinition crd)
        {
            var entity = Attribute(type, "KubernetesEntityAttribute");
            var group = Named(entity, "Group") ?? string.Empty;
            var kind = Named(entity, "Kind") ?? type.Name;
            return crd.Spec.Group == group && crd.Spec.Names.Kind == kind;
        }

        private static string ApiVersion(Type type) => Named(Attribute(type, "KubernetesEntityAttribute"), "ApiVersion") ?? "v1";

        private static void ApplyPropertyAttributes(Type type, V1JSONSchemaProps schema, int depth)
        {
            if (schema?.Properties == null || depth > 32)
                return;
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!schema.Properties.TryGetValue(JsonName(property), out var propertySchema))
                    continue;
                if (Attribute(property, "XListTypeAttribute") is { } listType)
                    propertySchema.XKubernetesListType = ((string[])["atomic", "set", "map"])[Convert.ToInt32(listType.ConstructorArguments[0].Value)];
                if (Attribute(property, "ListMapKeysAttribute") is { } keys && keys.ConstructorArguments[0].Value is IEnumerable<CustomAttributeTypedArgument> values)
                    propertySchema.XKubernetesListMapKeys = values.Select(x => (string)x.Value).ToList();
                if (Attribute(property, "XMapTypeAttribute") is { } mapType)
                    propertySchema.XKubernetesMapType = ((string[])["granular", "atomic"])[Convert.ToInt32(mapType.ConstructorArguments[0].Value)];
                var elementType = ElementType(property.PropertyType);
                if (elementType != null && propertySchema.Items is V1JSONSchemaProps items)
                    ApplyPropertyAttributes(elementType, items, depth + 1);
                else if (property.PropertyType.IsClass && property.PropertyType.FullName != "System.String")
                    ApplyPropertyAttributes(property.PropertyType, propertySchema, depth + 1);
            }
        }

        private static void ApplyPrinterColumnPriorities(Type type, V1CustomResourceDefinitionVersion version)
        {
            if (version.AdditionalPrinterColumns == null)
                return;
            var wide = new HashSet<string>(StringComparer.Ordinal);
            foreach (var attribute in CustomAttributeData.GetCustomAttributes(type).Where(a => a.AttributeType.Name == "GenericAdditionalPrinterColumnAttribute"))
            {
                var priority = attribute.NamedArguments.FirstOrDefault(x => x.MemberName == "Priority").TypedValue.Value;
                if (priority != null && Convert.ToInt32(priority) == 1)
                    wide.Add((string)attribute.ConstructorArguments[1].Value);
            }

            CollectWideColumns(type, wide, 0);
            foreach (var column in version.AdditionalPrinterColumns.Where(c => wide.Contains(c.Name)))
                column.Priority = 1;
        }

        private static void CollectWideColumns(Type type, HashSet<string> wide, int depth)
        {
            if (depth > 32)
                return;
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (Attribute(property, "AdditionalPrinterColumnAttribute") is { } column && column.ConstructorArguments.Count > 0 && Convert.ToInt32(column.ConstructorArguments[0].Value) == 1)
                    wide.Add(column.ConstructorArguments.Count > 1 && column.ConstructorArguments[1].Value is string name ? name : JsonName(property));
                if (property.PropertyType.IsClass && property.PropertyType.FullName != "System.String" && property.PropertyType.Namespace?.StartsWith("System", StringComparison.Ordinal) != true)
                    CollectWideColumns(property.PropertyType, wide, depth + 1);
            }
        }

        private static Type ElementType(Type type)
        {
            if (type.IsArray)
                return type.GetElementType();
            var enumerable = type.GetInterfaces().Append(type).FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition().FullName == "System.Collections.Generic.IEnumerable`1");
            return type.FullName != "System.String" && enumerable != null && !type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition().FullName == "System.Collections.Generic.IDictionary`2")
                ? enumerable.GenericTypeArguments[0]
                : null;
        }

        private static string JsonName(PropertyInfo property) =>
            JsonNamingPolicy.CamelCase.ConvertName(Attribute(property, "JsonPropertyNameAttribute")?.ConstructorArguments[0].Value as string ?? property.Name);

        private static bool HasAttribute(MemberInfo member, string name) => Attribute(member, name) != null;

        private static CustomAttributeData Attribute(MemberInfo member, string name) => CustomAttributeData.GetCustomAttributes(member).FirstOrDefault(a => a.AttributeType.Name == name);

        private static string Named(CustomAttributeData attribute, string name) => attribute?.NamedArguments.FirstOrDefault(x => x.MemberName == name).TypedValue.Value as string;
    }
}
