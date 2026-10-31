using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using AwesomeAssertions;
using k8s;
using k8s.Models;
using Steeltoe.Informers.Entities;
using Xunit;

namespace Steeltoe.Informers.Crds.Tests
{
    /// <summary>
    ///     R1.11: runs KubeOps' Transpiler (through the generator) over our own attributes, so a change to how it matches attributes (by simple
    ///     name) or to their constructor shapes fails this build instead of silently dropping schema rules.
    /// </summary>
    public class CompatibilityTests
    {
        private static readonly IReadOnlyList<V1CustomResourceDefinition> Crds = CrdGenerator.Generate(typeof(CompatibilityTests).Assembly.Location);

        private static V1CustomResourceDefinition Crd(string plural) => Crds.Single(x => x.Spec.Names.Plural == plural);

        private static V1JSONSchemaProps Spec(string plural, string version = "v1") =>
            Crd(plural).Spec.Versions.Single(v => v.Name == version).Schema.OpenAPIV3Schema.Properties["spec"];

        [Fact]
        public void EntityMetadata_ShortNamesScopeAndStatusSubresource()
        {
            var crd = Crd("widgets");
            crd.Metadata.Name.Should().Be("widgets.compat.example.com");
            crd.Spec.Names.Kind.Should().Be("Widget");
            crd.Spec.Names.ShortNames.Should().Equal("wd");
            crd.Spec.Scope.Should().Be("Namespaced");
            crd.Spec.Versions.Single().Subresources.Status.Should().NotBeNull();
            crd.Spec.Versions.Single().Subresources.Scale.SpecReplicasPath.Should().Be(".spec.size");
            Crd("clusterwidgets").Spec.Scope.Should().Be("Cluster", "EntityScope is an enum argument, applied by the generator");
        }

        [Fact]
        public void Documentation_DescriptionTitleExternalDocsExample()
        {
            var crd = Crd("widgets").Spec.Versions.Single().Schema.OpenAPIV3Schema;
            crd.Description.Should().Be("A test widget.");
            var size = Spec("widgets").Properties["size"];
            size.Description.Should().Be("How big.");
            size.Title.Should().Be("Size");
            size.ExternalDocs.Url.Should().Be("https://example.com/size");
            size.Example.Should().Be(3L);
        }

        [Fact]
        public void Validation_RangesLengthsPatternsEnumsDefaultsAndCel()
        {
            var spec = Spec("widgets");
            spec.Required.Should().Contain("size");
            var size = spec.Properties["size"];
            size.Minimum.Should().Be(1);
            size.Maximum.Should().Be(100);
            size.ExclusiveMaximum.Should().BeTrue();
            size.MultipleOf.Should().Be(1);
            size.DefaultProperty.Should().Be(3L);
            var name = spec.Properties["displayName"];
            name.MinLength.Should().Be(1);
            name.MaxLength.Should().Be(63);
            name.Pattern.Should().Be("^[a-z]+$");
            name.Format.Should().Be("hostname");
            spec.Properties["color"].EnumProperty.Should().Equal("red", "green");
            spec.Properties["tags"].MinItems.Should().Be(1);
            spec.Properties["labels"].MaxProperties.Should().Be(10);
            spec.Properties["labels"].XKubernetesValidations.Single().Rule.Should().Be("self.size() <= 10");
            Crd("widgets").Spec.Versions.Single().Schema.OpenAPIV3Schema.XKubernetesValidations.Single().Message.Should().Be("spec required");
        }

        [Fact]
        public void Structure_IgnoredPreservedEmbeddedAndKubernetesTypes()
        {
            var spec = Spec("widgets");
            spec.Properties.Should().NotContainKey("internalOnly");
            spec.Properties["raw"].XKubernetesPreserveUnknownFields.Should().BeTrue();
            spec.Properties["template"].XKubernetesEmbeddedResource.Should().BeTrue();
            spec.Properties["port"].XKubernetesIntOrString.Should().BeTrue();
            spec.Properties["memory"].Type.Should().Be("string");
            Crds.Should().NotContain(x => x.Spec.Names.Kind == "Ignored");
        }

        [Fact]
        public void ServerSideApply_ListAndMapTypes()
        {
            var spec = Spec("widgets");
            spec.Properties["tags"].XKubernetesListType.Should().Be("set", "XListType is an enum argument, applied by the generator");
            spec.Properties["ports"].XKubernetesListType.Should().Be("map");
            spec.Properties["ports"].XKubernetesListMapKeys.Should().Equal("name");
            spec.Properties["labels"].XKubernetesMapType.Should().Be("atomic");
        }

        [Fact]
        public void PrinterColumns_WithWideViewPriority()
        {
            var columns = Crd("widgets").Spec.Versions.Single().AdditionalPrinterColumns;
            columns.Single(c => c.Name == "Size").Should().Match<V1CustomResourceColumnDefinition>(c => c.JsonPath == ".spec.size" && c.Priority == 0);
            columns.Single(c => c.Name == "Color").Priority.Should().Be(1, "PrinterColumnPriority is an enum argument, applied by the generator");
            columns.Single(c => c.Name == "Age").Should().Match<V1CustomResourceColumnDefinition>(c => c.JsonPath == ".metadata.creationTimestamp" && c.Type == "date" && c.Priority == 1);
        }

        [Fact]
        public void Versions_CombineIntoOneCrd_WithOneStorageVersion()
        {
            var versions = Crd("gadgets").Spec.Versions;
            versions.Select(v => v.Name).Should().BeEquivalentTo("v1", "v2");
            versions.Single(v => v.Storage).Name.Should().Be("v1");
            versions.Should().OnlyContain(v => v.Served);
        }
    }

    [KubernetesEntity(Group = "compat.example.com", ApiVersion = "v1", Kind = "Widget", PluralName = "widgets")]
    [KubernetesEntityShortNames("wd")]
    [Description("A test widget.")]
    [ScaleSubresource(".spec.size", ".status.size")]
    [ValidationRule("has(self.spec)", message: "spec required")]
    [GenericAdditionalPrinterColumn(".metadata.creationTimestamp", "Age", "date", Priority = PrinterColumnPriority.WideView)]
    public sealed class Widget : IKubernetesObject<V1ObjectMeta>, ISpec<WidgetSpec>, IStatus<WidgetStatus>
    {
        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }
        public WidgetSpec Spec { get; set; }
        public WidgetStatus Status { get; set; }
    }

    public sealed class WidgetSpec
    {
        [Required]
        [Description("How big.")]
        [Title("Size")]
        [ExternalDocs("https://example.com/size")]
        [Example(3L)]
        [RangeMinimum(1)]
        [RangeMaximum(100, exclusiveMaximum: true)]
        [MultipleOf(1)]
        [DefaultValue(3L)]
        [AdditionalPrinterColumn(name: "Size")]
        public int Size { get; set; }

        [JsonPropertyName("displayName")]
        [Length(1, 63)]
        [Pattern("^[a-z]+$")]
        [Format("hostname")]
        public string Name { get; set; }

        [EnumValues("red", "green")]
        [AdditionalPrinterColumn(PrinterColumnPriority.WideView, "Color")]
        public string Color { get; set; }

        [Items(minItems: 1)]
        [XListType(XListType.Set)]
        public List<string> Tags { get; set; }

        [XListType(XListType.Map)]
        [ListMapKeys("name")]
        public List<WidgetPort> Ports { get; set; }

        [PropertyLimits(maxProperties: 10)]
        [ValidationRule("self.size() <= 10")]
        [XMapType(XMapType.Atomic)]
        public Dictionary<string, string> Labels { get; set; }

        [Ignore]
        public string InternalOnly { get; set; }

        [PreserveUnknownFields]
        public object Raw { get; set; }

        [EmbeddedResource]
        public V1ConfigMap Template { get; set; }

        public IntOrString Port { get; set; }

        public ResourceQuantity Memory { get; set; }
    }

    public sealed class WidgetPort
    {
        public string Name { get; set; }
        public int Number { get; set; }
    }

    public sealed class WidgetStatus
    {
        public int Size { get; set; }
    }

    [KubernetesEntity(Group = "compat.example.com", ApiVersion = "v1", Kind = "ClusterWidget", PluralName = "clusterwidgets")]
    [EntityScope(EntityScope.Cluster)]
    public sealed class ClusterWidget : IKubernetesObject<V1ObjectMeta>
    {
        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }
    }

    [KubernetesEntity(Group = "compat.example.com", ApiVersion = "v1", Kind = "Gadget", PluralName = "gadgets")]
    [StorageVersion]
    public sealed class GadgetV1 : IKubernetesObject<V1ObjectMeta>
    {
        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }
        public int Size { get; set; }
    }

    [KubernetesEntity(Group = "compat.example.com", ApiVersion = "v2", Kind = "Gadget", PluralName = "gadgets")]
    public sealed class GadgetV2 : IKubernetesObject<V1ObjectMeta>
    {
        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }
        public string Size { get; set; }
    }

    [KubernetesEntity(Group = "compat.example.com", ApiVersion = "v1", Kind = "Ignored", PluralName = "ignoreds")]
    [Ignore]
    public sealed class Ignored : IKubernetesObject<V1ObjectMeta>
    {
        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }
    }
}
