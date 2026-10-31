using System;

namespace Steeltoe.Informers.Entities
{
    /// <summary>The schema description of a type or property.</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class)]
    public class DescriptionAttribute(string description) : Attribute
    {
        public string Description => description;
    }

    /// <summary>The schema title of a type or property.</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class)]
    public class TitleAttribute(string title) : Attribute
    {
        public string Title => title;
    }

    /// <summary>A link to external documentation of the property.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ExternalDocsAttribute(string url, string description = null) : Attribute
    {
        public string Description => description;
        public string Url => url;
    }

    /// <summary>An example value of the property (<c>Json = true</c>: the string is JSON).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ExampleAttribute : Attribute
    {
        public ExampleAttribute(bool value) => Value = value;
        public ExampleAttribute(long value) => Value = value;
        public ExampleAttribute(double value) => Value = value;
        public ExampleAttribute(string value) => Value = value;
        public bool Json { get; init; }
        public object Value { get; }
    }
}
