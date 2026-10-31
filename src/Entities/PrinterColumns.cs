using System;

namespace Steeltoe.Informers.Entities
{
    /// <summary>Whether a printer column shows in the standard view or only in the wide view (<c>-o wide</c>).</summary>
    public enum PrinterColumnPriority
    {
        StandardView,
        WideView,
    }

    /// <summary>Shows the property as a column of <c>kubectl get</c>.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class AdditionalPrinterColumnAttribute(PrinterColumnPriority priority = default, string name = null) : Attribute
    {
        public string Name => name;
        public PrinterColumnPriority Priority => priority;
    }

    /// <summary>A <c>kubectl get</c> column from any JSON path of the object.</summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class GenericAdditionalPrinterColumnAttribute(string jsonPath, string name, string type) : Attribute
    {
        public string JsonPath => jsonPath;
        public string Name => name;
        public string Type => type;
        public string Description { get; init; }
        public string Format { get; init; }
        public PrinterColumnPriority Priority { get; init; }
    }
}
