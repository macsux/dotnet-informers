using System;
using System.Collections.Generic;

namespace Steeltoe.Informers.Entities
{
    /// <summary>The property must be set.</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class)]
    public class RequiredAttribute : Attribute;

    /// <summary>The default value the API server fills in (<c>Json = true</c>: the string is JSON).</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class, Inherited = true)]
    public sealed class DefaultValueAttribute : Attribute
    {
        public DefaultValueAttribute(bool value) => Value = value;
        public DefaultValueAttribute(long value) => Value = value;
        public DefaultValueAttribute(double value) => Value = value;
        public DefaultValueAttribute(string value) => Value = value;
        public bool Json { get; init; }
        public object Value { get; }
    }

    /// <summary>The allowed values.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class EnumValuesAttribute : Attribute
    {
        public EnumValuesAttribute(params string[] values) => Values = values;
        public EnumValuesAttribute(params long[] values) => Values = Array.ConvertAll(values, x => (object)x);
        public EnumValuesAttribute(params double[] values) => Values = Array.ConvertAll(values, x => (object)x);
        public IReadOnlyList<object> Values { get; }
    }

    /// <summary>The OpenAPI format (e.g. <c>date-time</c>, <c>uri</c>).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class FormatAttribute(string format) : Attribute
    {
        public string Format => format;
    }

    /// <summary>Bounds on the number of list items (-1: unbounded).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ItemsAttribute(long minItems = -1, long maxItems = -1) : Attribute
    {
        public long? MinItems => minItems == -1 ? null : minItems;
        public long? MaxItems => maxItems == -1 ? null : maxItems;
    }

    /// <summary>Bounds on a string's length (-1: unbounded).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class LengthAttribute(long minLength = -1, long maxLength = -1) : Attribute
    {
        public long? MinLength => minLength == -1 ? null : minLength;
        public long? MaxLength => maxLength == -1 ? null : maxLength;
    }

    /// <summary>Bounds on the number of a map's entries (-1: unbounded).</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class PropertyLimitsAttribute(long minProperties = -1, long maxProperties = -1) : Attribute
    {
        public long? MinProperties => minProperties == -1 ? null : minProperties;
        public long? MaxProperties => maxProperties == -1 ? null : maxProperties;
    }

    /// <summary>The number must be a multiple of this.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class MultipleOfAttribute(double value) : Attribute
    {
        public double Value => value;
    }

    /// <summary>The string must match this regular expression.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class PatternAttribute(string regexPattern) : Attribute
    {
        public string RegexPattern => regexPattern;
    }

    /// <summary>The upper bound of a number.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class RangeMaximumAttribute(double maximum, bool exclusiveMaximum = false) : Attribute
    {
        public double Maximum => maximum;
        public bool ExclusiveMaximum => exclusiveMaximum;
    }

    /// <summary>The lower bound of a number.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class RangeMinimumAttribute(double minimum, bool exclusiveMinimum = false) : Attribute
    {
        public double Minimum => minimum;
        public bool ExclusiveMinimum => exclusiveMinimum;
    }

    /// <summary>
    ///     The list holds no duplicates. Kubernetes CRD schemas reject <c>uniqueItems</c>, so generation fails with it; use
    ///     <c>[XListType(XListType.Set)]</c>. Kept for compatibility with KubeOps' attribute set.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class UniqueItemsAttribute : Attribute;

    /// <summary>A CEL validation rule (<c>x-kubernetes-validations</c>), e.g. <c>self.size() &lt;= 10</c>.</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class, AllowMultiple = true)]
    public sealed class ValidationRuleAttribute(string rule, string fieldPath = null, string message = null, string messageExpression = null, string reason = null) : Attribute
    {
        public string Rule => rule;
        public string Message => message;
        public string MessageExpression => messageExpression;
        public string Reason => reason;
        public string FieldPath => fieldPath;
    }
}
