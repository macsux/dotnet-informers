using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Translates <c>Where</c> terms on Kubernetes objects into a namespace, label selectors and field selectors. Recognized terms:
    ///     <list type="bullet">
    ///         <item><c>x.Metadata.NamespaceProperty == "ns"</c> or <c>x.Namespace() == "ns"</c>; <c>!=</c> becomes a field selector</item>
    ///         <item><c>x.Metadata.Name == "n"</c> or <c>x.Name()</c>, <c>==</c> and <c>!=</c></item>
    ///         <item><c>x.GetLabel("k") == "v"</c>, <c>!= "v"</c>, <c>== null</c>, <c>!= null</c>; <c>x.Metadata.Labels["k"] == "v"</c></item>
    ///         <item><c>x.Metadata.Labels.ContainsKey("k")</c> and its negation</item>
    ///         <item><c>values.Contains(x.GetLabel("k"))</c> (<c>in</c>) and its negation (<c>notin</c>)</item>
    ///         <item>field selectors the resource type supports, e.g. pod <c>x.Spec.NodeName</c> and <c>x.Status.Phase</c></item>
    ///     </list>
    ///     Compared values must be constants or captured variables. Anything else stays client-side.
    /// </summary>
    public sealed class KubernetesFilterTranslator<T> : IFilterTranslator<T> where T : IKubernetesObject<V1ObjectMeta>
    {
        public bool TryTranslate(Expression term, ParameterExpression parameter, ServerFilter current, out ServerFilter result) =>
            KubernetesFilterTranslator.TryTranslate(typeof(T), term, parameter, current, out result);
    }

    /// <summary>Term kinds of the Kubernetes server filter and the translation logic.</summary>
    public static class KubernetesFilterTranslator
    {
        public const string Namespace = "namespace";
        public const string Label = "label";
        public const string Field = "field";

        private static readonly Regex LabelKeyPattern = new Regex("^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$", RegexOptions.Compiled);
        private static readonly Regex LabelValuePattern = new Regex("^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$", RegexOptions.Compiled);
        private static readonly Regex FieldValuePattern = new Regex("^[^,=!\\\\()\\s]+$", RegexOptions.Compiled);

        /// <summary>Field selectors per model type, keyed by the property path from the object (e.g. <c>Spec.NodeName</c>).</summary>
        private static readonly Dictionary<Type, Dictionary<string, string>> FieldSelectors = new Dictionary<Type, Dictionary<string, string>>
        {
            [typeof(V1Pod)] = new Dictionary<string, string>
            {
                ["Spec.NodeName"] = "spec.nodeName",
                ["Spec.RestartPolicy"] = "spec.restartPolicy",
                ["Spec.SchedulerName"] = "spec.schedulerName",
                ["Spec.ServiceAccountName"] = "spec.serviceAccountName",
                ["Status.Phase"] = "status.phase",
                ["Status.PodIP"] = "status.podIP",
                ["Status.NominatedNodeName"] = "status.nominatedNodeName",
            },
            [typeof(V1Secret)] = new Dictionary<string, string> { ["Type"] = "type" },
            [typeof(V1Namespace)] = new Dictionary<string, string> { ["Status.Phase"] = "status.phase" },
            [typeof(Corev1Event)] = new Dictionary<string, string>
            {
                ["InvolvedObject.Kind"] = "involvedObject.kind",
                ["InvolvedObject.Name"] = "involvedObject.name",
                ["InvolvedObject.NamespaceProperty"] = "involvedObject.namespace",
                ["InvolvedObject.Uid"] = "involvedObject.uid",
                ["Reason"] = "reason",
                ["Type"] = "type",
            },
        };

        internal static bool TryTranslate(Type resourceType, Expression term, ParameterExpression parameter, ServerFilter current, out ServerFilter result)
        {
            result = null;
            var negated = false;
            while (term is UnaryExpression { NodeType: ExpressionType.Not } not)
            {
                negated = !negated;
                term = not.Operand;
            }

            switch (term)
            {
                case BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual } binary:
                    var equal = (binary.NodeType == ExpressionType.Equal) != negated;
                    return TryComparison(resourceType, binary.Left, binary.Right, equal, parameter, current, out result)
                           || TryComparison(resourceType, binary.Right, binary.Left, equal, parameter, current, out result);
                case MethodCallExpression call:
                    return TryMethod(call, negated, parameter, current, out result);
                default:
                    return false;
            }
        }

        private static bool TryComparison(Type resourceType, Expression accessor, Expression valueExpression, bool equal, ParameterExpression parameter, ServerFilter current, out ServerFilter result)
        {
            result = null;
            if (!TryEvaluate(valueExpression, parameter, out var value) || (value != null && value is not string))
                return false;
            var text = (string)value;
            var target = Classify(resourceType, Strip(accessor), parameter);
            switch (target.Kind)
            {
                case TargetKind.Namespace when text != null:
                    if (!equal)
                        return TryAdd(current, Field, $"metadata.namespace!={text}", FieldValuePattern.IsMatch(text), out result);
                    var existing = current.ValuesOf(Namespace).ToList();
                    if (existing.Count > 0)
                    {
                        // ns == a && ns == a is redundant; ns == a && ns == b can't be pushed (one namespace per subscription)
                        result = existing.Contains(text) ? current : null;
                        return result != null;
                    }

                    return TryAdd(current, Namespace, text, FieldValuePattern.IsMatch(text), out result);
                case TargetKind.Name when text != null:
                    return TryAdd(current, Field, $"metadata.name{(equal ? "=" : "!=")}{text}", FieldValuePattern.IsMatch(text), out result);
                case TargetKind.Field when text != null:
                    return TryAdd(current, Field, $"{target.Path}{(equal ? "=" : "!=")}{text}", FieldValuePattern.IsMatch(text), out result);
                case TargetKind.LabelValue:
                    if (!LabelKeyPattern.IsMatch(target.Path))
                        return false;
                    if (text == null)
                        return TryAdd(current, Label, equal ? $"!{target.Path}" : target.Path, true, out result);
                    return TryAdd(current, Label, $"{target.Path}{(equal ? "=" : "!=")}{text}", LabelValuePattern.IsMatch(text), out result);
                case TargetKind.LabelIndexer when equal && text != null:
                    // Labels["k"] throws client-side for objects without the label, so only == is pushed (it excludes them on both sides)
                    return TryAdd(current, Label, $"{target.Path}={text}", LabelKeyPattern.IsMatch(target.Path) && LabelValuePattern.IsMatch(text), out result);
                default:
                    return false;
            }
        }

        private static bool TryMethod(MethodCallExpression call, bool negated, ParameterExpression parameter, ServerFilter current, out ServerFilter result)
        {
            result = null;
            // x.Metadata.Labels.ContainsKey("k")
            if (call.Method.Name == "ContainsKey" && call.Object != null && IsLabels(Strip(call.Object), parameter) && call.Arguments.Count == 1)
            {
                if (!TryEvaluate(call.Arguments[0], parameter, out var key) || key is not string labelKey || !LabelKeyPattern.IsMatch(labelKey))
                    return false;
                return TryAdd(current, Label, negated ? $"!{labelKey}" : labelKey, true, out result);
            }

            // values.Contains(x.GetLabel("k")) / Enumerable.Contains(values, x.GetLabel("k"))
            if (call.Method.Name == "Contains")
            {
                Expression collection, item;
                if (call.Object == null && call.Arguments.Count == 2)
                {
                    collection = call.Arguments[0];
                    item = call.Arguments[1];
                }
                else if (call.Object != null && call.Arguments.Count == 1)
                {
                    collection = call.Object;
                    item = call.Arguments[0];
                }
                else
                {
                    return false;
                }

                // C# 14 binds array.Contains(x) to MemoryExtensions.Contains(ReadOnlySpan<T>, T) through an implicit span conversion
                if (Strip(collection) is MethodCallExpression { Method.Name: "op_Implicit", Arguments.Count: 1 } spanConversion)
                    collection = spanConversion.Arguments[0];
                var target = Classify(null, Strip(item), parameter);
                if (target.Kind != TargetKind.LabelValue && !(target.Kind == TargetKind.LabelIndexer && !negated))
                    return false;
                if (!LabelKeyPattern.IsMatch(target.Path) || !TryEvaluate(collection, parameter, out var values) || values is not IEnumerable enumerable || values is string)
                    return false;
                var list = new SortedSet<string>(StringComparer.Ordinal);
                foreach (var v in enumerable)
                {
                    if (v is not string s || !LabelValuePattern.IsMatch(s))
                        return false;
                    list.Add(s);
                }

                if (list.Count == 0)
                    return false;
                return TryAdd(current, Label, $"{target.Path} {(negated ? "notin" : "in")} ({string.Join(",", list)})", true, out result);
            }

            return false;
        }

        private static bool TryAdd(ServerFilter current, string kind, string value, bool valid, out ServerFilter result)
        {
            result = valid ? current.With(new ServerFilterTerm(kind, value)) : null;
            return valid;
        }

        private enum TargetKind
        {
            None,
            Namespace,
            Name,
            LabelValue,
            LabelIndexer,
            Field,
        }

        private readonly struct Target
        {
            public Target(TargetKind kind, string path = null)
            {
                Kind = kind;
                Path = path;
            }

            public TargetKind Kind { get; }
            public string Path { get; }
        }

        private static Target Classify(Type resourceType, Expression accessor, ParameterExpression parameter)
        {
            switch (accessor)
            {
                // x.Namespace(), x.Name(), x.GetLabel("k") (k8s.Models.ModelExtensions)
                case MethodCallExpression { Method.DeclaringType: var type } call when type == typeof(ModelExtensions) && call.Arguments.Count >= 1 && Strip(call.Arguments[0]) == parameter:
                    switch (call.Method.Name)
                    {
                        case nameof(ModelExtensions.Namespace) when call.Arguments.Count == 1:
                            return new Target(TargetKind.Namespace);
                        case nameof(ModelExtensions.Name) when call.Arguments.Count == 1:
                            return new Target(TargetKind.Name);
                        case nameof(ModelExtensions.GetLabel) when call.Arguments.Count == 2 && TryEvaluate(call.Arguments[1], parameter, out var key) && key is string labelKey:
                            return new Target(TargetKind.LabelValue, labelKey);
                    }

                    return default;
                // x.Metadata.Labels["k"]
                case MethodCallExpression { Method.Name: "get_Item" } indexer when indexer.Object != null && IsLabels(Strip(indexer.Object), parameter) && indexer.Arguments.Count == 1:
                    return TryEvaluate(indexer.Arguments[0], parameter, out var indexKey) && indexKey is string k ? new Target(TargetKind.LabelIndexer, k) : default;
                case MemberExpression member:
                    var path = MemberPath(member, parameter);
                    if (path == null)
                        return default;
                    if (path == "Metadata.NamespaceProperty")
                        return new Target(TargetKind.Namespace);
                    if (path == "Metadata.Name")
                        return new Target(TargetKind.Name);
                    if (resourceType != null && FieldSelectors.TryGetValue(resourceType, out var fields) && fields.TryGetValue(path, out var field) && member.Type == typeof(string))
                        return new Target(TargetKind.Field, field);
                    return default;
                default:
                    return default;
            }
        }

        private static bool IsLabels(Expression expression, ParameterExpression parameter) =>
            expression is MemberExpression member && MemberPath(member, parameter) == "Metadata.Labels";

        /// <summary>"A.B.C" for <c>x.A.B.C</c> on the parameter, or null.</summary>
        private static string MemberPath(MemberExpression member, ParameterExpression parameter)
        {
            var names = new Stack<string>();
            Expression current = member;
            while (current is MemberExpression m)
            {
                names.Push(m.Member.Name);
                current = Strip(m.Expression);
            }

            return current == parameter ? string.Join(".", names) : null;
        }

        private static Expression Strip(Expression expression)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs } unary)
                expression = unary.Operand;
            return expression;
        }

        /// <summary>Evaluates an expression that doesn't depend on the parameter (a constant or captured variable).</summary>
        private static bool TryEvaluate(Expression expression, ParameterExpression parameter, out object value)
        {
            value = null;
            if (expression == null || References(expression, parameter))
                return false;
            if (expression is ConstantExpression constant)
            {
                value = constant.Value;
                return true;
            }

            try
            {
                value = Expression.Lambda(Expression.Convert(expression, typeof(object))).Compile(preferInterpretation: true).DynamicInvoke();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static bool References(Expression expression, ParameterExpression parameter)
        {
            var finder = new ParameterFinder(parameter);
            finder.Visit(expression);
            return finder.Found;
        }

        private sealed class ParameterFinder : ExpressionVisitor
        {
            private readonly ParameterExpression _parameter;

            public ParameterFinder(ParameterExpression parameter) => _parameter = parameter;

            public bool Found { get; private set; }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (node == _parameter)
                    Found = true;
                return node;
            }
        }
    }
}
