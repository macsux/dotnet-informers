using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     Turns a selected value into something that compares by value with <see cref="object.Equals(object, object)" />, without reflection:
    ///     primitives, strings, enums and anonymous types stay as they are; dictionaries and collections become snapshots compared entry by
    ///     entry; other objects become their JSON through <see cref="InformerJson" /> (or stay as they are when no metadata exists for them, so
    ///     they compare with their own <c>Equals</c>).
    /// </summary>
    internal static class StructuralValue
    {
        public static object Capture(object value)
        {
            switch (value)
            {
                case null:
                case string:
                case Enum:
                case ValueType when value is not JsonElement:
                    return value;
                case JsonElement element:
                    return new JsonText(element.GetRawText());
                case IDictionary dictionary:
                {
                    var entries = new List<KeyValuePair<string, object>>(dictionary.Count);
                    foreach (DictionaryEntry entry in dictionary)
                        entries.Add(new KeyValuePair<string, object>(Convert.ToString(entry.Key, System.Globalization.CultureInfo.InvariantCulture), Capture(entry.Value)));
                    entries.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
                    return new DictionarySnapshot(entries);
                }

                case IEnumerable sequence:
                {
                    var items = new List<object>();
                    foreach (var item in sequence)
                        items.Add(Capture(item));
                    return new SequenceSnapshot(items);
                }
            }

            var type = value.GetType();
            if (IsAnonymous(type))
                return value; // compiler-generated Equals compares the members
            return InformerJson.TryGetTypeInfo(type, out var info) ? new JsonText(JsonSerializer.Serialize(value, info)) : value;
        }

        private static bool IsAnonymous(Type type) => type.Name.StartsWith("<>f__AnonymousType", StringComparison.Ordinal) || type.Name.StartsWith("VB$AnonymousType", StringComparison.Ordinal);

        private sealed record JsonText(string Json);

        private sealed class SequenceSnapshot : IEquatable<SequenceSnapshot>
        {
            private readonly List<object> _items;

            public SequenceSnapshot(List<object> items) => _items = items;

            public bool Equals(SequenceSnapshot other) => other != null && _items.SequenceEqual(other._items);
            public override bool Equals(object obj) => Equals(obj as SequenceSnapshot);
            public override int GetHashCode() => _items.Count;
        }

        private sealed class DictionarySnapshot : IEquatable<DictionarySnapshot>
        {
            private readonly List<KeyValuePair<string, object>> _entries;

            public DictionarySnapshot(List<KeyValuePair<string, object>> entries) => _entries = entries;

            public bool Equals(DictionarySnapshot other) =>
                other != null && _entries.Count == other._entries.Count && _entries.Zip(other._entries).All(x => x.First.Key == x.Second.Key && Equals(x.First.Value, x.Second.Value));

            public override bool Equals(object obj) => Equals(obj as DictionarySnapshot);
            public override int GetHashCode() => _entries.Count;
        }
    }
}
