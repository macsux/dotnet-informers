using System.Collections.Generic;
using System.Linq;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    /// <summary>
    /// A consumer's materialized view, built only from the events it receives, plus checks of the reset/delta protocol.
    /// Values are produced from a global counter, so a key's value only ever grows: seeing a smaller one means a stale read.
    /// </summary>
    internal sealed class ConsumerView
    {
        private readonly Dictionary<int, long> _view = new Dictionary<int, long>();
        private readonly Dictionary<int, long> _maxSeen = new Dictionary<int, long>();
        private bool _started;
        private bool _inReset;

        public ConsumerView(int id) => Id = id;

        public int Id { get; }
        public List<string> Errors { get; } = new List<string>();
        public int Events { get; private set; }
        public int Resets { get; private set; }
        public IReadOnlyDictionary<int, long> View => _view;
        public bool InReset => _inReset;

        public void Apply(ResourceEvent<int, Item> e)
        {
            Events++;
            var f = e.EventFlags;
            if (f.HasFlag(EventTypeFlags.ResetEmpty))
            {
                if (_inReset)
                    Fail($"ResetEmpty inside an unfinished reset");
                _view.Clear();
                _started = true;
                Resets++;
                return;
            }

            if (f.HasFlag(EventTypeFlags.Reset))
            {
                if (f.HasFlag(EventTypeFlags.ResetStart))
                {
                    if (_inReset)
                        Fail("ResetStart before the previous reset ended");
                    _view.Clear();
                    _inReset = true;
                    _started = true;
                    Resets++;
                }
                else if (!_inReset)
                {
                    Fail($"reset item for {e.Key} outside a reset block");
                }

                if (!_view.TryAdd(e.Key, e.Value.Value))
                    Fail($"key {e.Key} twice in one reset");
                CheckMonotonic(e.Key, e.Value.Value, allowEqual: true);
                if (f.HasFlag(EventTypeFlags.ResetEnd))
                    _inReset = false;
                return;
            }

            if (!_started)
                Fail($"delta {f} for {e.Key} before the first reset");
            if (_inReset)
                Fail($"delta {f} for {e.Key} inside a reset block");

            if (f.HasFlag(EventTypeFlags.Add))
            {
                if (!_view.TryAdd(e.Key, e.Value.Value))
                    Fail($"Add for {e.Key} already in view");
                CheckMonotonic(e.Key, e.Value.Value, allowEqual: false);
            }
            else if (f.HasFlag(EventTypeFlags.Modify))
            {
                if (!_view.ContainsKey(e.Key))
                    Fail($"Modify for {e.Key} not in view");
                _view[e.Key] = e.Value.Value;
                CheckMonotonic(e.Key, e.Value.Value, allowEqual: f.HasFlag(EventTypeFlags.Sync) || f.HasFlag(EventTypeFlags.Computed));
            }
            else if (f.HasFlag(EventTypeFlags.Delete))
            {
                if (!_view.Remove(e.Key, out var old))
                    Fail($"Delete for {e.Key} not in view");
                else if (old != e.Value.Value)
                    Fail($"Delete for {e.Key} carries {e.Value.Value}, view had {old}");
            }
            else
            {
                Fail($"unexpected flags {f}");
            }
        }

        private void CheckMonotonic(int key, long value, bool allowEqual)
        {
            if (_maxSeen.TryGetValue(key, out var max) && (value < max || (value == max && !allowEqual)))
                Fail($"stale value for {key}: got {value} after {max}");
            if (value > max)
                _maxSeen[key] = value;
        }

        /// <summary>Compares the view to the cache; returns null when equal, otherwise a description.</summary>
        public string Diff(IReadOnlyDictionary<int, long> expected)
        {
            var missing = expected.Keys.Where(k => !_view.ContainsKey(k)).ToList();
            var ghosts = _view.Keys.Where(k => !expected.ContainsKey(k)).ToList();
            var stale = expected.Where(p => _view.TryGetValue(p.Key, out var v) && v != p.Value).Select(p => p.Key).ToList();
            if (missing.Count == 0 && ghosts.Count == 0 && stale.Count == 0)
                return null;
            return $"consumer {Id}: missing {missing.Count} [{string.Join(",", missing.Take(5))}], ghosts {ghosts.Count} [{string.Join(",", ghosts.Take(5))}], stale {stale.Count} [{string.Join(",", stale.Take(5))}]";
        }

        private void Fail(string message)
        {
            if (Errors.Count < 20)
                Errors.Add($"consumer {Id}: {message}");
        }
    }
}
