using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;


namespace OnlineMongoMigrationProcessor.Helpers
{
    internal  class SafeDictionary<TKey, TValue> where TKey : notnull
    {
        private  readonly ConcurrentDictionary<TKey, TValue> _dictionary = new();

        public int Count => _dictionary.Count;

        public  void AddOrUpdate(TKey key, TValue value)
        {
            _dictionary[key] = value; // Adds or updates safely
        }

        public  bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            return _dictionary.TryGetValue(key, out value);
        }

        public  bool Remove(TKey key)
        {
            return _dictionary.TryRemove(key, out _);
        }

        public  bool ContainsKey(TKey key)
        {
            return _dictionary.ContainsKey(key);
        }

        public  void Clear()
        {
            _dictionary.Clear(); // Not atomic per se, but thread-safe for general use
        }

        public  IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            return _dictionary.ToArray(); // Snapshot of the current state
        }

        public bool TryGetFirst(out KeyValuePair<TKey, TValue> mu)
        {
            foreach (var kvp in _dictionary)
            {
                mu = kvp;
                return true;
            }

            mu = default!;
            return false;
        }
    }
}
