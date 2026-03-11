using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class JobList
    {
        // Legacy property for backward compatibility - will be removed in future versions
        // This will only be deserialized if present in JSON, but never serialized
        [JsonProperty("MigrationJobs")]
        private List<MigrationJob>? _migrationJobsBackingField
        {
            get => null; // Never serialize this property - returns null so JSON.NET won't include it
            set => MigrationJobs = value; // Allow deserialization - set the public property
        }

        [JsonIgnore]
        public List<MigrationJob>? MigrationJobs { get; set; }

        public List<string>? MigrationJobIds { get; set; }
               
        private static readonly object _writeLock = new object();
        private static readonly object _loadLock = new object();
        private Log _log;

        public class ConnectionAccessor
        {
            private readonly Dictionary<string, string> _dict;

            public ConnectionAccessor(Dictionary<string, string> dict)
            {
                _dict = dict;
            }

            // Indexer to get/set by jobId
            public string this[string jobId]
            {
                get => _dict.TryGetValue(jobId, out var value) ? value : null;
                set => _dict[jobId] = value;
            }

            // Add this property to expose dictionary keys
            public IEnumerable<string> Keys => _dict.Keys;
        }

        public bool Persist()
        {
            lock (_writeLock)
            {
                var filePath = $"migrationjobs\\joblist.json";
                string json = JsonConvert.SerializeObject(this, Formatting.Indented);
                return MigrationJobContext.Store.UpsertDocument(filePath, json);
            }
        }

        public void SetLog(Log _log)
        {
            this._log = _log;
        }


         
    }
}