using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    public class CollectionInfo
    {
        public required string CollectionName { get; set; }
        public required string DatabaseName { get; set; }
        public string? TargetCollectionName { get; set; }
        public string? TargetDatabaseName { get; set; }
        public string? Filter { get; set; }

        // Null leaves the per-unit default (append, and the default indexing strategy), so an
        // existing payload that omits these behaves exactly as before.
        public bool? Overwrite { get; set; }
        public IndexingStrategy? IndexingStrategy { get; set; }
    }
}
