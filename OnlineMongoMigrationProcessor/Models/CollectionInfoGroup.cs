using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor.Models
{
    /// <summary>
    /// Wrapper for one or more CollectionInfo items added in a single operation.
    /// Preserves the group boundary for bulk-add semantics.
    /// </summary>
    public class CollectionInfoGroup
    {
        public List<CollectionInfo> Collections { get; set; } = new();
        
        /// <summary>
        /// Per-collection overwrite mode. When TRUE, target collection is dropped before migration.
        /// Defaults to false (append mode) when not set.
        /// </summary>
        public bool? Overwrite { get; set; }
        
        /// <summary>
        /// Per-collection indexing strategy. Defaults to migrating indexes when not set.
        /// </summary>
        public IndexingStrategy? IndexingStrategy { get; set; }
        
        /// <summary>
        /// Per-collection sharding strategy. Null inherits existing behavior.
        /// </summary>
        public ShardingStrategy? ShardingStrategy { get; set; }
        
        /// <summary>
        /// Target shard/node identifier for unsharded collections. Only applies when ShardingStrategy = DontShard.
        /// </summary>
        public string? MoveToShard { get; set; }
    }
    
    public enum IndexingStrategy
    {
        SameAsSource,           // Migrate indexes from source (non-blocking)
        SameAsSourceBlocking,   // Migrate indexes from source (blocking)
        DontIndex               // Skip index migration
    }
    
    public enum ShardingStrategy
    {
        SameAsSource,   // Preserve source sharding configuration
        DontShard       // Create unsharded collection
    }
}
