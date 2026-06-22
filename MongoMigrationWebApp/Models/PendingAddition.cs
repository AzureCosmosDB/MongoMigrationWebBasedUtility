using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Models;
using System;

namespace MongoMigrationWebApp.Models
{
    /// <summary>
    /// Represents a draft collection addition before it's committed to the job.
    /// Used in ManageCollections for temporary state management.
    /// </summary>
    public record PendingAddition
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public required string DatabaseName { get; init; }
        public required string CollectionName { get; init; }
        public string? TargetDatabaseName { get; init; }
        public string? TargetCollectionName { get; init; }
        public string? Filter { get; init; }
        
        // Per-collection options
        public bool? Overwrite { get; init; }
        public IndexingStrategy? IndexingStrategy { get; init; }
        public ShardingStrategy? ShardingStrategy { get; init; }
        public string? MoveToShard { get; init; }
        // User-pinned single _id BSON type. Null means "Unknown / Multiple" (let partitioner detect).
        public DataType? DataTypeForId { get; init; }
        
        /// <summary>
        /// Converts this draft to a MigrationUnit for commitment.
        /// </summary>
        public void ApplyToMigrationUnit(MigrationUnit unit)
        {
            unit.DatabaseName = DatabaseName;
            unit.CollectionName = CollectionName;
            unit.TargetDatabaseName = TargetDatabaseName ?? DatabaseName;
            unit.TargetCollectionName = TargetCollectionName ?? CollectionName;
            unit.UserFilter = Filter;
            unit.Overwrite = Overwrite;
            unit.IndexingStrategy = IndexingStrategy;
            unit.ShardingStrategy = ShardingStrategy;
            unit.MoveToShard = MoveToShard;
            unit.DataTypeForId = DataTypeForId;
        }
    }
}
