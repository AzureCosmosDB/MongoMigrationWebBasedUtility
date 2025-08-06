using MongoDB.Bson;
using System;

namespace OnlineMongoMigrationProcessor.Models
{
    public class PartitionWatchState
    {
        public BsonDocument? ResumeToken { get; set; }
        public BsonDocument? StopFeedItem { get; set; }
        public string PartitionId { get; set; } = string.Empty;
        public bool IsComplete { get; set; }
        public DateTime LastProcessedTime { get; set; }
        public long ProcessedDocumentCount { get; set; }
        public string CollectionKey { get; set; } = string.Empty; // DatabaseName.CollectionName
    }
}