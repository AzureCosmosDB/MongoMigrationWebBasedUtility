using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class MigrationUnit
    {
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public string? ResumeToken { get; set; }
        public string? OriginalResumeToken { get; set; }

        public bool InitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }

        [JsonProperty("ResumeDocumentId")]
        private object? _resumeDocumentIdRaw { get; set; }

        [JsonIgnore]
        public BsonDocument? ResumeDocumentId
        {
            get => _resumeDocumentIdRaw != null ? BsonDocument.Create(_resumeDocumentIdRaw) : null;
            set => _resumeDocumentIdRaw = value;
        }


        public DateTime? BulkCopyStartedOn { get; set; }
        public DateTime? BulkCopyEndedOn { get; set; }
        public int IndexesMigrated { get; set; }

        public DateTime? ComparedOn { get; set; }
        public int VarianceCount { get; set; }

        public DateTime? ChangeStreamStartedOn { get; set; }
        public DateTime CursorUtcTimestamp { get; set; }
        public long CSUpdatesInLastBatch { get; set; }
        public long CSNormalizedUpdatesInLastBatch { get; set; }
        public int CSLastBatchDurationSeconds { get; set; }

        public string? UserFilter { get; set; }
        public string? SyncBackResumeToken { get; set; }
        public DateTime? SyncBackChangeStreamStartedOn { get; set; }
        public DateTime SyncBackCursorUtcTimestamp { get; set; }

        public double DumpPercent { get; set; }
        public double RestorePercent { get; set; }
        public bool DumpComplete { get; set; }
        public bool RestoreComplete { get; set; }
        public bool ResetChangeStream { get; set; }
        public long EstimatedDocCount { get; set; }
        public CollectionStatus SourceStatus { get; set; }
        public long ActualDocCount { get; set; }
        public long SourceCountDuringCopy { get; set; }
        public long DumpGap { get; set; }
        public long RestoreGap { get; set; }

        public long CSDInsertEvents { get; set; }
        public long CSDeleteEvents { get; set; }
        public long CSUpdateEvents { get; set; }
        public long CSErrors { get; set; }

        public long CSDocsInserted { get; set; }
        public long CSDocsDeleted { get; set; }
        public long CSDocsUpdated { get; set; }
        public long CSDuplicateDocsSkipped { get; set; }

        public long SyncBackInsertEvents { get; set; }
        public long SyncBackDeleteEvents { get; set; }
        public long SyncBackUpdateEvents { get; set; }
        public long SyncBackErrors { get; set; }

        public long SyncBackDocsInserted { get; set; }
        public long SyncBackDocsDeleted { get; set; }
        public long SyncBackDocsUpdated { get; set; }
        public long SyncBackDuplicateDocsSkipped { get; set; }

        public List<MigrationChunk> MigrationChunks { get; set; }

        public MigrationUnit(string databaseName, string collectionName, List<MigrationChunk> migrationChunks)
        {
            DatabaseName = databaseName;
            CollectionName = collectionName;
            MigrationChunks = migrationChunks;
        }
    }
}