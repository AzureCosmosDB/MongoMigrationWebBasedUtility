using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class NameValuePair
    {
        public string Name { get; set; } = "";
        public string? Value { get; set; }
    }

    public  class MigrationUnitBasic
    {

        [JsonIgnore]
        public MigrationJob? ParentJob;

        public string Id { get; set; }
        public string JobId { get; set; }

        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public string? TargetDatabaseName { get; set; }
        public string? TargetCollectionName { get; set; }
        public long CSUpdatesInLastBatch { get; set; }

        public double CSAvgReadLatencyInMS { get; set; }
        public double CSAvgWriteLatencyInMS { get; set; }

        public DateTime? CSLastChecked { get; set; }

        public DateTime CursorUtcTimestamp { get; set; }
        public DateTime SyncBackCursorUtcTimestamp { get; set; }

        public DateTime GetCursorUtcTimestamp(bool syncBack)
            => syncBack ? SyncBackCursorUtcTimestamp : CursorUtcTimestamp;

        public void SetCursorUtcTimestamp(bool syncBack, DateTime value)
        {
            if (syncBack) SyncBackCursorUtcTimestamp = value;
            else CursorUtcTimestamp = value;
        }
        public double DumpPercent { get; set; }
        public double RestorePercent { get; set; }
        /// <summary>
        /// Tracks progress of non-unique index builds after offline data copy (0–100).
        /// Only applies when IndexingStrategy is SameAsSource or SameAsSourceBlocking.
        /// </summary>
        public double IndexPercent { get; set; }
        public bool DumpComplete { get; set; }
        public bool RestoreComplete { get; set; }
        /// <summary>
        /// Set to true when all non-unique index builds have completed on the target.
        /// Also reported true when there are no indexes to build (IndexesExpected == 0
        /// and IndexesFailed == 0) and the dump + restore for the collection are done.
        /// </summary>
        public bool IndexBuildComplete
        {
            get => _indexBuildComplete
                || (IndexesExpected == 0 && IndexesFailed == 0 && DumpComplete && RestoreComplete);
            set => _indexBuildComplete = value;
        }
        private bool _indexBuildComplete;

        /// <summary>
        /// Backing-field-only view of <see cref="IndexBuildComplete"/>. The public getter
        /// reports true whenever <see cref="IndexesExpected"/> == 0 alongside dump/restore
        /// completion, which is misleading during the post-copy window before the non-unique
        /// pre-count has run. Callers that need to know whether the index-build phase has
        /// actually finished (vs. simply not started) should read this property instead.
        /// </summary>
        [JsonIgnore]
        [BsonIgnore]
        public bool IndexBuildCompleteExplicit => _indexBuildComplete;
        /// <summary>
        /// Count of non-unique indexes that have been created on the target collection.
        /// </summary>
        public int IndexesMigrated { get; set; }
        /// <summary>
        /// Expected number of non-unique indexes to build for the collection.
        /// Used for in-progress UI display while server-side blocking builds are queued/running.
        /// </summary>
        public int IndexesExpected { get; set; }
        /// <summary>
        /// Number of non-unique indexes the server failed to build (or that never appeared on the
        /// target) after the build phase was unblocked due to a stall. Surfaced in the UI alongside
        /// the partial IndexPercent so the discrepancy is visible to the user.
        /// </summary>
        public int IndexesFailed { get; set; }

        public CollectionStatus SourceStatus { get; set; }
        public bool ResetChangeStream { get; set; }
        // Set to true when partitioning detected exactly one _id BSON type (or the user-pinned one),
        // letting downstream readers omit the $type predicate.
        public bool SkipDataTypeFilterForId { get; set; }
        // User-pinned single _id BSON type. When set, partitioning skips type probing and uses
        // only this type; null means "unknown / multiple" and lets the partitioner detect.
        public DataType? DataTypeForId { get; set; }
        public DateTime? CSLastChangeUTCTime { get; set; }
        public DateTime? SyncBackCSLastChangeUTCTime { get; set; }

        public DateTime? GetCSLastChangeUTCTime(bool syncBack)
            => syncBack ? SyncBackCSLastChangeUTCTime : CSLastChangeUTCTime;

        public ChangeStreamError OpLogError { get; set; } = ChangeStreamError.None;

        // Skip tracking for max retries exceeded
        public bool SkippedDueToMaxRetries { get; set; } = false;
        public string? FailedOperation { get; set; } = null; // "Dump" or "Restore"

        public bool Remove()
        {

            if (this.ParentJob == null) return false;

            try
            {
                var index = ParentJob.MigrationUnitBasics.FindIndex(mu => mu.Id == this.Id);
                if (index == -1) return false; // not found

                ParentJob.MigrationUnitBasics.RemoveAt(index);

                var filePath = $"migrationjobs\\{this.JobId}\\{this.Id}.json";
                MigrationJobContext.Store.DeleteDocument(filePath);

                bool saved = MigrationJobContext.SaveMigrationJob(ParentJob);
                return saved;

            }
            catch
            {
                return false;
            }
            
        }


        public bool Persist()
        {

            var filePath = $"migrationjobs\\{this.JobId}\\{this.Id}.json";

            string json = JsonConvert.SerializeObject(this, Formatting.Indented);

            return MigrationJobContext.Store.UpsertDocument(filePath, json);
        }

        public string GetEffectiveTargetDatabaseName()
        {
            return string.IsNullOrWhiteSpace(TargetDatabaseName) ? DatabaseName : TargetDatabaseName;
        }

        public string GetEffectiveTargetCollectionName()
        {
            return string.IsNullOrWhiteSpace(TargetCollectionName) ? CollectionName : TargetCollectionName;
        }

       

    }


    public class MigrationUnit:MigrationUnitBasic
    {
       
       
        public string? ResumeToken { get; set; }

        public string? GetResumeToken(bool syncBack)
            => syncBack ? SyncBackResumeToken : ResumeToken;

        public void SetResumeToken(bool syncBack, string? value)
        {
            if (syncBack) SyncBackResumeToken = value;
            else ResumeToken = value;
        }
        public string? OriginalResumeToken { get; set; }

        public bool InitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }

        [Obsolete("Use ResumeDocumentKey instead - this only contains _id, not full shard key")]
        public string? ResumeDocumentId { get; set; }
        public string? ResumeDocumentKey { get; set; }
        public DateTime? BulkCopyStartedOn { get; set; }
        public DateTime? BulkCopyEndedOn { get; set; }
        public bool TargetCreated { get; set; }

        public DateTime? ComparedOn { get; set; }
        public int VarianceCount { get; set; }

        public DateTime? ChangeStreamStartedOn { get; set; }
        
        public long CSNormalizedUpdatesInLastBatch { get; set; }
        public int CSLastBatchDurationSeconds { get; set; }
        public string? CSLastResumeTokenWithChange { get; set; }
        public string? SyncBackCSLastResumeTokenWithChange { get; set; }

        public string? UserFilter { get; set; }
        
        // Per-collection options (nullable = use defaults when not set)
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

        /// <summary>
        /// True once moveCollection has succeeded for this unit. When false and <see cref="MoveToShard"/>
        /// is set, the worker will attempt the move once before data copy begins.
        /// </summary>
        public bool MoveStatus { get; set; }

        public string? SyncBackResumeToken { get; set; }
        public string? SyncBackOriginalResumeToken { get; set; }
        public bool SyncBackInitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType SyncBackResumeTokenOperation { get; set; }
        public string? SyncBackResumeDocumentKey { get; set; }
        public DateTime? SyncBackChangeStreamStartedOn { get; set; }

        public DateTime? GetChangeStreamStartedOn(bool syncBack)
            => syncBack ? SyncBackChangeStreamStartedOn : ChangeStreamStartedOn;

        // Set-when-empty only: never overwrite or clear an existing ChangeStreamStartedOn.
        // For explicit user-initiated resets, use ForceResetChangeStreamStartedOn.
        public void SetChangeStreamStartedOn(bool syncBack, DateTime? value)
        {
            var current = syncBack ? SyncBackChangeStreamStartedOn : ChangeStreamStartedOn;
            if (current.HasValue) return;
            if (syncBack) SyncBackChangeStreamStartedOn = value;
            else ChangeStreamStartedOn = value;
        }

        // Forces a forward/rewind of ChangeStreamStartedOn, bypassing the set-when-empty guard
        // on SetChangeStreamStartedOn, and clears the matching resume token so the next
        // watch opens a fresh cursor. Also backs up the prior value into
        // OriginalChangeStreamStartedOn (one-shot: only when the backup is empty) so the
        // original user-intended start time is preserved across recovery rewinds.
        // Use only for explicit recovery/reset scenarios.
        public void ForceResetChangeStreamStartedOn(bool syncBack, DateTime newStartedOn)
        {
            var currentStartedOn = syncBack ? SyncBackChangeStreamStartedOn : ChangeStreamStartedOn;
            var originalBackup = syncBack ? SyncBackOriginalChangeStreamStartedOn : OriginalChangeStreamStartedOn;
            if (currentStartedOn.HasValue && !originalBackup.HasValue)
            {
                if (syncBack) SyncBackOriginalChangeStreamStartedOn = currentStartedOn;
                else OriginalChangeStreamStartedOn = currentStartedOn;
            }

            if (syncBack) SyncBackChangeStreamStartedOn = newStartedOn;
            else ChangeStreamStartedOn = newStartedOn;
            SetResumeToken(syncBack, null);
        }

        // Captures the ChangeStreamStartedOn value that was in effect before the
        // stuck-cursor self-healing path rewound it. Written once, on the first
        // rewind, so the original boot value is preserved for audit/diagnostics.
        public DateTime? OriginalChangeStreamStartedOn { get; set; }
        public DateTime? SyncBackOriginalChangeStreamStartedOn { get; set; }

        public DateTime? GetOriginalChangeStreamStartedOn(bool syncBack)
            => syncBack ? SyncBackOriginalChangeStreamStartedOn : OriginalChangeStreamStartedOn;

        public void SetOriginalChangeStreamStartedOn(bool syncBack, DateTime? value)
        {
            if (syncBack) SyncBackOriginalChangeStreamStartedOn = value;
            else OriginalChangeStreamStartedOn = value;
        }

        public long EstimatedDocCount { get; set; }
       
        public long ActualDocCount { get; set; }
        public long SourceCountDuringCopy { get; set; }
        public long DumpGap { get; set; }
        public long RestoreGap { get; set; }

        // Populated from collStats.avgObjSize at partition prep. 0 = unknown.
        // Used by ProcessExecutor to convert mongorestore's byte-progress output
        // into an approximate restored doc count so % completion updates mid-restore.
        public long AvgDocSizeBytes { get; set; }

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
         // Aggressive Change Stream cleanup tracking
        public bool AggressiveCacheDeleted { get; set; } = false;
        public DateTime? AggressiveCacheDeletedOn { get; set; }

        public long MaxDocsPerChunk;

        public List<MigrationChunk> MigrationChunks { get; set; }

        #region SyncBack-aware helpers

        public bool GetInitialDocumenReplayed(bool syncBack)
            => syncBack ? SyncBackInitialDocumenReplayed : InitialDocumenReplayed;

        public void SetInitialDocumenReplayed(bool syncBack, bool value)
        {
            if (syncBack) SyncBackInitialDocumenReplayed = value;
            else InitialDocumenReplayed = value;
        }

        public string? GetOriginalResumeToken(bool syncBack)
            => syncBack ? SyncBackOriginalResumeToken : OriginalResumeToken;

        public void SetOriginalResumeToken(bool syncBack, string? value)
        {
            if (syncBack) SyncBackOriginalResumeToken = value;
            else OriginalResumeToken = value;
        }

        public string? GetResumeDocumentKeyForDirection(bool syncBack)
            => syncBack ? SyncBackResumeDocumentKey : (ResumeDocumentKey ?? ResumeDocumentId);

        public ChangeStreamOperationType GetResumeTokenOperationForDirection(bool syncBack)
            => syncBack ? SyncBackResumeTokenOperation : ResumeTokenOperation;

        public void SetResumeDocumentInfo(bool syncBack, ChangeStreamOperationType opType, string? documentKey)
        {
            if (syncBack)
            {
                SyncBackResumeTokenOperation = opType;
                SyncBackResumeDocumentKey = documentKey;
            }
            else
            {
                ResumeTokenOperation = opType;
                ResumeDocumentId = documentKey;
                ResumeDocumentKey = documentKey;
            }
        }

        public string? GetCSLastResumeTokenWithChange(bool syncBack)
            => syncBack ? SyncBackCSLastResumeTokenWithChange : CSLastResumeTokenWithChange;

        public void SetCSLastChange(bool syncBack, DateTime? time, string? token)
        {
            if (syncBack)
            {
                SyncBackCSLastChangeUTCTime = time;
                SyncBackCSLastResumeTokenWithChange = token;
            }
            else
            {
                CSLastChangeUTCTime = time;
                CSLastResumeTokenWithChange = token;
            }
        }

        public void ClearResumeDocumentInfo(bool syncBack)
        {
            if (syncBack)
            {
                SyncBackInitialDocumenReplayed = false;
                SyncBackResumeDocumentKey = null;
                SyncBackResumeTokenOperation = default;
                SyncBackCSLastResumeTokenWithChange = null;
                SyncBackCSLastChangeUTCTime = null;
            }
            else
            {
                InitialDocumenReplayed = false;
                ResumeDocumentKey = null;
                ResumeTokenOperation = default;
                CSLastResumeTokenWithChange = null;
                CSLastChangeUTCTime = null;
            }
        }

        public void ResetChangeStreamCounters(bool syncBack)
        {
            if (syncBack)
            {
                SyncBackInsertEvents = 0;
                SyncBackDeleteEvents = 0;
                SyncBackUpdateEvents = 0;
                SyncBackErrors = 0;
                SyncBackDocsInserted = 0;
                SyncBackDocsDeleted = 0;
                SyncBackDocsUpdated = 0;
                SyncBackDuplicateDocsSkipped = 0;
            }
            else
            {
                CSDInsertEvents = 0;
                CSDeleteEvents = 0;
                CSUpdateEvents = 0;
                CSErrors = 0;
                CSDocsInserted = 0;
                CSDocsDeleted = 0;
                CSDocsUpdated = 0;
                CSDuplicateDocsSkipped = 0;
            }

            CSUpdatesInLastBatch = 0;
            CSNormalizedUpdatesInLastBatch = 0;
        }

        #endregion

        public MigrationUnit(MigrationJob job, string databaseName, string collectionName, List<MigrationChunk> migrationChunks)
        {
            this.Id = Helper.GenerateMigrationUnitId(databaseName, collectionName);
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;
            this.TargetDatabaseName = databaseName;
            this.TargetCollectionName = collectionName;
            this.MigrationChunks = migrationChunks;
            if (job !=null)
            {
                this.JobId = job.Id;
                this.ParentJob = job;
            }
        }


        public bool UpdateParentJob()
        {
            if (this.ParentJob == null) return false;

            try
            {
                var index = ParentJob.MigrationUnitBasics.FindIndex(mu => mu.Id == this.Id);
                if (index == -1)
                {
                    return false;
                }

                var basic = ParentJob.MigrationUnitBasics[index];

                GetBasic(basic);

                return true;
            }
            catch
            {
                return false;
            }
        }

        public MigrationUnitBasic GetBasic(MigrationUnitBasic? mub=null)
        {
            if (mub == null)
                mub = new MigrationUnitBasic();

            mub.Id = Helper.GenerateMigrationUnitId(this.DatabaseName, this.CollectionName);
            mub.JobId = this.JobId;
            mub.DatabaseName = this.DatabaseName;
            mub.CollectionName = this.CollectionName;
            mub.TargetDatabaseName = this.TargetDatabaseName;
            mub.TargetCollectionName = this.TargetCollectionName;
            mub.CSUpdatesInLastBatch = this.CSUpdatesInLastBatch;
            mub.CSAvgReadLatencyInMS = this.CSAvgReadLatencyInMS;
            mub.CSAvgWriteLatencyInMS = this.CSAvgWriteLatencyInMS;
            mub.CSLastChecked = this.CSLastChecked;
            mub.CursorUtcTimestamp = this.CursorUtcTimestamp;
            mub.SyncBackCursorUtcTimestamp = this.SyncBackCursorUtcTimestamp;
            mub.DumpPercent = this.DumpPercent;
            mub.RestorePercent = this.RestorePercent;
            mub.IndexPercent = this.IndexPercent;
            mub.DumpComplete = this.DumpComplete;
            mub.RestoreComplete = this.RestoreComplete;
            mub.IndexBuildComplete = this.IndexBuildComplete;
            mub.IndexesMigrated = this.IndexesMigrated;
            mub.IndexesExpected = this.IndexesExpected;
            mub.IndexesFailed = this.IndexesFailed;
            mub.SourceStatus = this.SourceStatus;
            mub.ResetChangeStream = this.ResetChangeStream;
            mub.SkipDataTypeFilterForId = this.SkipDataTypeFilterForId;
            mub.DataTypeForId = this.DataTypeForId;
            mub.SkippedDueToMaxRetries = this.SkippedDueToMaxRetries;
            mub.FailedOperation = this.FailedOperation;
            mub.CSLastChangeUTCTime = this.CSLastChangeUTCTime;
            mub.SyncBackCSLastChangeUTCTime = this.SyncBackCSLastChangeUTCTime;
            mub.OpLogError = this.OpLogError;
            return mub;
        }

    }
}


