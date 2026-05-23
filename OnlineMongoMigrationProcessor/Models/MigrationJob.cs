using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Models;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using OnlineMongoMigrationProcessor.Context;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class MigrationJob
    {

        public string Id { get; set; }
        public string? Name { get; set; }
        public string? SourceEndpoint { get; set; }
        public string? TargetEndpoint { get; set; }

        public string? SourceServerVersion { get; set; }
        public string? NameSpaces { get; set; }
        
        public bool AllCollectionsUseObjectId { get; set; }
        
        public DateTime? StartedOn { get; set; }
        public bool IsCompleted { get; set; }

        public bool IsCancelled { get; set; }
        public bool IsStarted { get; set; }
        public JobType JobType { get; set; } = JobType.MongoDriver;        
       
        public CDCMode CDCMode { get; set; } = CDCMode.Offline;

        public bool IsSimulatedRun { get; set; }
        public bool SkipIndexes { get; set; }
        public bool AppendMode { get; set; }
        public bool SyncBackEnabled { get; set; }
        public bool ProcessingSyncBack { get; set; }
        public bool RunComparison { get; set; }

        public DateTime? CSLastChecked { get; set; }

        public ChangeStreamMode ChangeStreamMode { get; set; } = ChangeStreamMode.Immediate;
        
               
        public bool CSPostProcessingStarted { get; set; }
        public ChangeStreamLevel ChangeStreamLevel { get; set; }

        /// <summary>
        /// When true, server-level change stream skips the $match pipeline and uses
        /// client-side namespace filtering. Set automatically when cursor creation
        /// times out with a server-side filter.
        /// </summary>
        public bool UseClientSideCSFilter { get; set; }
        
        /// <summary>
        /// Minimum log level to write to logs. Default is Info (Error=0, Info=1, Debug=2, Verbose=3)
        /// </summary>
        public LogType LogLevel { get; set; } = LogType.Info;
        
        /// <summary>
        /// UI auto-refresh enabled state. Default is true. Not persisted - transient UI state only.
        /// </summary>
        [JsonIgnore]
        public bool AutoRefreshEnabled { get; set; } = true;
        
        // Parallel Processing Configuration
        /// <summary>
        /// Maximum number of parallel mongodump processes. Null = auto-calculate based on CPU cores.
        /// </summary>
        public int? MaxParallelDumpProcesses { get; set; }
        
        /// <summary>
        /// Maximum number of parallel mongorestore processes. Null = auto-calculate based on CPU cores.
        /// </summary>
        public int? MaxParallelRestoreProcesses { get; set; }
        
        /// <summary>
        /// Enable parallel processing feature. Default: true
        /// </summary>
        public bool EnableParallelProcessing { get; set; } = true;
        
        /// <summary>
        /// Current number of active dump workers. Used for runtime monitoring and adjustment.
        /// </summary>
        [JsonIgnore]
        public int CurrentDumpWorkers { get; set; }
        
        /// <summary>
        /// Current number of active restore workers. Used for runtime monitoring and adjustment.
        /// </summary>
        [JsonIgnore]
        public int CurrentRestoreWorkers { get; set; }
        
        /// <summary>
        /// Maximum number of insertion workers per collection for mongorestore. Null = auto-calculate based on CPU cores and doc count.
        /// </summary>
        public int? MaxInsertionWorkersPerCollection { get; set; }
        
        /// <summary>
        /// Current number of insertion workers per collection. Used for runtime monitoring and adjustment.
        /// </summary>
        public int CurrentInsertionWorkers { get; set; }
        
        /// <summary>
        /// Number of parallel threads for MongoDB Driver document copy operations. 
        /// Controls the SemaphoreSlim concurrency limit. Default = Environment.ProcessorCount * 5.
        /// </summary>
        public int ParallelThreads { get; set; } = Environment.ProcessorCount * 5;
        
        // Global resume token properties for server-level change streams (Forward sync)
        public string? ResumeToken { get; set; }
        public string? OriginalResumeToken { get; set; }
        public bool InitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }
        [Obsolete("Use ResumeDocumentKey instead - this only contains _id, not full shard key")]
        public string? ResumeDocumentId { get; set; }
        public string? ResumeDocumentKey { get; set; }
        public string? ResumeCollectionKey { get; set; } // CollectionKey (database.collection) for auto replay
        public DateTime? ChangeStreamStartedOn { get; set; }
        public DateTime CursorUtcTimestamp { get; set; }

        // Global resume token properties for server-level change streams (Sync back)
        public string? SyncBackResumeToken { get; set; }
        public string? SyncBackOriginalResumeToken { get; set; }
        public bool SyncBackInitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType SyncBackResumeTokenOperation { get; set; }
        [Obsolete("Use SyncBackResumeDocumentKey instead - this only contains _id, not full shard key")]
        public string? SyncBackResumeDocumentId { get; set; }
        public string? SyncBackResumeDocumentKey { get; set; }
        public string? SyncBackResumeCollectionKey { get; set; } // CollectionKey (database.collection) for sync back auto replay
        public DateTime? SyncBackChangeStreamStartedOn { get; set; }
        public DateTime SyncBackCursorUtcTimestamp { get; set; }

        #region SyncBack-aware helpers

        public DateTime GetCursorUtcTimestamp(bool syncBack)
            => syncBack ? SyncBackCursorUtcTimestamp : CursorUtcTimestamp;

        public void SetCursorUtcTimestamp(bool syncBack, DateTime value)
        {
            if (syncBack) SyncBackCursorUtcTimestamp = value;
            else CursorUtcTimestamp = value;
        }

        public DateTime? GetChangeStreamStartedOn(bool syncBack)
            => syncBack ? SyncBackChangeStreamStartedOn : ChangeStreamStartedOn;

        public void SetChangeStreamStartedOn(bool syncBack, DateTime? value)
        {
            if (syncBack) SyncBackChangeStreamStartedOn = value;
            else ChangeStreamStartedOn = value;
        }

        public string GetResumeToken(bool syncBack)
            => (syncBack ? SyncBackResumeToken : ResumeToken) ?? string.Empty;

        public void SetResumeToken(bool syncBack, string? value)
        {
            if (syncBack) SyncBackResumeToken = value;
            else ResumeToken = value;
        }

        public string? GetOriginalResumeToken(bool syncBack)
            => syncBack ? SyncBackOriginalResumeToken : OriginalResumeToken;

        public void SetOriginalResumeToken(bool syncBack, string? value)
        {
            if (syncBack) SyncBackOriginalResumeToken = value;
            else OriginalResumeToken = value;
        }

        public bool GetInitialDocumenReplayed(bool syncBack)
            => syncBack ? SyncBackInitialDocumenReplayed : InitialDocumenReplayed;

        public void SetInitialDocumenReplayed(bool syncBack, bool value)
        {
            if (syncBack) SyncBackInitialDocumenReplayed = value;
            else InitialDocumenReplayed = value;
        }

        public ChangeStreamOperationType GetResumeTokenOperation(bool syncBack)
            => syncBack ? SyncBackResumeTokenOperation : ResumeTokenOperation;

        public string GetResumeDocumentKey(bool syncBack)
        {
            if (syncBack)
                return SyncBackResumeDocumentKey ?? SyncBackResumeDocumentId ?? string.Empty;
            return ResumeDocumentKey ?? ResumeDocumentId ?? string.Empty;
        }

        public string GetResumeCollectionKey(bool syncBack)
            => (syncBack ? SyncBackResumeCollectionKey : ResumeCollectionKey) ?? string.Empty;

        public void SetResumeTokenInfo(bool syncBack, string resumeToken, ChangeStreamOperationType operationType, string documentKey, string collectionKey)
        {
            if (syncBack)
            {
                SyncBackResumeToken = resumeToken;
                if (string.IsNullOrEmpty(SyncBackOriginalResumeToken))
                    SyncBackOriginalResumeToken = resumeToken;
                SyncBackResumeTokenOperation = operationType;
                SyncBackResumeDocumentId = documentKey;
                SyncBackResumeDocumentKey = documentKey;
                SyncBackResumeCollectionKey = collectionKey;
            }
            else
            {
                ResumeToken = resumeToken;
                if (string.IsNullOrEmpty(OriginalResumeToken))
                    OriginalResumeToken = resumeToken;
                ResumeTokenOperation = operationType;
                ResumeDocumentId = documentKey;
                ResumeDocumentKey = documentKey;
                ResumeCollectionKey = collectionKey;
            }
        }

        #endregion

        public List<MigrationUnitBasic>? MigrationUnitBasics { get; set; }

        public bool Persist()
        {
            MigrationJobContext.AddVerboseLog($"MigrationJob.Persist: jobId={this.Id}, jobName={this.Name}");

            //Helper.CreateFolderIfNotExists($"{Helper.GetWorkingFolder()}migrationjobs\\{this.Id}");
            var filePath = $"migrationjobs\\{this.Id}\\jobdefinition.json";

            string json = JsonConvert.SerializeObject(this, Formatting.Indented);

            return MigrationJobContext.Store.UpsertDocument(filePath, json);
 
        }
    }
}