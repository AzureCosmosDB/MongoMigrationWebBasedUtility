using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
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
        public long CSUpdatesInLastBatch { get; set; }

        public double CSAvgReadLatencyInMS { get; set; }
        public double CSAvgWriteLatencyInMS { get; set; }

        public DateTime? CSLastChecked { get; set; }

        public DateTime CursorUtcTimestamp { get; set; }
        public DateTime SyncBackCursorUtcTimestamp { get; set; }
        public double DumpPercent { get; set; }
        public double RestorePercent { get; set; }
        public bool DumpComplete { get; set; }
        public bool RestoreComplete { get; set; }

        public CollectionStatus SourceStatus { get; set; }
        public bool ResetChangeStream { get; set; }
        public DataType? DataTypeFor_Id { get; set; } = null;

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

                return MigrationJobContext.SaveMigrationJob(ParentJob);

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

       

    }


    public class MigrationUnit:MigrationUnitBasic
    {
       
       
        public string? ResumeToken { get; set; }
        public string? OriginalResumeToken { get; set; }

        public bool InitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }

        [Obsolete("Use ResumeDocumentKey instead - this only contains _id, not full shard key")]
        public string? ResumeDocumentId { get; set; }
        public string? ResumeDocumentKey { get; set; }
        public DateTime? BulkCopyStartedOn { get; set; }
        public DateTime? BulkCopyEndedOn { get; set; }
        public bool TargetCreated { get; set; }
        public int IndexesMigrated { get; set; }

        public DateTime? ComparedOn { get; set; }
        public int VarianceCount { get; set; }

        public DateTime? ChangeStreamStartedOn { get; set; }
        
        public long CSNormalizedUpdatesInLastBatch { get; set; }
        public int CSLastBatchDurationSeconds { get; set; }

        public string? UserFilter { get; set; }
        public string? SyncBackResumeToken { get; set; }
        public DateTime? SyncBackChangeStreamStartedOn { get; set; }

        public long EstimatedDocCount { get; set; }
       
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
         // Aggressive Change Stream cleanup tracking
        public bool AggressiveCacheDeleted { get; set; } = false;
        public DateTime? AggressiveCacheDeletedOn { get; set; }

        public List<MigrationChunk> MigrationChunks { get; set; }

        public MigrationUnit(MigrationJob job, string databaseName, string collectionName, List<MigrationChunk> migrationChunks)
        {
            this.Id = Helper.GenerateMigrationUnitId(databaseName, collectionName);            
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;
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
                if (index == -1) return false; // not found

                GetBasic(ParentJob.MigrationUnitBasics[index]);

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
            mub.CSUpdatesInLastBatch = this.CSUpdatesInLastBatch;
            mub.CSAvgReadLatencyInMS = this.CSAvgReadLatencyInMS;
            mub.CSAvgWriteLatencyInMS = this.CSAvgWriteLatencyInMS;
            mub.CursorUtcTimestamp = this.CursorUtcTimestamp;
            mub.SyncBackCursorUtcTimestamp = this.SyncBackCursorUtcTimestamp;
            mub.DumpPercent = this.DumpPercent;
            mub.RestorePercent = this.RestorePercent;
            mub.DumpComplete = this.DumpComplete;
            mub.RestoreComplete = this.RestoreComplete;
            mub.SourceStatus = this.SourceStatus;
            mub.ResetChangeStream = this.ResetChangeStream;
            mub.DataTypeFor_Id = this.DataTypeFor_Id;
            return mub;
        }

    }
}