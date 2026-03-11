using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    public class AccumulatedChangesTracker
    {
        // Thread-safety lock for dictionary operations
        private readonly object _lock = new object();
               
        public Dictionary<string, ChangeStreamDocument<BsonDocument>> DocsToBeInserted { get; private set; } = new();
        public Dictionary<string, ChangeStreamDocument<BsonDocument>> DocsToBeUpdated { get; private set; } = new();
        public Dictionary<string, ChangeStreamDocument<BsonDocument>> DocsToBeDeleted { get; private set; } = new();

        public long TotalChangesCount
        {
            get
            {
                //lock (_lock)
                //{
                    return DocsToBeInserted.Count + DocsToBeUpdated.Count + DocsToBeDeleted.Count;
                //}
            }
        }

        private long _totalEventCount = 0;
        public long TotalEventCount
        {
            get
            {
                //lock (_lock)
                //{
                    return _totalEventCount;
                //}
            }
        }

        // Track the latest resume token for checkpoint on success
        public string CollectionKey { get; private set; } = string.Empty;
        public string LatestResumeToken { get; private set; } = string.Empty;
        public DateTime LatestTimestamp { get; private set; } = DateTime.MinValue;
        public ChangeStreamOperationType LatestOperationType { get; private set; }
        public string LatestDocumentKey { get; private set; } = string.Empty;

        // Track batch latencies
        public long CSTotalReadDurationInMS { get; set; } = 0;
        public long CSTotaWriteDurationInMS { get; set; } = 0;

        private string _collectionKey = string.Empty;
        public AccumulatedChangesTracker(string collectionKey)
        {
            _collectionKey = collectionKey;
            CollectionKey = _collectionKey;
        }


        public void AddInsert(ChangeStreamDocument<BsonDocument> change)
        {

            lock (_lock)
            {
                _totalEventCount++;
#if LEGACY_MONGODB_DRIVER
                var changeCollectionKey = $"{change.CollectionNamespace?.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
#else
                var changeCollectionKey = $"{change.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
#endif
                if (changeCollectionKey != _collectionKey)
                    return;

                var id = change.DocumentKey.ToJson();
                if(string.IsNullOrEmpty(id))
                    return;

                //To deduplicate
                DocsToBeUpdated.Remove(id);
                DocsToBeDeleted.Remove(id);
                
                // Directly set/overwrite
                DocsToBeInserted[id] = change;

                UpdateMetadata(change);
            }
        }

        public void AddUpdate(ChangeStreamDocument<BsonDocument> change)
        {

            lock (_lock)
            {
                _totalEventCount++;
#if LEGACY_MONGODB_DRIVER
                var changeCollectionKey = $"{change.CollectionNamespace?.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
#else
                var changeCollectionKey = $"{change.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
#endif
                if (changeCollectionKey != _collectionKey)
                    return;

                var id = change.DocumentKey.ToJson();
                if (string.IsNullOrEmpty(id))
                    return;

                //To deduplicate
                DocsToBeDeleted.Remove(id);
                DocsToBeUpdated[id] = change;

                // Don't remove from insert — updates after insert are valid

                UpdateMetadata(change);
            }
        }

        public void AddDelete(ChangeStreamDocument<BsonDocument> change)
        {
            lock (_lock)
            {
                _totalEventCount++;
#if LEGACY_MONGODB_DRIVER
                var changeCollectionKey = $"{change.CollectionNamespace?.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
#else
                var changeCollectionKey = $"{change.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
#endif
                if (changeCollectionKey != _collectionKey)
                    return;

                var id = change.DocumentKey.ToJson();
                if (string.IsNullOrEmpty(id))
                    return;
                
                //To deduplicate
                DocsToBeInserted.Remove(id);
                DocsToBeUpdated.Remove(id);
                DocsToBeDeleted[id] = change;

                UpdateMetadata(change);
            }
        }

        /// <summary>
        /// Track both earliest and latest change metadata for safe checkpoint updates.
        /// - Earliest token: Used for rollback if batch fails
        /// - Latest token: Used for checkpoint if batch succeeds
        /// This ensures resume tokens are only updated AFTER successful batch processing.
        /// </summary>
        private void UpdateMetadata(ChangeStreamDocument<BsonDocument> change)
        {
            DateTime changeTimestamp = DateTime.MinValue;

#if !LEGACY_MONGODB_DRIVER
            // Extract timestamp from ClusterTime or WallTime
            if (change.ClusterTime != null)
            {
                changeTimestamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
            }
            else if (change.WallTime != null)
            {
                changeTimestamp = change.WallTime.Value;
            }
#endif
           
            // Always track latest (last change in batch) - for checkpoint on success
            if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
            {
                if (changeTimestamp >= LatestTimestamp && !string.IsNullOrEmpty(change.ResumeToken.ToJson()))
                {
                    LatestResumeToken = change.ResumeToken.ToJson();
                    LatestOperationType = change.OperationType;
                    LatestDocumentKey = change.DocumentKey.ToJson();
                    LatestTimestamp = changeTimestamp;
                }
            }
        }
        public bool Reset(bool isFinalFlush = true)
        {
            lock (_lock)
            {
                DocsToBeInserted.Clear();
                DocsToBeUpdated.Clear();
                DocsToBeDeleted.Clear();

                if (isFinalFlush)
                {
                    _totalEventCount = 0;
                    LatestResumeToken = string.Empty;
                    LatestTimestamp = DateTime.MinValue;
                    LatestDocumentKey = string.Empty;
                }
                return true;
                
            }
        }

        /// <summary>
        /// Clear all metadata after successful checkpoint update or on failure rollback.
        /// </summary>
        public void ClearMetadata()
        {
            lock (_lock)
            {
                // Clear latest
                LatestResumeToken = string.Empty;
                LatestTimestamp = DateTime.MinValue;
                LatestDocumentKey = string.Empty;
            }
        }       
        
    }
}
