using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OnlineMongoMigrationProcessor
{
    public static class CSWatchLogHelper
    {
        private const string DATABASE_NAME = "mongomigrationwebapplog";
        private const string COLLECTION_NAME_PREFIX = "cswatchlog";

        /// <summary>
        /// Inserts a change stream watch log document into the cswatchlog collection on the target server.
        /// Collection name is scoped per AppId and JobId: cswatchlog_{appId}_{jobId}
        /// This is fire-and-forget; failures are logged but do not propagate.
        /// </summary>
        private static readonly HashSet<string> _indexedCollections = new HashSet<string>();

        public static void InsertCSWatchLog(IMongoClient targetClient, string appId, string collectionNamespace, string jobId,
            string resumeTokenStart, string resumeTokenEnd, long totalChanges, long watchDurationMs, bool isSyncBack,
            DateTime firstChangeTimestamp, DateTime lastChangeTimestamp)
        {
            if (targetClient == null)
                return;

            try
            {
                var collectionName = $"{COLLECTION_NAME_PREFIX}_{appId}_{jobId}";
                var db = targetClient.GetDatabase(DATABASE_NAME);

                EnsureCollectionAndIndexes(db, collectionName);

                var collection = db.GetCollection<BsonDocument>(collectionName);

                var doc = new BsonDocument
                {
                    { "CollectionNamespace", collectionNamespace },
                    { "TimeUtc", DateTime.UtcNow },
                    { "ResumeTokenStart", resumeTokenStart ?? string.Empty },
                    { "ResumeTokenEnd", resumeTokenEnd ?? string.Empty },
                    { "TotalChanges", totalChanges },
                    { "WatchDurationMs", watchDurationMs },
                    { "IsSyncBack", isSyncBack },
                    { "FirstChangeTimestamp", firstChangeTimestamp },
                    { "LastChangeTimestamp", lastChangeTimestamp }
                };

                collection.InsertOne(doc);
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[CSWatchLogHelper] Error inserting cswatchlog for {collectionNamespace}: {ex.Message}", "CSWatchLog.txt");
            }
        }

        private static void EnsureCollectionAndIndexes(IMongoDatabase db, string collectionName)
        {
            if (_indexedCollections.Contains(collectionName))
                return;

            // Check if collection already exists on the server
            var filter = new BsonDocument("name", collectionName);
            bool exists = db.ListCollectionNames(new ListCollectionNamesOptions { Filter = filter }).Any();

            if (exists)
            {
                // Collection already exists (e.g. after a restart) — skip index creation
                _indexedCollections.Add(collectionName);
                return;
            }

            db.CreateCollection(collectionName);

            var collection = db.GetCollection<BsonDocument>(collectionName);

            // Compound index: CollectionNamespace + ResumeTokenStart + TotalChanges + FirstChangeTimestamp
            var compoundIndex = Builders<BsonDocument>.IndexKeys
                .Ascending("CollectionNamespace")
                .Ascending("ResumeTokenStart")
                .Ascending("TotalChanges")
                .Ascending("FirstChangeTimestamp");
            collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(compoundIndex));

            // Single field index: TotalChanges
            var totalChangesIndex = Builders<BsonDocument>.IndexKeys.Ascending("TotalChanges");
            collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(totalChangesIndex));

            _indexedCollections.Add(collectionName);
        }
    }
}
