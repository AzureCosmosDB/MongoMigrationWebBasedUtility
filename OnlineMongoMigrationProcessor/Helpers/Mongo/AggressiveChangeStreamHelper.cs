using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    /// <summary>
    /// Helper class for managing aggressive change stream functionality.
    /// Handles temporary storage of document keys during delete operations before bulk copy completion.
    /// </summary>
    public class AggressiveChangeStreamHelper
    {
        private readonly MongoClient _targetClient;
        private readonly Log _log;
        private readonly string _jobId;

        public AggressiveChangeStreamHelper(MongoClient targetClient, Log log, string jobId)
        {
            _targetClient = targetClient ?? throw new ArgumentNullException(nameof(targetClient));
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _jobId = $"MigrationCache_{jobId}" ?? throw new ArgumentNullException(nameof(jobId));
        }

        

        /// <summary>
        /// Stores multiple document keys in the temporary collection for later deletion after bulk copy completion.
        /// This is a batch version for better performance.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="documentKeys">Collection of document keys from change stream delete events</param>
        /// <param name="operationType">Type of operation (delete, insert, update)</param>
        /// <returns>Number of successfully stored documents</returns>
        public async Task<int> StoreDocumentKeysAsync(string sourceDatabaseName, string sourceCollectionName, IEnumerable<BsonDocument> documentKeys, string operationType = "delete")
        {
            MigrationJobContext.AddVerboseLog($"Processing StoreDocumentKeysAsync for {sourceDatabaseName}.{sourceCollectionName} documentKeysCount= {documentKeys.Count()} operationType= {operationType}");

            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                var tempDocuments = documentKeys.Select(documentKey => new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(), // Use generated ObjectId for temp collection
                    ["documentKey"] = documentKey,
                    ["operationType"] = operationType,
                    ["createdAt"] = DateTime.UtcNow
                }).ToList();

                if (tempDocuments.Count == 0)
                {
                    return 0;
                }

                await tempCollection.InsertManyAsync(tempDocuments, new InsertManyOptions { IsOrdered = false });
                _log.ShowInMonitor($"Stored {tempDocuments.Count} document keys for aggressive change stream {operationType} in temp collection {tempCollectionName}");
                return tempDocuments.Count;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error storing document keys for aggressive change stream. Details: {ex}", LogType.Error);
                return 0;
            }
        }

        
        /// <summary>
        /// Removes multiple document keys from the temporary collection when inserts/updates are processed for the same keys.
        /// This is a batch version for better performance.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="documentKeys">Collection of document keys to remove</param>
        /// <returns>Number of successfully removed documents</returns>
        public async Task<long> RemoveDocumentKeysAsync(string sourceDatabaseName, string sourceCollectionName, IEnumerable<BsonDocument> documentKeys)
        {
            MigrationJobContext.AddVerboseLog($"Processing RemoveDocumentKeysAsync for {sourceDatabaseName}.{sourceCollectionName} documentKeysCount= {documentKeys.Count()}");

            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                var filters = documentKeys.Select(documentKey => 
                    Builders<BsonDocument>.Filter.Eq("documentKey", documentKey)).ToList();

                if (filters.Count == 0)
                {
                    return 0;
                }

                // Use $or filter to match any of the document keys
                var orFilter = Builders<BsonDocument>.Filter.Or(filters);
                var result = await tempCollection.DeleteManyAsync(orFilter);

                if (result.DeletedCount > 0)
                {
                    _log.ShowInMonitor($"Removed {result.DeletedCount} document key(s) from temp collection {tempCollectionName}");
                }

                return result.DeletedCount;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error removing document keys from temp collection. Details: {ex}", LogType.Error);
                return 0;
            }
        }

        /// <summary>
        /// Processes all stored document keys for deletion after bulk copy completion.
        /// Uses pagination to handle large collections efficiently.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <returns>Number of documents deleted from target collection</returns>
        public async Task<long> DeleteStoredDocsAsync(string sourceDatabaseName, string sourceCollectionName)
        {
            MigrationJobContext.AddVerboseLog($"Processing DeleteStoredDocsAsync for {sourceDatabaseName}.{sourceCollectionName}");

            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                // Check if temp collection exists
                var collectionNames = await tempDb.ListCollectionNamesAsync();
                var collectionList = await collectionNames.ToListAsync();
                if (!collectionList.Contains(tempCollectionName))
                {
                    return 0;
                }

                // Get target collection
                var targetDb = _targetClient.GetDatabase(sourceDatabaseName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(sourceCollectionName);

                long totalDeletedCount = 0;
                const int pageSize = 1000; // Process 1000 documents at a time from temp collection
                const int deleteBatchSize = 100; // Process 100 deletes at a time for target collection
                int pageNumber = 0;

                // Process documents in pages to avoid loading everything into memory
                while (true)
                {
                    var skip = pageNumber * pageSize;
                    
                    // Get a page of stored document keys
                    var storedKeysPage = await tempCollection
                        .Find(Builders<BsonDocument>.Filter.Empty)
                        .Skip(skip)
                        .Limit(pageSize)
                        .ToListAsync();

                    if (storedKeysPage.Count == 0)
                    {
                        // No more documents to process
                        break;
                    }

                    _log.ShowInMonitor($"Processing page {pageNumber + 1} with {storedKeysPage.Count} stored document keys for {sourceDatabaseName}.{sourceCollectionName}");

                    // Process deletes in batches within this page
                    for (int i = 0; i < storedKeysPage.Count; i += deleteBatchSize)
                    {
                        var batch = storedKeysPage.Skip(i).Take(deleteBatchSize);
                        var deleteModels = new List<DeleteOneModel<BsonDocument>>();

                        foreach (var storedDoc in batch)
                        {
                            if (storedDoc.Contains("documentKey"))
                            {
                                var documentKey = storedDoc["documentKey"].AsBsonDocument;
                                var filter = MongoHelper.BuildFilterFromDocumentKey(documentKey);
                                deleteModels.Add(new DeleteOneModel<BsonDocument>(filter));
                            }
                        }

                        if (deleteModels.Count > 0)
                        {
                            try
                            {
                                var result = await targetCollection.BulkWriteAsync(deleteModels, new BulkWriteOptions { IsOrdered = false });
                                totalDeletedCount += result.DeletedCount;
                                _log.ShowInMonitor($"Deleted {result.DeletedCount} documents from {sourceDatabaseName}.{sourceCollectionName} (page {pageNumber + 1}, batch {i / deleteBatchSize + 1})");
                            }
                            catch (MongoBulkWriteException<BsonDocument> ex)
                            {
                                totalDeletedCount += ex.Result?.DeletedCount ?? 0;
                                _log.WriteLine($"Bulk delete partially failed for {sourceDatabaseName}.{sourceCollectionName} (page {pageNumber + 1}, batch {i / deleteBatchSize + 1}). Details: {ex}", LogType.Error);
                            }
                        }
                    }

                    pageNumber++;
                }

                // Clean up temp collection after processing
                await tempDb.DropCollectionAsync(tempCollectionName);
                _log.WriteLine($"Processed aggressive change stream deletes for {sourceDatabaseName}.{sourceCollectionName}: {totalDeletedCount} documents deleted, temp collection cleaned up");

                return totalDeletedCount;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing stored deletes for {sourceDatabaseName}.{sourceCollectionName}.Details : {ex}", LogType.Error);
                return 0;
            }
        }

        /// <summary>
        /// Applies all stored insert and update operations after bulk copy completion.
        /// Reads documents from source collection and applies them to target.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="sourceClient">Source MongoDB client to read documents from</param>
        /// <returns>Tuple with counts of (inserted, updated, skipped) documents</returns>
        public async Task<(long Inserted, long Updated, long Skipped)> ApplyStoredChangesAsync(string sourceDatabaseName, string sourceCollectionName, MongoClient sourceClient)
        {
            MigrationJobContext.AddVerboseLog($"Processing ApplyStoredChangesAsync for {sourceDatabaseName}.{sourceCollectionName}");
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                // Check if temp collection exists
                var collectionNames = await tempDb.ListCollectionNamesAsync();
                var collectionList = await collectionNames.ToListAsync();
                if (!collectionList.Contains(tempCollectionName))
                {
                    return (0, 0, 0);
                }

                // Get source and target collections
                var sourceDb = sourceClient.GetDatabase(sourceDatabaseName);
                var sourceCollection = sourceDb.GetCollection<BsonDocument>(sourceCollectionName);
                var targetDb = _targetClient.GetDatabase(sourceDatabaseName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(sourceCollectionName);

                long totalInserted = 0;
                long totalUpdated = 0;
                long totalSkipped = 0;
                const int pageSize = 1000;
                const int operationBatchSize = 100;

                // Process inserts and updates (skip deletes as they're handled by DeleteStoredDocsAsync)
                var filter = Builders<BsonDocument>.Filter.In("operationType", new[] { "insert", "update" });
                
                int pageNumber = 0;
                while (true)
                {
                    var skip = pageNumber * pageSize;
                    
                    // Get a page of stored document keys
                    var storedKeysPage = await tempCollection
                        .Find(filter)
                        .Skip(skip)
                        .Limit(pageSize)
                        .ToListAsync();

                    if (storedKeysPage.Count == 0)
                    {
                        break;
                    }

                    _log.ShowInMonitor($"Processing page {pageNumber + 1} with {storedKeysPage.Count} stored changes for {sourceDatabaseName}.{sourceCollectionName}");

                    // Group by operation type
                    var insertKeys = storedKeysPage
                        .Where(doc => doc.GetValue("operationType", "").AsString == "insert" && doc.Contains("documentKey"))
                        .Select(doc => doc["documentKey"].AsBsonDocument)
                        .ToList();

                    var updateKeys = storedKeysPage
                        .Where(doc => doc.GetValue("operationType", "").AsString == "update" && doc.Contains("documentKey"))
                        .Select(doc => doc["documentKey"].AsBsonDocument)
                        .ToList();

                    // Read documents from source and process inserts
                    if (insertKeys.Count > 0)
                    {
                        var insertDocs = await ReadDocumentsFromSourceAsync(sourceCollection, insertKeys);
                        var (inserted, skipped) = await ProcessStoredInsertsAsync(targetCollection, insertDocs, operationBatchSize, sourceDatabaseName, sourceCollectionName, pageNumber);
                        totalInserted += inserted;
                        totalSkipped += skipped;
                    }

                    // Read documents from source and process updates
                    if (updateKeys.Count > 0)
                    {
                        var updateDocs = await ReadDocumentsFromSourceAsync(sourceCollection, updateKeys);
                        var (updated, skipped) = await ProcessStoredUpdatesAsync(targetCollection, updateDocs, operationBatchSize, sourceDatabaseName, sourceCollectionName, pageNumber);
                        totalUpdated += updated;
                        totalSkipped += skipped;
                    }

                    pageNumber++;
                }

                _log.WriteLine($"Applied stored changes for {sourceDatabaseName}.{sourceCollectionName}: {totalInserted} inserted, {totalUpdated} updated, {totalSkipped} skipped");

                return (totalInserted, totalUpdated, totalSkipped);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error applying stored changes for {sourceDatabaseName}.{sourceCollectionName}. Details: {ex}", LogType.Error);
                return (0, 0, 0);
            }
        }

        private async Task<List<BsonDocument>> ReadDocumentsFromSourceAsync(
            IMongoCollection<BsonDocument> sourceCollection,
            List<BsonDocument> documentKeys)
        {
            MigrationJobContext.AddVerboseLog($"Processing ReadDocumentsFromSourceAsync for {sourceCollection.Database.DatabaseNamespace.DatabaseName}.{sourceCollection.CollectionNamespace.CollectionName} documentKeysCount= {documentKeys.Count}");

            try
            {
                // Extract _id values from document keys
                var ids = documentKeys
                    .Where(key => key.Contains("_id"))
                    .Select(key => key["_id"])
                    .ToList();

                if (ids.Count == 0)
                {
                    return new List<BsonDocument>();
                }

                // Read documents from source
                var filter = Builders<BsonDocument>.Filter.In("_id", ids);
                var documents = await sourceCollection.Find(filter).ToListAsync();

                return documents;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error reading documents from source. Details {ex}", LogType.Error);
                return new List<BsonDocument>();
            }
        }

        private async Task<(long Inserted, long Skipped)> ProcessStoredInsertsAsync(
            IMongoCollection<BsonDocument> targetCollection,
            List<BsonDocument> documents,
            int batchSize,
            string databaseName,
            string collectionName,
            int pageNumber)
        {

            MigrationJobContext.AddVerboseLog($"Processing ProcessStoredInsertsAsync for {databaseName}.{collectionName} pageNumber= {pageNumber} batchSize={batchSize} documentsCount= {documents.Count}");

            long totalInserted = 0;
            long totalSkipped = 0;

            for (int i = 0; i < documents.Count; i += batchSize)
            {
                var batch = documents.Skip(i).Take(batchSize).ToList();
                var insertModels = batch.Select(doc =>
                {
                    var id = doc["_id"];
                    if (id.IsObjectId)
                        doc["_id"] = id.AsObjectId;
                    return new InsertOneModel<BsonDocument>(doc);
                }).ToList();

                if (insertModels.Count > 0)
                {
                    try
                    {
                        var result = await targetCollection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
                        totalInserted += result.InsertedCount;
                        _log.ShowInMonitor($"Inserted {result.InsertedCount} documents into {databaseName}.{collectionName} (page {pageNumber + 1}, batch {i / batchSize + 1})");
                        MigrationJobContext.AddVerboseLog($"ProcessStoredInsertsAsync inserted for {databaseName}.{collectionName} , {result.InsertedCount} documents into {databaseName}.{collectionName} (page {pageNumber + 1}, batch {i / batchSize + 1})");
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        totalInserted += ex.Result?.InsertedCount ?? 0;
                        var duplicateErrors = ex.WriteErrors.Count(e => e.Code == 11000);
                        totalSkipped += duplicateErrors;
                        
                        var otherErrors = ex.WriteErrors.Count(e => e.Code != 11000);
                        if (otherErrors > 0)
                        {
                            _log.WriteLine($"Insert errors for {databaseName}.{collectionName} (page {pageNumber + 1}, batch {i / batchSize + 1}): {otherErrors} non-duplicate errors", LogType.Error);
                        }
                    }
                }
            }

            MigrationJobContext.AddVerboseLog($"Completed ProcessStoredInsertsAsync for {databaseName}.{collectionName}. totalInserted ={totalInserted}, totalSkipped={totalSkipped}");
            return (totalInserted, totalSkipped);
        }

        private async Task<(long Updated, long Skipped)> ProcessStoredUpdatesAsync(
            IMongoCollection<BsonDocument> targetCollection,
            List<BsonDocument> documents,
            int batchSize,
            string databaseName,
            string collectionName,
            int pageNumber)
        {

            MigrationJobContext.AddVerboseLog($"Processing ProcessStoredUpdatesAsync for {databaseName}.{collectionName} pageNumber= {pageNumber} batchSize={batchSize} documentsCount= {documents.Count}");

            long totalUpdated = 0;
            long totalSkipped = 0;

            // Group by _id and take the latest (last in the list for each _id)
            var deduplicatedDocs = documents
                .GroupBy(doc => doc["_id"].ToString())
                .Select(g => g.Last())
                .ToList();

            for (int i = 0; i < deduplicatedDocs.Count; i += batchSize)
            {
                var batch = deduplicatedDocs.Skip(i).Take(batchSize).ToList();
                var updateModels = batch.Select(doc =>
                {
                    var id = doc["_id"];
                    if (id.IsObjectId)
                        id = id.AsObjectId;
                    
                    var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
                    return new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                }).ToList();

                if (updateModels.Count > 0)
                {
                    try
                    {
                        var result = await targetCollection.BulkWriteAsync(updateModels, new BulkWriteOptions { IsOrdered = false });
                        totalUpdated += result.ModifiedCount + result.Upserts.Count;
                        _log.ShowInMonitor($"Updated {result.ModifiedCount + result.Upserts.Count} documents in {databaseName}.{collectionName} (page {pageNumber + 1}, batch {i / batchSize + 1})");
                        MigrationJobContext.AddVerboseLog($"ProcessStoredInsertsAsync updated for {databaseName}.{collectionName} , {result.InsertedCount} documents into {databaseName}.{collectionName} (page {pageNumber + 1}, batch {i / batchSize + 1})");
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        totalUpdated += (ex.Result?.ModifiedCount ?? 0) + (ex.Result?.Upserts?.Count ?? 0);
                        var duplicateErrors = ex.WriteErrors.Count(e => e.Code == 11000);
                        totalSkipped += duplicateErrors;
                        
                        var otherErrors = ex.WriteErrors.Count(e => e.Code != 11000);
                        if (otherErrors > 0)
                        {
                            _log.WriteLine($"Update errors for {databaseName}.{collectionName} (page {pageNumber + 1}, batch {i / batchSize + 1}): {otherErrors} non-duplicate errors", LogType.Error);
                        }
                    }
                }
            }

            MigrationJobContext.AddVerboseLog($"Completed ProcessStoredUpdatesAsync for {databaseName}.{collectionName}. totalInserted ={totalUpdated}, totalSkipped={totalSkipped}");
            return (totalUpdated, totalSkipped);
        }

        /// <summary>
        /// Cleans up all temporary collections for the job.
        /// </summary>
        /// <returns>True if cleanup was successful, false otherwise</returns>
        public async Task<bool> CleanupTempDatabaseAsync()
        {
            MigrationJobContext.AddVerboseLog($"Processing CleanupTempDatabaseAsync");
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                await _targetClient.DropDatabaseAsync(_jobId);
                _log.WriteLine($"Cleaned up all temporary collections", LogType.Debug);
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error cleaning up temporary collections.Details: {ex}", LogType.Error);
                return false;
            }
        }
    }
}