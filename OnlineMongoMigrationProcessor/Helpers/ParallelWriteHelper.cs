using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Handles parallel write operations with sequence preservation, retry logic, and resource-based thread calculation
    /// </summary>
    internal class ParallelWriteHelper
    {
        private readonly Log _log;
        private readonly string _logPrefix;
        private readonly int _maxThreads;
        private const int MAX_RETRIES = 10;
        private const int INITIAL_RETRY_DELAY_MS = 500;
        private const int MAX_RETRY_DELAY_MS = 300000; // 5 minutes max delay

        public ParallelWriteHelper(Log log, string logPrefix = "")
        {
            MigrationJobContext.AddVerboseLog($"ParallelWriteHelper: Constructor called, logPrefix={logPrefix}");
            _log = log;
            _logPrefix = logPrefix;
            _maxThreads = CalculateOptimalThreadCount();

        }

        /// <summary>
        /// Calculate optimal number of threads based on available CPU cores and RAM
        /// </summary>
        private int CalculateOptimalThreadCount()
        {
            try
            {
                // Get CPU core count
                int processorCount = Environment.ProcessorCount;
                
                // Estimate available RAM - simplified approach without PerformanceCounter
                // Use GC memory info as a proxy for available memory
                GCMemoryInfo gcInfo = GC.GetGCMemoryInfo();
                long totalMemoryBytes = gcInfo.TotalAvailableMemoryBytes;
                float availableRamGB = totalMemoryBytes / (1024f * 1024f * 1024f);
                
                // Formula: 
                // - Database writes are I/O-bound, not CPU-bound
                // - Use 2x CPU cores to better utilize I/O wait time
                // - Limit by RAM: assume each thread needs ~200MB for buffers and processing
                int cpuBasedThreads = processorCount * 2; // 2x multiplier for I/O-bound operations
                int ramBasedLimit = (int)(availableRamGB * 1024 / 200); // Each thread ~200MB
                
                // Use the minimum of CPU-based (2x) and RAM-based limits
                // But ensure at least 2 threads and cap at 32 for safety
                int optimalThreads = Math.Min(cpuBasedThreads, ramBasedLimit);
                optimalThreads = Math.Max(2, Math.Min(32, optimalThreads));
                
                _log.WriteLine($"{_logPrefix}Thread calculation: CPU cores={processorCount}, CPU-based threads (2x)={cpuBasedThreads}, Available RAM={availableRamGB:F2}GB, " +
                              $"RAM-based limit={ramBasedLimit}, Optimal threads={optimalThreads}", LogType.Debug);
                
                return optimalThreads;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_logPrefix}Error calculating optimal threads, defaulting to 4. Error: {ex}", LogType.Warning);
                return 4; // Safe default
            }
        }

        /// <summary>
        /// Process write operations in parallel while maintaining sequence order
        /// </summary>
        public async Task<WriteResult> ProcessWritesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null,
            bool isSimulatedRun = false)
        {
            var result = new WriteResult();
            
            // Create a combined, ordered sequence of all operations
            var orderedOperations = CreateOrderedOperationSequence(insertEvents, updateEvents, deleteEvents);
            
                        
            if (!orderedOperations.Any())
            {
                MigrationJobContext.AddVerboseLog($"{_logPrefix}No operations to process for {collection.CollectionNamespace}, returning empty result");
                return result; // No operations to process
            }

            MigrationJobContext.AddVerboseLog($"{_logPrefix}Processing {orderedOperations.Count} operations across {_maxThreads} threads for {collection.CollectionNamespace.FullName}");

            // Partition operations into batches for parallel processing
            var operationBatches = PartitionOperations(orderedOperations, _maxThreads);
            MigrationJobContext.AddVerboseLog($"{_logPrefix}Partitioned operations into {operationBatches.Count} batches for {collection.CollectionNamespace}");
            
            // Process batches in parallel with sequence preservation
            var tasks = new List<Task<WriteResult>>();
            
            for (int i = 0; i < operationBatches.Count; i++)
            {
                var batch = operationBatches[i];
                var batchIndex = i; // Capture for closure
                MigrationJobContext.AddVerboseLog($"{_logPrefix}Creating task for batch {batchIndex} with {batch.Count} operations for {collection.CollectionNamespace}");
                
                var task = Task.Run(async () =>
                {
                    try
                    {
                        MigrationJobContext.AddVerboseLog($"{_logPrefix}Starting batch {batchIndex} processing for {collection.CollectionNamespace}");
                        var batchResult = await ProcessBatchWithRetryAsync(
                            mu,
                            collection,
                            batch,
                            counterDelegate,
                            batchSize,
                            isAggressive,
                            isAggressiveComplete,
                            jobId,
                            targetClient,
                            isSimulatedRun);
                        MigrationJobContext.AddVerboseLog($"{_logPrefix}Completed batch {batchIndex} processing for {collection.CollectionNamespace}: success={batchResult.Success}, processed={batchResult.Processed}, failures={batchResult.Failures}");
                        return batchResult;
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        // CRITICAL errors must propagate to stop the entire job
                        _log.WriteLine($"{_logPrefix}CRITICAL error in batch {batchIndex} for {collection.CollectionNamespace}. Details: {ex}", LogType.Error);
                        throw; // Re-throw to stop the job
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_logPrefix}Exception in batch {batchIndex} processing for {collection.CollectionNamespace}. Details: {ex}", LogType.Error);
                        
                        // Return a failed result instead of throwing to allow other batches to continue
                        return new WriteResult
                        {
                            Success = false,
                            Processed = 0,
                            Failures = batch.Count,
                            Skipped = 0,
                            Errors = new List<string> { $"Batch {batchIndex} failed. Details: {ex}" }
                        };
                    }
                });
                
                tasks.Add(task);
            }

            // Wait for all batches to complete
            try
            {
                MigrationJobContext.AddVerboseLog($"{_logPrefix}Waiting for all {tasks.Count} batch tasks to complete for {collection.CollectionNamespace}");
                var batchResults = await Task.WhenAll(tasks);

                
                // Aggregate results
                foreach (var batchResult in batchResults)
                {
                    result.Processed += batchResult.Processed;
                    result.Failures += batchResult.Failures;
                    result.Skipped += batchResult.Skipped;
                    result.WriteLatencyMS += batchResult.WriteLatencyMS;
                    
                    if (!batchResult.Success)
                    {
                        result.Success = false;
                        result.Errors.AddRange(batchResult.Errors);
                    }
                }

                MigrationJobContext.AddVerboseLog($"{_logPrefix}Result aggregation completed for {collection.CollectionNamespace}: totalProcessed={result.Processed}, totalFailures={result.Failures}, totalSkipped={result.Skipped}, success={result.Success}");
                
                if (result.Failures > 0)
                {
                    _log.WriteLine($"{_logPrefix}Parallel processing completed with {result.Failures} failures for {collection.CollectionNamespace.FullName}", LogType.Warning);
                }
                else
                {
                    MigrationJobContext.AddVerboseLog($"{_logPrefix}Parallel processing completed successfully: {result.Processed} processed, {result.Skipped} skipped for {collection.CollectionNamespace.FullName}");
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Errors.Add($"Critical error in parallel processing. Details: {ex}");
                _log.WriteLine($"{_logPrefix}Critical error in parallel write processing for {collection.CollectionNamespace.FullName}. Details: {ex}", LogType.Error);
                throw; // Re-throw to stop the job
            }

            MigrationJobContext.AddVerboseLog($"{_logPrefix}ParallelWriteHelper.ProcessWritesAsync completed for {collection.CollectionNamespace}");
            return result;
        }

        /// <summary>
        /// Create an ordered sequence of operations based on timestamps to maintain write order
        /// </summary>
        private List<OrderedOperation> CreateOrderedOperationSequence(
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents)
        {
            var operations = new List<OrderedOperation>();
            long sequenceNumber = 0;

            // Add all operations with their sequence numbers based on ClusterTime
            foreach (var evt in insertEvents.Where(e => e != null).OrderBy(e => e.ClusterTime ?? new BsonTimestamp(0, 0)))
            {
                operations.Add(new OrderedOperation
                {
                    SequenceNumber = sequenceNumber++,
                    OperationType = ChangeStreamOperationType.Insert,
                    Event = evt,
                    Timestamp = evt.ClusterTime ?? new BsonTimestamp(0, 0)
                });
            }

            foreach (var evt in updateEvents.Where(e => e != null).OrderBy(e => e.ClusterTime ?? new BsonTimestamp(0, 0)))
            {
                operations.Add(new OrderedOperation
                {
                    SequenceNumber = sequenceNumber++,
                    OperationType = ChangeStreamOperationType.Update,
                    Event = evt,
                    Timestamp = evt.ClusterTime ?? new BsonTimestamp(0, 0)
                });
            }

            foreach (var evt in deleteEvents.Where(e => e != null).OrderBy(e => e.ClusterTime ?? new BsonTimestamp(0, 0)))
            {
                operations.Add(new OrderedOperation
                {
                    SequenceNumber = sequenceNumber++,
                    OperationType = ChangeStreamOperationType.Delete,
                    Event = evt,
                    Timestamp = evt.ClusterTime ?? new BsonTimestamp(0, 0)
                });
            }

            // Sort by timestamp to maintain chronological order
            return operations.OrderBy(op => op.Timestamp).ThenBy(op => op.SequenceNumber).ToList();
        }

        /// <summary>
        /// Partition operations into batches for parallel processing while maintaining sequence
        /// </summary>
        private List<List<OrderedOperation>> PartitionOperations(List<OrderedOperation> operations, int partitionCount)
        {
            var partitions = new List<List<OrderedOperation>>();
            
            // Group operations by document key to ensure operations on the same document stay together
            var groupedByDocument = operations
                .GroupBy(op => op.Event.DocumentKey.ToJson())
                .ToList();
            
            // Distribute document groups across partitions using round-robin
            for (int i = 0; i < partitionCount; i++)
            {
                partitions.Add(new List<OrderedOperation>());
            }
            
            int partitionIndex = 0;
            foreach (var documentGroup in groupedByDocument)
            {
                // Add all operations for this document to the same partition
                partitions[partitionIndex].AddRange(documentGroup.OrderBy(op => op.SequenceNumber));
                partitionIndex = (partitionIndex + 1) % partitionCount;
            }
            
            // Remove empty partitions
            return partitions.Where(p => p.Any()).ToList();
        }

        /// <summary>
        /// Process a batch of operations with retry logic and exponential backoff
        /// </summary>
        private async Task<WriteResult> ProcessBatchWithRetryAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<OrderedOperation> operations,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize,
            bool isAggressive,
            bool isAggressiveComplete,
            string jobId,
            MongoClient? targetClient,
            bool isSimulatedRun)
        {
            var result = new WriteResult { Success = true };
            
            if (!operations.Any())
            {
                return result;
            }

            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;
            
            // Initialize aggressive change stream helper if needed
            AggressiveChangeStreamHelper? aggressiveHelper = null;
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                aggressiveHelper = new AggressiveChangeStreamHelper(targetClient, _log, jobId);
            }

            // Group operations by type for bulk processing
            var insertOps = operations.Where(op => op.OperationType == ChangeStreamOperationType.Insert).Select(op => op.Event).ToList();
            var updateOps = operations.Where(op => op.OperationType == ChangeStreamOperationType.Update || op.OperationType == ChangeStreamOperationType.Replace).Select(op => op.Event).ToList();
            var deleteOps = operations.Where(op => op.OperationType == ChangeStreamOperationType.Delete).Select(op => op.Event).ToList();

            // Process each operation type with retry logic
            if (insertOps.Any())
            {
                var insertResult = await ProcessInsertsWithRetryAsync(
                    mu, collection, insertOps, counterDelegate, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun);
                result.Processed += insertResult.Processed;
                result.Failures += insertResult.Failures;
                result.Skipped += insertResult.Skipped;
                result.WriteLatencyMS += insertResult.WriteLatencyMS;
                result.Success &= insertResult.Success;
                result.Errors.AddRange(insertResult.Errors);
            }

            if (updateOps.Any())
            {
                var updateResult = await ProcessUpdatesWithRetryAsync(
                    mu, collection, updateOps, counterDelegate, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun);
                result.Processed += updateResult.Processed;
                result.Failures += updateResult.Failures;
                result.Skipped += updateResult.Skipped;
                result.WriteLatencyMS += updateResult.WriteLatencyMS;
                result.Success &= updateResult.Success;
                result.Errors.AddRange(updateResult.Errors);
            }

            if (deleteOps.Any())
            {
                var deleteResult = await ProcessDeletesWithRetryAsync(
                    mu, collection, deleteOps, counterDelegate, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun);
                result.Processed += deleteResult.Processed;
                result.Failures += deleteResult.Failures;
                result.Skipped += deleteResult.Skipped;
                result.WriteLatencyMS += deleteResult.WriteLatencyMS;
                result.Success &= deleteResult.Success;
                result.Errors.AddRange(deleteResult.Errors);
            }

            return result;
        }


        internal async Task<bool> ProcessAggresiveCSAsync(Object unit,
         IMongoCollection<BsonDocument> collection,
         List<ChangeStreamDocument<BsonDocument>> events,
         int batchSize,
         bool isAggressive,
         bool isAggressiveComplete,
         AggressiveChangeStreamHelper? aggressiveHelper,
         string databaseName,
         string collectionName,
         bool isSimulatedRun,
         string eventType)
        {

            if(isSimulatedRun)
                return true;

            // Handle aggressive mode: store insert entries and remove delete entries
            if (isAggressive && aggressiveHelper != null)
            {
                var documentKeys = events
                    .Where(i => i.DocumentKey != null)
                    .Select(i => i.DocumentKey)
                    .ToList();

                if (documentKeys.Count > 0)
                {
                    if (!isAggressiveComplete)
                    {
                        // Remove any delete entries for these document keys
                        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                        // Store insert entries for later processing
                        await aggressiveHelper.StoreDocumentKeysAsync(databaseName, collectionName, documentKeys, eventType);

                        // Exit early - don't write to destination during aggressive mode
                        return false;
                    }

                    var mu = unit as MigrationUnit;
                    if (isAggressiveComplete && !mu.AggressiveCacheDeleted)
                    {
                        // Remove any entries for these document keys
                        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                    }
                }
            }

            return true;
        }


        /// <summary>
        /// Process inserts with retry logic
        /// </summary>
        internal async Task<WriteResult> ProcessInsertsWithRetryAsync<TMigration>(
         TMigration unit,
         IMongoCollection<BsonDocument> collection,
         List<ChangeStreamDocument<BsonDocument>> events,
         CounterDelegate<TMigration> counterDelegate,
         int batchSize,
         bool isAggressive,
         bool isAggressiveComplete,
         AggressiveChangeStreamHelper? aggressiveHelper,
         string databaseName,
         string collectionName,
         bool isSimulatedRun ) // <-- New parameter
        {
            var result = new WriteResult { Success = true };

            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate inserts by document key
                var deduplicatedInserts = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson())
                    .Select(g => g.First())
                    .ToList();

                // Handle aggressive mode: store event entries in temp and remove old entries
                if(await ProcessAggresiveCSAsync(unit, collection, deduplicatedInserts, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun,"insert") == false)
                {
                    continue;
                }

                //if (isAggressive && aggressiveHelper != null)
                //{
                //    var documentKeys = deduplicatedInserts
                //        .Where(insertEvent => insertEvent.DocumentKey != null)
                //        .Select(insertEvent => insertEvent.DocumentKey)
                //        .ToList();

                //    if (documentKeys.Count > 0)
                //    {  
                //        if (!isAggressiveComplete)
                //        {
                //            // Remove any delete entries for these document keys
                //            await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                //            // Store insert entries for later processing
                //            await aggressiveHelper.StoreDocumentKeysAsync(databaseName, collectionName, documentKeys, "insert");

                //            // Exit early - don't write to destination during aggressive mode
                //            continue;
                //        }

                //        var mu = unit as MigrationUnit;
                //        if(isAggressiveComplete && !mu.AggressiveCacheDeleted)
                //        {
                //            // Remove any entries for these document keys
                //            await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                //        }
                //    }


                           

                var insertDocs = deduplicatedInserts
                    .Select(e =>
                    {
                        var doc = e.FullDocument;
                        var id = doc["_id"];
                        if (id.IsObjectId)
                            doc["_id"] = id.AsObjectId;
                        return doc;
                    })
                    .ToList();

                // Retry loop with exponential backoff
                var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
                try
                {
                    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++)
                    {
                        try
                        {
                            long insertCount = 0;

                            if (insertDocs.Any())
                            {
                                if (!isSimulatedRun)
                                {
                                    if (!MongoHelper.IsCosmosRUEndpoint(collection))
                                    {
                                        // Use BulkWriteAsync
                                        var insertModels = insertDocs.Select(d => new InsertOneModel<BsonDocument>(d)).ToList();
                                        var writeResult = await collection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
                                        insertCount = writeResult.InsertedCount;
                                    }
                                    else
                                    {
                                        // Use InsertManyAsync
                                        await collection.InsertManyAsync(insertDocs, new InsertManyOptions { IsOrdered = false });
                                        insertCount = insertDocs.Count;
                                    }
                                }
                                else
                                {
                                    insertCount = insertDocs.Count;
                                }

                                counterDelegate(unit, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);
                                result.Processed += (int)insertCount;
                            }

                            break; // Success - exit retry loop
                        }
                        catch (MongoBulkWriteException<BsonDocument> ex)
                        {
                            long insertCount = ex.Result?.InsertedCount ?? 0;
                            result.Processed += (int)insertCount;
                            counterDelegate(unit, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);

                            var duplicateKeyErrors = ex.WriteErrors.Where(err => err.Code == 11000).ToList();
                            result.Skipped += duplicateKeyErrors.Count;
                            counterDelegate(unit, CounterType.Skipped, ChangeStreamOperationType.Insert, duplicateKeyErrors.Count);

                            var otherErrors = ex.WriteErrors.Where(err => err.Code != 11000).ToList();
                            if (otherErrors.Any())
                            {
                                result.Failures += otherErrors.Count;
                                _log.WriteLine($"{_logPrefix}Insert errors in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}", LogType.Warning);
                            }
                            break; // Exit retry loop after handling BulkWriteException
                        }
                        catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                        {
                            if (attempt < MAX_RETRIES)
                            {
                                int delay = CalculateRetryDelay(attempt);
                                _log.WriteLine($"{_logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms...", LogType.Warning);
                                await Task.Delay(delay);
                            }
                            else
                            {
                                string errorMsg = $"CRITICAL: Unable to process insert batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts due to persistent deadlock. Data loss prevention requires job termination.";
                                _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                                result.Success = false;
                                result.Errors.Add(errorMsg);
                                throw new InvalidOperationException(errorMsg);
                            }
                        }
                        catch (Exception ex) when (IsTransientException(ex))
                        {
                            if (attempt < MAX_RETRIES)
                            {
                                int delay = CalculateRetryDelay(attempt);
                                string errorType = GetTransientErrorType(ex);
                                _log.WriteLine($"{_logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms... Details: {ex}", LogType.Warning);
                                await Task.Delay(delay);
                            }
                            else
                            {
                                string errorMsg = $"CRITICAL: Unable to process insert batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts. Detials: {ex}";
                                _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                                result.Success = false;
                                result.Errors.Add(errorMsg);
                                throw new InvalidOperationException(errorMsg);
                            }
                        }
                    }
                }
                finally
                {
                    writeStopwatch.Stop();
                    result.WriteLatencyMS += writeStopwatch.ElapsedMilliseconds;
                }
            }

            return result;
        }


        /// <summary>
        /// Process updates with retry logic
        /// </summary>
        private async Task<WriteResult> ProcessUpdatesWithRetryAsync<TMigration>(
            TMigration unit,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize,
            bool isAggressive,
            bool isAggressiveComplete,
            AggressiveChangeStreamHelper? aggressiveHelper,
            string databaseName,
            string collectionName,
            bool isSimulatedRun)
        {
            var result = new WriteResult { Success = true };

            foreach (var batch in events.Chunk(batchSize))
            {
                // Group by document key, take latest update
                var groupedUpdates = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson())
                    .Select(g => g.OrderByDescending(e => e.ClusterTime ?? new BsonTimestamp(0, 0)).First())
                    .ToList();

                if (await ProcessAggresiveCSAsync(unit, collection, groupedUpdates, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun, "update") == false)
                {
                    continue;
                }

                //// Handle aggressive mode: store insert entries and remove delete entries
                //if (isAggressive && aggressiveHelper != null)
                //{
                //    var documentKeys = groupedUpdates
                //        .Where(insertEvent => insertEvent.DocumentKey != null)
                //        .Select(insertEvent => insertEvent.DocumentKey)
                //        .ToList();

                //    if (documentKeys.Count > 0)
                //    {
                //        if (!isAggressiveComplete)
                //        {
                //            // Remove any delete entries for these document keys
                //            await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                //            // Store insert entries for later processing
                //            await aggressiveHelper.StoreDocumentKeysAsync(databaseName, collectionName, documentKeys, "update");

                //            // Exit early - don't write to destination during aggressive mode
                //            continue;
                //        }

                //        var mu = unit as MigrationUnit;
                //        if (isAggressiveComplete && !mu.AggressiveCacheDeleted)
                //        {
                //            // Remove any entries for these document keys
                //            await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                //        }
                //    }

                //}



                var updateModels = groupedUpdates
                    .Select(e =>
                    {
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", e.DocumentKey["_id"]);
                        return new ReplaceOneModel<BsonDocument>(filter, e.FullDocument) { IsUpsert = true };
                    })
                    .ToList();

                // Retry loop with exponential backoff
                var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
                try
                {
                    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++)
                    {
                        try
                        {
                            if (updateModels.Any())
                            {
                                long updateCount = 0;
                                
                                if (!isSimulatedRun)
                                {
                                    var writeResult = await collection.BulkWriteAsync(updateModels, new BulkWriteOptions { IsOrdered = false });
                                    updateCount = writeResult.ModifiedCount + writeResult.Upserts.Count;
                                }
                                else
                                {
                                    updateCount = updateModels.Count;
                                }
                                
                                counterDelegate(unit, CounterType.Processed, ChangeStreamOperationType.Update, (int)updateCount);
                                result.Processed += (int)updateCount;
                            }
                            break; // Success - exit retry loop
                        }
                        catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                        {
                            if (attempt < MAX_RETRIES)
                            {
                                int delay = CalculateRetryDelay(attempt);
                                _log.WriteLine($"{_logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms...", LogType.Warning);
                                await Task.Delay(delay);
                            }
                            else
                            {
                                string errorMsg = $"CRITICAL: Unable to process update batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts due to persistent deadlock.";
                                _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                                result.Success = false;
                                result.Errors.Add(errorMsg);
                                throw new InvalidOperationException(errorMsg);
                            }
                        }
                        catch (Exception ex) when (IsTransientException(ex))
                        {
                            if (attempt < MAX_RETRIES)
                            {
                                int delay = CalculateRetryDelay(attempt);
                                string errorType = GetTransientErrorType(ex);
                                _log.WriteLine($"{_logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms... Details: {ex}", LogType.Warning);
                                await Task.Delay(delay);
                            }
                            else
                            {
                                string errorMsg = $"CRITICAL: Unable to process update batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts. Details: {ex}";
                                _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                                result.Success = false;
                                result.Errors.Add(errorMsg);
                                throw new InvalidOperationException(errorMsg);
                            }
                        }
                    }
                }
                finally
                {
                    writeStopwatch.Stop();
                    result.WriteLatencyMS += writeStopwatch.ElapsedMilliseconds;
                }
            }

            return result;
        }

        /// <summary>
        /// Process deletes with retry logic
        /// </summary>
        private async Task<WriteResult> ProcessDeletesWithRetryAsync<TMigration>(
            TMigration unit,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize,
            bool isAggressive,
            bool isAggressiveComplete,
            AggressiveChangeStreamHelper? aggressiveHelper,
            string databaseName,
            string collectionName,
            bool isSimulatedRun)
        {
            var result = new WriteResult { Success = true };
            var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();

            try
            {

                // Handle aggressive mode: store event entries in temp and remove old entries
                if (await ProcessAggresiveCSAsync(unit, collection, events, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun, "delete") == false)
                {
                    return result;
                }

                //// Handle aggressive mode: remove any insert/update entries for these document keys
                //if (isAggressive && !isAggressiveComplete && aggressiveHelper != null)
                //{
                //    var documentKeys = events
                //        .Where(deleteEvent => deleteEvent.DocumentKey != null)
                //        .Select(deleteEvent => deleteEvent.DocumentKey)
                //        .ToList();

                //    if (documentKeys.Count > 0)
                //    {
                //        // Remove any insert/update entries for these document keys since they're now being deleted
                //        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                //    }

                //    // Exit early - don't write to destination during aggressive mode
                //    return result;
                //}

                //// Clean up stale entries from temp collection after aggressive mode completes but cache not yet deleted
                //if (isAggressive && isAggressiveComplete && !mu.AggressiveCacheDeleted && aggressiveHelper != null)
                //{
                //    var documentKeys = events
                //        .Where(deleteEvent => deleteEvent.DocumentKey != null)
                //        .Select(deleteEvent => deleteEvent.DocumentKey)
                //        .ToList();

                //    if (documentKeys.Count > 0)
                //    {
                //        // Remove any stale entries for these document keys from temp collection
                //        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeys);
                //    }
                //}

                // Use the internal ProcessDeletesAsync method
                int failures = await ProcessDeletesAsync(
                    unit,
                    collection,
                    events,
                    counterDelegate,
                    batchSize,
                    isAggressive,
                    isAggressiveComplete,
                    string.Empty, // jobId
                    null, // targetClient
                    isSimulatedRun);

                if (failures > 0)
                {
                    result.Failures += failures;
                    _log.WriteLine($"{_logPrefix}ProcessDeletesAsync reported {failures} failures", LogType.Debug);
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
            {
                // Critical error - propagate up
                result.Success = false;
                result.Errors.Add(ex.Message);
                throw;
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Failures++;
                result.Errors.Add($"Delete processing error. Details: {ex}");
                _log.WriteLine($"{_logPrefix}Error processing deletes. Details: {ex}", LogType.Error);
            }
            finally
            {
                writeStopwatch.Stop();
                result.WriteLatencyMS += writeStopwatch.ElapsedMilliseconds;
            }

            return result;
        }

        /// <summary>
        /// Calculate retry delay with exponential backoff
        /// </summary>
        private int CalculateRetryDelay(int attempt)
        {
            int delay = INITIAL_RETRY_DELAY_MS * (int)Math.Pow(2, attempt);
            return Math.Min(delay, MAX_RETRY_DELAY_MS);
        }

        /// <summary>
        /// Check if exception is transient and should be retried
        /// </summary>
        private bool IsTransientException(Exception ex)
        {
            return ex is MongoConnectionException ||
                   ex is MongoExecutionTimeoutException ||
                   ex is TimeoutException ||
                   (ex is MongoCommandException cmdEx && 
                    (cmdEx.Message.Contains("operation exceeded time limit") ||
                     cmdEx.Message.Contains("network error") ||
                     cmdEx.Message.Contains("connection")));
        }

        /// <summary>
        /// Process deletes with standard retry logic (not using parallel processing)
        /// </summary>
        internal async Task<int> ProcessDeletesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null,
            bool isSimulatedRun = false)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            var aggressiveHelper = InitializeAggressiveHelper(isAggressive, isAggressiveComplete, targetClient, jobId);

            // Delete operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate deletes by _id within the same batch
                var deduplicatedDeletes = batch
                    .GroupBy(e => e.DocumentKey != null ? e.DocumentKey.ToJson() : string.Empty) // safe null handling
                    .Where(g => !string.IsNullOrEmpty(g.Key))
                    .Select(g => g.First())
                    .ToList();


                // Handle aggressive change stream scenario - store document keys instead of deleting
                var (shouldSkipDelete, deleteFailures) = await StoreDocumentKeysForAggressiveMode(
                    isAggressive, isAggressiveComplete, aggressiveHelper, isSimulatedRun,
                    deduplicatedDeletes.Where(e => e.DocumentKey != null).Select(e => e.DocumentKey).ToList(),
                    databaseName, collectionName, "delete",
                    mu, incrementCounter, ChangeStreamOperationType.Delete);
                
                failures += deleteFailures;
                
                if (shouldSkipDelete)
                {
                    continue; // Skip normal delete processing, move to next batch
                }
                // Normal delete processing
                var deleteModels = deduplicatedDeletes
                    .Select(e =>
                    {
                        try
                        {
                            if (!e.DocumentKey.Contains("_id"))
                            {
                                _log.WriteLine($"{_logPrefix} Delete event missing _id in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                return null;
                            }

                            var id = e.DocumentKey.GetValue("_id", null);
                            if (id == null)
                            {
                                _log.WriteLine($"{_logPrefix} _id is null in DocumentKey in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                return null;
                            }

                            if (id.IsObjectId)
                                id = id.AsObjectId;

                            var filter = MongoHelper.BuildFilterFromDocumentKey(e.DocumentKey);
                            return new DeleteOneModel<BsonDocument>(filter);
                        }
                        catch (Exception dex)
                        {
                            _log.WriteLine($"{_logPrefix} Error building delete model in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}. Details: {dex}"); 
                            return null;
                        }
                    })
                    .Where(model => model != null)
                    .Cast<DeleteOneModel<BsonDocument>>()
                    .ToList();

                if (deleteModels.Any())
                {
                    try
                    {
                        var (success, deletedCount) = await ExecuteBulkWriteWithRetry(
                            collection, deleteModels, "delete", isSimulatedRun,
                            result => result.DeletedCount);

                        if (success)
                        {
                            incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, (int)deletedCount);
                        }
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        // Count successful deletes even when some fail
                        long deletedCount = ex.Result?.DeletedCount ?? 0;

                        // Log errors that are not "document not found" (which is expected for deletes)
                        var significantErrors = ex.WriteErrors
                            .Where(err => !err.Message.Contains("not found") && err.Code != 11000)
                            .ToList();

                        if (significantErrors.Any())
                        {
                            failures += significantErrors.Count;
                            _log.WriteLine($"{_logPrefix} Bulk delete error in {collection.CollectionNamespace.FullName}: {string.Join(", ", significantErrors.Select(e => e.Message))}");
                        }

                        incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, (int)deletedCount);
                    }
                }
            }
            return failures; // Return the count of failures encountered during processing
        }

        /// <summary>
        /// Initializes aggressive change stream helper if needed
        /// </summary>
        private AggressiveChangeStreamHelper? InitializeAggressiveHelper(
            bool isAggressive, 
            bool isAggressiveComplete, 
            MongoClient? targetClient, 
            string jobId)
        {
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                return new AggressiveChangeStreamHelper(targetClient, _log, jobId);
            }
            return null;
        }

        /// <summary>
        /// Stores document keys for aggressive change stream processing during bulk copy
        /// Returns (shouldSkipNormalProcessing, failureCount)
        /// </summary>
        private async Task<(bool ShouldSkip, int FailureCount)> StoreDocumentKeysForAggressiveMode<TMigration>(
            bool isAggressive,
            bool isAggressiveComplete,
            AggressiveChangeStreamHelper? aggressiveHelper,
            bool isSimulatedRun,
            IEnumerable<BsonDocument> documentKeys,
            string databaseName,
            string collectionName,
            string operationType,
            TMigration mu,
            CounterDelegate<TMigration> incrementCounter,
            ChangeStreamOperationType csOperationType)
        {
            if (!isAggressive || isAggressiveComplete || aggressiveHelper == null || isSimulatedRun)
            {
                return (false, 0);
            }

            if (documentKeys == null || !documentKeys.Any())
            {
                return (true, 0); // Still skip normal processing
            }

            try
            {
                int storedCount = await aggressiveHelper.StoreDocumentKeysAsync(databaseName, collectionName, documentKeys, operationType);
                incrementCounter(mu, CounterType.Processed, csOperationType, storedCount);
                return (true, 0); // Skip normal processing, no failures
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_logPrefix} Error storing {operationType} document keys for aggressive change stream. Details: {ex}", LogType.Error);
                return (true, 1); // Skip normal processing, 1 failure
            }
        }

        /// <summary>
        /// Executes bulk write operation with retry logic for transient errors
        /// </summary>
        private async Task<(bool Success, long ProcessedCount)> ExecuteBulkWriteWithRetry<TModel>(
            IMongoCollection<BsonDocument> collection,
            List<TModel> models,
            string operationName,
            bool isSimulatedRun,
            Func<BulkWriteResult<BsonDocument>, long> getCountFunc) where TModel : WriteModel<BsonDocument>
        {
            if (!models.Any())
            {
                return (true, 0);
            }

            int maxRetries = MAX_RETRIES;
            int retryDelayMs = INITIAL_RETRY_DELAY_MS;
            long processedCount = 0;

            for (int attempt = 0; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (!isSimulatedRun)
                    {
                        var result = await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false });
                        processedCount = getCountFunc(result);
                    }
                    else
                    {
                        processedCount = models.Count;
                    }
                    return (true, processedCount);
                }
                catch (MongoBulkWriteException<BsonDocument>)
                {
                    // Let MongoBulkWriteException bubble up to caller for specialized handling
                    throw;
                }
                catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                {
                    if (attempt < maxRetries)
                    {
                        int delay = CalculateRetryDelay(attempt);
                        _log.WriteLine($"{_logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{maxRetries} after {delay}ms...", LogType.Warning);
                        await Task.Delay(delay);
                    }
                    else
                    {
                        _log.WriteLine($"{_logPrefix}Deadlock persisted after {maxRetries} retries for {collection.CollectionNamespace.FullName}. ", LogType.Error);
                        throw new InvalidOperationException(
                            $"CRITICAL: Unable to process {operationName} batch for {collection.CollectionNamespace.FullName} after {maxRetries} retry attempts due to persistent deadlock.");
                    }
                }
                catch (Exception ex) when (IsTransientException(ex))
                {
                    if (attempt < maxRetries)
                    {
                        int delay = CalculateRetryDelay(attempt);
                        string errorType = GetTransientErrorType(ex);
                        _log.WriteLine($"{_logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{maxRetries} after {delay}ms... Delays: {ex}", LogType.Warning);
                        await Task.Delay(delay);
                    }
                    else
                    {
                        _log.WriteLine($"{_logPrefix}Transient error persisted after {maxRetries} retries for {collection.CollectionNamespace.FullName}.Batch size: {models.Count} documents.", LogType.Error);
                        throw new InvalidOperationException(
                            $"CRITICAL: Unable to process {operationName} batch for {collection.CollectionNamespace.FullName} after {maxRetries} retry attempts due to persistent errors. Delays: {ex}");
                    }
                }
            }

            return (false, 0);
        }

        /// <summary>
        /// Get human-readable error type for logging
        /// </summary>
        private string GetTransientErrorType(Exception ex)
        {
            if (ex is MongoConnectionException) return "Connection error";
            if (ex is MongoExecutionTimeoutException) return "Execution timeout";
            if (ex is TimeoutException) return "Timeout";
            if (ex is MongoCommandException) return "Command error";
            return "Transient error";
        }

        /// <summary>
        /// Represents an ordered operation in the write sequence
        /// </summary>
        private class OrderedOperation
        {
            public long SequenceNumber { get; set; }
            public ChangeStreamOperationType OperationType { get; set; }
            public ChangeStreamDocument<BsonDocument> Event { get; set; } = null!;
            public BsonTimestamp Timestamp { get; set; } = new BsonTimestamp(0, 0);
        }
    }

    /// <summary>
    /// Result of write processing (single batch or aggregated)
    /// </summary>
    public class WriteResult
    {
        public bool Success { get; set; } = true;
        public int Processed { get; set; }
        public int Failures { get; set; }
        public int Skipped { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
        public long WriteLatencyMS { get; set; } = 0;
    }
}
