using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Partitioner;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;
using static System.Runtime.InteropServices.JavaScript.JSType;

// Nullability and fire-and-forget warnings addressed in code; no pragmas required.

namespace OnlineMongoMigrationProcessor.Processors
{
    /// <summary>
    /// RU (Request Unit) Copy Processor - Implements incremental Change Feed processing via extension command
    /// for Cosmos DB MongoDB API to efficiently handle large collections with partition-based processing.
    /// </summary>
    internal class RUCopyProcessor : MigrationProcessor
    {

        // RU-specific configuration
        private static readonly TimeSpan BatchDuration = TimeSpan.FromSeconds(60);
        private static readonly object _processingLock = new object();
        private readonly ParallelWriteHelper _parallelWriteHelper;

        public RUCopyProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
           : base(log, sourceClient, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog("RUCopyProcessor: Constructor called");
            _parallelWriteHelper = new ParallelWriteHelper(log, "RUCopyProcessor");
        }

        private async Task<TaskResult> ProcessChunksAsync(MigrationUnit mu, ProcessorContext ctx)
        {
            MigrationJobContext.AddVerboseLog($"RUCopyProcessor.ProcessChunksAsync: mu.Id={mu.Id}, database={ctx.DatabaseName}, collection={ctx.CollectionName}");
            
            // Setup target client and collection
            if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

            IMongoCollection<BsonDocument>? targetCollection = null;
            if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                var targetDatabase = _targetClient!.GetDatabase(ctx.DatabaseName);
                targetCollection = targetDatabase.GetCollection<BsonDocument>(ctx.CollectionName);
            }

            // Process partitions in sequential batches
            while (mu.MigrationChunks.Any(s => s.IsUploaded == false) && !_cts.Token.IsCancellationRequested)
            {
                // Check for cancellation
                if (_cts.Token.IsCancellationRequested)
                    return TaskResult.Canceled;

                int maxConcurrentPartitions = MigrationJobContext.CurrentlyActiveJob?.ParallelThreads ?? Environment.ProcessorCount * 5;
  
                var chunksToProcess = mu.MigrationChunks
                    .Where(s => s.IsUploaded == false)
                    .Take(maxConcurrentPartitions)
                    .ToList();

                // If no chunks to process, break the loop
                if (!chunksToProcess.Any())
                    break;

                var batchCts = new CancellationTokenSource(BatchDuration);

                // Check for cancellation
                if (_cts.Token.IsCancellationRequested)
                    return TaskResult.Canceled;

                SemaphoreSlim semaphore = new SemaphoreSlim(maxConcurrentPartitions);
                List<Task<TaskResult>> tasks = new List<Task<TaskResult>>();
                
                foreach (var chunk in chunksToProcess)
                {
                    await semaphore.WaitAsync(_cts.Token);
                    
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            if (targetCollection != null)
                            {
                                return await ProcessChunksInBatchesAsync(chunk, mu, ctx.Collection, targetCollection, batchCts.Token, _cts.Token, MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun);
                            }
                            else
                            {
                                _log.WriteLine($"Target collection is null for {ctx.DatabaseName}.{ctx.CollectionName}.", LogType.Error);
                                return TaskResult.Retry;
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }));
                }
                
                TaskResult[] results = await Task.WhenAll(tasks);

                if (results.Any(r => r == TaskResult.Canceled))
                {
                    _log.WriteLine($"RU copy operation for {ctx.DatabaseName}.{ctx.CollectionName} was paused.");
                    return TaskResult.Canceled;
                }

                // Check for cancellation
                if (_cts.Token.IsCancellationRequested)
                {
                    return TaskResult.Canceled;
                }

                _cts.Token.ThrowIfCancellationRequested();

                var completedCount = mu.MigrationChunks.Count(s => s.IsUploaded == true);
                var totalProcessed = mu.MigrationChunks.Sum(s => (long)s.DocCountInTarget);

                _log.WriteLine($"Batch with {chunksToProcess.Count} RU partitions processed {totalProcessed} documents. Completed: {completedCount}/{mu.MigrationChunks.Count}, " +
                                $"New batch to continue processing");

                // Update progress
                var progressPercent = Math.Min(100, (double)totalProcessed / Math.Max(mu.EstimatedDocCount, mu.ActualDocCount) * 100);
                mu.DumpPercent = progressPercent;
                mu.RestorePercent = progressPercent;

                MigrationJobContext.SaveMigrationUnit(mu,true);

            }

            if (mu.MigrationChunks.All(s => s.IsUploaded == true))
            {
                mu.DumpComplete = true;
                mu.RestoreComplete = true;
                mu.BulkCopyEndedOn = DateTime.UtcNow;

                // Start change stream processing for the completed migration unit
                AddCollectionToChangeStreamQueue(mu);

                MigrationJobContext.SaveMigrationUnit(mu,true);
                
                MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(mu.Id);
                _log.WriteLine($"RU copy completed for {ctx.DatabaseName}.{ctx.CollectionName}.");
                return TaskResult.Success;
            }

            return TaskResult.Retry; // If we reach here, it means processing was cancelled or timed out
        }





        /// <summary>
        /// Process one partition's historical changes for a batch duration
        /// </summary>
        private async Task<TaskResult> ProcessChunksInBatchesAsync(MigrationChunk chunk,MigrationUnit mu, IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection, CancellationToken timeoutCts, CancellationToken manualToken, bool isSimulated)
        {
            int counter = 0;
            List<ChangeStreamDocument<BsonDocument>> accumulatedChangesInColl = new List<ChangeStreamDocument<BsonDocument>>();

            long currentLSN;
            BsonDocument? resumeToken = null;

            if (chunk.IsUploaded == true)
                return TaskResult.Success;

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts, manualToken);    

            _log.ShowInMonitor("Processing RU partition: " + chunk.Id);

            try
            {
                var options = new ChangeStreamOptions
                {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    ResumeAfter = BsonDocument.Parse(chunk.RUPartitionResumeToken) 
                };

               
                var pipeline = new BsonDocument[]
                {
                    new BsonDocument("$match", new BsonDocument("operationType",
                        new BsonDocument("$in", new BsonArray { "insert","update","replace"})
                    )),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "_id", 1 },
                        { "fullDocument", 1 },
                        { "ns", 1 },
                        { "documentKey", 1 }
                    })
                };

                
                // Create the change stream cursor
                using var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);

                
                
                while (cursor.MoveNext(linkedCts.Token))
                {
                    timeoutCts.ThrowIfCancellationRequested();

                    foreach (var change in cursor.Current)
                    {

                        var document = change.FullDocument;

                        accumulatedChangesInColl.Add(change);

                        // Save the latest token
                        resumeToken = change.ResumeToken;
                        counter++;
       
                        // Check for cancellation
                        if (manualToken.IsCancellationRequested)
                            return TaskResult.Canceled;

                        if (accumulatedChangesInColl.Count > _config.ChangeStreamMaxDocsInBatch)
                        {
                            await BulkProcessChangesAsync(chunk, targetCollection, accumulatedChangesInColl);
                         }
                    }
                   
                    _log.ShowInMonitor($"Processing partition {mu.DatabaseName}.{mu.CollectionName}.[{chunk.Id}], processed {counter}.");
                    await BulkProcessChangesAsync(chunk, targetCollection, accumulatedChangesInColl);

                    if (resumeToken == null)
                        continue;

                    // Save resume token
                    lock (_processingLock)
                    {
                        // Save the latest resume token to the chunk
                        chunk.RUPartitionResumeToken = resumeToken.ToJson();
                        MigrationJobContext.SaveMigrationUnit(mu,false);
                    }
                    

                    try
                    {
                        // Extract the LSN from the resume token using ExtractValuesFromResumeToken helper method
                        var (lsn, rid, min, max) = MongoHelper.ExtractValuesFromResumeToken(resumeToken);
                        currentLSN = lsn;
                       
                        if (currentLSN >= chunk.RUStopLSN)
                        {
                            lock (_processingLock)
                            {
                                chunk.IsUploaded = true;
                                MigrationJobContext.SaveMigrationUnit(mu,false);
                            }
                            _log.WriteLine($"Partition {mu.DatabaseName}.{mu.CollectionName}.[{chunk.Id}] offline copy completed.");
                            return TaskResult.Success;
                        }
                    }
                    catch (OperationCanceledException) when (!timeoutCts.IsCancellationRequested && manualToken.IsCancellationRequested)
                    {
                        return TaskResult.Canceled;
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Error processing partition {mu.DatabaseName}.{mu.CollectionName}.[{chunk.Id}]: {ex}", LogType.Error);
                        IncrementSkippedCounter(chunk);
                        continue;
                    }                    
                }
                lock (_processingLock)
                {
                    // Save the latest resume token to the chunk
                    chunk.RUPartitionResumeToken = resumeToken.ToJson();
                    MigrationJobContext.SaveMigrationUnit(mu,false);
                }

            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !manualToken.IsCancellationRequested)
            {
                //if full batch duration was spent and then operation was cancelled without any change we can assume that partition processing is complete
                if (counter==0)
                {
                    lock (_processingLock)
                    {
                        chunk.IsUploaded = true;
                        MigrationJobContext.SaveMigrationUnit(mu,false);
                    }
                    _log.WriteLine($"Partition {mu.DatabaseName}.{mu.CollectionName}.[{chunk.Id}] offline copy completed.");
                    return TaskResult.Success;
                }

                //save the remaining items in the batch
                if (accumulatedChangesInColl.Count>0)
                {
                    _log.ShowInMonitor($"Processing partition {mu.DatabaseName}.{mu.CollectionName}.[{chunk.Id}], processed {counter}.");
                    if (resumeToken != null)
                    {
                        chunk.RUPartitionResumeToken = resumeToken.ToJson();
                        MigrationJobContext.SaveMigrationUnit(mu,false);
                    }
                    await BulkProcessChangesAsync(chunk, targetCollection, accumulatedChangesInColl);
                }
            }
            catch (OperationCanceledException) when (!timeoutCts.IsCancellationRequested && manualToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing partition {mu.DatabaseName}.{mu.CollectionName}.[{chunk.Id}]: {ex}", LogType.Error);
                return TaskResult.Retry;
            }
            return TaskResult.Retry;
        }

        private async Task BulkProcessChangesAsync(MigrationChunk chunk, IMongoCollection<BsonDocument> targetCollection, List<ChangeStreamDocument<BsonDocument>> accumulatedChangesInColl)
        {
            if(targetCollection==null || accumulatedChangesInColl.Count == 0)
            {
                // No changes to process
                return;
            }

            if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                // Create the counter delegate implementation
                CounterDelegate<MigrationChunk> counterDelegate = (t, counterType, operationType, count) => IncrementDocCounter(chunk, count);
                
                // Use ParallelWriteHelper for retry logic and better error handling
                var result = await _parallelWriteHelper.ProcessInsertsWithRetryAsync<MigrationChunk>(
                    chunk,
                    targetCollection,
                    accumulatedChangesInColl,
                    counterDelegate,
                    batchSize: 50,
                    isAggressive: false,
                    isAggressiveComplete: true,
                    aggressiveHelper: null,
                    targetCollection.Database.DatabaseNamespace.DatabaseName,
                    targetCollection.CollectionNamespace.CollectionName,
                    MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun);
                
                if (!result.Success)
                {
                    _log.WriteLine($"Failed to process inserts for partition {chunk.Id}: {string.Join(", ", result.Errors)}", LogType.Error);
                }
            }
            else
                IncrementDocCounter(chunk, accumulatedChangesInColl.Count);

            accumulatedChangesInColl.Clear();
        }

        private void IncrementSkippedCounter(MigrationChunk chunk, int incrementBy = 1)
        {            
            chunk.SkippedAsDuplicateCount += incrementBy;            
        }

        private void IncrementDocCounter(MigrationChunk chunk, int incrementBy = 1)
        {           
            chunk.DocCountInTarget += incrementBy;
            chunk.RestoredSuccessDocCount += incrementBy;
        }


        /// <summary>
        /// Custom exception handler for RU processing
        /// </summary>
        private Task<TaskResult> RUProcess_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, string partitionId, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"RU copy operation was paused for {dbName}.{colName} partition {partitionId}");
                return Task.FromResult(TaskResult.Abort);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {dbName}.{colName} partition {partitionId} failed due to timeout. Details: {ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else if (ex.Message.Contains("Change Stream Token"))
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {dbName}.{colName} partition {partitionId} failed. Retrying in {currentBackoff} seconds...");
                return Task.FromResult(TaskResult.Retry);
            }
            else if (ex.Message.Contains("New partitions found during copy process"))
            {
                _log.WriteLine(ex.Message,LogType.Error);
                return Task.FromResult(TaskResult.Abort);
            }
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {dbName}.{colName} failed. Error details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }
                        

        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
           
            ProcessRunning = true;
            var mu = MigrationJobContext.GetMigrationUnit(migrationUnitId);

            if (mu.DumpComplete && mu.RestoreComplete)
            {
                _log.WriteLine($"Document copy operation for {mu.DatabaseName}.{mu.CollectionName} already completed.", LogType.Debug);
                return TaskResult.Success;
            }


            mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
            if (MigrationJobContext.CurrentlyActiveJob != null)
                MigrationJobContext.CurrentlyActiveJob.IsStarted = true;

            var ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            _log.WriteLine($"RU copy Processor started for {ctx.DatabaseName}.{ctx.CollectionName}");

            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                    mu.BulkCopyStartedOn = DateTime.UtcNow;


                // Process using RU-optimized partition approach
                TaskResult result = await new RetryHelper().ExecuteTask(
                    () => ProcessChunksAsync(mu, ctx),
                    (ex, attemptCount, currentBackoff) => RUProcess_ExceptionHandler(
                        ex, attemptCount,
                        "RU chunk processor", ctx.DatabaseName, ctx.CollectionName, "all", currentBackoff
                    ),
                    _log
                );

                if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                {
                    _log.WriteLine($"RU copy operation for {ctx.DatabaseName}.{ctx.CollectionName} failed after multiple attempts.", LogType.Error);
                    StopProcessing();
                    return TaskResult.FailedAfterRetries;
                }
                else if (result == TaskResult.Canceled)
                {
                    _log.WriteLine($"RU copy operation for {ctx.DatabaseName}.{ctx.CollectionName} was paused.");
                    StopProcessing();
                    return TaskResult.Canceled;
                }
            }

            //check if any partiton split ocuured during the copy process
            TaskResult vresult = await new RetryHelper().ExecuteTask(
                () => CheckForPartitionSplitsAsync(mu, MigrationJobContext.CurrentlyActiveJob),
                (ex, attemptCount, currentBackoff) => RUProcess_ExceptionHandler(
                    ex, attemptCount,
                    "Verification processor", ctx.DatabaseName, ctx.CollectionName, "all", currentBackoff
                ),
                _log
            );

            if (vresult == TaskResult.Abort || vresult == TaskResult.FailedAfterRetries)
            {
                StopProcessing();
                return vresult;
            }

            StopOfflineOrInvokeChangeStreams();

            return TaskResult.Success;
        }

        private async Task<TaskResult> CheckForPartitionSplitsAsync(MigrationUnit mu, MigrationJob job)
        {
            bool newChunksFound = false;
            List<string> intactPartitions = new List<string>();
            //add a dumy await to ensure the method is async    
            await Task.Yield();

            //get listof new partitions
            var chunks = new RUPartitioner().CreatePartitions(_log, _sourceClient!, mu.DatabaseName, mu.CollectionName, _cts.Token,true);

            //compare with current chunks to identify new chunks tobe added            
            foreach (var chunk in chunks)
            {
                var matchingChunk = mu.MigrationChunks
                        .FirstOrDefault(c => c.Lt == chunk.Lt && c.Gte == chunk.Gte);
                if (matchingChunk != null)
                {
                    intactPartitions.Add(matchingChunk.Id); //partition is safe
                }
                else
                {
                    // Add new chunk
                    chunk.Id = mu.MigrationChunks.Count.ToString();
                    mu.MigrationChunks.Add(chunk);
                    mu.ChangeStreamStartedOn = DateTime.UtcNow;
                    newChunksFound = true;                    
                }
            }

            //need to stop processing the parent partitions
            var stopChunks = mu.MigrationChunks
                .Where(c => !intactPartitions.Contains(c.Id))
                .ToList();

            MigrationJobContext.SaveMigrationUnit(mu,false);

            foreach (var chunk in stopChunks)
            {
                chunk.IsDownloaded = true;
                chunk.IsUploaded = true;
            }

            // stop processing if new chunks found.
            if (newChunksFound)
            {
                mu.DumpComplete = false;
                mu.RestoreComplete = false;

                MigrationJobContext.SaveMigrationUnit(mu,true);
                
                throw new Exception("New partitions found during copy process. Please pause and re-run the job to process new partitions.");
            }
            else
            {
                MigrationJobContext.SaveMigrationUnit(mu,true);
                return TaskResult.Success;
            }
        }
    }
}
