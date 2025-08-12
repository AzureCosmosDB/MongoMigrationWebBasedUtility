using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Partitioner;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.MongoHelper;
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
        private const int MaxConcurrentPartitions = 20;
        private static readonly TimeSpan BatchDuration = TimeSpan.FromSeconds(60);
        private static readonly object _processingLock = new object();

        public RUCopyProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
           : base(log, jobList, job, sourceClient, config)
        {
            // Constructor body can be empty or contain initialization logic if needed
        }

        private async Task<TaskResult> ProcessChunksAsync(MigrationUnit mu, ProcessorContext ctx)
        {
            
            // Setup target client and collection
            if (_targetClient == null && !_job.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

            IMongoCollection<BsonDocument>? targetCollection = null;
            if (!_job.IsSimulatedRun)
            {
                var targetDatabase = _targetClient!.GetDatabase(ctx.DatabaseName);
                targetCollection = targetDatabase.GetCollection<BsonDocument>(ctx.CollectionName);
            }

            // Process partitions in batches
            while (mu.MigrationChunks.Any(s => s.IsUploaded == false) && !_cts.Token.IsCancellationRequested)
            {
                // Check for cancellation
                if (_cts.Token.IsCancellationRequested)
                {
                    return TaskResult.Abort;
                }

                var chunksToProcess = mu.MigrationChunks
                    .Where(s => s.IsUploaded == false)
                    .Take(MaxConcurrentPartitions)
                    .ToList();

                var batchCts = new CancellationTokenSource(BatchDuration);

                // Check for cancellation
                if (_cts.Token.IsCancellationRequested)
                {
                    return TaskResult.Abort;
                }

                SemaphoreSlim semaphore = new SemaphoreSlim(MaxConcurrentPartitions);
                List<Task<TaskResult>> tasks = new List<Task<TaskResult>>();
                foreach (var chunk in chunksToProcess)
                {

                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            if (targetCollection != null)
                            {
                                return await ProcessChunksInBatchesAsync(chunk, mu, ctx.Collection, targetCollection, batchCts.Token, _cts.Token, _job.IsSimulatedRun);
                            }
                            else
                            {
                                _log.WriteLine($"Target collection is null for {ctx.DatabaseName}.{ctx.CollectionName}. Skipping chunk {chunk.Id} processing.",LogType.Error);
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
                    _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName} was cancelled.");
                    return TaskResult.Canceled;
                }

                // Check for cancellation
                if (_cts.Token.IsCancellationRequested)
                {
                    return TaskResult.Abort;
                }

                _cts.Token.ThrowIfCancellationRequested();

                var completedCount = mu.MigrationChunks.Count(s => s.IsUploaded == true);
                var totalProcessed = mu.MigrationChunks.Sum(s => (long)s.DocCountInTarget);

                _log.WriteLine($"Batch completed. RU partitions completed: {completedCount}/{mu.MigrationChunks.Count}, " +
                                $"Total documents processed: {totalProcessed}");

                // Update progress
                var progressPercent = Math.Min(100, (double)totalProcessed/ Math.Max(mu.EstimatedDocCount,mu.ActualDocCount) * 100);
                mu.DumpPercent = progressPercent;
                mu.RestorePercent = progressPercent;
                
                _jobList?.Save();                
            } 
            
            if(mu.MigrationChunks.All(s => s.IsUploaded == true))
            {

                mu.DumpComplete = true;
                mu.RestoreComplete = true;
                mu.BulkCopyEndedOn = DateTime.UtcNow;
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
     
            if (chunk.IsUploaded == true)
                return TaskResult.Success;

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts, manualToken);

            int counter = 0;

            _log.AddVerboseMessage("Processing RU partition: " + chunk.Id);

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

                List<ChangeStreamDocument<BsonDocument>> changeStreamDocuments = new List<ChangeStreamDocument<BsonDocument>>();
                // Create the change stream cursor
                using var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);

                long currentLSN;
                long batchCounter=0;
                BsonDocument? resumeToken = null;
                while (cursor.MoveNext(linkedCts.Token))
                {
                    timeoutCts.ThrowIfCancellationRequested();

                    foreach (var change in cursor.Current)
                    {

                        var document = change.FullDocument;

                        changeStreamDocuments.Add(change);

                        // Save the latest token
                        resumeToken = change.ResumeToken;
                        batchCounter++;
                        counter++;
                        // Check for cancellation
                        if (manualToken.IsCancellationRequested)
                        {
                            return TaskResult.Canceled;
                        }

                        if (counter > _config.ChangeStreamMaxDocsInBatch)
                        {
                            await BulkProcessChangesAsync(chunk, targetCollection, changeStreamDocuments);
                            counter = 0;
                        }
                    }
                   
                    _log.AddVerboseMessage($"Processing RU partition {chunk.Id}, processed {batchCounter}");
                    await BulkProcessChangesAsync(chunk, targetCollection, changeStreamDocuments);

                    if (resumeToken == null)
                        continue;

                    // persits every 1000 docs
                    int lockCount = 0;
                    if (lockCount < (int)(batchCounter / 1000))
                    {
                        lockCount = (int)(batchCounter / 1000);
                        lock (_processingLock)
                        {
                            // Save the latest resume token to the chunk
                            chunk.RUPartitionResumeToken = resumeToken.ToJson();
                            _jobList?.Save();
                        }
                    }

                    try
                    {
                        // Extract the LSN from the resume token using ExtractValuesFromResumeToken helper method
                        var (lsn, rid, min, max) = MongoHelper.ExtractValuesFromResumeToken(resumeToken);
                        currentLSN = lsn;
                       
                        if (currentLSN >= chunk.RUStopLSN)
                        {
                            chunk.IsUploaded = true;
                            _log.WriteLine($"Chunk [{chunk.Id}] offline copy completed.");
                            return TaskResult.Success;
                        }
                    }
                    catch (OperationCanceledException) when (!timeoutCts.IsCancellationRequested && manualToken.IsCancellationRequested)
                    {
                        return TaskResult.Canceled;
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Error processing chunk {chunk.Id}: {ex}", LogType.Error);
                        IncrementSkippedCounter(chunk);
                        continue;
                    }                    
                }
                lock (_processingLock)
                {
                    // Save the latest resume token to the chunk
                    chunk.RUPartitionResumeToken = resumeToken.ToJson();
                    _jobList?.Save();
                }

            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !manualToken.IsCancellationRequested)
            {
                //if full batch duration was spent and then operation was cancelled without any change we can assume that partition processing is complete
                if (counter==0)
                {
                    chunk.IsUploaded = true;
                    _log.WriteLine($"Chunk [{chunk.Id}] offline copy completed.");
                    return TaskResult.Success;
                }
            }
            catch (OperationCanceledException) when (!timeoutCts.IsCancellationRequested && manualToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing chunk {chunk.Id}: {ex}", LogType.Error);
                return TaskResult.Retry;
            }
            return TaskResult.Retry;
        }

        private async Task BulkProcessChangesAsync(MigrationChunk chunk, IMongoCollection<BsonDocument> targetCollection, List<ChangeStreamDocument<BsonDocument>> changeStreamDocuments)
        {
            if(targetCollection==null || changeStreamDocuments.Count == 0)
            {
                // No changes to process
                return;
            }

            if (!_job.IsSimulatedRun)
            {
                // Create the counter delegate implementation
                CounterDelegate<MigrationChunk> counterDelegate = (t, counterType, operationType, count) => IncrementDocCounter(chunk, count);
                await MongoHelper.ProcessInsertsAsync<MigrationChunk>(chunk, targetCollection, changeStreamDocuments, counterDelegate, _log, $"Processing RU partition {chunk.Id} - ");
            }
            else
                IncrementDocCounter(chunk, changeStreamDocuments.Count);

            changeStreamDocuments.Clear();
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
                _log.WriteLine($"RU copy operation was cancelled for {dbName}.{colName} partition {partitionId}");
                return Task.FromResult(TaskResult.Abort);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed due to timeout. Details: {ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else if (ex.Message.Contains("Change Stream Token"))
            {
                _log.WriteLine($"{processName} attempt for {dbName}.{colName} partition {partitionId} failed. Retrying in {currentBackoff} seconds...");
                return Task.FromResult(TaskResult.Retry);
            }
            else if (ex.Message.Contains("New partitions found during copy process"))
            {
                _log.WriteLine(ex.Message,LogType.Error);
                return Task.FromResult(TaskResult.Abort);
            }
            else
            {
                _log.WriteLine($"{processName} error: {ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }
                        

        public override async Task StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            ProcessRunning = true;

            if (_job != null)
                _job.IsStarted = true;

            var ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            // Check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return;

            _log.WriteLine($"RU Copy Processor started for {ctx.DatabaseName}.{ctx.CollectionName}");

            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                    mu.BulkCopyStartedOn = DateTime.UtcNow;


                // Process using RU-optimized partition approach
                TaskResult result = await new RetryHelper().ExecuteTask(
                    () => ProcessChunksAsync(mu, ctx),
                    (ex, attemptCount, currentBackoff) => RUProcess_ExceptionHandler(
                        ex, attemptCount,
                        "RU Chunk processor", ctx.DatabaseName, ctx.CollectionName, "all", currentBackoff
                    ),
                    _log
                );

                if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                {
                    _log.WriteLine($"RU Copy operation for {ctx.DatabaseName}.{ctx.CollectionName} failed after multiple attempts.", LogType.Error);
                    StopProcessing();
                    return;
                }
                else if (result == TaskResult.Canceled)
                {
                    _log.WriteLine($"RU Copy operation for {ctx.DatabaseName}.{ctx.CollectionName} was cancelled.");
                    StopProcessing();
                    return;
                }
            }

            //check if any partiton split ocuured during the copy process
            TaskResult vresult = await new RetryHelper().ExecuteTask(
                () => CheckForPartitionSplitsAsync(mu),
                (ex, attemptCount, currentBackoff) => RUProcess_ExceptionHandler(
                    ex, attemptCount,
                    "Verification processor", ctx.DatabaseName, ctx.CollectionName, "all", currentBackoff
                ),
                _log
            );

            if (vresult == TaskResult.Abort || vresult == TaskResult.FailedAfterRetries)
            {
                StopProcessing();
                return;
            }

            await PostCopyChangeStreamProcessor(ctx, mu);
        }

        private async Task<TaskResult> CheckForPartitionSplitsAsync(MigrationUnit unit)
        {
            bool newChunksFound = false;
            List<string> intactPartitions = new List<string>();
            //add a dumy await to ensure the method is async    
            await Task.Yield();

            //get listof new partitions
            var chunks = new RUPartitioner().CreatePartitions(_log, _sourceClient!, unit.DatabaseName, unit.CollectionName, _cts.Token,true);

            //compare with current chunks to identify new chunks tobe added            
            foreach (var chunk in chunks)
            {
                var matchingChunk = unit.MigrationChunks
                        .FirstOrDefault(c => c.Lt == chunk.Lt && c.Gte == chunk.Gte);
                if (matchingChunk != null)
                {
                    intactPartitions.Add(matchingChunk.Id); //partition is safe
                }
                else
                {
                    // Add new chunk
                    chunk.Id = (unit.MigrationChunks.Count + 1).ToString();
                    unit.MigrationChunks.Add(chunk);
                    unit.ChangeStreamStartedOn = DateTime.UtcNow;
                    newChunksFound = true;                    
                }
            }

            //need to stop processing the parent partitions
            var stopChunks = unit.MigrationChunks
                .Where(c => !intactPartitions.Contains(c.Id))
                .ToList();
            foreach (var chunk in stopChunks)
            {
                chunk.IsDownloaded = true;
                chunk.IsUploaded = true;
            }

            // stop processing if new chunks found.
            if (newChunksFound)
            {
                unit.DumpComplete = false;
                unit.RestoreComplete = false;
                _jobList?.Save();
                throw new Exception("New partitions found during copy process. Please pause and re-run the job to process new partitions.");
            }
            else
                return TaskResult.Success;
        }
    }
}
