using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
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
        private const int MaxConcurrentPartitions = 1;
        private static readonly TimeSpan BatchDuration = TimeSpan.FromSeconds(60);


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

                List<Task> tasks = new List<Task>();
                foreach (var chunk in chunksToProcess)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            if (targetCollection != null)
                            {
                                await ProcessChunksInBatchesAsync(chunk, mu, ctx.Collection, targetCollection, batchCts.Token, _cts.Token, _job.IsSimulatedRun);
                            }
                        }
                        finally
                        {
                            //semaphore.Release();
                        }
                    }));
                    
                }
                await Task.WhenAll(tasks);


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

                //if (progressPercent == 100)
                //{
                //    mu.DumpComplete = true;
                //    mu.RestoreComplete = true;
                //    mu.BulkCopyEndedOn = DateTime.UtcNow;
                //    batchCts.Dispose();                  

                //    return TaskResult.Success;
                //}
                _jobList?.Save();                
            }            
            return TaskResult.Retry; // If we reach here, it means processing was cancelled or timed out
        }


        List <ChangeStreamDocument<BsonDocument>> _changeStreamDocuments = new List<ChangeStreamDocument<BsonDocument>>();


        /// <summary>
        /// Process one partition's historical changes for a batch duration
        /// </summary>
        private Task ProcessChunksInBatchesAsync(MigrationChunk chunk,MigrationUnit mu, IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection, CancellationToken timeoutCts, CancellationToken manualToken, bool isSimulated)
        {
            if (chunk.IsUploaded == true)
                return Task.CompletedTask;


            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts, manualToken);

            int counter = 0;

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
                using var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options);

                long currentLSN;

                BsonDocument? resumeToken = null;
                while (cursor.MoveNext(linkedCts.Token))
                {
                    timeoutCts.ThrowIfCancellationRequested();

                    foreach (var change in cursor.Current)
                    {
                        var document = change.FullDocument;

                        Console.WriteLine(change.DocumentKey.ToJson());

                        _changeStreamDocuments.Add(change);

                        // Save the latest token
                        resumeToken = change.ResumeToken;


                        counter++;
                        // Check for cancellation
                        if (linkedCts.IsCancellationRequested)
                        {
                            return Task.CompletedTask;
                        }

                        if (counter > _config.ChangeStreamMaxDocsInBatch)
                        {
                            BulkProcessChangesAsync(chunk, targetCollection).GetAwaiter().GetResult();
                        }

                    }

                    BulkProcessChangesAsync(chunk, targetCollection).GetAwaiter().GetResult();

                    if (resumeToken == null)
                        continue;

                    try
                    {
                        currentLSN = MongoHelper.ExtractLSNFromResumeToken(resumeToken);

                        if (currentLSN >= chunk.RUStopLSN)
                        {
                            chunk.IsUploaded = true;
                            _log.WriteLine($"Chunk [{chunk.Id}] offline copy completed.");
                            return Task.CompletedTask;
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Error processing chunk {chunk.Id}: {ex}", LogType.Error);
                        IncrementSkippedCounter(chunk);
                        continue;

                    }
                }
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !manualToken.IsCancellationRequested)
            {
                //if full batch duration was spent and then operation was cancelled without any change we can assume that partition processing is complete
                if (counter==0)
                {
                    chunk.IsUploaded = true;
                    _log.WriteLine($"Chunk [{chunk.Id}] offline copy completed.");
                    return Task.CompletedTask;
                }
            }

            catch (Exception ex)
            {
                _log.WriteLine($"Error processing chunk {chunk.Id}: {ex}", LogType.Error);
                //throw;
            }
            return Task.CompletedTask;
        }

        private async Task BulkProcessChangesAsync(MigrationChunk chunk, IMongoCollection<BsonDocument> targetCollection)
        {
            if(targetCollection==null || _changeStreamDocuments.Count == 0)
            {
                // No changes to process
                return;
            }
            // Create the counter delegate implementation
            CounterDelegate<MigrationChunk> counterDelegate = (t, counterType, operationType, count) => IncrementDocCounter(chunk, count);
            await MongoHelper.ProcessInsertsAsync<MigrationChunk>(chunk, targetCollection, _changeStreamDocuments, counterDelegate, _log);
            _changeStreamDocuments.Clear();
        }

        private void IncrementSkippedCounter(MigrationChunk chunk, int incrementBy = 1)
        {
            chunk.SkippedAsDuplicateCount += incrementBy;
        }

        private void IncrementDocCounter(MigrationChunk chunk, int incrementBy = 1)
        {
            chunk.DocCountInTarget += incrementBy;
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
            else
            {
                _log.WriteLine($"{processName} error: {ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }


        private Task PostCopyChangeStreamProcessor(ProcessorContext ctx, MigrationUnit mu)
        {
            if (mu.RestoreComplete && mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (_job.IsOnline && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads)
                    {
                        if (_targetClient == null && !_job.IsSimulatedRun)
                            _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                        if (_changeStreamProcessor == null && _targetClient != null && _sourceClient != null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                        _changeStreamProcessor?.AddCollectionsToProcess(mu, _cts);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        var migrationJob = _jobList.MigrationJobs?.Find(m => m.Id == ctx.JobId);
                        if (migrationJob != null && !_job.IsOnline && Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            _log.WriteLine($"{migrationJob.Id} completed.");
                            migrationJob.IsCompleted = true;
                            StopProcessing(true);
                        }
                        else if (migrationJob != null && _job.IsOnline && _job.CSStartsAfterAllUploads && Helper.IsOfflineJobCompleted(migrationJob) && !_postUploadCSProcessing)
                        {
                            _postUploadCSProcessing = true;

                            if (_targetClient == null && !_job.IsSimulatedRun)
                                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                            if (_changeStreamProcessor == null && _targetClient != null && _sourceClient != null)
                                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                            if (_changeStreamProcessor != null)
                            {
                                var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                            }
                        }
                    }
                }
                catch
                {
                    // Do nothing
                }
            }
            return Task.CompletedTask;
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
            }

            await PostCopyChangeStreamProcessor(ctx, mu);
        }
    }
}
