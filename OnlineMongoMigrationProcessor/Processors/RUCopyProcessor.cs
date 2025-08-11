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
using static System.Runtime.InteropServices.JavaScript.JSType;

// Nullability and fire-and-forget warnings addressed in code; no pragmas required.

namespace OnlineMongoMigrationProcessor.Processors
{
    /// <summary>
    /// RU (Request Unit) Copy Processor - Implements incremental Change Feed processing via extension command
    /// for Cosmos DB MongoDB API to efficiently handle large collections with partition-based processing.
    /// </summary>
    internal class RUCopyProcessor : IMigrationProcessor
    {
        private JobList _jobList;
        private MigrationJob _job;
        private MongoClient _sourceClient;
        private MongoClient? _targetClient;
        private MigrationSettings _config;
        private CancellationTokenSource _cts;
        private MongoChangeStreamProcessor? _changeStreamProcessor;
        private bool _postUploadCSProcessing = false;
        private Log _log;

        // RU-specific configuration
        private const int MaxConcurrentPartitions = 1;
        private static readonly TimeSpan BatchDuration = TimeSpan.FromSeconds(60);

        public bool ProcessRunning { get; set; }

        public RUCopyProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
        {
            _log = log;
            _jobList = jobList;
            _job = job;
            _sourceClient = sourceClient;
            _config = config;
            _cts = new CancellationTokenSource();
        }

        public void StopProcessing(bool updateStatus = true)
        {
            if (_job != null)
                _job.IsStarted = false;

            _jobList?.Save();

            if (updateStatus)
                ProcessRunning = false;

            _cts?.Cancel();

            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
        }

        private ProcessorContext SetProcessorContext(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            var databaseName = mu.DatabaseName;
            var collectionName = mu.CollectionName;
            var database = _sourceClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            var context = new ProcessorContext
            {
                Item = mu,
                SourceConnectionString = sourceConnectionString,
                TargetConnectionString = targetConnectionString,
                JobId = _job?.Id ?? string.Empty,
                DatabaseName = databaseName,
                CollectionName = collectionName,
                //MaxRetries = 10,
                Database = database,
                Collection = collection
            };

            return context;
        }

        private bool CheckChangeStreamAlreadyProcessingAsync(ProcessorContext ctx)
        {
            if (_postUploadCSProcessing)
                return true; // Skip processing if post-upload CS processing is already in progress

            if (_job.IsOnline && Helper.IsOfflineJobCompleted(_job) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !_job.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                    if (_changeStreamProcessor == null && _targetClient != null)
                        _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                if (_changeStreamProcessor != null)
                {
                    var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                }
                return true;
            }

            return false;
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
                //    combinedCts.Dispose();
                //    combinedCts = null;

                //    return TaskResult.Success;
                //}
                _jobList?.Save();                
            }            
            return TaskResult.Retry; // If we reach here, it means processing was cancelled or timed out
        }


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
                // async call to get stop document for this partition
                if (!string.IsNullOrEmpty(chunk.RUStopDocumentKey) || chunk.RUStopDocumentKey == string.Empty)
                    Task.Run(() => GetChunksStopDocumentAsync(chunk, mu, sourceCollection, linkedCts.Token), linkedCts.Token);

                var options = new ChangeStreamOptions
                {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    ResumeAfter = BsonDocument.Parse(chunk.RUResumeStopToken) 
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
                
                while (cursor.MoveNext(linkedCts.Token))
                {
                    timeoutCts.ThrowIfCancellationRequested();

                    foreach (var change in cursor.Current)
                    {
                        var resumeToken = change.ResumeToken;
                        var document = change.FullDocument;
                        var namespaceInfo = change.CollectionNamespace; // optional

                        // TODO: Process the document
                        Console.WriteLine(" ******************" );
                        Console.WriteLine(chunk.RUStopDocumentKey.ToJson());
                        Console.WriteLine(change.DocumentKey.ToJson());                                                                      

                        // Process the change document
                        //await ProcessChangeDocumentAsync(change, targetCollection, state, isSimulated, token);                                                
                       
                        // If current resume token == StopFeedItem, this partition's history is complete
                        if (EqualsDocumentKey(change.DocumentKey, chunk.RUStopDocumentKey))
                        {
                            chunk.IsUploaded = true;
                            _log.WriteLine($"Chunk [{chunk.Id}] offline copy completed.");
                            return Task.CompletedTask;
                        }

                        // Save the latest token
                        chunk.RUPartitionResumeToken = change.ResumeToken.ToJson();
                        chunk.DocCountInTarget++;

                        counter++;
                        // Check for cancellation
                        if (linkedCts.IsCancellationRequested)
                        {
                            return Task.CompletedTask;
                        }
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


        private Task GetChunksStopDocumentAsync(MigrationChunk chunk, MigrationUnit mu, IMongoCollection<BsonDocument> sourceCollection,
             CancellationToken token)
        {
            if (!string.IsNullOrEmpty(chunk.RUResumeStopToken) || chunk.IsUploaded == true)
                return Task.CompletedTask;

            try            {
                var options = new ChangeStreamOptions
                {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    ResumeAfter = BsonDocument.Parse(chunk.RUPartitionResumeToken)
                };

                var pipeline = new BsonDocument[]
                {
                    new BsonDocument("$match", new BsonDocument("operationType",
                        new BsonDocument("$in", new BsonArray { "insert", "update", "replace","delete" })
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

                while (cursor.MoveNext(token))
                {
                    token.ThrowIfCancellationRequested();

                    foreach (var change in cursor.Current)
                    {

                        Console.WriteLine(" ###########################");
                        Console.WriteLine(change.DocumentKey.ToJson());
                        Console.WriteLine(" ###########################");

                        chunk.RUPartitionResumeToken = change.DocumentKey.ToJson();
                        return Task.CompletedTask;
                    }

                    // Check for cancellation
                    if (token.IsCancellationRequested)
                    {
                        return Task.CompletedTask;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error getting stop document for  partition {chunk.Id}: {ex}", LogType.Error);
                //throw;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Compare two resume tokens for equality
        /// </summary>
        private static bool EqualsDocumentKey(BsonDocument? a, string b)
        {
            if (a == null || b == null)
                return false;
            return a.ToJson() == b;
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

                        if (_changeStreamProcessor == null && _targetClient != null)
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

                            if (_changeStreamProcessor == null && _targetClient != null)
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

        public async Task StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
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
