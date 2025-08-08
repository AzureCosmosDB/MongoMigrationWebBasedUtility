using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using static System.Reflection.Metadata.BlobBuilder;

#pragma warning disable CS8602
#pragma warning disable CS8604
#pragma warning disable CS8600
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

namespace OnlineMongoMigrationProcessor
{
    internal class CopyProcessor : IMigrationProcessor
    {
        private JobList? _jobList;
        private MigrationJob? _job;
        private MongoClient? _sourceClient;
        private MongoClient? _targetClient;
        private MigrationSettings? _config;
        private CancellationTokenSource _cts;
        private MongoChangeStreamProcessor _changeStreamProcessor;
        private bool _postUploadCSProcessing = false;
        private Log _log;

		public bool ProcessRunning { get; set; }


        public CopyProcessor(Log log,JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
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

            if(updateStatus)
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
                Database = database,
                Collection = collection,
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

                if (_changeStreamProcessor == null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _jobList, _job, _config);

                var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                return true;
            }

            return false;
        }


        // Custom exception handler delegate with logic to control retry flow
    private Task<TaskResult> CopyProcess_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Document copy operation was cancelled for {dbName}.{colName}-{chunkIndex}");
        return Task.FromResult(TaskResult.Abort);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} failed due to timeout.Details:{ex.ToString()}", LogType.Error);
        return Task.FromResult(TaskResult.Retry);
            }
            else if (ex.Message== "Copy Document Failed")
            {
                _log.WriteLine($"{processName} attempt for {dbName}.{colName}-{chunkIndex} failed. Retrying in {currentBackoff} seconds...");
        return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine(ex.ToString(), LogType.Error);
        return Task.FromResult(TaskResult.Retry);
            }
        }

        private async Task <TaskResult> ProcessChunkAsync(MigrationUnit mu, int chunkIndex, ProcessorContext ctx, double initialPercent, double contributionFactor)
        {
            long docCount;
            FilterDefinition<BsonDocument> filter;

            if (mu.MigrationChunks.Count > 1)
            {
                var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[chunkIndex].Gte, mu.MigrationChunks[chunkIndex].Lt, mu.MigrationChunks[chunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName}-Chunk [{chunkIndex}] generating query");

                // Generate query and get document count
                filter = MongoHelper.GenerateQueryFilter(gte, lt, mu.MigrationChunks[chunkIndex].DataType);

                docCount = MongoHelper.GetDocumentCount(ctx.Collection, filter);
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;

                ctx.DownloadCount += mu.MigrationChunks[chunkIndex].DumpQueryDocCount;

                _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName}- Chunk [{chunkIndex}] Count is  {docCount}");

            }
            else
            {
                filter = Builders<BsonDocument>.Filter.Empty;
                docCount = MongoHelper.GetDocumentCount(ctx.Collection, filter);

                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
                ctx.DownloadCount = docCount;
            }

            if (_targetClient == null && !_job.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

            var documentCopier = new MongoDocumentCopier();
            documentCopier.Initialize(_log, _targetClient, ctx.Collection, ctx.DatabaseName, ctx.CollectionName, _config.MongoCopyPageSize);
            var result = await documentCopier.CopyDocumentsAsync(_jobList, mu, chunkIndex, initialPercent, contributionFactor, docCount, filter, _cts.Token, _job.IsSimulatedRun);

            if (result == TaskResult.Success)
            {
                if (!_cts.Token.IsCancellationRequested)
                {                        
                    mu.MigrationChunks[chunkIndex].IsDownloaded = true;
                    mu.MigrationChunks[chunkIndex].IsUploaded = true;
                }
                _jobList?.Save(); // Persist state
                return TaskResult.Success;
            }
            else if(result == TaskResult.Canceled)
            {
                _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}-{chunkIndex} was cancelled.");
				return TaskResult.Canceled;
			}
			else
            {
                _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}-{chunkIndex} failed.", LogType.Error);
				return TaskResult.Retry;
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

                        if (_changeStreamProcessor == null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _jobList, _job, _config);

                        _changeStreamProcessor.AddCollectionsToProcess(mu, _cts);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        var migrationJob = _jobList.MigrationJobs.Find(m => m.Id == ctx.JobId);
                        if (!_job.IsOnline && Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            _log.WriteLine($"{migrationJob.Id} completed.");

                            migrationJob.IsCompleted = true;
                            StopProcessing(true);
                        }
                        else if (_job.IsOnline && _job.CSStartsAfterAllUploads && Helper.IsOfflineJobCompleted(migrationJob) && !_postUploadCSProcessing)
                        {
                            // If CSStartsAfterAllUploads is true and the offline job is completed, run post-upload change stream processing
                            _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                            if (_targetClient == null && !_job.IsSimulatedRun)
                                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                            if (_changeStreamProcessor == null)
                                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _jobList, _job, _config);

                            var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
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

            ProcessorContext ctx;
            ctx=SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            //when resuming a job, we need to check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return;

            // starting the  regular document copy process
            _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName} Document copy started");

            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                mu.EstimatedDocCount = ctx.Collection.EstimatedDocumentCount();

                Task.Run(() =>
                {
                    long count = MongoHelper.GetActualDocumentCount(ctx.Collection, mu);
                    mu.ActualDocCount = count;
                    _jobList?.Save();
                }, _cts.Token);

                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent = ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / mu.MigrationChunks.Count;

                    if (!mu.MigrationChunks[i].IsDownloaded == true)
                    {
                        TaskResult result = await new RetryHelper().ExecuteTask(
                            () => ProcessChunkAsync(mu, i, ctx, initialPercent, contributionFactor),
                            (ex, attemptCount, currentBackoff) => CopyProcess_ExceptionHandler(
                                ex, attemptCount,
                                "Chunk processor", ctx.DatabaseName, ctx.CollectionName, i, currentBackoff
                            ),
                            _log
                        );


                        if (result== TaskResult.Abort || result== TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}-{i} failed after multiple attempts.", LogType.Error);
                            StopProcessing();
                        }
                    }
                    else
                    {
                        ctx.DownloadCount += mu.MigrationChunks[i].DumpQueryDocCount;
                    }
                }

                mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => chunk.Segments.Sum(mu => mu.QueryDocCount));
                mu.DumpGap = Math.Max(mu.ActualDocCount, mu.EstimatedDocCount) - mu.SourceCountDuringCopy;
                mu.RestoreGap = mu.SourceCountDuringCopy - mu.MigrationChunks.Sum(chunk => chunk.DumpResultDocCount) ;
                

                long failed= mu.MigrationChunks.Sum(chunk => chunk.RestoredFailedDocCount);
                // don't compare counts source vs target as some documents may have been deleted in source
                //only  check for failed documents
                if (failed == 0)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;

                    mu.DumpPercent = 100;
                    mu.DumpComplete = true;

                    mu.RestorePercent = 100;
                    mu.RestoreComplete = true;
                }
                             
  
            }

            await PostCopyChangeStreamProcessor(ctx, mu);
        }
    }
}
