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

        private CopyProcessContext InitializeCopyProcessContex(MigrationUnit item, string sourceConnectionString, string targetConnectionString)
        {
            var context = new CopyProcessContext
            {
                Item = item,
                SourceConnectionString = sourceConnectionString,
                TargetConnectionString = targetConnectionString,
                JobId = _job.Id,
                DatabaseName = item.DatabaseName,
                CollectionName = item.CollectionName,
                MaxRetries = 10
            };

            context.Database = _sourceClient.GetDatabase(context.DatabaseName);
            context.Collection = context.Database.GetCollection<BsonDocument>(context.CollectionName);
            context.MigrationJobStartTime = DateTime.Now;

            return context;
        }

        private bool CheckChangeStreamAlreadyProcessingAsync(CopyProcessContext ctx)
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
        private async Task<TaskResult> CopyProcess_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Document copy operation was cancelled for {dbName}.{colName}-{chunkIndex}");
                return TaskResult.Abort;
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} failed due to timeout.Details:{ex.ToString()}", LogType.Error);
                return TaskResult.Retry;
            }
            else if (ex.Message== "Copy Document Failed")
            {
                _log.WriteLine($"{processName} attempt for {dbName}.{colName}-{chunkIndex} failed. Retrying in {currentBackoff} seconds...");
                return TaskResult.Retry;
            }
            else
            {
                _log.WriteLine(ex.ToString(), LogType.Error);
                return TaskResult.Retry;
            }
        }

        private async Task <TaskResult> ProcessChunkAsync(MigrationUnit item, int chunkIndex, CopyProcessContext ctx, double initialPercent, double contributionFactor)
        {
            long docCount;
            FilterDefinition<BsonDocument> filter;

            if (item.MigrationChunks.Count > 1)
            {
                var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[chunkIndex].Gte, item.MigrationChunks[chunkIndex].Lt, item.MigrationChunks[chunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName}-Chunk [{chunkIndex}] generating query");

                // Generate query and get document count
                filter = MongoHelper.GenerateQueryFilter(gte, lt, item.MigrationChunks[chunkIndex].DataType);

                docCount = MongoHelper.GetDocumentCount(ctx.Collection, filter);
                item.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;

                ctx.DownloadCount += item.MigrationChunks[chunkIndex].DumpQueryDocCount;

                _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName}- Chunk [{chunkIndex}] Count is  {docCount}");

            }
            else
            {
                filter = Builders<BsonDocument>.Filter.Empty;
                docCount = MongoHelper.GetDocumentCount(ctx.Collection, filter);

                item.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
                ctx.DownloadCount = docCount;
            }

            if (_targetClient == null && !_job.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

            var documentCopier = new MongoDocumentCopier();
            documentCopier.Initialize(_log, _targetClient, ctx.Collection, ctx.DatabaseName, ctx.CollectionName, _config.MongoCopyPageSize);
            var result = await documentCopier.CopyDocumentsAsync(_jobList, item, chunkIndex, initialPercent, contributionFactor, docCount, filter, _cts.Token, _job.IsSimulatedRun);

            if (result)
            {                
                if (!_cts.Token.IsCancellationRequested)
                {                        
                    item.MigrationChunks[chunkIndex].IsDownloaded = true;
                    item.MigrationChunks[chunkIndex].IsUploaded = true;
                }
                _jobList?.Save(); // Persist state
                return TaskResult.Success;
            }
            else
            {
                throw new Exception("Copy Document Failed");                   
            }
            
        }

        private async Task PostCopyChangeStreamProcessor(CopyProcessContext ctx, MigrationUnit item)
        {
            if (item.RestoreComplete && item.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (_job.IsOnline && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads)
                    {
                        if (_targetClient == null && !_job.IsSimulatedRun)
                            _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                        if (_changeStreamProcessor == null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _jobList, _job, _config);

                        _changeStreamProcessor.AddCollectionsToProcess(item, _cts);
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
        }

        public async Task StartProcessAsync(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {

            CopyProcessContext ctx;
            ctx=InitializeCopyProcessContex(item, sourceConnectionString, targetConnectionString);

            //when resuming a job, we need to check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return;

            // starting the  regular document copy process
            _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName} Document copy started");

            if (!item.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                item.EstimatedDocCount = ctx.Collection.EstimatedDocumentCount();

                Task.Run(() =>
                {
                    long count = MongoHelper.GetActualDocumentCount(ctx.Collection, item);
                    item.ActualDocCount = count;
                    _jobList?.Save();
                }, _cts.Token);

                for (int i = 0; i < item.MigrationChunks.Count; i++)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent = ((double)100 / item.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / item.MigrationChunks.Count;

                    if (!item.MigrationChunks[i].IsDownloaded == true)
                    {
                        TaskResult result = await new RetryHelper().ExecuteTask(
                            () => ProcessChunkAsync(item, i, ctx, initialPercent, contributionFactor),
                            (ex, attemptCount, currentBackoff) => CopyProcess_ExceptionHandler(
                                ex, attemptCount,
                                "Chunk processor", ctx.DatabaseName, ctx.CollectionName, i, currentBackoff
                            ),
                            _log
                        );


                        if (result== TaskResult.Abort || result== TaskResult.Failed)
                        {
                            _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}-{i} failed after multiple attempts.", LogType.Error);
                            StopProcessing();
                        }
                    }
                    else
                    {
                        ctx.DownloadCount += item.MigrationChunks[i].DumpQueryDocCount;
                    }
                }

                item.SourceCountDuringCopy = item.MigrationChunks.Sum(chunk => chunk.Segments.Sum(item => item.QueryDocCount));
                item.DumpGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - item.SourceCountDuringCopy;
                item.RestoreGap = item.SourceCountDuringCopy - item.MigrationChunks.Sum(chunk => chunk.DumpResultDocCount) ;
                

                long failed= item.MigrationChunks.Sum(chunk => chunk.RestoredFailedDocCount);
                // don't compare counts source vs target as some documents may have been deleted in source
                //only  check for failed documents
                if (failed == 0)
                {
                    item.BulkCopyEndedOn = DateTime.UtcNow;

                    item.DumpPercent = 100;
                    item.DumpComplete = true;

                    item.RestorePercent = 100;
                    item.RestoreComplete = true;
                }
                             
  
            }

            await PostCopyChangeStreamProcessor(ctx, item);
        }
    }
}
