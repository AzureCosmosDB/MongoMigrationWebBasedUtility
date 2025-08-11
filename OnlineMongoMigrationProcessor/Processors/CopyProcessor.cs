using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Processors;
using OnlineMongoMigrationProcessor.Workers;


namespace OnlineMongoMigrationProcessor
{
    internal class CopyProcessor: MigrationProcessor
    {

        public CopyProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
            : base(log, jobList, job, sourceClient, config)
        {
            // Constructor body can be empty or contain initialization logic if needed
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
                var gteStr = mu.MigrationChunks[chunkIndex].Gte ?? string.Empty;
                var ltStr = mu.MigrationChunks[chunkIndex].Lt ?? string.Empty;
                var bounds = SamplePartitioner.GetChunkBounds(gteStr, ltStr, mu.MigrationChunks[chunkIndex].DataType);
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

            var documentCopier = new DocumentCopyWorker();
            documentCopier.Initialize(_log, _targetClient!, ctx.Collection, ctx.DatabaseName, ctx.CollectionName, _config.MongoCopyPageSize);
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



        public override async Task StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
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

                mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => (long)(chunk.Segments?.Sum(seg => seg.QueryDocCount) ?? 0));
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
