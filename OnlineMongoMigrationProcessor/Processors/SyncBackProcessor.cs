using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Reflection.Metadata.BlobBuilder;

namespace OnlineMongoMigrationProcessor.Processors
{
    internal class SyncBackProcessor : MigrationProcessor
    {

        public SyncBackProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
           : base(log, jobList, job, sourceClient, config)
        {
            // Constructor body can be empty or contain initialization logic if needed
        }


        // Exception handler for RetryHelper
        private Task<TaskResult> SyncBack_ExceptionHandler(Exception ex, int attemptCount, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"SyncBack operation was cancelled");
                return Task.FromResult(TaskResult.Abort);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"SyncBack attempt {attemptCount} failed due to timeout. Details:{ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine(ex.ToString(), LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        private Task<TaskResult> SyncBackAttemptAsync()
        {
            if (_cts == null)
            {
                _log.WriteLine("Cancellation token source not initialized for SyncBack.", LogType.Error);
                return Task.FromResult(TaskResult.Abort);
            }

            _cts.Token.ThrowIfCancellationRequested();

            _log.WriteLine($"Sync back to source starting.");

            var units = _job.MigrationUnits;
            if (units != null)
            {
                foreach (MigrationUnit unit in units)
                {
                    if (!unit.SyncBackChangeStreamStartedOn.HasValue)
                    {
                        unit.SyncBackChangeStreamStartedOn = DateTime.UtcNow;
                    }
                }
            }

            var _ = _changeStreamProcessor!.RunCSPostProcessingAsync(_cts);
            return Task.FromResult(TaskResult.Success);
        }

        public override async Task<TaskResult> StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            ProcessRunning = true;

            _job.IsStarted = true;

            if (string.IsNullOrWhiteSpace(sourceConnectionString)) throw new ArgumentNullException(nameof(sourceConnectionString));
            if (string.IsNullOrWhiteSpace(targetConnectionString)) throw new ArgumentNullException(nameof(targetConnectionString));
            var sourceClient = MongoClientFactory.Create(_log, sourceConnectionString, false);
            var targetClient = MongoClientFactory.Create(_log, targetConnectionString);

            _changeStreamProcessor = null;
            _changeStreamProcessor = new MongoChangeStreamProcessor(_log, sourceClient, targetClient, _jobList, _job, _config, true);

            _cts=new CancellationTokenSource();

            // Use RetryHelper instead of manual retry loop
            var resultStatus = await new RetryHelper().ExecuteTask(
                () => SyncBackAttemptAsync(),
                (ex, attemptCount, currentBackoff) => SyncBack_ExceptionHandler(ex, attemptCount, currentBackoff),
                _log
            );

            if (resultStatus == TaskResult.Abort || resultStatus == TaskResult.FailedAfterRetries)
            {
                _log.WriteLine("SyncBack failed after multiple attempts. Aborting operation.", LogType.Error);
                StopProcessing();
                if (_changeStreamProcessor != null)
                {
                    _changeStreamProcessor.ExecutionCancelled = true;
                    _changeStreamProcessor = null;
                }                
            }

            return resultStatus;
        }
    }
}
