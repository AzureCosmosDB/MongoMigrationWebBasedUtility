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
    internal class SyncBackProcessor : IMigrationProcessor
    {
        public bool ProcessRunning { get; set; }
        private MongoChangeStreamProcessor? _syncBackToSource;
        private readonly JobList _jobList;
        private readonly MigrationJob _job;
        private readonly MigrationSettings _config;
        private CancellationTokenSource? _cts;
        private Log _log;

        public SyncBackProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config, string toolsLaunchFolder)
        {
            _log = log;
			_jobList = jobList ?? throw new ArgumentNullException(nameof(jobList), "JobList cannot be null.");
            _job = job ?? throw new ArgumentNullException(nameof(job), "MigrationJob cannot be null.");
            _config = config ?? throw new ArgumentNullException(nameof(config), "MigrationSettings cannot be null.");
        }

        public void StopProcessing(bool updateStatus = true)
        {
            _job.IsStarted = false;
            _jobList.Save();

            if(updateStatus) 
                ProcessRunning = false;
            _cts?.Cancel();
            if (_syncBackToSource != null)
                _syncBackToSource.ExecutionCancelled = true;

            _syncBackToSource = null;   
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

            var _ = _syncBackToSource!.RunCSPostProcessingAsync(_cts);
            return Task.FromResult(TaskResult.Success);
        }

        public async Task StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            ProcessRunning = true;

            _job.IsStarted = true;

            if (string.IsNullOrWhiteSpace(sourceConnectionString)) throw new ArgumentNullException(nameof(sourceConnectionString));
            if (string.IsNullOrWhiteSpace(targetConnectionString)) throw new ArgumentNullException(nameof(targetConnectionString));
            var sourceClient = MongoClientFactory.Create(_log, sourceConnectionString, false);
            var targetClient = MongoClientFactory.Create(_log, targetConnectionString);

            _syncBackToSource = null;
            _syncBackToSource = new MongoChangeStreamProcessor(_log, sourceClient, targetClient, _jobList, _job, _config, true);

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
                if (_syncBackToSource != null)
                {
                    _syncBackToSource.ExecutionCancelled = true;
                    _syncBackToSource = null;
                }
            }
        }
    }
}
