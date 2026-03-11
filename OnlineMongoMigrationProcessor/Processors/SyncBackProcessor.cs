using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Reflection.Metadata.BlobBuilder;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Context;

namespace OnlineMongoMigrationProcessor.Processors
{
    internal class SyncBackProcessor : MigrationProcessor
    {

        public SyncBackProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
           : base(log, sourceClient, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog("SyncBackProcessor: Constructor called");
            // Constructor body can be empty or contain initialization logic if needed
        }


        // Exception handler for RetryHelper
        private Task<TaskResult> SyncBack_ExceptionHandler(Exception ex, int attemptCount, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"SyncBack operation was paused");
                return Task.FromResult(TaskResult.Abort);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"SyncBack attempt {attemptCount} failed due to timeout. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
				_log.WriteLine($"SyncBack attempt {attemptCount} failed. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
				return Task.FromResult(TaskResult.Retry);
            }
        }

        private Task<TaskResult> SyncBackAttemptAsync()
        {
#if LEGACY_MONGODB_DRIVER
            _log.WriteLine("SyncBack is not supported with legacy MongoDB driver.", LogType.Warning);
            return Task.FromResult(TaskResult.Abort);
#else
            MigrationJobContext.AddVerboseLog("SyncBackProcessor.SyncBackAttemptAsync: starting");
            if (_cts == null)
            {
                _log.WriteLine("Cancellation token source not initialized for SyncBack.", LogType.Error);
                return Task.FromResult(TaskResult.Abort);
            }

            _cts.Token.ThrowIfCancellationRequested();

            _log.WriteLine($"SyncBack to source starting.");

            var units = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);
                       
            

            if (units != null)
            {
                foreach (MigrationUnit mu in units)
                {                   
                    AddCollectionToChangeStreamQueue(mu);
				}
            }


            var _ = _changeStreamProcessor!.RunChangeStreamProcessorForAllCollections(_cts);
            return Task.FromResult(TaskResult.Success);
#endif
        }


        public override async Task<TaskResult> StartProcessAsync(string MigrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
#if LEGACY_MONGODB_DRIVER
            _log.WriteLine("SyncBack is not supported with legacy MongoDB driver.", LogType.Warning);
            return TaskResult.Abort;
#else
            ProcessRunning = true;

             MigrationJobContext.CurrentlyActiveJob.IsStarted = true;

            if (string.IsNullOrWhiteSpace(sourceConnectionString)) throw new ArgumentNullException(nameof(sourceConnectionString));
            if (string.IsNullOrWhiteSpace(targetConnectionString)) throw new ArgumentNullException(nameof(targetConnectionString));
            var sourceClient = MongoClientFactory.Create(_log, sourceConnectionString, false);
            var targetClient = MongoClientFactory.Create(_log, targetConnectionString);


            _changeStreamProcessor = null;
            _changeStreamProcessor = new MongoChangeStreamProcessor(_log, sourceClient, targetClient, MigrationJobContext.MigrationUnitsCache, _config, true, _migrationWorker);

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
#endif
        }
    }
}
