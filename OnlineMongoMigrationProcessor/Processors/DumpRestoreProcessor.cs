using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Linq;
using System.Threading.Tasks;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Processors;
using System.Reflection.Metadata;

namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// DumpRestoreProcessor wraps MongoDumpRestoreCordinator to provide MigrationProcessor compatibility.
    /// All dump/restore logic is delegated to the coordinator for centralized management.
    /// </summary>
    internal class DumpRestoreProcessor : MigrationProcessor
    {
        private readonly string _jobId;
        private readonly string _processorRunId;
        private MongoDumpRestoreCordinator _coordinator;

        public DumpRestoreProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog("DumpRestoreProcessor: Constructor called");
            _jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? throw new InvalidOperationException("Job ID cannot be null");
            _processorRunId = Guid.NewGuid().ToString("N");
            _log.WriteLine($"DumpRestoreProcessor initialized with ProcessorRunId={_processorRunId} for JobId={_jobId}", LogType.Info);
            
        }

        private void OnPendingTasksCompleted()
        {
            MigrationJobContext.AddVerboseLog("DumpRestoreProcessor.OnPendingTasksCompleted: all pending tasks completed");

            if (MigrationJobContext.ControlledPauseRequested)
            {
                StopProcessing();
            }
        }

        /// <summary>
        /// Callback invoked by coordinator when a migration unit completes dump/restore.
        /// Handles post-processing like non-unique index building and change stream setup.
        /// </summary>
        private async void OnMigrationUnitCompleted(MigrationUnit mu)
        {
            try
            {
                MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.OnMigrationUnitCompleted: mu={mu.DatabaseName}.{mu.CollectionName}");
                _log.WriteLine($"Processing completion callback for migration unit {mu.DatabaseName}. {mu.CollectionName}", LogType.Debug);

                if (MigrationJobContext.ControlledPauseRequested || _cts.Token.IsCancellationRequested || !ProcessRunning)
                {
                    _log.WriteLine("Pause/stop active - skipping post-processing callback", LogType.Debug);
                    return;
                }

                // Hand off non-unique index build (and change-stream enqueue on success) to a
                // background task so the post-copy callback returns immediately and the coordinator
                // can pick up the next completed unit. StopOfflineOrInvokeChangeStreams already
                // defers final completion while pending background index builds are in flight.
                _migrationWorker?.StartBackgroundIndexBuildAndQueue(mu);

                PercentageUpdater.RemovePercentageTracker(mu.Id, false, _log);
                PercentageUpdater.RemovePercentageTracker(mu.Id, true, _log);

                _log.WriteLine($"Offline dump/restore processing completed for {mu.DatabaseName}. {mu.CollectionName}",LogType.Debug);

                // Handle post-completion logic -stop if offline, else invoke change streams
                StopOfflineOrInvokeChangeStreams();
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"Post-processing canceled for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Post-processing callback failed for {mu.DatabaseName}.{mu.CollectionName}: {ex.Message}", LogType.Warning);
            }
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            if (_coordinator == null)
                InitializeCoordinator();

            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.AdjustDumpWorkers: newCount={newCount}");
            _coordinator.AdjustDumpWorkers(newCount);
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            if (_coordinator == null)
                InitializeCoordinator();

            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.AdjustRestoreWorkers: newCount={newCount}");
            _coordinator.AdjustRestoreWorkers(newCount);
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            if (_coordinator == null)
                InitializeCoordinator();

            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.AdjustInsertionWorkers: newCount={newCount}");
            _coordinator.AdjustInsertionWorkers(newCount);
        }

        private void InitializeCoordinator()
        {

            MigrationJobContext.AddVerboseLog("DumpRestoreProcessor.InitializeCoordinator: initializing coordinator");

            if (_coordinator == null)
            {
                // Create instance coordinator with completion callback
                _coordinator = new MongoDumpRestoreCordinator();
                _coordinator.Initialize(
                    _jobId,
                    _log,
                    MongoToolsFolder,
                    _config.MongoDumpToolPath,
                    _config.MongoRestoreToolPath,
                    _processorRunId,
                    onMigrationUnitCompleted: OnMigrationUnitCompleted,
                    onPendingTasksCompleted: OnPendingTasksCompleted
                );
            }

        }


        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.StartProcessAsync: migrationUnitId={migrationUnitId}");


            // Perform initial setup required by MigrationProcessor
            MigrationJobContext.ResetControlledPause();
            ProcessRunning = true;


            var mu = MigrationJobContext.GetMigrationUnit(migrationUnitId);
            mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;

            if (mu.DumpComplete && mu.RestoreComplete)
            {
                _log.WriteLine($"Document copy operation for {mu.DatabaseName}.{mu.CollectionName} already completed.", LogType.Debug);
                return TaskResult.Success;
            }

            var ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);
                             

            //initialize coordinator if not already done
            InitializeCoordinator();
            PercentageUpdater.Initialize();

            // Delegate dump/restore coordination to the coordinator
            _coordinator.StartCoordinatedProcess(ctx);

            _log.WriteLine($"Started coordinated dump/restore processing for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);

            
            return TaskResult.Success;
        }     

        /// <summary>
        /// Signals the coordinator to stop immediately (sets flags + cancels CTS)
        /// without awaiting in-flight workers. Call before KillAllMigrationProcesses.
        /// </summary>
        public override void SignalStop()
        {
            base.SignalStop();
            _coordinator?.SignalStop();
        }

        public override void StopProcessing(bool updateStatus = true)
        {
            _log.WriteLine("Stopping DumpRestoreProcessor...");

            // Stop the coordinator timer, clear manifests, and await in-flight workers
            if (_coordinator!=null)
              _coordinator.StopCoordinatedProcessing();
            
            // Call base implementation
            base.StopProcessing(updateStatus);
            
            _log.WriteLine("DumpRestoreProcessor stopped");
        }

        public override void MarkAllUnitsDispatched()
        {
            _coordinator?.CloseRegistration();
        }
    }
}
