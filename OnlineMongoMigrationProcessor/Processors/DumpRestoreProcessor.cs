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
        private MongoDumpRestoreCordinator _coordinator;

        public DumpRestoreProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog("DumpRestoreProcessor: Constructor called");
            _jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? throw new InvalidOperationException("Job ID cannot be null");            
            
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
        /// Handles post-processing like change stream setup.
        /// </summary>
        private void OnMigrationUnitCompleted(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.OnMigrationUnitCompleted: mu={mu.DatabaseName}.{mu.CollectionName}");
            _log.WriteLine($"Processing completion callback for migration unit {mu.DatabaseName}. {mu.CollectionName}", LogType.Debug);

            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine("Controlled pause active - skipping post-processing",LogType.Debug);
                return;
            }

            // Start change stream processing for the completed migration unit
            AddCollectionToChangeStreamQueue(mu);

            PercentageUpdater.RemovePercentageTracker(mu.Id, false, _log);
            PercentageUpdater.RemovePercentageTracker(mu.Id, true, _log);

            _log.WriteLine($"Offline dump/restore processing completed for {mu.DatabaseName}. {mu.CollectionName}",LogType.Debug);

            // Handle post-completion logic -stop if offline, else invoke change streams
            StopOfflineOrInvokeChangeStreams();
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
                    onMigrationUnitCompleted: OnMigrationUnitCompleted,
                    onPendingTasksCompleted: OnPendingTasksCompleted
                );
            }

        }

        private void PrepareDumpProcess(MigrationUnit mu)
        {
            // starting the regular dump and restore process
            if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                mu.BulkCopyStartedOn = DateTime.UtcNow;

            MigrationJobContext.SaveMigrationUnit(mu, true);            
        }

        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
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
            PrepareDumpProcess(mu);                                

            //initialize coordinator if not already done
            InitializeCoordinator();
            PercentageUpdater.Initialize();

            // Delegate dump/restore coordination to the coordinator
            _coordinator.StartCoordinatedProcess(ctx);

            _log.WriteLine($"Started coordinated dump/restore processing for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);

            
            return TaskResult.Success;
        }     

        

        public override void StopProcessing(bool updateStatus = true)
        {
            _log.WriteLine("Stopping DumpRestoreProcessor...");

            // Stop the coordinator timer and clear manifests
            if (_coordinator!=null)
              _coordinator.StopCoordinatedProcessing();
            
            // Give time for any active timer callbacks to complete
            System.Threading.Thread.Sleep(500);
            
            // Call base implementation
            base.StopProcessing(updateStatus);
            
            _log.WriteLine("DumpRestoreProcessor stopped");
        }
    }
}
