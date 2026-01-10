using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Processors
{
    public abstract class MigrationProcessor
    {
     
        protected MongoClient? _sourceClient;
        protected MongoClient? _targetClient;
        protected IMongoCollection<BsonDocument>? _sourceCollection;
        protected IMongoCollection<BsonDocument>? _targetCollection;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
                
        protected Log _log;
        protected MigrationWorker? _migrationWorker;

        public bool ProcessRunning { get; set; }
        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        public bool IsChangeStreamRunning = false;

        // Expose WaitForResumeTokenTaskDelegate from the change stream processor
        public Func<string, Task>? WaitForResumeTokenTaskDelegate
        {
            get => _changeStreamProcessor?.WaitForResumeTokenTaskDelegate;
            set
            {
                if (_changeStreamProcessor != null)
                    _changeStreamProcessor.WaitForResumeTokenTaskDelegate = value;
            }
        }

        protected MigrationProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
        {
            _log = log;
            _sourceClient = sourceClient;
            _targetClient = null;
            _config = config;
            _cts = new CancellationTokenSource();
            _migrationWorker = migrationWorker;
        }

        public virtual void StopProcessing(bool updateStatus = true)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.StopProcessing: updateStatus={updateStatus}");

            if (MigrationJobContext.CurrentlyActiveJob != null)
            {
                MigrationJobContext.CurrentlyActiveJob.IsStarted = false;
            }

            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

            if (updateStatus)
                ProcessRunning = false;


            _cts?.Cancel();

            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
        }

        /// <summary>
        /// Signals processor to stop accepting new work but complete current tasks
        /// </summary>


        protected ProcessorContext SetProcessorContext(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            var databaseName = mu.DatabaseName;
            var collectionName = mu.CollectionName;
            var database = _sourceClient?.GetDatabase(databaseName);
            var collection = database?.GetCollection<BsonDocument>(collectionName);

            var context = new ProcessorContext
            {
                MigrationUnitId = mu.Id,
                SourceConnectionString = sourceConnectionString,
                TargetConnectionString = targetConnectionString,
                JobId = MigrationJobContext.CurrentlyActiveJob?.Id ?? string.Empty,
                DatabaseName = databaseName,
                CollectionName = collectionName,
                Database = database!,
                Collection = collection!,
            };

            return context;
        }

        public bool AddCollectionToChangeStreamQueue(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.AddCollectionToChangeStreamQueue: migrationUnitId={mu.Id}");

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;
            
            if (_targetClient == null)
                _targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config);


            _log.WriteLine($"Adding {mu.DatabaseName}.{mu.CollectionName} to Change Stream processing queue", LogType.Debug);
            _changeStreamProcessor?.AddCollectionsToProcess(mu.Id, _cts);

            return true;
        }


        public bool RunChangeStreamProcessorForAllCollections()
        {
            MigrationJobContext.AddVerboseLog("MigrationProcessor.RunChangeStreamProcessorForAllCollections: called");

            //only once allowed per job
            if(IsChangeStreamRunning)
                return false;

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;

            //for delayed mode only
            if (!Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return false;

            //for delayed mode only, at the start no collections are valid, hence IsOfflineJobCompleted gives false positive
            if (!Helper.AnyValidCollection(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return false;            


            //only once allowed per job, checking again
            if (IsChangeStreamRunning)
                return false;

            IsChangeStreamRunning = true; // Set flag to indicate post-upload CS processing is in progress

            string targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id];
            if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, targetConnStr);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);
#pragma warning restore CS8604 // Possible null reference argument.

            if (_changeStreamProcessor != null)
            {
                var result = _changeStreamProcessor.RunChangeStreamProcessorForAllCollections(_cts);
            }
            return true;            
        }

        
        public void StopOfflineOrInvokeChangeStreams()
        {
            // Handle offline completion and post-upload CS logic

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
            {
                // Don't mark as completed if this is a controlled pause
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} Completed");
                    MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
                StopProcessing();
            }
            else
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Invoke RunChangeStreamProcessorForAllCollections.", LogType.Debug);

                    RunChangeStreamProcessorForAllCollections();
                }
            }

        }
        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        { return Task.FromResult(TaskResult.Success); }
    }
}
