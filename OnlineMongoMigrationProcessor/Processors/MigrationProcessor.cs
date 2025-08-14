using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Processors
{
    public abstract class MigrationProcessor
    {
        protected JobList _jobList;
        protected MigrationJob _job;
        // Fix: Make _sourceClient, _sourceCollection, _targetCollection fields and MongoToolsFolder property nullable to resolve CS8618

        protected MongoClient? _sourceClient;
        protected MongoClient? _targetClient;
        protected IMongoCollection<BsonDocument>? _sourceCollection;
        protected IMongoCollection<BsonDocument>? _targetCollection;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
        protected bool _postUploadCSProcessing = false;
        protected Log _log;

        public bool ProcessRunning { get; set; }
        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        protected MigrationProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
        {
            _log = log;
            _jobList = jobList;
            _job = job;
            _sourceClient = sourceClient;
            _targetClient = null;
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

        protected ProcessorContext SetProcessorContext(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            var databaseName = mu.DatabaseName;
            var collectionName = mu.CollectionName;
            var database = _sourceClient?.GetDatabase(databaseName);
            var collection = database?.GetCollection<BsonDocument>(collectionName);

            var context = new ProcessorContext
            {
                Item = mu,
                SourceConnectionString = sourceConnectionString,
                TargetConnectionString = targetConnectionString,
                JobId = _job?.Id ?? string.Empty,
                DatabaseName = databaseName,
                CollectionName = collectionName,
                Database = database!,
                Collection = collection!,
            };

            return context;
        }

        // Fix for CS8604: Ensure _sourceClient is not null before passing to MongoChangeStreamProcessor

        protected bool CheckChangeStreamAlreadyProcessingAsync(ProcessorContext ctx)
        {
            if (_postUploadCSProcessing)
                return true; // Skip processing if post-upload CS processing is already in progress

            if (_job.IsOnline && Helper.IsOfflineJobCompleted(_job) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !_job.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _targetClient != null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _jobList, _job, _config);

                if (_changeStreamProcessor != null)
                {
                    var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                }
                return true;
            }

            return false;
        }

        protected void AddCollectionToChangeStreamQueue(MigrationUnit mu, string targetConnectionString)
        {
            if (_job.IsOnline && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads && !_job.IsSimulatedRun)
            {
                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                _changeStreamProcessor?.AddCollectionsToProcess(mu, _cts);
            }
        }

        protected void RunChangeStreamProcessorForAllCollections(string targetConnectionString)
        {
            if (_job.IsOnline && _job.CSStartsAfterAllUploads && Helper.IsOfflineJobCompleted(_job) && !_postUploadCSProcessing && !_job.IsSimulatedRun)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                var _ = _changeStreamProcessor?.RunCSPostProcessingAsync(_cts);
            }
        }


        protected Task PostCopyChangeStreamProcessor(ProcessorContext ctx, MigrationUnit mu)
        {
            if (mu.RestoreComplete && mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (_job.IsOnline && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads)
                    {
                        AddCollectionToChangeStreamQueue(mu, ctx.TargetConnectionString);
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
                        else if (!_postUploadCSProcessing)
                        {
                            // If CSStartsAfterAllUploads is true and the offline job is completed, run post-upload change stream processing
                            RunChangeStreamProcessorForAllCollections(ctx.TargetConnectionString);
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


        public virtual Task<TaskResult> StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        { return Task.FromResult(TaskResult.Success); }
    }
}
