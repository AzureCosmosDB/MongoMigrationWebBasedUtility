using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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


        public void StartProcess(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            ProcessRunning = true;
            int maxRetries = 10;
            string jobId = _job.Id;
            //_job.CurrentlyActive = true;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string dbName = item.DatabaseName;
            string colName = item.CollectionName;

            var database = _sourceClient.GetDatabase(dbName);
            var collection = database.GetCollection<BsonDocument>(colName);

            DateTime migrationJobStartTime = DateTime.Now;

            //when resuming a job, we need to check if post-upload change stream processing is already in progress

            if (_postUploadCSProcessing)
                return; // Skip processing if post-upload CS processing is already in progress

            if (_job.IsOnline && Helper.IsOfflineJobCompleted(_job) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !_job.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                if (_changeStreamProcessor == null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log,_sourceClient, _targetClient, _jobList, _job, _config);

                var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                return;
            }

            // starting the  regular document copy process

            _log.WriteLine($"{dbName}.{colName} Document copy started");

            if (!item.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                item.EstimatedDocCount = collection.EstimatedDocumentCount();

                Task.Run(() =>
                {
                    long count = MongoHelper.GetActualDocumentCount(collection, item);
                    item.ActualDocCount = count;
                    _jobList?.Save();
                }, _cts.Token);

                long downloadCount = 0;

                for (int i = 0; i < item.MigrationChunks.Count; i++)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent = ((double)100 / item.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / item.MigrationChunks.Count;

                    long docCount = 0;

                    if (!item.MigrationChunks[i].IsDownloaded == true)
                    {
                        int dumpAttempts = 0;
                        backoff = TimeSpan.FromSeconds(2);
                        bool continueProcessing = true;

                        while (dumpAttempts < maxRetries && !_cts.Token.IsCancellationRequested && continueProcessing)
                        {
                            dumpAttempts++;
                            FilterDefinition<BsonDocument> filter;
                            try
                            {
                                if (item.MigrationChunks.Count > 1)
                                {
                                    var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i].Gte, item.MigrationChunks[i].Lt, item.MigrationChunks[i].DataType);
                                    var gte = bounds.gte;
                                    var lt = bounds.lt;

                                    _log.WriteLine($"{dbName}.{colName}-Chunk [{i}] generating query");
                                    

                                    // Generate query and get document count
                                    filter = MongoHelper.GenerateQueryFilter(gte, lt, item.MigrationChunks[i].DataType);

                                    docCount = MongoHelper.GetDocumentCount(collection, filter);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;

                                    downloadCount += item.MigrationChunks[i].DumpQueryDocCount;

                                    _log.WriteLine($"{dbName}.{colName}- Chunk [{i}] Count is  {docCount}");
                                    
                                }
                                else
                                {
                                    filter = Builders<BsonDocument>.Filter.Empty;
                                    docCount = MongoHelper.GetDocumentCount(collection, filter);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;
                                    downloadCount = docCount;
                                }

                                if (_targetClient == null && !_job.IsSimulatedRun)
                                    _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                                var documentCopier = new MongoDocumentCopier();
                                documentCopier.Initialize(_log,_targetClient, collection, dbName, colName, _config.MongoCopyPageSize);
                                var result = documentCopier.CopyDocumentsAsync(_jobList, item, i, initialPercent, contributionFactor, docCount, filter, _cts.Token,_job.IsSimulatedRun).GetAwaiter().GetResult();

                                if (result)
                                {
                                    if (!_cts.Token.IsCancellationRequested)
                                    {
                                        continueProcessing = false;
                                        item.MigrationChunks[i].IsDownloaded = true;
                                        item.MigrationChunks[i].IsUploaded = true;                                        
                                    }
                                    _jobList?.Save(); // Persist state
                                    dumpAttempts = 0;
                                }
                                else
                                {
                                    _log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of Document copy failed. Retrying in {backoff.TotalSeconds} seconds...");
                                    
                                    // Use cancellation token aware delay instead of Thread.Sleep
                                    try
                                    {
                                        Task.Delay(backoff, _cts.Token).Wait(_cts.Token);
                                    }
                                    catch (OperationCanceledException)
                                    {
                                        return; // Exit if cancellation was requested during delay
                                    }
                                    
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                _log.WriteLine($"Document copy operation was cancelled for {dbName}.{colName}-{i}");
                                return;
                            }
                            catch (MongoExecutionTimeoutException ex)
                            {
                                _log.WriteLine($" Document copy attempt {dumpAttempts} failed due to timeout.Details:{ex.ToString()}", LogType.Error);

                                if (dumpAttempts >= maxRetries)
                                {
                                    _log.WriteLine("Maximum Document copy attempts reached. Aborting operation.", LogType.Error);
                                    StopProcessing();
                                }

                                // Wait for the backoff duration before retrying
                                _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                
                                // Use cancellation token aware delay instead of Thread.Sleep
                                try
                                {
                                    Task.Delay(backoff, _cts.Token).Wait(_cts.Token);
                                }
                                catch (OperationCanceledException)
                                {
                                    return; // Exit if cancellation was requested during delay
                                }
                                

                                // Exponentially increase the backoff duration
                                backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                            }
                            catch (Exception ex)
                            {
                                _log.WriteLine(ex.ToString(), LogType.Error);
                                StopProcessing();
                            }
                        }
                        if (dumpAttempts == maxRetries)
                        {
                            StopProcessing();                          
                        }
                    }
                    else
                    {
                        downloadCount += item.MigrationChunks[i].DumpQueryDocCount;
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
                    item.DumpPercent = 100;
                    item.DumpComplete = true;

                    item.RestorePercent = 100;
                    item.RestoreComplete = true;
                }
                             
  
            }
            if (item.RestoreComplete && item.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Process change streams
                    if (_job.IsOnline && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads)
                    {
                        if (_targetClient == null && !_job.IsSimulatedRun)
                            _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                        if (_changeStreamProcessor == null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_log,_sourceClient, _targetClient, _jobList, _job, _config);

                        _changeStreamProcessor.AddCollectionsToProcess(item, _cts);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        var migrationJob = _jobList.MigrationJobs.Find(m => m.Id == jobId);
                        if (!_job.IsOnline &&  Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            _log.WriteLine($"{migrationJob.Id} completed.");

                            migrationJob.IsCompleted = true;
                            //migrationJob.CurrentlyActive = false;
                            StopProcessing(true);
                        }
                        else if (_job.IsOnline &&_job.CSStartsAfterAllUploads && Helper.IsOfflineJobCompleted(migrationJob) && !_postUploadCSProcessing)
                        {
                            // If CSStartsAfterAllUploads is true and the offline job is completed, run post-upload change stream processing
                            _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                            if (_targetClient == null && !_job.IsSimulatedRun)
                                _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                            if (_changeStreamProcessor == null)
                                _changeStreamProcessor = new MongoChangeStreamProcessor(_log,_sourceClient, _targetClient, _jobList, _job, _config);

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
    }
}
