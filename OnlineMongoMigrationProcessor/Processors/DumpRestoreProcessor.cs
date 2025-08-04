using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8604 // Possible null reference argument.\
#pragma warning disable CS8600 // Possible null reference argument.

namespace OnlineMongoMigrationProcessor
{
    internal class DumpRestoreProcessor : IMigrationProcessor
    {
        private JobList? _jobList;
        private MigrationJob? _job;
        private string _toolsLaunchFolder = string.Empty;
        private string _mongoDumpOutputFolder = $"{Helper.GetWorkingFolder()}mongodump";
        private MongoClient? _sourceClient;
        private MongoClient? _targetClient;
        private MigrationSettings? _config;
        private ProcessExecutor _processExecutor;
        private MongoChangeStreamProcessor _changeStreamProcessor;
        private CancellationTokenSource _cts;
        private Log _log;

        //private bool _uploaderProcessing = false;
        private static readonly SemaphoreSlim _uploadLock = new(1, 1);
        private bool _postUploadCSProcessing = false;

        private SafeDictionary<string, MigrationUnit> MigrationUnitsPendingUpload = new SafeDictionary<string, MigrationUnit>();


        public bool ProcessRunning { get; set; }


        public DumpRestoreProcessor(Log log,JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config, string toolsLaunchFolder)

        {
            _log = log;
            _jobList = jobList;
            _job = job;
            _toolsLaunchFolder = toolsLaunchFolder;
            _sourceClient = sourceClient;
            _config = config;

            _processExecutor = new ProcessExecutor(_log);
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
            _processExecutor.Terminate();

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

            // Create mongodump output folder if it does not exist
            string folder = $"{_mongoDumpOutputFolder}\\{jobId}\\{Helper.SafeFileName($"{dbName}.{colName}")}";
            Directory.CreateDirectory(folder);

            var database = _sourceClient.GetDatabase(dbName);
            var collection = database.GetCollection<BsonDocument>(colName);

            try
            {
                _uploadLock.Release(); // reset the flag 
            }
            catch
            {
                // Do nothing, just reset the flag
            }
            DateTime migrationJobStartTime = DateTime.Now;

            //when resuming a job, we need to check if post-upload change stream processing is already in progress

            if (_postUploadCSProcessing)
                return; // S

            //if (_job.CSPostProcessingStarted && !Helper.IsOfflineJobCompleted(_job))
            //{
            //    _job.CSPostProcessingStarted = false;
            //    _jobList?.Save(); // Save the job state to indicate that CS post-processing has started
            //}

            if (_job.IsOnline && Helper.IsOfflineJobCompleted(_job) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                if (_changeStreamProcessor == null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log,_sourceClient, _targetClient, _jobList, _job, _config);

                var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                return;
            }

            // starting the  regular dump and restore process                       

            // MongoDump
            if (!item.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"{dbName}.{colName} download started");

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
                            string args = $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection={colName}  --out {folder}\\{i}.bson";
                            try
                            {
                                //checking if there are too many downloads or disk full. Caused by limited uploads.
                                bool continueDownlods;
                                double pendingUploadsGB = 0;
                                double freeSpaceGB = 0;
                                while (true)
                                {
                                    continueDownlods = Helper.CanProceedWithDownloads(folder, _config.ChunkSizeInMb * 2, out pendingUploadsGB, out freeSpaceGB);

                                    if (!continueDownlods)
                                    {
                                        _log.WriteLine($"{dbName}.{colName} added to upload queue");
                                        
                                        MigrationUnitsPendingUpload.AddOrUpdate($"{item.DatabaseName}.{item.CollectionName}",item);
                                        Task.Run(() => Upload(item, targetConnectionString), _cts.Token);

                                        _log.WriteLine($"Disk space is running low, with only {freeSpaceGB}GB available. Pending jobList are using {pendingUploadsGB}GB of space. Free up disk space by deleting unwanted jobList. Alternatively, you can scale up tp Premium App Service plan, which will reset the WebApp. New downloads will resume in 5 minutes...", LogType.Error);
                                        
                                        try
                                        {
                                            Task.Delay(TimeSpan.FromMinutes(5), _cts.Token).Wait(_cts.Token);
                                        }
                                        catch (OperationCanceledException)
                                        {
                                            return; // Exit if cancellation was requested during delay
                                        }
                                    }
                                    else
                                        break;
                                }


                                if (item.MigrationChunks.Count > 1)
                                {
                                    var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i].Gte, item.MigrationChunks[i].Lt, item.MigrationChunks[i].DataType);
                                    var gte = bounds.gte;
                                    var lt = bounds.lt;

                                    _log.WriteLine($"{dbName}.{colName}-Chunk [{i}] generating query");
                                    

                                    // Generate query and get document count
                                    string query = MongoHelper.GenerateQueryString(gte, lt, item.MigrationChunks[i].DataType);

                                    docCount = MongoHelper.GetDocumentCount(collection, gte, lt, item.MigrationChunks[i].DataType);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;

                                    downloadCount += item.MigrationChunks[i].DumpQueryDocCount;

                                    _log.WriteLine($"{dbName}.{colName}- Chunk [{i}] Count is  {docCount}");
                                    

                                    args = $"{args} --query=\"{query}\"";
                                }
                                else
                                {
                                    docCount = Math.Max(item.ActualDocCount, item.EstimatedDocCount);
                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;
                                }

                                if (Directory.Exists($"folder\\{i}.bson"))
                                    Directory.Delete($"folder\\{i}.bson", true);

                                var task = Task.Run(() => _processExecutor.Execute(_jobList, item, item.MigrationChunks[i],i, initialPercent, contributionFactor, docCount, $"{_toolsLaunchFolder}\\mongodump.exe", args), _cts.Token);
                                task.Wait(_cts.Token); // Wait for the task to complete with cancellation support
                                bool result = task.Result; // Capture the result after the task completes

                                if (result)
                                {
                                    continueProcessing = false;
                                    item.MigrationChunks[i].IsDownloaded = true;
                                    _jobList?.Save(); // Persist state
                                    dumpAttempts = 0;
         
                                    _log.WriteLine($"{dbName}.{colName} added to upload queue.");
                                    
                                    MigrationUnitsPendingUpload.AddOrUpdate($"{item.DatabaseName}.{item.CollectionName}", item);
                                    Task.Run(() => Upload(item, targetConnectionString), _cts.Token);
          
                                }
                                else
                                {
                                    if (!_cts.Token.IsCancellationRequested)
                                    {
                                        _log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of Dump Executor failed. Retrying in {backoff.TotalSeconds} seconds...");
                                        
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
                            }
                            catch (OperationCanceledException)
                            {
                                _log.WriteLine($"Dump operation was cancelled for {dbName}.{colName}-{i}");
                                return;
                            }
                            catch (MongoExecutionTimeoutException ex)
                            {
                                _log.WriteLine($" Dump attempt {dumpAttempts} failed due to timeout: {ex.ToString()}", LogType.Error);

                                if (dumpAttempts >= maxRetries)
                                {
                                    _log.WriteLine("Maximum dump attempts reached. Aborting operation.", LogType.Error);

                                    StopProcessing();
                                }

                                if (!_cts.Token.IsCancellationRequested)
                                {
                                    // Wait for the backoff duration before retrying
                                    _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                    
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
                if (!_cts.Token.IsCancellationRequested)
                {
                    item.SourceCountDuringCopy = item.MigrationChunks.Sum(chunk => chunk.DumpQueryDocCount);
                    item.DumpGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - downloadCount;
                    item.DumpPercent = 100;
                    item.DumpComplete = true;
                }
            }
            else if (item.DumpComplete && !item.RestoreComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"{dbName}.{colName} added to upload queue");
                
                MigrationUnitsPendingUpload.AddOrUpdate($"{item.DatabaseName}.{item.CollectionName}", item);
                Task.Run(() => Upload(item, targetConnectionString), _cts.Token);
            }

        }
        

        private void Upload(MigrationUnit item, string targetConnectionString, bool force=false)
        {

            if (!_uploadLock.WaitAsync(0).GetAwaiter().GetResult()) // don't wait, just check
            {
                return; // Prevent concurrent uploads
            }
                      

            string dbName = item.DatabaseName;
            string colName = item.CollectionName;
            int maxRetries = 10;
            string jobId = _job.Id;

            string key = $"{item.DatabaseName}.{item.CollectionName}";

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string folder = $"{_mongoDumpOutputFolder}\\{jobId}\\{Helper.SafeFileName($"{dbName}.{colName}")}";

            _log.WriteLine($"{dbName}.{colName} upload started.");

            while (!item.RestoreComplete && Directory.Exists(folder) && !_cts.Token.IsCancellationRequested && !_job.IsSimulatedRun)
            {
                int restoredChunks = 0;
                long restoredDocs = 0;

                // MongoRestore
                if (!item.RestoreComplete && !_cts.Token.IsCancellationRequested && item.SourceStatus == CollectionStatus.OK)
                {
                    for (int i = 0; i < item.MigrationChunks.Count; i++)
                    {
                        _cts.Token.ThrowIfCancellationRequested();

                        if (!item.MigrationChunks[i].IsUploaded == true && item.MigrationChunks[i].IsDownloaded == true)
                        {
                            string args = $" --uri=\"{targetConnectionString}\" --gzip {folder}\\{i}.bson";

                            // If first item, drop collection, else append. Also No drop in AppendMode
                            if (i == 0 && !_job.AppendMode)
                            {
                                args = $"{args} --drop";
                                if (_job.SkipIndexes)
                                {
                                    args = $"{args} --noIndexRestore"; // No index to create for all chunks.
                                }
                            }
                            else
                            {
                                args = $"{args} --noIndexRestore"; // No index to create. Index restore only for 1st chunk.

                            }

                            double initialPercent = ((double)100 / item.MigrationChunks.Count) * i;
                            double contributionFactor = (double)item.MigrationChunks[i].DumpQueryDocCount / Math.Max(item.ActualDocCount, item.EstimatedDocCount);
                            if (item.MigrationChunks.Count == 1) contributionFactor = 1;

                            _log.WriteLine($"{dbName}.{colName}-{i} uploader processing");

                            int restoreAttempts = 0;
                            backoff = TimeSpan.FromSeconds(2);
                            bool continueProcessing = true;
                            bool skipRestore = false;
                            while (restoreAttempts < maxRetries && !_cts.Token.IsCancellationRequested && continueProcessing && !item.RestoreComplete)
                            {
                                restoreAttempts++;
                                skipRestore=false;
                                try
                                {
                                    long docCount;
                                    if (item.MigrationChunks.Count > 1)
                                        docCount = item.MigrationChunks[i].DumpQueryDocCount; 
                                    else
                                        docCount = Math.Max(item.ActualDocCount, item.EstimatedDocCount);


                                    var task = Task.Run(() => _processExecutor.Execute(_jobList, item, item.MigrationChunks[i],i, initialPercent, contributionFactor, docCount, $"{_toolsLaunchFolder}\\mongorestore.exe", args), _cts.Token);
                                    task.Wait(_cts.Token); // Wait for the task to complete with cancellation support
                                    bool result = task.Result; // Capture the result after the task completes
                                    _log.WriteLine($"{dbName}.{colName}-{i} uploader processing completed");
                                    if (result)
                                    {                                       

                                        if (item.MigrationChunks[i].RestoredFailedDocCount > 0)
                                        {
                                            if (_targetClient == null)
                                                _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                                            var targetDb = _targetClient.GetDatabase(item.DatabaseName);
                                            var targetCollection = targetDb.GetCollection<BsonDocument>(item.CollectionName);

                                            var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i].Gte, item.MigrationChunks[i].Lt, item.MigrationChunks[i].DataType);
                                            var gte = bounds.gte;
                                            var lt = bounds.lt;

                                            // get count in target collection
                                            try
                                            {
                                                item.MigrationChunks[i].DocCountInTarget = MongoHelper.GetDocumentCount(targetCollection, gte, lt, item.MigrationChunks[i].DataType);
                                            }
                                            catch (Exception ex)
                                            {
                                                _log.WriteLine($"Restore for {dbName}.{colName}-{i} encountered error while counting documents on target. Chunk will be reprocessed. Details: {ex.ToString()}", LogType.Error);
                                                                                                
                                            }

                                            // checking if source  and target doc counts are same
                                            if (item.MigrationChunks[i].DocCountInTarget == item.MigrationChunks[i].DumpQueryDocCount)
                                            {
                                                _log.WriteLine($"Restore for {dbName}.{colName}-{i} No documents missing, count in Target: {item.MigrationChunks[i].DocCountInTarget}");
                                                
                                            }
                                            else
                                            {
                                                //since count is mismatched, we will reprocess the chunk
                                                skipRestore = true;
                                                _log.WriteLine($"Restore for {dbName}.{colName}-{i} Documents missing, Chunk will be reprocessed", LogType.Error);
                                                
                                            }

                                            _jobList?.Save(); // Persist state
                                        }

                                        //skip updating the chunk status as we are reprocessing the chunk
                                        if (!skipRestore)
                                        {
                                            
                                            continueProcessing = false;
                                            item.MigrationChunks[i].IsUploaded = true;
                                            _jobList?.Save(); // Persist state

                                            restoreAttempts = 0;

                                            restoredChunks++;
                                            restoredDocs += Math.Max(item.MigrationChunks[i].RestoredSuccessDocCount, item.MigrationChunks[i].DocCountInTarget);


                                            try
                                            {
                                                Directory.Delete($"{folder}\\{i}.bson", true);
                                            }
                                            catch { }
                                        }                                        
                                    }
                                    else
                                    {
                                        if (item.MigrationChunks[i].IsUploaded == true)
                                        {
                                            continueProcessing = false;
                                            _jobList?.Save(); // Persist state
                                        }
                                        else if (!_cts.Token.IsCancellationRequested)
                                        {
                                            _log.WriteLine($"Restore attempt {restoreAttempts} {dbName}.{colName}-{i} failed", LogType.Error);
                                            // Wait for the backoff duration before retrying
                                            _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                            
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
                                    }
                                }
                                catch (OperationCanceledException)
                                {
                                    _log.WriteLine($"Restore operation was cancelled for {dbName}.{colName}-{i}");
                                    return;
                                }
                                catch (MongoExecutionTimeoutException ex)
                                {
                                    _log.WriteLine($" Restore attempt {restoreAttempts} failed due to timeout: {ex.ToString()}", LogType.Error);
                                    i--;

                                    if (restoreAttempts >= maxRetries)
                                    {
                                        if (!_cts.Token.IsCancellationRequested)
                                        {
                                            _log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                                            
                                        }

                                        StopProcessing();
                                    }

                                    if (!_cts.Token.IsCancellationRequested)
                                    {
                                        // Wait for the backoff duration before retrying
                                        _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                        
                                        try
                                        {
                                            Task.Delay(backoff, _cts.Token).Wait(_cts.Token);
                                        }
                                        catch (OperationCanceledException)
                                        {
                                            return; // Exit if cancellation was requested during delay
                                        }
                                        
                                    }

                                    // Exponentially increase the backoff duration
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                }
                                catch (Exception ex)
                                {
                                    if (!_cts.Token.IsCancellationRequested)
                                    {
                                        _log.WriteLine(ex.ToString(), LogType.Error);
                                        

                                    }

                                    StopProcessing();
                                }
                            }
                            if (restoreAttempts == maxRetries)
                            {
                                _log.WriteLine("Maximum restore attempts reached. Aborting operations.", LogType.Error);

                                StopProcessing();
                            }
                        }
                        else if (item.MigrationChunks[i].IsUploaded == true)
                        {
                            restoredChunks++;
                            restoredDocs += item.MigrationChunks[i].RestoredSuccessDocCount;
                        }
                    }

                    if (restoredChunks == item.MigrationChunks.Count && !_cts.Token.IsCancellationRequested)
                    {
                        item.RestoreGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - restoredDocs;
                        item.RestorePercent = 100;
                        item.RestoreComplete = true;
                        _jobList?.Save(); // Persist state
                    }
                    else
                    {
                        try
                        {
                            Task.Delay(10000, _cts.Token).Wait(_cts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            return; // Exit if cancellation was requested during delay
                        }
                    }
                }
            }
            if ((item.RestoreComplete && item.DumpComplete)|| (item.DumpComplete && _job.IsSimulatedRun))
            {
                try
                {
                    if (Directory.Exists(folder))
                        Directory.Delete(folder, true);

                    // Process change streams
                    if (_job.IsOnline && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads)
                    {
                        if (_targetClient == null)
                            _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                        if (_changeStreamProcessor == null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_log,_sourceClient, _targetClient, _jobList,_job, _config);

                        _changeStreamProcessor.AddCollectionsToProcess(item, _cts);
                    }

                    //clear curretn item from upload queue
                    MigrationUnitsPendingUpload.Remove(key);
               

                    //check if migration units items to upload.
                    if (MigrationUnitsPendingUpload.TryGetFirst(out var nextItem))
                    {                        
                        _log.WriteLine($"Processing {nextItem.Value.DatabaseName}.{nextItem.Value.CollectionName} from upload queue");
                                                
                        Upload(nextItem.Value, targetConnectionString,true);
                        return;
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {                       
                        var migrationJob = _jobList.MigrationJobs.Find(m => m.Id == jobId);
                        if (!_job.IsOnline && Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            _log.WriteLine($"{migrationJob.Id} Completed");

                            migrationJob.IsCompleted = true;
                            //migrationJob.CurrentlyActive = false;
                            StopProcessing();
                        }
                        else if(_job.IsOnline && _job.CSStartsAfterAllUploads && Helper.IsOfflineJobCompleted(migrationJob) && !_postUploadCSProcessing)
                        {
                            // If CSStartsAfterAllUploads is true and the offline job is completed, run post-upload change stream processing
                            _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                            if (_targetClient == null)
                                _targetClient = MongoClientFactory.Create(_log,targetConnectionString);

                            if (_changeStreamProcessor == null)
                                _changeStreamProcessor = new MongoChangeStreamProcessor(_log,_sourceClient, _targetClient, _jobList, _job, _config);


                            var result=_changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                        }
                       
                    }

                    _uploadLock.Release(); // reset the flag to allow next upload to invoke uploader

                }
                catch
                {
                    // Do nothing
                }

                
            }
        }
    }
}
