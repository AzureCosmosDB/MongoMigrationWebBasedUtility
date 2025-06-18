using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace OnlineMongoMigrationProcessor
{
    internal class DumpRestoreProcessor : IMigrationProcessor
    {
        private JobList? _jobs;
        private MigrationJob? _job;
        private string _toolsLaunchFolder = string.Empty;
        private bool _executionCancelled = false;
        private string _mongoDumpOutputFolder = $"{Helper.GetWorkingFolder()}mongodump";
        private MongoClient? _sourceClient;
        private MongoClient? _targetClient;
        private MigrationSettings? _config;
        private ProcessExecutor _processExecutor;
        private MongoChangeStreamProcessor _changeStreamProcessor;


        public bool ProcessRunning { get; set; }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
        public DumpRestoreProcessor(JobList jobs, MigrationJob job, MongoClient sourceClient, MigrationSettings config, string toolsLaunchFolder)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
        {
            _jobs = jobs;
            _job = job;
            _toolsLaunchFolder = toolsLaunchFolder;
            _sourceClient = sourceClient;
            _config = config;

            _processExecutor = new ProcessExecutor();

        }

        public void StopProcessing()
        {
            ProcessRunning = false;
            _executionCancelled = true;
            _processExecutor.Terminate();

            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
            
        }

        public void Migrate(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8604 // Possible null reference argument.\
#pragma warning disable CS8600 // Possible null reference argument.
            int maxRetries = 10;
            string jobId = _job.Id;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string dbName = item.DatabaseName;
            string colName = item.CollectionName;

            // Create mongodump output folder if it does not exist
            string folder = $"{_mongoDumpOutputFolder}\\{jobId}\\{dbName}.{colName}";
            Directory.CreateDirectory(folder);

            var database = _sourceClient.GetDatabase(dbName);
            var collection = database.GetCollection<BsonDocument>(colName);

            bool restoreInvoked = false;
            
            DateTime migrationJobStartTime = DateTime.Now;
            

            Log.WriteLine($"{dbName}.{colName} Downloader started");

            // MongoDump
            if (!item.DumpComplete && !_executionCancelled)
            {
                item.EstimatedDocCount = collection.EstimatedDocumentCount();

                Task.Run(() =>
                {
                    long count = MongoHelper.GetActualDocumentCount(collection, item);
                    item.ActualDocCount = count;
                    _jobs?.Save();
                });

                long downloadCount = 0;

                for (int i = 0; i < item.MigrationChunks.Count; i++)
                {
                    if (_executionCancelled || _job == null || !_job.CurrentlyActive) return;

                    double initialPercent = ((double)100 / item.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / item.MigrationChunks.Count;

                    long docCount = 0;

                    if (!item.MigrationChunks[i].IsDownloaded == true)
                    {
                        int dumpAttempts = 0;
                        backoff = TimeSpan.FromSeconds(2);
                        bool continueProcessing = true;

                        while (dumpAttempts < maxRetries && !_executionCancelled && continueProcessing && _job.CurrentlyActive)
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
                                        // invoke the uploader if not invoked already. let uploads cleanup the disk.
                                        if (!restoreInvoked)
                                        {
                                            Log.WriteLine($"{dbName}.{colName} Uploader invoked");
                                            Log.Save();
                                            restoreInvoked = true;
                                            Task.Run(() => Upload(item, targetConnectionString));
                                        }

                                        Log.WriteLine($"Disk space is running low, with only {freeSpaceGB}GB available. Pending jobs are using {pendingUploadsGB}GB of space. Free up disk space by deleting unwanted jobs. Alternatively, you can scale up tp Premium App Service plan, which will reset the WebApp. New downloads will resume in 5 minutes...", LogType.Error);
                                        Log.Save();
                                        Thread.Sleep(TimeSpan.FromMinutes(5));
                                    }
                                    else
                                        break;
                                }


                                if (item.MigrationChunks.Count > 1)
                                {
                                    var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i].Gte, item.MigrationChunks[i].Lt, item.MigrationChunks[i].DataType);
                                    var gte = bounds.gte;
                                    var lt = bounds.lt;

                                    Log.WriteLine($"{dbName}.{colName}-Chunk [{i}] generating query");
                                    Log.Save();

                                    // Generate query and get document count
                                    string query = MongoHelper.GenerateQueryString(gte, lt, item.MigrationChunks[i].DataType);

                                    docCount = MongoHelper.GetDocumentCount(collection, gte, lt, item.MigrationChunks[i].DataType);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;

                                    downloadCount += item.MigrationChunks[i].DumpQueryDocCount;

                                    Log.WriteLine($"{dbName}.{colName}- Chunk [{i}] Count is  {docCount}");
                                    Log.Save();

                                    args = $"{args} --query=\"{query}\"";
                                }
                                else
                                {
                                    docCount = item.MigrationChunks[i].DumpQueryDocCount;
                                }


                                if (Directory.Exists($"folder\\{i}.bson"))
                                    Directory.Delete($"folder\\{i}.bson", true);

                                var task = Task.Run(() => _processExecutor.Execute(_jobs, item, item.MigrationChunks[i], initialPercent, contributionFactor, docCount, $"{_toolsLaunchFolder}\\mongodump.exe", args));
                                task.Wait(); // Wait for the task to complete
                                bool result = task.Result; // Capture the result after the task completes

                                if (result)
                                {
                                    continueProcessing = false;
                                    item.MigrationChunks[i].IsDownloaded = true;
                                    _jobs?.Save(); // Persist state
                                    dumpAttempts = 0;

                                    if (!restoreInvoked )
                                    {
                                        Log.WriteLine($"{dbName}.{colName} Uploader invoked");

                                        restoreInvoked = true;
                                        Task.Run(() => Upload(item, targetConnectionString));
                                    }
                                }
                                else
                                {
                                    if (!_executionCancelled)
                                    {
                                        Log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of Dump Executor failed. Retrying in {backoff.TotalSeconds} seconds...");
                                        Thread.Sleep(backoff);
                                        backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                    }
                                }
                            }
                            catch (MongoExecutionTimeoutException ex)
                            {
                                Log.WriteLine($" Dump attempt {dumpAttempts} failed due to timeout: {ex.ToString()}", LogType.Error);

                                if (dumpAttempts >= maxRetries)
                                {
                                    Log.WriteLine("Maximum dump attempts reached. Aborting operation.", LogType.Error);
                                    Log.Save();

                                    _job.CurrentlyActive = false;
                                    _jobs?.Save();

                                    ProcessRunning = false;
                                }

                                if (!_executionCancelled)
                                {
                                    // Wait for the backoff duration before retrying
                                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                    Thread.Sleep(backoff);
                                    Log.Save();

                                    // Exponentially increase the backoff duration
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                }
                            }
                            catch (Exception ex)
                            {
                                Log.WriteLine(ex.ToString(), LogType.Error);
                                Log.Save();

                                _job.CurrentlyActive = false;
                                _jobs?.Save();
                                ProcessRunning = false;
                            }
                        }
                        if (dumpAttempts == maxRetries)
                        {
                            _job.CurrentlyActive = false;
                            _jobs?.Save();
                        }
                    }
                    else
                    {
                        downloadCount += item.MigrationChunks[i].DumpQueryDocCount;
                    }
                }
                if (!_executionCancelled)
                {
                    item.DumpGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - downloadCount;
                    item.DumpPercent = 100;
                    item.DumpComplete = true;
                }
            }
            else if (item.DumpComplete && !_executionCancelled)
            {
                if (!restoreInvoked)
                {
                    Log.WriteLine($"{dbName}.{colName} Uploader invoked");

                    restoreInvoked = true;
                    Task.Run(() => Upload(item, targetConnectionString));
                }
            }
#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8604 // Possible null reference argument.
#pragma warning restore CS8600 // 
        }

        private void Upload(MigrationUnit item, string targetConnectionString)
        {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8604 // Possible null reference argument.
#pragma warning disable CS8600
            string dbName = item.DatabaseName;
            string colName = item.CollectionName;
            int maxRetries = 10;
            string jobId = _job.Id;


            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string folder = $"{_mongoDumpOutputFolder}\\{jobId}\\{dbName}.{colName}";

            Log.WriteLine($"{dbName}.{colName} Uploader started");

            while (!item.RestoreComplete && Directory.Exists(folder) && !_executionCancelled && _job.CurrentlyActive && !_job.IsSimulatedRun)
            {
                int restoredChunks = 0;
                long restoredDocs = 0;

                // MongoRestore
                if (!item.RestoreComplete && !_executionCancelled && item.SourceStatus == CollectionStatus.OK)
                {
                    for (int i = 0; i < item.MigrationChunks.Count; i++)
                    {
                        if (_executionCancelled) return;

                        if (!item.MigrationChunks[i].IsUploaded == true && item.MigrationChunks[i].IsDownloaded == true)
                        {
                            string args = $" --uri=\"{targetConnectionString}\" --gzip {folder}\\{i}.bson";
                            args = $"{args} --noIndexRestore"; // No index to create.

                            //index creation is now done by worker, so no need to restore indexes or drop collection

                            /*
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

                            }*/

                            double initialPercent = ((double)100 / item.MigrationChunks.Count) * i;
                            double contributionFactor = (double)item.MigrationChunks[i].DumpQueryDocCount / Math.Max(item.ActualDocCount, item.EstimatedDocCount);
                            if (item.MigrationChunks.Count == 1) contributionFactor = 1;

                            Log.WriteLine($"{dbName}.{colName}-{i} Uploader processing");

                            int restoreAttempts = 0;
                            backoff = TimeSpan.FromSeconds(2);
                            bool continueProcessing = true;
                            bool skipRestore = false;
                            while (restoreAttempts < maxRetries && !_executionCancelled && continueProcessing && !item.RestoreComplete && _job.CurrentlyActive )
                            {
                                restoreAttempts++;
                                skipRestore=false;
                                try
                                {
                                    if (_processExecutor.Execute(_jobs, item, item.MigrationChunks[i], initialPercent, contributionFactor, item.MigrationChunks[i].DumpQueryDocCount, $"{_toolsLaunchFolder}\\mongorestore.exe", args))
                                    {
                                        

                                        if (item.MigrationChunks[i].RestoredFailedDocCount > 0)
                                        {
                                            if (_targetClient == null)
                                                _targetClient = new MongoClient(targetConnectionString);

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
                                                Log.WriteLine($"Restore for {dbName}.{colName}-{i} encountered error while counting documents on target. Chunk will be reprocessed. Details: {ex.ToString()}", LogType.Error);
                                                Log.Save();                                                
                                            }

                                            // checking if source  and target doc counts are same
                                            if (item.MigrationChunks[i].DocCountInTarget == item.MigrationChunks[i].DumpQueryDocCount)
                                            {
                                                Log.WriteLine($"Restore for {dbName}.{colName}-{i} No documents missing, count in Target: {item.MigrationChunks[i].DocCountInTarget}");
                                                Log.Save();
                                            }
                                            else
                                            {
                                                //since count is mismatched, we will reprocess the chunk
                                                skipRestore = true;
                                                Log.WriteLine($"Restore for {dbName}.{colName}-{i} Documents missing, Chunk will be reprocessed", LogType.Error);
                                                Log.Save();
                                            }

                                            _jobs?.Save(); // Persist state
                                        }

                                        //skip updating the chunk status as we are reprocessing the chunk
                                        if (!skipRestore)
                                        {
                                            
                                            continueProcessing = false;
                                            item.MigrationChunks[i].IsUploaded = true;
                                            _jobs?.Save(); // Persist state

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
                                            _jobs?.Save(); // Persist state
                                        }
                                        else if (!_executionCancelled)
                                        {
                                            Log.WriteLine($"Restore attempt {restoreAttempts} {dbName}.{colName}-{i} failed", LogType.Error);
                                            // Wait for the backoff duration before retrying
                                            Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                            Thread.Sleep(backoff);
                                            // Exponentially increase the backoff duration
                                            backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                            Log.Save();
                                        }
                                    }
                                }
                                catch (MongoExecutionTimeoutException ex)
                                {
                                    Log.WriteLine($" Restore attempt {restoreAttempts} failed due to timeout: {ex.ToString()}", LogType.Error);
                                    i--;

                                    if (restoreAttempts >= maxRetries)
                                    {
                                        if (!_executionCancelled)
                                        {
                                            Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                                            Log.Save();
                                        }

                                        _job.CurrentlyActive = false;
                                        _jobs?.Save();

                                        ProcessRunning = false;
                                    }

                                    if (!_executionCancelled)
                                    {
                                        // Wait for the backoff duration before retrying
                                        Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                        Thread.Sleep(backoff);
                                        Log.Save();
                                    }

                                    // Exponentially increase the backoff duration
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                }
                                catch (Exception ex)
                                {
                                    if (!_executionCancelled)
                                    {
                                        Log.WriteLine(ex.ToString(), LogType.Error);
                                        Log.Save();

                                    }

                                    _job.CurrentlyActive = false;
                                    _jobs?.Save();
                                    ProcessRunning = false;
                                }
                            }
                            if (restoreAttempts == maxRetries)
                            {
                                Log.WriteLine("Maximum restore attempts reached. Aborting operations.", LogType.Error);
                                Log.Save();

                                _job.CurrentlyActive = false;
                                _jobs?.Save();
                                ProcessRunning = false;
                            }
                        }
                        else if (item.MigrationChunks[i].IsUploaded == true)
                        {
                            restoredChunks++;
                            restoredDocs += item.MigrationChunks[i].RestoredSuccessDocCount;
                        }
                    }

                    if (restoredChunks == item.MigrationChunks.Count && !_executionCancelled)
                    {
                        item.RestoreGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - restoredDocs;
                        item.RestorePercent = 100;
                        item.RestoreComplete = true;
                        _jobs?.Save(); // Persist state
                    }
                    else
                    {
                        Thread.Sleep(10000);
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
                    if (_job.IsOnline && !_executionCancelled)
                    {
                        if (_targetClient == null)
                            _targetClient = new MongoClient(targetConnectionString);

                        Log.WriteLine($"{dbName}.{colName} change stream processing started");

                        if (_changeStreamProcessor == null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_sourceClient, _targetClient, _jobs, _config);

                        Task.Run(() => _changeStreamProcessor.ProcessCollectionChangeStream(_job, item));
                    }

                    if (!_job.IsOnline && !_executionCancelled)
                    {
                        var migrationJob = _jobs.MigrationJobs.Find(m => m.Id == jobId);
                        if (Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            Log.WriteLine($"{migrationJob.Id} Completed");

                            migrationJob.IsCompleted = true;
                            migrationJob.CurrentlyActive = false;
                            _job.CurrentlyActive = false;
                            ProcessRunning = false;
                            _jobs?.Save();
                        }
                    }
                }
                catch
                {
                    // Do nothing
                }
            }
#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8604 // Possible null reference argument.
        }
    }
}
