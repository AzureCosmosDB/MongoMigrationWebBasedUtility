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
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Processors;
using OnlineMongoMigrationProcessor.Workers;

// CS4014: Use explicit discards for intentional fire-and-forget tasks.

namespace OnlineMongoMigrationProcessor
{
    internal class DumpRestoreProcessor : MigrationProcessor
    {

        //private string _toolsLaunchFolder = string.Empty;
        private string _mongoDumpOutputFolder = $"{Helper.GetWorkingFolder()}mongodump";
        private ProcessExecutor? _processExecutor=null;
        private static readonly SemaphoreSlim _uploadLock = new(1, 1);

        private SafeDictionary<string, MigrationUnit> MigrationUnitsPendingUpload = new SafeDictionary<string, MigrationUnit>();

        public DumpRestoreProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
            : base(log, jobList, job, sourceClient, config)
        {
            // Constructor body can be empty or contain initialization logic if needed
            _processExecutor = new ProcessExecutor(_log);
        }

        // Custom exception handler delegate with logic to control retry flow (parity with CopyProcessor)
        private Task<TaskResult> DumpChunk_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Dump operation was cancelled for {dbName}.{colName}-{chunkIndex}");
                return Task.FromResult(TaskResult.Canceled);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} failed due to timeout. Details:{ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine(ex.ToString(), LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        private Task<TaskResult> DumpChunkAsync(MigrationUnit mu, int chunkIndex, IMongoCollection<BsonDocument> collection,
            string folder, string sourceConnectionString, string targetConnectionString,
            double initialPercent, double contributionFactor, string dbName, string colName)
        {
            _cts.Token.ThrowIfCancellationRequested();

            // Build base args per attempt
            string args = $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection={colName}  --out {folder}\\{chunkIndex}.bson";

            // Disk space/backpressure check (retain existing behavior)
            bool continueDownloads;
            double pendingUploadsGB = 0;
            double freeSpaceGB = 0;
            while (true)
            {
                continueDownloads = Helper.CanProceedWithDownloads(folder, _config.ChunkSizeInMb * 2, out pendingUploadsGB, out freeSpaceGB);
                if (!continueDownloads)
                {
                    _log.WriteLine($"{dbName}.{colName} added to upload queue");
                    MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                    _ = Task.Run(() => Upload(mu, targetConnectionString), _cts.Token);

                    _log.WriteLine($"Disk space is running low, with only {freeSpaceGB}GB available. Pending jobList are using {pendingUploadsGB}GB of space. Free up disk space by deleting unwanted jobList. Alternatively, you can scale up tp Premium App Service plan, which will reset the WebApp. New downloads will resume in 5 minutes...", LogType.Error);

                    try { Task.Delay(TimeSpan.FromMinutes(5), _cts.Token).Wait(_cts.Token); }
                    catch (OperationCanceledException) { return Task.FromResult(TaskResult.Canceled); }
                }
                else break;
            }

            long docCount = 0;
            if (mu.MigrationChunks.Count > 1)
            {
                var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[chunkIndex].Gte!, mu.MigrationChunks[chunkIndex].Lt!, mu.MigrationChunks[chunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                _log.WriteLine($"{dbName}.{colName}-Chunk [{chunkIndex}] generating query");

                string query = MongoHelper.GenerateQueryString(gte, lt, mu.MigrationChunks[chunkIndex].DataType);
                docCount = MongoHelper.GetDocumentCount(collection, gte, lt, mu.MigrationChunks[chunkIndex].DataType);
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
                _log.WriteLine($"{dbName}.{colName}- Chunk [{chunkIndex}] Count is  {docCount}");
                args = $"{args} --query=\"{query}\"";
            }
            else
            {
                docCount = Math.Max(mu.ActualDocCount, mu.EstimatedDocCount);
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
            }

            // Ensure previous dump file (if any) is removed before fresh dump
            var dumpFilePath = $"{folder}\\{chunkIndex}.bson";
            if (File.Exists(dumpFilePath))
            {
                try { File.Delete(dumpFilePath); } catch { }
            }

            try
            {
                if(_processExecutor== null)
                    _processExecutor = new ProcessExecutor(_log);

                var task = Task.Run(() => _processExecutor.Execute(_jobList, mu, mu.MigrationChunks[chunkIndex], chunkIndex, initialPercent, contributionFactor, docCount, $"{MongoToolsFolder}\\mongodump.exe", args), _cts.Token);
                task.Wait(_cts.Token);
                bool result = task.Result;

                if (result)
                {
                    mu.MigrationChunks[chunkIndex].IsDownloaded = true;
                    _jobList.Save();
                    _log.WriteLine($"{dbName}.{colName} added to upload queue.");
                    MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                    Task.Run(() => Upload(mu, targetConnectionString), _cts.Token);
                    return Task.FromResult(TaskResult.Success);
                }
                else
                {
                    return Task.FromResult(TaskResult.Retry);
                }
            }
            catch (OperationCanceledException)
            {
                return Task.FromResult(TaskResult.Canceled);
            }
        }

        // Custom exception handler delegate for restore path (parity with CopyProcessor)
        private Task<TaskResult> RestoreChunk_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Restore operation was cancelled for {dbName}.{colName}-{chunkIndex}");
                return Task.FromResult(TaskResult.Canceled);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} failed due to timeout. Details:{ex}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine(ex.ToString(), LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        // 2. In RestoreChunkAsync, _targetClient can be null. Add null check before using _targetClient.
        private Task<TaskResult> RestoreChunkAsync(MigrationUnit mu, int chunkIndex,
            string folder, string targetConnectionString,
            double initialPercent, double contributionFactor,
            string dbName, string colName)
        {
            _cts.Token.ThrowIfCancellationRequested();

            // Build args per attempt
            string args = $" --uri=\"{targetConnectionString}\" --gzip {folder}\\{chunkIndex}.bson";

            // If first mu, drop collection, else append. Also No drop in AppendMode
            if (chunkIndex == 0 && !_job.AppendMode)
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

            long docCount = (mu.MigrationChunks.Count > 1)
                ? mu.MigrationChunks[chunkIndex].DumpQueryDocCount
                : Math.Max(mu.ActualDocCount, mu.EstimatedDocCount);

            try
            {
                if(_processExecutor == null)
                    _processExecutor = new ProcessExecutor(_log);

                var task = Task.Run(() => _processExecutor.Execute(_jobList, mu, mu.MigrationChunks[chunkIndex], chunkIndex, initialPercent, contributionFactor, docCount, $"{MongoToolsFolder}\\mongorestore.exe", args), _cts.Token);
                task.Wait(_cts.Token);
                bool result = task.Result;
                
                if (result)
                {
                    bool skipFinalize = false;

                    if (mu.MigrationChunks[chunkIndex].RestoredFailedDocCount > 0)
                    {
                        if (_targetClient == null && !_job.IsSimulatedRun)
                            _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                        try
                        {
                            var targetDb = _targetClient!.GetDatabase(mu.DatabaseName);
                            var targetCollection = targetDb.GetCollection<BsonDocument>(mu.CollectionName);

                            var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[chunkIndex].Gte!, mu.MigrationChunks[chunkIndex].Lt!, mu.MigrationChunks[chunkIndex].DataType);
                            var gte = bounds.gte;
                            var lt = bounds.lt;

                            // get count in target collection
                            mu.MigrationChunks[chunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(targetCollection, gte, lt, mu.MigrationChunks[chunkIndex].DataType);

                            // checking if source and target doc counts are same or more
                            if (mu.MigrationChunks[chunkIndex].DocCountInTarget >= mu.MigrationChunks[chunkIndex].DumpQueryDocCount)
                            {
                                _log.WriteLine($"Restore for {dbName}.{colName}-{chunkIndex} No documents missing, count in Target: {mu.MigrationChunks[chunkIndex].DocCountInTarget}");
                            }
                            else
                            {
                                // since count is mismatched, we will reprocess the chunk
                                skipFinalize = true;
                                _log.WriteLine($"Restore for {dbName}.{colName}-{chunkIndex} Documents missing, Chunk will be reprocessed", LogType.Error);
                            }

                            _jobList?.Save();
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Restore for {dbName}.{colName}-{chunkIndex} encountered error while counting documents on target. Chunk will be reprocessed. Details: {ex}", LogType.Error);
                            skipFinalize = true;
                        }
                    }

                    if (!skipFinalize)
                    {
                        mu.MigrationChunks[chunkIndex].IsUploaded = true;
                        _jobList?.Save();

                        try { File.Delete($"{folder}\\{chunkIndex}.bson"); } catch { }

                        return Task.FromResult(TaskResult.Success);
                    }
                    else
                    {
                        return Task.FromResult(TaskResult.Retry);
                    }
                }
                else
                {
                    if (mu.MigrationChunks[chunkIndex].IsUploaded == true)
                    {
                        // Already uploaded, treat as success
                        _jobList?.Save();
                        return Task.FromResult(TaskResult.Success);
                    }

                    return Task.FromResult(TaskResult.Retry);
                }
            }
            catch (OperationCanceledException)
            {
                return Task.FromResult(TaskResult.Canceled);
            }
        }

        public override async Task StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            ProcessRunning = true;

            // Initialize processor context (parity with CopyProcessor)
            ProcessorContext ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            string jobId = ctx.JobId;
            string dbName = ctx.DatabaseName;
            string colName = ctx.CollectionName;

            // Create mongodump output folder if it does not exist
            string folder = $"{_mongoDumpOutputFolder}\\{jobId}\\{Helper.SafeFileName($"{dbName}.{colName}")}";
            Directory.CreateDirectory(folder);

            try
            {
                _uploadLock.Release(); // reset the flag
            }
            catch
            {
                // Do nothing, just reset the flag
            }

            // when resuming a job, check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return;

            // starting the regular dump and restore process
            if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                mu.BulkCopyStartedOn = DateTime.UtcNow;

            // DumpAndRestore
            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"{dbName}.{colName} download started");

                mu.EstimatedDocCount = ctx.Collection.EstimatedDocumentCount();

                try
                {
                    var count = await Task.Run(() => MongoHelper.GetActualDocumentCount(ctx.Collection, mu), _cts.Token);
                    mu.ActualDocCount = count;
                    _jobList.Save();
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }

                long downloadCount = 0;

                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent = ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / mu.MigrationChunks.Count;

                    // docCount is computed inside DumpChunkAsync now
                    if (!mu.MigrationChunks[i].IsDownloaded == true)
                    {
                        TaskResult result = await new RetryHelper().ExecuteTask(
                            () => DumpChunkAsync(mu, i, ctx.Collection, folder, ctx.SourceConnectionString, ctx.TargetConnectionString, initialPercent, contributionFactor, dbName, colName),
                            (ex, attemptCount, currentBackoff) => DumpChunk_ExceptionHandler(ex, attemptCount, "Dump Executor", dbName, colName, i, currentBackoff),
                            _log
                        );

                        if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine($"Dump operation for {dbName}.{colName}-{i} failed after multiple attempts.", LogType.Error);
                            StopProcessing();
                        }
                    }
                    else
                    {
                        downloadCount += mu.MigrationChunks[i].DumpQueryDocCount;
                    }
                }

                if (!_cts.Token.IsCancellationRequested)
                {
                    mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => chunk.DumpQueryDocCount);
                    downloadCount = mu.SourceCountDuringCopy; // recompute from chunks to avoid incremental tracking
                    mu.DumpGap = Math.Max(mu.ActualDocCount, mu.EstimatedDocCount) - downloadCount;
                    mu.DumpPercent = 100;
                    mu.DumpComplete = true;
                }
            }
            else if (mu.DumpComplete && !mu.RestoreComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"{dbName}.{colName} added to upload queue");

                MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                _ = Task.Run(() => Upload(mu, ctx.TargetConnectionString), _cts.Token);
            }
        }
        

        private void Upload(MigrationUnit mu, string targetConnectionString, bool force=false)
        {
            if (!TryEnterUploadLock())
            {
                return; // Prevent concurrent uploads
            }

            string dbName = mu.DatabaseName;
            string colName = mu.CollectionName;
            string jobId = _job.Id ?? string.Empty;
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            string folder = GetDumpFolder(jobId, dbName, colName);

            _log.WriteLine($"{dbName}.{colName} upload started.");

            try
            {
                ProcessRestoreLoop(mu, folder, targetConnectionString, dbName, colName);

                if ((mu.RestoreComplete && mu.DumpComplete) || (mu.DumpComplete && _job.IsSimulatedRun))
                {
                    FinalizeUpload(mu, key, folder, targetConnectionString, jobId);
                }
            }
            finally
            {
                // Always release the upload lock if we acquired it
                try { _uploadLock.Release(); } catch { }
            }
        }

        // Attempts to enter the upload semaphore without waiting
        private bool TryEnterUploadLock() => _uploadLock.WaitAsync(0).GetAwaiter().GetResult();

        // Builds the dump folder path for a db/collection under the current job
        private string GetDumpFolder(string jobId, string dbName, string colName)
            => $"{_mongoDumpOutputFolder}\\{jobId}\\{Helper.SafeFileName($"{dbName}.{colName}")}";

        // Core restore loop: iterates until all chunks are restored or cancellation/simulation stops it
        private void ProcessRestoreLoop(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {

            while (ShouldContinueUploadLoop(mu, folder))
            {
                // MongoRestore
                if (!mu.RestoreComplete && !_cts.Token.IsCancellationRequested && mu.SourceStatus == CollectionStatus.OK)
                {
                    int restoredChunks;
                    long restoredDocs;
                    RestoreAllPendingChunksOnce(mu, folder, targetConnectionString, dbName, colName, out restoredChunks, out restoredDocs);

                    if (restoredChunks == mu.MigrationChunks.Count && !_cts.Token.IsCancellationRequested)
                    {
                        mu.RestoreGap = Math.Max(mu.ActualDocCount, mu.EstimatedDocCount) - restoredDocs;
                        mu.RestorePercent = 100;
                        mu.RestoreComplete = true;
                        if (mu.DumpComplete && mu.RestoreComplete)
                        {
                            mu.BulkCopyEndedOn = DateTime.UtcNow;
                        }
                        _jobList.Save(); // Persist state
                    }
                    else
                    {
                        // If there are no pending chunks to restore, exit the loop instead of sleeping
                        if (!HasPendingChunks(mu))
                        {
                            return;
                        }                       

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
        }

        // Returns true if there is at least one chunk that has been downloaded but not yet uploaded
        private bool HasPendingChunks(MigrationUnit mu)
        {
            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                if (mu.MigrationChunks[i].IsDownloaded == true && mu.MigrationChunks[i].IsUploaded != true)
                {
                    return true;
                }
            }
            return false;
        }

        private bool ShouldContinueUploadLoop(MigrationUnit mu, string folder)
            => !mu.RestoreComplete && Directory.Exists(folder) && !_cts.Token.IsCancellationRequested && !_job.IsSimulatedRun;

        // Performs a single pass over all chunks, restoring any downloaded-but-not-uploaded ones
        private void RestoreAllPendingChunksOnce(
            MigrationUnit mu,
            string folder,
            string targetConnectionString,
            string dbName,
            string colName,
            out int restoredChunks,
            out long restoredDocs)
        {
            restoredChunks = 0;
            restoredDocs = 0;

            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                _cts.Token.ThrowIfCancellationRequested();

                if (!mu.MigrationChunks[i].IsUploaded == true && mu.MigrationChunks[i].IsDownloaded == true)
                {
                    double initialPercent = ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor = (double)mu.MigrationChunks[i].DumpQueryDocCount / Math.Max(mu.ActualDocCount, mu.EstimatedDocCount);
                    if (mu.MigrationChunks.Count == 1) contributionFactor = 1;

                    _log.WriteLine($"{dbName}.{colName}-{i} uploader processing");

                    var restoreResult = new RetryHelper()
                        .ExecuteTask(
                            () => RestoreChunkAsync(mu, i, folder, targetConnectionString, initialPercent, contributionFactor, dbName, colName),
                            (ex, attemptCount, currentBackoff) => RestoreChunk_ExceptionHandler(ex, attemptCount, "Restore Executor", dbName, colName, i, currentBackoff),
                            _log
                        )
                        .GetAwaiter().GetResult();

                    if (restoreResult == TaskResult.Abort || restoreResult == TaskResult.FailedAfterRetries)
                    {
                        _log.WriteLine($"Restore operation for {dbName}.{colName}-{i} failed after multiple attempts.", LogType.Error);
                        StopProcessing();
                        return;
                    }

                    if (restoreResult == TaskResult.Success)
                    {
                        restoredChunks++;
                        restoredDocs += Math.Max(mu.MigrationChunks[i].RestoredSuccessDocCount, mu.MigrationChunks[i].DocCountInTarget);
                    }
                }
                else if (mu.MigrationChunks[i].IsUploaded == true)
                {
                    restoredChunks++;
                    restoredDocs += mu.MigrationChunks[i].RestoredSuccessDocCount;
                }
            }
        }

        // Finalization after restore completes or simulated run concludes
        private void FinalizeUpload(MigrationUnit mu, string key, string folder, string targetConnectionString, string jobId)
        {
            // Best-effort cleanup of local dump folder
            try
            {
                if (Directory.Exists(folder))
                    Directory.Delete(folder, true);
            }
            catch { }

            // Start change stream immediately per-mu if configured
            AddCollectionToChangeStreamQueue(mu, targetConnectionString);

            // Remove from upload queue
            MigrationUnitsPendingUpload.Remove(key);

            // Process next pending upload if any
            if (MigrationUnitsPendingUpload.TryGetFirst(out var nextItem))
            {
                _log.WriteLine($"Processing {nextItem.Value.DatabaseName}.{nextItem.Value.CollectionName} from upload queue");
                Upload(nextItem.Value, targetConnectionString, true);
                return;
            }

            // Handle offline completion and post-upload CS logic
            if (!_cts.Token.IsCancellationRequested)
            {
                var migrationJob = _jobList.MigrationJobs?.Find(m => m.Id == jobId);
                if (migrationJob != null)
                {
                    if (!_job.IsOnline && Helper.IsOfflineJobCompleted(migrationJob))
                    {
                        _log.WriteLine($"{migrationJob.Id} Completed");
                        migrationJob.IsCompleted = true;
                        StopProcessing();
                    }
                    else
                    {
                        RunChangeStreamProcessorForAllCollections(targetConnectionString);
                    }
                }
            }
        }

       
    }
}
