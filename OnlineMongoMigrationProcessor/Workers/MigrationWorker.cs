using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Partitioner;
using OnlineMongoMigrationProcessor.Processors;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;


namespace OnlineMongoMigrationProcessor.Workers
{


    public class MigrationWorker
    {
        
        public bool ProcessRunning { get; set; }
        

        private string _toolsDestinationFolder = $"{Helper.GetWorkingFolder()}mongo-tools";
        private string _toolsLaunchFolder = string.Empty;
        private bool _migrationCancelled = false;

        private string _activeJobId = string.Empty;
        private Log _log;
        private MongoClient? _sourceClient;
        private MigrationProcessor? _migrationProcessor;
        public MigrationSettings? _config;
        bool _syncBack = false;
        private string? _webAppBaseUrl = null;

        // Process-wide SSL failure tracking for keep-alive. Some hosts (e.g. behind a proxy that intermittently
        // breaks TLS) produce a flood of SSL errors that the retry loop alone can't recover from; once we hit
        // the threshold we stop calling KeepAlive for the rest of the app lifetime to avoid noisy logs and CPU.
        private const int KeepAliveSslFailureThreshold = 10;
        private static int _keepAliveSslFailureCount = 0;
        private static bool _keepAliveDisabledForProcess = false;

        // Serialize moveCollection admin commands across all MUs in the process. The partition-prep
        // pipeline runs up to 8 MUs concurrently; firing 8 simultaneous moveCollection + dropDatabase
        // calls against the target makes the server return transient internal errors and deadlocks.
        private static readonly SemaphoreSlim _moveCollectionGate = new SemaphoreSlim(1, 1);

        private CancellationTokenSource? _compare_cts;
        private CancellationTokenSource? _cts;

        private bool JobStarting = false;
        
        // Track resume token setup tasks per collection to enable per-collection waiting
        private Dictionary<string, Task> _resumeTokenTasksByCollection = new Dictionary<string, Task>();

        // Pending post-copy index-build tasks per migration unit. Lets blocking index builds run
        // in the background so the main migration loop is not held up while waiting for one
        // collection's indexes to finish. Entry is removed once the task completes.
        private readonly ConcurrentDictionary<string, Task> _pendingIndexBuildTasksByUnit = new ConcurrentDictionary<string, Task>();

        public MigrationWorker()
        {            
            _log = new Log();          
            MigrationJobContext.JobList.SetLog(_log);
        }

        /// <summary>
        /// Gets the effective overwrite setting for a migration unit.
        /// Per-unit Overwrite value determines behavior.
        /// Returns: true if unit should overwrite (drop target first), false if append.
        /// </summary>
        private bool GetEffectiveOverwrite(MigrationUnit mu)
        {
            // Simulated run always skips overwrite
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                return false;

            // Per-unit override (defaults to false / append if not set)
            if (mu.Overwrite.HasValue)
                return mu.Overwrite.Value;

            // Default: append mode (don't overwrite)
            return false;
        }

        /// <summary>
        /// Gets the effective skip-indexes setting for a migration unit.
        /// Per-unit IndexingStrategy determines behavior.
        /// Returns: true if indexes should be skipped, false if indexes should be migrated.
        /// </summary>
        private bool GetEffectiveSkipIndexes(MigrationUnit mu)
        {
            // Simulated run always skips indexes
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                return true;

            // Per-unit override
            if (mu.IndexingStrategy.HasValue)
                return mu.IndexingStrategy.Value == IndexingStrategy.DontIndex;

            // Default: migrate indexes
            return false;
        }

        /// <summary>
        /// Gets whether blocking index creation should be used for a migration unit.
        /// Returns: true if indexes should be created in blocking mode, false for non-blocking (background).
        /// </summary>
        private bool GetEffectiveBlockingIndexes(MigrationUnit mu)
        {
            if (mu.IndexingStrategy.HasValue)
                return mu.IndexingStrategy.Value == IndexingStrategy.SameAsSourceBlocking;

            // Default: non-blocking
            return false;
        }

        /// <summary>
        /// Gets the effective sharding strategy for a migration unit.
        /// Per-unit value wins; null means "inherit existing pre-feature behaviour".
        /// Simulated runs force <see cref="ShardingStrategy.DontShard"/> regardless of the stored value
        /// so that no shardCollection command can be issued against the target.
        /// Note: actual shardCollection execution today lives in the Python SchemaMigration tool;
        /// the .NET worker exposes this helper so any future .NET-side sharding path consumes the
        /// per-unit value through one entry point.
        /// </summary>
        private ShardingStrategy GetEffectiveShardingStrategy(MigrationUnit mu)
        {
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                return ShardingStrategy.DontShard;

            return mu.ShardingStrategy ?? ShardingStrategy.SameAsSource;
        }

        /// <summary>
        /// Gets the effective target shard/node identifier for a migration unit.
        /// Only meaningful when <see cref="GetEffectiveShardingStrategy"/> returns
        /// <see cref="ShardingStrategy.DontShard"/>; null means "let the server decide".
        /// Simulated runs always return null.
        /// </summary>
        private string? GetEffectiveMoveToShard(MigrationUnit mu)
        {
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                return null;

            if (GetEffectiveShardingStrategy(mu) != ShardingStrategy.DontShard)
                return null;

            // "Auto" should have been resolved by ResolveAutoShardAssignmentsAsync;
            // if it wasn't, treat as null (no move)
            if (string.Equals(mu.MoveToShard, "Auto", StringComparison.OrdinalIgnoreCase))
                return null;

            return mu.MoveToShard;
        }

        /// <summary>
        /// Sets the web app base URL for Keep-Alive functionality
        /// </summary>
        public void SetWebAppBaseUrl(string webAppBaseUrl)
        {
            _webAppBaseUrl = webAppBaseUrl;
        }

        public LogBucket? GetLogBucket(string jobId)
        {
            // only for active job in migration worker
            if (MigrationJobContext.CurrentlyActiveJob != null && MigrationJobContext.CurrentlyActiveJob.Id == jobId)
                return _log.GetCurentLogBucket(jobId);
            else
                return null;
        }

        public List<LogObject>? GetMonitorMessages(string jobId)
        {
            // only for active job in migration worker
            if (MigrationJobContext.CurrentlyActiveJob != null && MigrationJobContext.CurrentlyActiveJob.Id == jobId)
                return _log.GetMonitorMessages();
            else
                return null;
        }

        public string GetRunningJobId()
        {
            Console.WriteLine($"GetRunningJobId :{_activeJobId}");
            return _activeJobId;           
        }

        public bool IsProcessRunning(string id)
        {
            if (string.IsNullOrWhiteSpace(_activeJobId))
            {
                Console.WriteLine( $"IsProcessRunning false,  IsNullOrWhiteSpace :true");
                return false;
            }
            else
            {
                if (JobStarting && _activeJobId == id)//To handle the time between setting JobStarting and actual start of processor
                {
                    Console.WriteLine($"IsProcessRunning true, JobStarting :true");
                    return true;
                }
                else
                {
                    if (_activeJobId == id && _migrationProcessor != null && _migrationProcessor.ProcessRunning)
                    {
                        Console.WriteLine($"IsProcessRunning true,  ProcessorRunning :true");
                        return true;
                    }
                    else
                    {
                        Console.WriteLine($"IsProcessRunning false,  ProcessorRunning :false");
                        return false;
                    }
                }
            }                        
        }

        public void StopMigration()
        {
            MigrationJobContext.AddVerboseLog($"StopMigration: _activeJobId={_activeJobId}");
            // Signal the processor BEFORE cancelling tokens or killing processes so that:
            //   - DumpRestoreProcessor stops the coordinator queue (prevents re-enqueue of killed chunks).
            //   - CopyProcessor/RUCopyProcessor in-flight async continuations (UpdateProgress,
            //     ProcessSegmentAsync, UpdateDocumentCountsAsync) see StopRequested and skip
            //     mid-cancel MU writes.
            _migrationProcessor?.SignalStop();
            try
            {
                _activeJobId=string.Empty;
                _log.WriteLine("Pause Migration called - cancelling all tokens and stopping processor", LogType.Warning);
                _cts?.Cancel();
                _compare_cts?.Cancel();

                // Kill all active mongodump and mongorestore processes
                MigrationJobContext.KillAllMigrationProcesses();
                
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                _migrationCancelled = true;
                
                // Force complete cleanup of processor
                if (_migrationProcessor != null)
                {
                    _migrationProcessor.StopProcessing(true);
                    _migrationProcessor = null;
                    MigrationJobContext.ActiveMigrationProcessor = null;
                }
                
                ProcessRunning = false;
                MigrationJobContext.ResetControlledPause(); // Reset controlled pause flag
                MigrationJobContext.MigrationUnitsCache = null;
                
                // Clear the centralized cache when stopping
                MigrationJobContext.ClearCurrentlyActiveJobCache();

                // Stop percentage timer
                PercentageUpdater.StopPercentageTimer();

                _log.WriteLine("Pause Migration completed - all resources released", LogType.Debug);
            }
            catch { }
        }


        /// <summary>
        /// Adjusts the number of dump workers at runtime for DumpAndRestore jobs.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"AdjustDumpWorkers: newCount={newCount}");
            if (_migrationProcessor is DumpRestoreProcessor dumpRestoreProcessor)
            {
                dumpRestoreProcessor.AdjustDumpWorkers(newCount);
            }
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime for DumpAndRestore jobs.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"AdjustRestoreWorkers: newCount={newCount}");
            if (_migrationProcessor is DumpRestoreProcessor dumpRestoreProcessor)
            {
                dumpRestoreProcessor.AdjustRestoreWorkers(newCount);
            }
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"AdjustInsertionWorkers: newCount={newCount}");
            if (_migrationProcessor is DumpRestoreProcessor dumpRestoreProcessor)
            {
                dumpRestoreProcessor.AdjustInsertionWorkers(newCount);
            }
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
        }

        public async Task WaitForResumeTokenTask(string collectionKey)
        {
            MigrationJobContext.AddVerboseLog($"WaitForResumeTokenTask: collectionKey={collectionKey}");
            if (_resumeTokenTasksByCollection.TryGetValue(collectionKey, out var task))
            {
                _log.WriteLine($"Waiting for resume token setup task to complete for {collectionKey}", LogType.Debug);
                try
                {
                    await task;
                    _log.WriteLine($"Resume token setup task completed for {collectionKey}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Resume token setup task failed for {collectionKey}. Details: {ex}", LogType.Error);
                }
            }
        }

        /// <summary>
        /// Ensures complete cleanup of the existing migration processor before creating a new one.
        /// Cancels all active operations, stops the processor, and reinitializes cancellation tokens.
        /// </summary>
        private async Task CleanupExistingProcessorAsync()
        {
            if (_migrationProcessor != null)
            {
                _log.WriteLine("Stopping existing processor before creating new one", LogType.Debug);
                try
                {
                    // Cancel any active operations
                    _cts?.Cancel();
                    _compare_cts?.Cancel();
                    
                    // Stop the processor with full cleanup
                    _migrationProcessor.StopProcessing(true);
                    _migrationProcessor = null;
                    MigrationJobContext.ActiveMigrationProcessor = null;
                    
                    // Give time for cleanup to complete
                    await Task.Delay(1000);
                    
                    // Create new cancellation tokens for the new job
                    _cts = new CancellationTokenSource();
                    _compare_cts = new CancellationTokenSource();
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error during processor cleanup. Details: {ex}", LogType.Warning);
                }
            }
        }

        private async Task<TaskResult> PrepareForMigration()
        {
            _log.WriteLine("PrepareForMigration started", LogType.Debug);
            if (MigrationJobContext.CurrentlyActiveJob == null)
                return TaskResult.FailedAfterRetries;
            if (_config == null)
                _config = new MigrationSettings();

            if (string.IsNullOrWhiteSpace(MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return TaskResult.FailedAfterRetries;

            

            _sourceClient = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, _config.CACertContentsForSourceServer ?? string.Empty);
            _log.WriteLine($"Source client initialized,  JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}, IsSimulated: {MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun}");
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                _log.WriteLine("Simulated Run. No changes will be made to the target.", LogType.Warning);
            }
            else
            {
                if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                {
                    _log.WriteLine("This migration job will not transfer the indexes to the target collections. Use the schema migration script at https://aka.ms/mongoruschemamigrationscript to create the indexes on the target collections.", LogType.Warning);
                }
            }
            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.DumpAndRestore)
            {
                _log.WriteLine($"IgnoreDuplicatesAndContinueRestore={_config.IgnoreDuplicatesAndContinueRestore}", LogType.Info);
            }
            _log.WriteLine("Verifying source server connectivity...", LogType.Debug);
            try
            {
                string version = await MongoHelper.GetServerVersionAsync(_sourceClient).ConfigureAwait(false);
                _log.WriteLine($"Source server version check passed: {version}", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Failed to connect to source server. Please verify the connection string and network connectivity. Error: {ex.Message}", LogType.Error);
                return TaskResult.Retry;
            }



            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                _log.WriteLine("Checking if change stream is enabled on source");

                if (MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics == null || MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics.Count == 0)
                    return TaskResult.FailedAfterRetries;

                MigrationUnit migrationUnit;

                migrationUnit= MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics[0].Id);
                
                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, _config.CACertContentsForSourceServer ?? string.Empty, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], migrationUnit);
                MigrationJobContext.CurrentlyActiveJob.SourceServerVersion = retValue.Version;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

                if (!retValue.IsCSEnabled)
                {
                    MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                    StopMigration();
                    return TaskResult.Abort;
                }

            }
            else
            {
                _log.WriteLine("Offline job - getting source server version...", LogType.Debug);
                //// Connect to the MongoDB server
                var client = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], true, _config.CACertContentsForSourceServer ?? string.Empty);
                var version = await MongoHelper.GetServerVersionAsync(client).ConfigureAwait(false);
                _log.WriteLine($"Source server version: {version}", LogType.Debug);
                MigrationJobContext.CurrentlyActiveJob.SourceServerVersion = version;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            }

            // Ensure complete cleanup of old processor before creating new one
            await CleanupExistingProcessorAsync();

            _log.WriteLine($"Creating migration processor for JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}", LogType.Debug);
            switch (MigrationJobContext.CurrentlyActiveJob.JobType)
            {
                case JobType.MongoDriver:
                    _migrationProcessor = new CopyProcessor(_log, _sourceClient!, _config, this);
                    _log.WriteLine("CopyProcessor created for MongoDriver job type", LogType.Debug);  
                    break;
                case JobType.DumpAndRestore:
                    _migrationProcessor = new DumpRestoreProcessor(_log,_sourceClient!, _config, this);
                    _migrationProcessor.MongoToolsFolder = _toolsLaunchFolder;
                    _log.WriteLine("DumpRestoreProcessor created for DumpAndRestore job type", LogType.Debug);
                    break;
                case JobType.RUOptimizedCopy:
            _migrationProcessor = new RUCopyProcessor(_log, _sourceClient!, _config, this);
                    _log.WriteLine("RUCopyProcessor created for RUOptimizedCopy job type", LogType.Debug);
                    break;
                default:
                    _log.WriteLine($"Unknown JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}. Defaulting to MongoDriver.", LogType.Error);
            _migrationProcessor = new CopyProcessor(_log, _sourceClient!, _config, this);
                    break;
            }
            _migrationProcessor.ProcessRunning = true;
            MigrationJobContext.ActiveMigrationProcessor = _migrationProcessor;
            // Clear the stop flag now that a fresh processor is in place. Late continuations
            // from a previous run would already have been short-circuited.
            MigrationJobContext.StopRequested = false;
            
#if !LEGACY_MONGODB_DRIVER
            // Set the delegate to wait for resume token tasks before processing collections
            _migrationProcessor.WaitForResumeTokenTaskDelegate = WaitForResumeTokenTask;
            _log.WriteLine("WaitForResumeTokenTaskDelegate set for migration processor", LogType.Debug);
#endif

            return TaskResult.Success;
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> Default_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            _log.WriteLine($"{processName} attempt {attemptCount} failed. Error details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
            return Task.FromResult(TaskResult.Retry);
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> MigrateCollections_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            if(ex is OperationCanceledException)
            {
                _log.WriteLine($"{processName} operation was paused", LogType.Debug);
                return Task.FromResult(TaskResult.Canceled);
			}
			if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed due to timeout: {ex}.", LogType.Error);
            }
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
            }
        
            return Task.FromResult(TaskResult.Retry);
        }


        private async Task<TaskResult> CreatePartitionsAsync(MigrationUnit mu,  CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"CreatePartitionsAsync: mu={mu.DatabaseName}.{mu.CollectionName}");

            bool useServerLevel = MigrationJobContext.CurrentlyActiveJob?.ChangeStreamLevel == ChangeStreamLevel.Server
                && MigrationJobContext.CurrentlyActiveJob?.JobType != JobType.RUOptimizedCopy
                && !(MigrationJobContext.CurrentlyActiveJob?.ProcessingSyncBack ?? false);

            
            if (!MigrationJobContext.CurrentlyActiveJob!.ChangeStreamStartedOn.HasValue)
            {
                MigrationJobContext.CurrentlyActiveJob.SetChangeStreamStartedOn(false, DateTime.UtcNow);
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            }

            mu.SetChangeStreamStartedOn(false, MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn.Value);
           

            if (mu.MigrationChunks!=null && mu.MigrationChunks.Count>0)
            {
                _log.WriteLine($"Partitions already exist for {mu.DatabaseName}.{mu.CollectionName} - Count: {mu.MigrationChunks.Count}", LogType.Debug);
                return TaskResult.Success; //partitions already created
            }

            _log.WriteLine($"No existing partitions found, will create new ones for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
			List<MigrationChunk>? chunks = null;

            DateTime currrentTime = useServerLevel
                ? MigrationJobContext.CurrentlyActiveJob!.ChangeStreamStartedOn!.Value
                : DateTime.UtcNow;
            _log.WriteLine($"Current time captured: {currrentTime}, JobType: {MigrationJobContext.CurrentlyActiveJob?.JobType}", LogType.Debug);
            
            try
            {
                if (MigrationJobContext.CurrentlyActiveJob?.JobType == JobType.RUOptimizedCopy)
                {
                    _log.WriteLine($"Creating RU-optimized partitions for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    chunks=new RUPartitioner().CreatePartitions(_log, _sourceClient!, mu.DatabaseName, mu.CollectionName, _cts);
                    _log.WriteLine($"RU partitioner completed, returned {(chunks == null ? "null" : chunks.Count.ToString())} chunks", LogType.Debug);
                }
                else
                {
                    _log.WriteLine($"About to call PartitionCollectionAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);                    
                    var val = await PartitionCollectionAsync(mu.DatabaseName, mu.CollectionName, _cts, mu);

                    chunks = val.Item1;
                    bool IsSucess = val.Item2;
                    if (!IsSucess)
                    {
                        _log.WriteLine($"Partitioning failed for {mu.DatabaseName}.{mu.CollectionName}", LogType.Error);
                        return TaskResult.Retry;
                    }
                    _log.WriteLine($"PartitionCollectionAsync completed for {mu.DatabaseName}.{mu.CollectionName}, returned {(chunks == null ? "null" : chunks.Count.ToString())} chunks", LogType.Debug);
                    
                    if (_cts.IsCancellationRequested)
                    {
                        _log.WriteLine($"Cancellation requested after partitioning {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
                        return TaskResult.Canceled;
                    }
                    
                    if (chunks == null)
                    {
                        _log.WriteLine($"Partitioning returned null for {mu.DatabaseName}.{mu.CollectionName}", LogType.Error);
                        return TaskResult.Retry;
                    }
                    
                    if (chunks.Count == 0)
                    {
                        _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has no records to migrate", LogType.Warning);
                    }

                    if (mu.UserFilter != null && mu.UserFilter.Any())
                    {
                        _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has {chunks!.Count} chunk(s) with user filter : {mu.UserFilter}");
                    }
                    else
                        _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has {chunks!.Count} chunk(s)");
                }
                
                _log.WriteLine($"Assigning chunks to migration unit for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                mu.MigrationChunks = chunks!;
                mu.SetChangeStreamStartedOn(false, currrentTime);
                _log.WriteLine($"Partitions created successfully - Chunks: {chunks!.Count}, ChangeStreamStartedOn: {mu.ChangeStreamStartedOn:O}", LogType.Debug);
                return TaskResult.Success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in CreatePartitionsAsync for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                throw;
            }
        }

        private async Task<TaskResult> SetCollectionResumeToken(MigrationUnit mu, bool syncBack, CancellationToken _cts, List<Task> resumeTokenTasks, bool runSynchronously = false)
        {
#if LEGACY_MONGODB_DRIVER
            // Change stream resume tokens not supported with legacy MongoDB driver
            return TaskResult.Success;
#else
            _log.WriteLine($"SetCollectionResumeToken called for {mu.DatabaseName}.{mu.CollectionName} - ResetChangeStream: {mu.ResetChangeStream}", LogType.Debug);
            bool useServerLevel = MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy;
            if (useServerLevel)
            {
                _log.WriteLine("Server-level change stream detected, skipping collection-level resume token setup", LogType.Debug);
                return TaskResult.Success; //server-level handled separately
            }

            // For collection-level, set up resume token for each collection
            int durationSeconds =60;          

            _log.WriteLine($"Asynchronous resume token setup initiated ({durationSeconds}s timeout) for {mu.DatabaseName}.{mu.CollectionName}. Reset CS {mu.ResetChangeStream}", LogType.Debug);                
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

            try
            {
                if (runSynchronously)
                {
                    // RU copy: the change stream always starts from "now" (Cosmos RU cannot open a
                    // change stream at a historical timestamp). Capture the resume token (open the
                    // cursor) BEFORE partitioning/bulk copy begins, otherwise change events that
                    // occur while the collection is being partitioned would be missed.
                    _log.WriteLine($"Capturing resume token synchronously (before partitioning) for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    await CaptureResumeTokenAsync(mu, syncBack, durationSeconds, _cts);
                }
                else
                {
                    _log.WriteLine($"Calling SetChangeStreamResumeTokenAsync as async for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    var task = Task.Run(() => CaptureResumeTokenAsync(mu, syncBack, durationSeconds, _cts));

                    // Store task in dictionary by collection key
                    _resumeTokenTasksByCollection[collectionKey] = task;
                    // Also add to list for tracking
                    resumeTokenTasks.Add(task);
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error creating async task for SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
            }
            
           
            return TaskResult.Success;
#endif
        }

#if !LEGACY_MONGODB_DRIVER
        private async Task CaptureResumeTokenAsync(MigrationUnit mu, bool syncBack, int durationSeconds, CancellationToken _cts)
        {
            try
            {
                MongoClient mongoClient = new MongoClient();
                if (syncBack)
                {
                    mongoClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, string.Empty);
                }
                else
                {
                    if (_sourceClient != null)
                    {
                        mongoClient = _sourceClient!;
                    }
                    else
                    {
                        mongoClient = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, string.Empty);
                    }
                }

                await MongoHelper.SetChangeStreamResumeTokenAsync(_log, mongoClient, MigrationJobContext.CurrentlyActiveJob, mu, durationSeconds, syncBack, _cts);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
            }
        }
#endif

        private async Task<TaskResult> PreparePartitionsAsync(CancellationToken _cts, bool skipPartitioning)
        {
            _log.WriteLine($"PreparePartitionsAsync started - SkipPartitioning: {skipPartitioning}", LogType.Debug);

            var validationResult = ValidateAndInitialize();
            if (!validationResult.IsValid)
            {
                _log.WriteLine("ValidateAndInitialize failed - returning FailedAfterRetries", LogType.Error);
                return TaskResult.FailedAfterRetries;
            }

            var prepContext = new PartitionPrepContext
            {
                CheckedCS = false,
                ServerLevelResumeTokenSet = false,
                UseServerLevel = validationResult.UseServerLevel,
                SkipPartitioning = skipPartitioning
            };

            _log.WriteLine("Calling Helper.GetMigrationUnitsToMigrate", LogType.Debug);
            
            List<MigrationUnit> unitsForPrep;
            try
            {
                unitsForPrep = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);
                _log.WriteLine($"GetMigrationUnitsToMigrate returned {(unitsForPrep == null ? "null" : unitsForPrep.Count.ToString())} units", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in GetMigrationUnitsToMigrate. Details: {ex}", LogType.Error);
                throw;
            }

            _log.WriteLine($"Preparing {unitsForPrep.Count} migration units", LogType.Debug);
            
            if (unitsForPrep == null)
            {
                _log.WriteLine("ERROR: GetMigrationUnitsToMigrate returned null", LogType.Error);
                return TaskResult.FailedAfterRetries;
            }
            
            if (unitsForPrep.Count == 0)
            {
                _log.WriteLine("WARNING: GetMigrationUnitsToMigrate returned empty list", LogType.Warning);
                return TaskResult.Success;
            }
            
            _log.WriteLine("About to enter foreach loop for migration units", LogType.Debug);
            _log.ShowInMonitor($"Preparing {unitsForPrep.Count} collection(s) for migration...");

            // Resolve "Auto" shard assignments before processing
            await ResolveAutoShardAssignmentsAsync(unitsForPrep);

            int unitIndex = 0;
            foreach (var mu in unitsForPrep)
            {
                unitIndex++;
                _log.WriteLine($"Entered foreach loop - iteration {unitIndex}", LogType.Debug);
                if (unitIndex == 1 || unitIndex % 10 == 0 || unitIndex == unitsForPrep.Count)
                {
                    _log.ShowInMonitor($"Preparing collection {unitIndex}/{unitsForPrep.Count}: {mu?.DatabaseName}.{mu?.CollectionName}");
                }
                
                if (mu == null)
                {
                    _log.WriteLine($"ERROR: Migration unit at index {unitIndex} is null", LogType.Error);
                    continue;
                }
                
                try
                {
                    _log.WriteLine($"Processing migration unit {unitIndex}/{unitsForPrep.Count}: {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    
                    if (HandleControlPause())
                    {
                        _log.WriteLine("Control pause detected during partition preparation", LogType.Info);
                        return TaskResult.Canceled;
                    }

                    var result = await PrepareUnitForCopyAsync(mu, prepContext, _cts);
                    if (result != TaskResult.Success)
                    {
                        _log.WriteLine($"PrepareUnitForCopyAsync returned {result} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Error);
                        return result;
                    }
                    
                    _log.WriteLine($"Successfully processed migration unit {unitIndex}/{unitsForPrep.Count}: {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error processing migration unit {mu.DatabaseName}.{mu.CollectionName}. Details:{ex}", LogType.Error);
                    throw;
                }
            }
            
            _log.WriteLine("PreparePartitionsAsync completed successfully for all units", LogType.Debug);
            
            // Note: No need to wait for all resume token tasks here.
            // The change stream processor checks mu.ResumeToken for each collection before processing.
            // This allows collections with ready tokens to start processing immediately.
            
            return TaskResult.Success;
        }

        private (bool IsValid, bool UseServerLevel) ValidateAndInitialize()
        {
            if (MigrationJobContext.CurrentlyActiveJob == null || _sourceClient == null)
                return (false, false);

            // Determine if we should use server-level processing
            bool useServerLevel = MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server 
                && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy;
            
            _log.WriteLine($"Change stream level determination - UseServerLevel: {useServerLevel}, ChangeStreamLevel: {MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel}, JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}", LogType.Debug);

            return (true, useServerLevel);
        }

        /// <summary>
        /// Shared fast-path for migration units whose offline copy is already complete.
        /// Ensures any pending blocking index builds finish (or non-blocking monitoring restarts)
        /// before the collection is added to the change-stream queue. Returns true if the caller
        /// can short-circuit and skip the normal validation/partition path.
        /// </summary>
        private Task<bool> TryQueueCompletedUnitForChangeStreamAsync(MigrationUnit mu, string logCallerTag)
        {
            bool allChunksComplete = mu.MigrationChunks != null
                && mu.MigrationChunks.Count > 0
                && mu.MigrationChunks.TrueForAll(c => c.IsDownloaded == true && c.IsUploaded == true);

            bool offlineCompleted = (mu.DumpComplete && mu.RestoreComplete) || allChunksComplete;

            if (!offlineCompleted)
                return Task.FromResult(false);

            // Resume repair: workers can finish flipping chunk flags right before a pause/restart,
            // leaving the coordinator's timer-driven aggregation (CheckForCompletedMigrationUnits)
            // unable to run. On resume the chunks say "done" but the MU-level fields are still at
            // their last-persisted values (0% / Complete=false), so the UI shows stale progress
            // forever. Promote the chunk-level truth to the MU now.
            if (allChunksComplete && (!mu.DumpComplete || !mu.RestoreComplete))
            {
                MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
                {
                    if (!m.DumpComplete) { m.DumpComplete = true; m.DumpPercent = 100; }
                    if (!m.RestoreComplete) { m.RestoreComplete = true; m.RestorePercent = 100; }
                    if (!m.BulkCopyEndedOn.HasValue || m.BulkCopyEndedOn.Value == DateTime.MinValue)
                    {
                        m.BulkCopyEndedOn = DateTime.UtcNow;
                    }
                    m.UpdateParentJob();
                }, updateParent: true);

                _log.WriteLine($"Resume: aggregated chunk-complete state to MU-level for {mu.DatabaseName}.{mu.CollectionName} [{logCallerTag}]", LogType.Debug);
            }

            if (_migrationProcessor == null)
                return Task.FromResult(true);

            // Use IndexBuildCompleteExplicit (backing field) not IndexBuildComplete (computed) —
            // the computed getter reports true whenever IndexesExpected==0 alongside
            // Dump/RestoreComplete, which is the entry state before the non-unique pre-count
            // inside BuildNonUniqueIndexesAfterCopyAsync has had a chance to populate it.
            bool indexesPending = !mu.IndexBuildCompleteExplicit
                && mu.IndexingStrategy.HasValue
                && mu.IndexingStrategy.Value != IndexingStrategy.DontIndex;

            bool isOnline = Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob);

            if (indexesPending)
            {
                // Covers both first-run and resume (e.g. paused mid-build) for offline AND online
                // jobs: re-enter the index-build flow so monitoring restarts. The build (and the
                // eventual change-stream queue submission for online jobs) runs in the background
                // so it cannot block the outer migration loop from advancing to other collections.
                bool isBlocking = GetEffectiveBlockingIndexes(mu);
                _log.WriteLine($"Index builds not complete for {mu.DatabaseName}.{mu.CollectionName} (blocking={isBlocking}, IndexesMigrated={mu.IndexesMigrated}/{mu.IndexesExpected}) - {(isBlocking ? "deferring change stream" : "resuming monitor")} [{logCallerTag}]", LogType.Debug);
                StartBackgroundIndexBuildAndQueue(mu);
            }
            else if (isOnline)
            {
                _migrationProcessor.AddCollectionToChangeStreamQueue(mu);
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Kicks off the post-copy index build (and change-stream enqueue on success) for a single
        /// migration unit on a background task. Idempotent per unit: while a task is in flight, a
        /// subsequent caller for the same unit will no-op. This decouples one collection's blocking
        /// index build from the main migration loop so other collections can keep progressing.
        /// </summary>
        internal void StartBackgroundIndexBuildAndQueue(MigrationUnit mu)
        {
            if (_migrationProcessor == null)
                return;

            _pendingIndexBuildTasksByUnit.GetOrAdd(mu.Id, _ => Task.Run(async () =>
            {
                try
                {
                    var processor = _migrationProcessor;
                    if (processor == null)
                        return;
                    bool canProceed = await processor.BuildNonUniqueIndexesAfterCopyAsync(mu);
                    if (canProceed)
                        processor.AddCollectionToChangeStreamQueue(mu);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Background index build for {mu.DatabaseName}.{mu.CollectionName} failed: {ex.Message}", LogType.Error);
                }
                finally
                {
                    _pendingIndexBuildTasksByUnit.TryRemove(mu.Id, out Task? _);
                    // Re-run the offline completion check now that this background task has drained.
                    // StopOfflineOrInvokeChangeStreams may have skipped earlier because this task was
                    // still in flight; without this nudge the job would stay InProgress forever on
                    // offline (DumpAndRestore) jobs whose only remaining work was a resumed index build.
                    try { _migrationProcessor?.StopOfflineOrInvokeChangeStreams(); } catch { }
                }
            }));
        }

        internal bool HasPendingIndexBuilds() => !_pendingIndexBuildTasksByUnit.IsEmpty;

        private async Task<TaskResult> PrepareUnitForCopyAsync(MigrationUnit mu, PartitionPrepContext context, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"PrepareUnitForCopyAsync: mu={mu.DatabaseName}.{mu.CollectionName}");
            _log.WriteLine($"Starting PrepareUnitForCopyAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);

            if (mu.SourceStatus == CollectionStatus.IsView || mu.SourceStatus == CollectionStatus.IsTimeSeries)
            {
                _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is a {(mu.SourceStatus == CollectionStatus.IsTimeSeries ? "time-series collection" : "view")} - skipping", LogType.Debug);
                return TaskResult.Success;
            }

            // Fast-path for collections that are already fully processed.
            // Avoid expensive source metadata checks for large jobs and queue directly for change stream monitoring.
            if (await TryQueueCompletedUnitForChangeStreamAsync(mu, "prepare-partition"))
            {
                _log.WriteLine($"Bypassing collection validation for completed unit {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                return TaskResult.Success;
            }

            _log.WriteLine($"Validating collection exists: {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
            var collectionStatus = await ValidateCollectionExistsAsync(mu, _cts);
            _log.WriteLine($"Collection validation result: {collectionStatus} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
            
            if (collectionStatus == CollectionValidationResult.NotFound)
            {
                _log.WriteLine($"Collection not found - handling missing collection: {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
                return await HandleMissingCollectionAsync(mu, _cts);
            }
            else if (collectionStatus == CollectionValidationResult.IsView)
            {
                return TaskResult.Success;
            }
            else if (collectionStatus == CollectionValidationResult.IsTimeSeries)
            {
                return TaskResult.Success;
            }
            else if (collectionStatus == CollectionValidationResult.Valid)
            {
                mu.SourceStatus = CollectionStatus.OK;
                MigrationJobContext.SaveMigrationUnit(mu, true);

                _log.WriteLine($"Updating document counts for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                await UpdateDocumentCountsAsync(mu, _cts);

                _log.WriteLine($"Setting up server-level resume token (if needed) for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                await SetupServerLevelResumeTokenAsync(mu, context, _cts, MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack);

                if (mu.MigrationChunks == null || mu.MigrationChunks.Count == 0)
                {
                    _log.WriteLine($"Preparing target collection for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    var prepResult = await PrepareTargetCollectionAsync(mu, context, _cts);
                    if (prepResult != TaskResult.Success)
                    {
                        _log.WriteLine($"PrepareTargetCollectionAsync returned {prepResult} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Error);
                        return prepResult;
                    }

                    if (!context.SkipPartitioning)
                    {
                        // RU copy change streams always start from "now" (Cosmos RU cannot open a
                        // change stream at a historical timestamp). Capture the resume token
                        // synchronously BEFORE partitioning so change events that occur while the
                        // collection is being partitioned/bulk-copied are not missed.
                        if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy
                            && Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                        {
                            await SetCollectionResumeToken(mu, MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack, _cts, new List<Task>(), runSynchronously: true);
                        }

                        _log.WriteLine($"Creating partitions for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                        var partResult = await CreatePartitionsAsync(mu, _cts);
                        if (partResult != TaskResult.Success)
                        {
                            _log.WriteLine($"CreatePartitionsAsync returned {partResult} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Error);
                            return partResult;
                        }
                        _log.WriteLine($"Partitions created successfully for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    }
                }
                else
                {
                    _log.WriteLine($"Migration chunks already exist for {mu.DatabaseName}.{mu.CollectionName} - Count: {mu.MigrationChunks.Count}", LogType.Debug);
                }
            }

            MigrationJobContext.SaveMigrationUnit(mu, false);
            _log.WriteLine($"Completed PrepareUnitForCopyAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
            return TaskResult.Success;
        }

        private async Task<CollectionValidationResult> ValidateCollectionExistsAsync(MigrationUnit mu, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"ValidateCollectionExistsAsync: mu={mu.DatabaseName}.{mu.CollectionName}");
            _log.WriteLine($"Checking if collection exists: {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);

            try
            {
                bool checkExist;
                if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                {
                    _log.WriteLine($"Using RU collection check for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, mu.DatabaseName, mu.CollectionName);
                }
                else
                {
                    _log.WriteLine($"Using standard collection check for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                    checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, mu.DatabaseName, mu.CollectionName);
                }

                if (!checkExist)
                {
                    _log.WriteLine($"Collection does not exist: {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
                    mu.SourceStatus = CollectionStatus.Unknown;
                    return CollectionValidationResult.NotFound;
                }

                _log.WriteLine($"Collection exists, checking if it's a collection (not a view): {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                bool isCollection = true;
                bool isTimeSeries = false;
                try
                {
                    var ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient, mu.DatabaseName, mu.CollectionName);
                    isCollection = checkExist && ret.IsCollection;
                    isTimeSeries = ret.IsTimeSeries;
                    _log.WriteLine($"CheckIsCollectionAsync result: isCollection={isCollection} isTimeSeries={isTimeSeries} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error checking if {mu.DatabaseName}.{mu.CollectionName} is a collection. Details: {ex}", LogType.Warning);
                    isCollection = true;
                }

                if (isTimeSeries)
                {
                    mu.SourceStatus = CollectionStatus.IsTimeSeries;
                    _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is a time-series collection. Time-series collections are not supported by the target (Cosmos DB for MongoDB / DocumentDB) and cannot be migrated as regular collections.", LogType.Warning);
                    return CollectionValidationResult.IsTimeSeries;
                }

                if (!isCollection)
                {
                    mu.SourceStatus = CollectionStatus.IsView;
                    _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Warning);
                    return CollectionValidationResult.IsView;
                }

                _log.WriteLine($"Collection validation successful: {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                return CollectionValidationResult.Valid;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error during collection validation for {mu.DatabaseName}.{mu.CollectionName}.Details: {ex}", LogType.Error);
                throw;
            }
        }

        private async Task UpdateDocumentCountsAsync(MigrationUnit mu, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"UpdateDocumentCountsAsync: mu={mu.DatabaseName}.{mu.CollectionName}");

            if (mu.MigrationChunks != null && mu.MigrationChunks.Count > 0)
                return;

            var db = _sourceClient!.GetDatabase(mu.DatabaseName);
            var coll = db.GetCollection<BsonDocument>(mu.CollectionName);

            bool gotCountFromStats = false;
            try
            {
                var statsCmd = new BsonDocument { { "collStats", mu.CollectionName } };
                var stats = db.RunCommand<BsonDocument>(statsCmd);

                if (stats.TryGetValue("count", out var countVal) && countVal.IsNumeric)
                {
                    mu.EstimatedDocCount = countVal.ToInt64();
                    gotCountFromStats = true;
                }
                if (stats.TryGetValue("avgObjSize", out var avgObjSizeVal) && avgObjSizeVal.IsNumeric)
                {
                    long avg = (long)avgObjSizeVal.ToDouble();
                    if (avg > 0) mu.AvgDocSizeBytes = avg;
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"collStats failed for {mu.DatabaseName}.{mu.CollectionName}; falling back to EstimatedDocumentCount, AvgDocSizeBytes left as 0. Details: {ex.Message}", LogType.Debug);
            }

            if (!gotCountFromStats)
                mu.EstimatedDocCount = coll.EstimatedDocumentCount();

            _ = Task.Run(() =>
            {
                long count = MongoHelper.GetActualDocumentCount(coll, mu);
                if (MigrationJobContext.StopRequested)
                    return;
                MigrationJobContext.MutateMigrationUnit(mu.Id, m => m.ActualDocCount = count, updateParent: false);
            }, _cts);
        }


        private async Task SetupServerLevelResumeTokenAsync(MigrationUnit mu, PartitionPrepContext context, CancellationToken _cts, bool syncBack)
        {
#if LEGACY_MONGODB_DRIVER
            // Change stream resume tokens not supported with legacy MongoDB driver
            return;
#else
            MigrationJobContext.AddVerboseLog($"SetupServerLevelResumeTokenAsync: mu={mu.DatabaseName}.{mu.CollectionName}, syncBack={syncBack}");

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) || !context.UseServerLevel)
                return;

            if (context.UseServerLevel && !context.ServerLevelResumeTokenSet)
            {
                if(syncBack)
                    _log.WriteLine($"SyncBack: Setting up server-level change stream resume token.");
                else
                    _log.WriteLine($"Setting up server-level change stream resume token.");

                ChangeStreamTransitionHelper.TryTransitionCollectionToServerResumeCheckpoint(_log, syncBack);


                MongoClient mongoClient;
                if (syncBack)
                {
                    mongoClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, string.Empty);
                }
                else
                {
                    if (_sourceClient != null)
                    {
                        mongoClient = _sourceClient!;
                    }
                    else
                    {
                        mongoClient = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, string.Empty);
                    }
                }

                await MongoHelper.SetChangeStreamResumeTokenAsync(_log, mongoClient, MigrationJobContext.CurrentlyActiveJob, mu, 30, syncBack, _cts);

                context.ServerLevelResumeTokenSet = true;
            }
#endif
        }

        private async Task<TaskResult> PrepareTargetCollectionAsync(MigrationUnit mu, PartitionPrepContext context, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"PrepareTargetCollectionAsync: mu={mu.DatabaseName}.{mu.CollectionName}");

            // Skip if simulated run, append mode (or per-unit Overwrite=false), or already created
            var effectiveOverwrite = GetEffectiveOverwrite(mu);
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun || !effectiveOverwrite || mu.TargetCreated)
                return TaskResult.Success;

            var database = _sourceClient!.GetDatabase(mu.DatabaseName);
            var collection = database.GetCollection<BsonDocument>(mu.CollectionName);
            
            if (string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return TaskResult.FailedAfterRetries;
            
            var targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id];
            var skipIndexes = GetEffectiveSkipIndexes(mu);
            // When indexing strategy requires index building, only create unique indexes
            // before data copy. Non-unique indexes will be built after offline copy completes.
            bool useUniqueOnly = !skipIndexes && (mu.IndexingStrategy == IndexingStrategy.SameAsSource || mu.IndexingStrategy == IndexingStrategy.SameAsSourceBlocking);
            var result = await MongoHelper.DeleteAndCopyIndexesAsync(_log, mu, 
                targetConnStr, collection, skipIndexes, uniqueOnly: useUniqueOnly);

            if (_cts.IsCancellationRequested)
                return TaskResult.Canceled;

            if (!result)
                return TaskResult.Retry;

            // Apply sharding or move collection after creation and before data copy
            var shardingResult = await ApplyShardingStrategyAsync(mu, _cts);
            if (shardingResult != TaskResult.Success)
                return shardingResult;

            MigrationJobContext.SaveMigrationUnit(mu, false);

            return TaskResult.Success;
        }

        /// <summary>
        /// Applies sharding strategy to the target collection after creation.
        /// SameAsSource: reads shard key from source, applies hashed shard key on target.
        /// DontShard + MoveToShard: moves collection to specified shard using moveCollection.
        /// </summary>
        private async Task<TaskResult> ApplyShardingStrategyAsync(MigrationUnit mu, CancellationToken _cts)
        {
            var strategy = GetEffectiveShardingStrategy(mu);
            var targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob!.Id];
            var targetClient = MongoClientFactory.Create(_log, targetConnStr);
            var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            var targetCollectionName = mu.GetEffectiveTargetCollectionName();
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

            if (strategy == ShardingStrategy.SameAsSource)
            {
                // Read shard key from source and apply on target
                var sourceShardKey = await MongoHelper.GetShardKeyFromSourceAsync(_log, _sourceClient!, mu.DatabaseName, mu.CollectionName);
                if (sourceShardKey != null)
                {
                    _log.WriteLine($"Applying shard key from source for {namespaceForLog}");
                    var shardResult = await MongoHelper.ShardCollectionAsync(_log, targetClient, targetDatabaseName, targetCollectionName, sourceShardKey);
                    if (!shardResult)
                    {
                        _log.WriteLine($"Failed to shard collection {namespaceForLog}, continuing without sharding", LogType.Warning);
                    }
                }
                else
                {
                    _log.WriteLine($"Source collection {namespaceForLog} is not sharded, skipping shard key migration", LogType.Debug);
                }
            }
            else if (strategy == ShardingStrategy.DontShard)
            {
                // Move collection to specified shard if MoveToShard is set. First attempt happens
                // here right after collection creation; if it fails (the server can return a
                // transient internal error when called too soon after create), MoveStatus stays
                // false and EnsureMoveAppliedAsync will retry once just before data copy starts.
                var moveToShard = GetEffectiveMoveToShard(mu);
                if (!string.IsNullOrWhiteSpace(moveToShard))
                {
                    if (mu.MoveStatus)
                    {
                        _log.WriteLine($"moveCollection for {namespaceForLog} -> {moveToShard} already applied; skipping", LogType.Debug);
                    }
                    else
                    {
                        // Brief cool-down: server can return a transient internal error if
                        // moveCollection is called immediately after collection creation.
                        try { await Task.Delay(5000, _cts); } catch (OperationCanceledException) { return TaskResult.Canceled; }

                        (bool moveOk, string? moveError) moveResult;
                        try { await _moveCollectionGate.WaitAsync(_cts); } catch (OperationCanceledException) { return TaskResult.Canceled; }
                        try
                        {
                            moveResult = await MongoHelper.MoveCollectionAsync(_log, targetClient, targetDatabaseName, targetCollectionName, moveToShard);
                        }
                        finally { _moveCollectionGate.Release(); }

                        if (moveResult.moveOk)
                        {
                            mu.MoveStatus = true;
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                        }
                        else
                        {
                            _log.WriteLine($"moveCollection for {namespaceForLog} -> {moveToShard} failed on first attempt; will retry before data copy starts. Error: {moveResult.moveError}", LogType.Warning);
                        }
                    }
                }
            }

            if (_cts.IsCancellationRequested)
                return TaskResult.Canceled;

            return TaskResult.Success;
        }

        /// <summary>
        /// Runs the deferred moveCollection for the given unit just before data copy starts when
        /// <see cref="MigrationUnit.MoveStatus"/> is false and a target shard is configured.
        /// Uses RetryHelper for up to 4 attempts with 2s initial exponential backoff. On success
        /// persists MoveStatus=true; on final failure leaves it false so it will be retried on
        /// the next job run.
        /// </summary>
        private async Task EnsureMoveAppliedAsync(MigrationUnit mu, CancellationToken ct)
        {
            if (mu.MoveStatus)
                return;

            var moveToShard = GetEffectiveMoveToShard(mu);
            if (string.IsNullOrWhiteSpace(moveToShard))
                return;

            if (ct.IsCancellationRequested)
                return;

            var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            var targetCollectionName = mu.GetEffectiveTargetCollectionName();
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

            // Skip the move once writes have started against the target (e.g. on pause/resume).
            // moveCollection is unsafe after data exists in the target collection.
            if (mu.BulkCopyStartedOn.HasValue && mu.BulkCopyStartedOn.Value != DateTime.MinValue)
            {
                _log.WriteLine($"Skipping moveCollection for {namespaceForLog} -> {moveToShard}: writes to target have already started (BulkCopyStartedOn={mu.BulkCopyStartedOn:O})", LogType.Warning);
                return;
            }

            var targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob!.Id];
            var targetClient = MongoClientFactory.Create(_log, targetConnStr);


            const int maxAttempts = 4;
            string? lastError = null;
            int attemptCounter = 0;
            var retry = new RetryHelper();
            var retryResult = await retry.ExecuteTask(
                async () =>
                {
                    if (ct.IsCancellationRequested) return TaskResult.Canceled;

                    try { await _moveCollectionGate.WaitAsync(ct); }
                    catch (OperationCanceledException) { return TaskResult.Canceled; }

                    int attempt = Interlocked.Increment(ref attemptCounter);
                    try
                    {
                        var (ok, err) = await MongoHelper.MoveCollectionAsync(_log, targetClient, targetDatabaseName, targetCollectionName, moveToShard);
                        if (ok)
                        {
                            if (attempt > 1)
                                _log.WriteLine($"moveCollection for {namespaceForLog} -> {moveToShard} succeeded on attempt {attempt}/{maxAttempts}");
                            return TaskResult.Success;
                        }
                        lastError = err;
                        _log.WriteLine($"moveCollection for {namespaceForLog} -> {moveToShard} attempt {attempt}/{maxAttempts} failed. Error: {err}", LogType.Warning);
                        return TaskResult.Retry;
                    }
                    finally { _moveCollectionGate.Release(); }
                },
                (ex, attempt, backoffSec) =>
                {
                    lastError = ex.Message;
                    _log.WriteLine($"moveCollection for {namespaceForLog} -> {moveToShard} attempt {attempt}/{maxAttempts} threw: {ex.Message}. Retrying in {backoffSec}s...", LogType.Warning);
                    return Task.FromResult(TaskResult.Retry);
                },
                _log,
                maxTries: maxAttempts,
                initialDelayMs: 2000);

            if (retryResult == TaskResult.Success)
            {
                mu.MoveStatus = true;
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }
            else if (retryResult != TaskResult.Canceled)
            {
                _log.WriteLine($"moveCollection for {namespaceForLog} -> {moveToShard} did not succeed after retries; data copy will proceed. Error: {lastError}", LogType.Error);
            }
        }

        /// <summary>
        /// Resolves "Auto" MoveToShard values by distributing collections across available shards
        /// using greedy bin-packing by source storage size (largest-first).
        /// Collections are assigned to the shard with the least accumulated storage.
        /// </summary>
        private async Task ResolveAutoShardAssignmentsAsync(List<MigrationUnit> units)
        {
            var autoUnits = units.Where(mu =>
                GetEffectiveShardingStrategy(mu) == ShardingStrategy.DontShard &&
                string.Equals(mu.MoveToShard, "Auto", StringComparison.OrdinalIgnoreCase)
            ).ToList();

            if (autoUnits.Count == 0)
                return;

            _log.WriteLine($"Resolving auto-shard assignments for {autoUnits.Count} collection(s)", LogType.Debug);
            _log.ShowInMonitor($"Resolving auto-shard assignments for {autoUnits.Count} collection(s) (fetching storage sizes from source)...");

            // Get available shards/nodes from the target using the same layered probe as the UI
            var targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob!.Id];
            var shards = await MongoHelper.GetClusterNodesAsync(targetConnStr);

            if (shards.Count == 0)
            {
                _log.WriteLine("No shards available on target - cannot auto-distribute. Clearing MoveToShard.", LogType.Warning);
                foreach (var mu in autoUnits)
                    mu.MoveToShard = null;
                return;
            }

            if (shards.Count == 1)
            {
                _log.WriteLine($"Only one shard available ({shards[0]}) - assigning all auto collections to it", LogType.Debug);
                foreach (var mu in autoUnits)
                    mu.MoveToShard = shards[0];
                return;
            }

            // Get storage sizes from source (throttled to avoid exhausting the driver's connection wait queue)
            using var sizeThrottle = new SemaphoreSlim(10);
            int sizeCompleted = 0;
            int sizeTotal = autoUnits.Count;
            var sizeTasks = autoUnits.Select(async mu =>
            {
                await sizeThrottle.WaitAsync();
                try
                {
                    var size = await MongoHelper.GetCollectionStorageSizeAsync(_log, _sourceClient!, mu.DatabaseName, mu.CollectionName);
                    int done = Interlocked.Increment(ref sizeCompleted);
                    if (done == 1 || done % 10 == 0 || done == sizeTotal)
                    {
                        _log.ShowInMonitor($"Auto-shard: fetched storage size {done}/{sizeTotal} (last: {mu.DatabaseName}.{mu.CollectionName})");
                    }
                    return (Unit: mu, Size: size);
                }
                finally
                {
                    sizeThrottle.Release();
                }
            });

            var sizeResults = await Task.WhenAll(sizeTasks);
            _log.ShowInMonitor($"Auto-shard: assigning {sizeTotal} collection(s) to shards (greedy bin-packing)...");

            // Greedy bin-packing: sort by size descending, assign to shard with least storage
            var sortedUnits = sizeResults.OrderByDescending(r => r.Size).ToList();
            var shardLoad = shards.ToDictionary(s => s, _ => 0L);

            foreach (var (unit, size) in sortedUnits)
            {
                var targetShard = shardLoad.OrderBy(kv => kv.Value).First().Key;
                unit.MoveToShard = targetShard;
                shardLoad[targetShard] += size;

                _log.WriteLine($"Auto-shard: {unit.DatabaseName}.{unit.CollectionName} ({size / (1024 * 1024):N0} MB) -> {targetShard}", LogType.Debug);
            }

            // Log distribution summary
            foreach (var kv in shardLoad.OrderBy(kv => kv.Key))
            {
                _log.WriteLine($"Shard {kv.Key} total assigned storage: {kv.Value / (1024 * 1024):N0} MB", LogType.Debug);
            }
        }

        private async Task<TaskResult> HandleMissingCollectionAsync(MigrationUnit mu, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"HandleMissingCollectionAsync: mu={mu.DatabaseName}.{mu.CollectionName}");

            if (_cts.IsCancellationRequested)
                return TaskResult.Canceled;

            var effectiveOverwrite = GetEffectiveOverwrite(mu);
            if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && 
                effectiveOverwrite && 
                !mu.TargetCreated)
            {
                try
                {
                    var database = _sourceClient!.GetDatabase(mu.DatabaseName);
                    var collection = database.GetCollection<BsonDocument>(mu.CollectionName);
                    var skipIndexes = GetEffectiveSkipIndexes(mu);
                    await MongoHelper.DeleteAndCopyIndexesAsync(_log, mu, 
                        MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], 
                        collection, skipIndexes);
                }
                catch
                {
                    // Intentionally empty - best effort attempt
                }
            }

            mu.SourceStatus = CollectionStatus.NotFound;
            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} does not exist on source", LogType.Error);
            MigrationJobContext.SaveMigrationUnit(mu, true);

            return TaskResult.Success;
        }

        private class PartitionPrepContext
        {
            public bool CheckedCS { get; set; }
            public bool ServerLevelResumeTokenSet { get; set; }
            public bool UseServerLevel { get; set; }
            public bool SkipPartitioning { get; set; }
        }

        private enum CollectionValidationResult
        {
            Valid,
            NotFound,
            IsView,
            IsTimeSeries
        }

        private async Task<TaskResult> MigrateJobCollections(bool syncBack,CancellationToken ctsToken)
        {
            _log.WriteLine("MigrateJobCollections started", LogType.Debug);

            if (MigrationJobContext.CurrentlyActiveJob == null)
                return TaskResult.FailedAfterRetries;

            // RU jobs use the sequential pipeline: all partitions first, then copy (no background partitioning).
            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
            {
                return await MigrateRUOptimizedJobCollectionsAsync(syncBack, ctsToken);
            }

            return await MigrateJobCollectionsWithAsyncPartitioningAsync(syncBack, ctsToken);
        }

        private async Task<TaskResult> MigrateJobCollectionsWithAsyncPartitioningAsync(bool syncBack, CancellationToken ctsToken)
        {
            if (MigrationJobContext.CurrentlyActiveJob == null)
                return TaskResult.FailedAfterRetries;

            List<Task> resumeTokenTasks = new List<Task>();
            var unitsForMigrate = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);
            _log.WriteLine($"Processing {unitsForMigrate.Count} migration units (async partitioning pipeline)", LogType.Debug);
            _log.ShowInMonitor($"Processing {unitsForMigrate.Count} collection(s) (async partitioning pipeline)...");

            // Lightweight global prerequisites only (no upfront per-collection partitioning).
            await ResolveAutoShardAssignmentsAsync(unitsForMigrate);

            // SyncBack target change-stream probe runs once for the whole job. The probe creates a
            // throwaway GUID database, inserts a doc, and then drops the database;
            if (MigrationJobContext.CurrentlyActiveJob.SyncBackEnabled &&
                !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun &&
                Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) &&
                unitsForMigrate.Count > 0)
            {
                var targetConnStrForProbe = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id];
                if (!string.IsNullOrWhiteSpace(targetConnStrForProbe))
                {
                    _log.WriteLine("SyncBack: Checking if change stream is enabled on target");
                    var probeResult = await MongoHelper.IsChangeStreamEnabledAsync(_log, string.Empty,
                        targetConnStrForProbe, unitsForMigrate[0], true);
                    if (!probeResult.IsCSEnabled)
                        return TaskResult.Abort;
                }
            }

            var serverCtx = new PartitionPrepContext
            {
                CheckedCS = false,
                ServerLevelResumeTokenSet = false,
                UseServerLevel = MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server
                    && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy,
                SkipPartitioning = true
            };

            if (serverCtx.UseServerLevel && unitsForMigrate.Count > 0)
            {
                await SetupServerLevelResumeTokenAsync(unitsForMigrate[0], serverCtx, ctsToken, syncBack);
            }

            // Partition in the background and start migration as each collection becomes ready.
            int maxPartitionConcurrency = Math.Max(1, Math.Min(8, MigrationJobContext.CurrentlyActiveJob.ParallelThreads > 0
                ? MigrationJobContext.CurrentlyActiveJob.ParallelThreads
                : Environment.ProcessorCount));
            var partitionGate = new SemaphoreSlim(maxPartitionConcurrency, maxPartitionConcurrency);

            int partitionPrepared = 0;
            int partitionTotal = unitsForMigrate.Count;
            _log.ShowInMonitor($"Preparing {partitionTotal} collection(s) in background ({maxPartitionConcurrency} concurrent)...");

            var partitionTasks = unitsForMigrate.ToDictionary(
                mu => mu.Id,
                mu => Task.Run(async () =>
                {
                    await partitionGate.WaitAsync(ctsToken);
                    try
                    {
                        if (_migrationCancelled || ctsToken.IsCancellationRequested)
                            return TaskResult.Canceled;

                        var (exists, isCollection, isTimeSeries) = await ValidateSourceCollectionAsync(mu);

                        // Order matters: a missing namespace returns (exists=false, isCollection=false).
                        // Handle the missing case first so we don't mis-classify it as a view.
                        if (!exists)
                        {
                            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} does not exist on source. Marking as NotFound and continuing.", LogType.Warning);
                            return await HandleMissingCollectionAsync(mu, ctsToken);
                        }

                        if (isTimeSeries)
                        {
                            mu.SourceStatus = CollectionStatus.IsTimeSeries;
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is a time-series collection. Time-series collections are not supported by the target (Cosmos DB for MongoDB / DocumentDB) — skipping.", LogType.Warning);
                            return TaskResult.Success;
                        }

                        if (!isCollection)
                        {
                            mu.SourceStatus = CollectionStatus.IsView;
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is a view. Only collections are supported for migration — skipping.", LogType.Warning);
                            return TaskResult.Success;
                        }

                        mu.SourceStatus = CollectionStatus.OK;
                        await UpdateDocumentCountsAsync(mu, ctsToken);

                        await ValidateTargetCollectionExistsAsync(mu);

                        if (mu.MigrationChunks == null || mu.MigrationChunks.Count == 0)
                        {
                            // Keep behavior aligned with the previous prep path.
                            var prepContext = new PartitionPrepContext
                            {
                                CheckedCS = false,
                                ServerLevelResumeTokenSet = false,
                                UseServerLevel = false,
                                SkipPartitioning = false
                            };

                            var prepResult = await PrepareTargetCollectionAsync(mu, prepContext, ctsToken);
                            if (prepResult != TaskResult.Success)
                                return prepResult;

                            var partResult = await CreatePartitionsAsync(mu, ctsToken);
                            if (partResult != TaskResult.Success)
                                return partResult;
                        }

                        MigrationJobContext.SaveMigrationUnit(mu, false);
                        return TaskResult.Success;
                    }
                    catch (OperationCanceledException)
                    {
                        return TaskResult.Canceled;
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Async partitioning failed for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                        return TaskResult.Retry;
                    }
                    finally
                    {
                        partitionGate.Release();
                        int done = Interlocked.Increment(ref partitionPrepared);
                        if (done == 1 || done % 10 == 0 || done == partitionTotal)
                        {
                            _log.ShowInMonitor($"Background preparation: {done}/{partitionTotal} collection(s) ready (last: {mu.DatabaseName}.{mu.CollectionName})");
                        }
                    }
                }, ctsToken));

            int migrateIndex = 0;
            foreach (var migrationUnit in unitsForMigrate)
            {
                if (_migrationCancelled)
                    return TaskResult.Canceled;

                if (HandleControlPause())
                    return TaskResult.Canceled;

                var prepResult = await partitionTasks[migrationUnit.Id];
                if (prepResult != TaskResult.Success)
                    return prepResult;

                MigrationJobContext.AddVerboseLog($"Before MigrateUnitEndToEndAsync for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");

                var result = await MigrateUnitEndToEndAsync(migrationUnit, syncBack, ctsToken, resumeTokenTasks);
                if (result != TaskResult.Success)
                    return result;

                migrateIndex++;
                if (migrateIndex == 1 || migrateIndex % 10 == 0 || migrateIndex == unitsForMigrate.Count)
                {
                    _log.ShowInMonitor($"Migration dispatch progress: {migrateIndex}/{unitsForMigrate.Count} (last: {migrationUnit.DatabaseName}.{migrationUnit.CollectionName})");
                }

                MigrationJobContext.AddVerboseLog($"Before ShouldBreakMigrationLoop, last processed {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");

                if (ShouldBreakMigrationLoop())
                {
                    _log.WriteLine("Breaking loop: CS post-processing started and offline job completed", LogType.Debug);
                    break;
                }
            }

            // Every MU has been dispatched (or the loop broke after offline completion). Allow the
            // coordinator to self-shutdown once its queues drain; without this it would block forever
            // on the registration gate when the last MU finishes.
            _migrationProcessor?.MarkAllUnitsDispatched();

            // Resume case: when every MU was already complete the fast-path bypasses StartProcessAsync,
            // so the coordinator never registers any MU and its OnMigrationUnitCompleted callback (the
            // normal trigger for StopOfflineOrInvokeChangeStreams) never fires — leaving the CS runner
            // unstarted and WaitForMigrationProcessorCompletionAsync looping forever. The method is
            // gated and re-entrant, so this nudge is safe when MUs were actually processed.
            try { _migrationProcessor?.StopOfflineOrInvokeChangeStreams(); } catch { }

            MigrationJobContext.AddVerboseLog($"Before WaitForMigrationProcessorCompletionAsync");
            return await WaitForMigrationProcessorCompletionAsync(ctsToken);
        }

        private async Task<TaskResult> MigrateRUOptimizedJobCollectionsAsync(bool syncBack, CancellationToken ctsToken)
        {
            List<Task> resumeTokenTasks = new List<Task>();
            var totalUnits = MigrationJobContext.CurrentlyActiveJob!.MigrationUnitBasics.Count;
            _log.WriteLine($"Processing {totalUnits} migration units", LogType.Debug);
            _log.ShowInMonitor($"Processing {totalUnits} collection(s) (RU-optimized pipeline)...");

            int ruIndex = 0;
            foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
            {
                if (_migrationCancelled)
                    return TaskResult.Canceled;

                if (HandleControlPause())
                    return TaskResult.Canceled;

                MigrationJobContext.AddVerboseLog($"Before MigrateUnitEndToEndAsync for {mub.DatabaseName}.{mub.CollectionName}");

                var migrationUnit = MigrationJobContext.GetMigrationUnit(mub.Id);
                var result = await MigrateUnitEndToEndAsync(migrationUnit, syncBack, ctsToken, resumeTokenTasks);
                if (result != TaskResult.Success)
                    return result;

                ruIndex++;
                if (ruIndex == 1 || ruIndex % 10 == 0 || ruIndex == totalUnits)
                {
                    _log.ShowInMonitor($"Migration dispatch progress: {ruIndex}/{totalUnits} (last: {mub.DatabaseName}.{mub.CollectionName})");
                }

                MigrationJobContext.AddVerboseLog($"Before ShouldBreakMigrationLoop, last processed {mub.DatabaseName}.{mub.CollectionName}");

                if (ShouldBreakMigrationLoop())
                {
                    _log.WriteLine("Breaking loop: CS post-processing started and offline job completed", LogType.Debug);
                    break;
                }
            }

            _migrationProcessor?.MarkAllUnitsDispatched();

            // Same resume-fast-path nudge as the async-partitioning loop (see comment above).
            try { _migrationProcessor?.StopOfflineOrInvokeChangeStreams(); } catch { }

            MigrationJobContext.AddVerboseLog($"Before WaitForMigrationProcessorCompletionAsync");
            return await WaitForMigrationProcessorCompletionAsync(ctsToken);
        }

        private async Task<TaskResult> MigrateUnitEndToEndAsync(
            MigrationUnit migrationUnit,
            bool syncBack,
            CancellationToken ctsToken,
            List<Task> resumeTokenTasks)
        {
            MigrationJobContext.AddVerboseLog($"MigrateUnitEndToEndAsync: mu.Id={migrationUnit.Id}");
            migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;

            if (!Helper.IsMigrationUnitValid(migrationUnit))
                return TaskResult.Success;

            if (migrationUnit.SourceStatus == CollectionStatus.IsView || migrationUnit.SourceStatus == CollectionStatus.IsTimeSeries)
                return TaskResult.Success;

            // Fast-path: if offline migration already completed for this unit,
            // skip source/target existence validation and queue directly for online processing.
            if (await TryQueueCompletedUnitForChangeStreamAsync(migrationUnit, "migrate-end-to-end"))
                return TaskResult.Success;

            var (exists, isCollection, isTimeSeries) = await ValidateSourceCollectionAsync(migrationUnit);

            if (isTimeSeries)
            {
                migrationUnit.SourceStatus = CollectionStatus.IsTimeSeries;
                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is a time-series collection. Time-series collections are not supported by the target (Cosmos DB for MongoDB / DocumentDB).", LogType.Warning);
                return TaskResult.Success;
            }

            if (!isCollection)
            {
                migrationUnit.SourceStatus = CollectionStatus.IsView;
                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Warning);
                return TaskResult.Success;
            }

            if (!exists)
            {
                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source. Marking skipped and continuing.", LogType.Warning);
                return await HandleMissingCollectionAsync(migrationUnit, ctsToken);
            }

            await ValidateTargetCollectionExistsAsync(migrationUnit);

            MigrationJobContext.AddVerboseLog($"Before ExecuteMigrationForUnitAsync {migrationUnit.Id}");
            return await ExecuteMigrationForUnitAsync(migrationUnit,  syncBack, ctsToken, resumeTokenTasks);
        }

        private async Task<(bool exists, bool isCollection, bool isTimeSeries)> ValidateSourceCollectionAsync(MigrationUnit migrationUnit)
        {
            MigrationJobContext.AddVerboseLog($"ValidateSourceCollectionAsync: mu={migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
            bool checkExist;
            if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.RUOptimizedCopy)
                checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
            else
                checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);

            if (!checkExist)
            {
                migrationUnit.SourceStatus = CollectionStatus.Unknown;
                return (false, false, false);
            }

            bool isCollection = true;
            bool isTimeSeries = false;
            try
            {
                var ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                isCollection = ret.IsCollection;
                isTimeSeries = ret.IsTimeSeries;
            }
            catch
            {
                isCollection = true;
            }

            if (isCollection)
                migrationUnit.SourceStatus = CollectionStatus.OK;

            return (checkExist, isCollection, isTimeSeries);
        }

        private async Task ValidateTargetCollectionExistsAsync(MigrationUnit migrationUnit)
        {
            MigrationJobContext.AddVerboseLog($"ValidateTargetCollectionExistsAsync: mu={migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
            if (MigrationJobContext.CurrentlyActiveJob!.IsSimulatedRun)
                return;

            if (string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return;

            MongoClient? targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
            var targetDatabaseName = migrationUnit.GetEffectiveTargetDatabaseName();
            var targetCollectionName = migrationUnit.GetEffectiveTargetCollectionName();

            bool checkExist;
            if (Helper.IsRU(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                checkExist = await MongoHelper.CheckRUCollectionExistsAsync(targetClient!, targetDatabaseName, targetCollectionName);
            else
                checkExist = await MongoHelper.CheckCollectionExistsAsync(targetClient!, targetDatabaseName, targetCollectionName);

            if (checkExist && !MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted)
            {
                var namespaceForLog = Log.FormatNamespaceForLog(
                    migrationUnit.DatabaseName,
                    migrationUnit.CollectionName,
                    targetDatabaseName,
                    targetCollectionName);

                _log.WriteLine($"{namespaceForLog} already exists on the target and is ready.", LogType.Debug);
            }
        }

        private async Task<TaskResult> ExecuteMigrationForUnitAsync(
            MigrationUnit migrationUnit,
            bool syncBack,
            CancellationToken ctsToken,
            List<Task> resumeTokenTasks)
        {
            MigrationJobContext.AddVerboseLog($"ExecuteMigrationForUnitAsync: mu={migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
            if (_migrationProcessor == null)
                return TaskResult.Abort;

            var createPartitionsResult = await CreatePartitionsAsync(migrationUnit, ctsToken);
            if (createPartitionsResult != TaskResult.Success)
                return createPartitionsResult;

            if (HandleControlPause())
                return TaskResult.Canceled;

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob!))
            {
                var setResumeResult = await SetCollectionResumeToken(migrationUnit,syncBack, ctsToken, resumeTokenTasks);
                if (setResumeResult != TaskResult.Success)
                    return setResumeResult;
            }

            if (HandleControlPause())
                return TaskResult.Canceled;

            // Apply any deferred moveCollection now so data copy starts on the correct shard.
            await EnsureMoveAppliedAsync(migrationUnit, ctsToken);

            MigrationJobContext.AddVerboseLog($"Before StartMigrationProcessorAsync {migrationUnit.Id}");
            return await StartMigrationProcessorAsync(migrationUnit);
        }


        private async Task<TaskResult> SetSyncBackResumeTokenAsync(CancellationToken ctsToken)
        {
            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob!))
                return TaskResult.Success;

            MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack = true;

            // Single sync-back anchor shared by the job and every MU, regardless of whether the
            // change stream is currently server- or collection-scoped. Later Collection<->Server
            // transitions use SyncBackChangeStreamStartedOn (not the original job StartedOn) to
            // replay reverse-direction changes from the moment sync-back was actually enabled.
            // SetChangeStreamStartedOn is set-when-empty, so existing anchors are preserved.
            var syncBackStartedOn = DateTime.UtcNow;
            MigrationJobContext.CurrentlyActiveJob.SetChangeStreamStartedOn(true, syncBackStartedOn);

            var units = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);

            // Seed every MU's sync-back anchor up front. Track freshly-seeded MUs so we set
            // resume tokens only for them below (existing MUs already have one).
            var freshlySeeded = new List<MigrationUnit>();
            foreach (var mub in units)
            {
                var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
                if (!mu.GetChangeStreamStartedOn(true).HasValue)
                {
                    mu.SetChangeStreamStartedOn(true, syncBackStartedOn);
                    mu.CSLastChecked = DateTime.MinValue;
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    freshlySeeded.Add(mu);
                }
            }

            List<Task> resumeTokenTasks = new List<Task>();

            // Setup server-level resume token if applicable
            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && units.Count > 0)
            {
                PartitionPrepContext ctx = new PartitionPrepContext();
                ctx.UseServerLevel = true;
                ctx.ServerLevelResumeTokenSet = false;

                await SetupServerLevelResumeTokenAsync(units[0], ctx, ctsToken, MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack);
                return TaskResult.Success;
            }

            foreach (var mu in freshlySeeded)
            {
                var setResumeResult = await SetCollectionResumeToken(mu, true, ctsToken, resumeTokenTasks);
                if (setResumeResult != TaskResult.Success)
                    return setResumeResult;
            }

            return TaskResult.Success;
        }


        private async Task<TaskResult> StartMigrationProcessorAsync(MigrationUnit migrationUnit)
        {
            MigrationJobContext.AddVerboseLog($"StartMigrationProcessorAsync: mu={migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
            if (string.IsNullOrWhiteSpace(MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob!.Id]) ||
                string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return TaskResult.Abort;

            _log.WriteLine($"Starting migration processor for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
            var result = await _migrationProcessor!.StartProcessAsync(
                migrationUnit.Id,
                MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id],
                MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            if (result == TaskResult.Success)
            {
                if (HandleControlPause())
                    return TaskResult.Canceled;

                LogMigrationSuccess(migrationUnit);
            }
            else
            {
                _log.WriteLine($"Migration processor returned {result} for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
            }

            return result;
        }

        private void LogMigrationSuccess(MigrationUnit migrationUnit)
        {
            if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.DumpAndRestore)
                _log.WriteLine($"Dump processor completed successfully for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
            else
                _log.WriteLine($"Migration processor completed successfully for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
        }

        private bool ShouldBreakMigrationLoop()
        {
            MigrationJobContext.AddVerboseLog($"In ShouldBreakMigrationLoop IsOnline:{ Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob!)},SyncBackEnabled: {MigrationJobContext.CurrentlyActiveJob.SyncBackEnabled}, CSPostProcessingStarted: {MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted}, Aggressive: {MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive},IsOfflineJobCompleted: { Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob)}");

            return Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob!) &&
                   MigrationJobContext.CurrentlyActiveJob.SyncBackEnabled &&
                   MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted &&
                   MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive &&
                   Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob);
        }

        private static bool IsSslFailure(Exception ex)
        {
            for (var cur = ex; cur != null; cur = cur.InnerException)
            {
                if (cur is System.Security.Authentication.AuthenticationException)
                    return true;
                if (cur.Message?.IndexOf("SSL", StringComparison.OrdinalIgnoreCase) >= 0)
                    return true;
            }
            return false;
        }

        private async Task<TaskResult> WaitForMigrationProcessorCompletionAsync(CancellationToken ctsToken)
        {
            MigrationJobContext.AddVerboseLog("Waiting for migration processor to complete all activities");
            
            // Get the web app base URL from class-level variable
            bool useKeepAlive = !string.IsNullOrEmpty(_webAppBaseUrl) && !_keepAliveDisabledForProcess;
            
            if (useKeepAlive)
            {
                _log.WriteLine($"Keep-alive mechanism enabled with base URL: {_webAppBaseUrl}", LogType.Debug);
            }
            else if (_keepAliveDisabledForProcess)
            {
                _log.WriteLine("Keep-alive disabled for this process due to repeated SSL failures; will remain off until app restart.", LogType.Info);
            }

            int counter = 0;
            while (_migrationProcessor != null && _migrationProcessor.ProcessRunning)
            {
                if (HandleControlPause())
                    return TaskResult.Canceled;

                
                // Call keep-alive API if configured
                if (useKeepAlive)
                {
                    counter++;
                    try
                    {
                        using (var httpClient = new HttpClient())
                        {
                            httpClient.Timeout = TimeSpan.FromSeconds(5);
                            var keepAliveUrl = $"{_webAppBaseUrl}/api/KeepAlive";
                            var response = await httpClient.GetAsync(keepAliveUrl, ctsToken);
                            
                            if (response.IsSuccessStatusCode)
                            {
                                var content = await response.Content.ReadAsStringAsync();
                                if (counter > 100000)
                                {
                                    _log.WriteLine($"Keep-alive response: {content}", LogType.Debug);
                                    counter = 0;
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        _log.WriteLine("Keep-alive cancelled - migration is stopping", LogType.Debug);
                        return TaskResult.Canceled;
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Keep-alive call failed. Details: {ex}", LogType.Debug);

                        if (IsSslFailure(ex))
                        {
                            int count = Interlocked.Increment(ref _keepAliveSslFailureCount);
                            if (count >= KeepAliveSslFailureThreshold && !_keepAliveDisabledForProcess)
                            {
                                _keepAliveDisabledForProcess = true;
                                useKeepAlive = false;
                                _log.WriteLine($"Keep-alive disabled for the rest of this process after {count} SSL failures. It will re-enable only on app restart.", LogType.Warning);
                            }
                        }
                    }
                }

                try
                {
                    await Task.Delay(10000, ctsToken);
                }
                catch (OperationCanceledException)
                {
                    _log.WriteLine("Wait cancelled - migration is stopping", LogType.Debug);
                    return TaskResult.Canceled;
                }
            }
            _activeJobId = string.Empty;
            _log.WriteLine("MigrateJobCollections completed - all activities finished", LogType.Debug);
            return TaskResult.Success;
        }



        private async Task<TaskResult> StartOnlineForJobCollections(CancellationToken ctsToken, MigrationProcessor processor, bool IsAggrssive, bool clearCache=false)
        {
            _log.WriteLine("StartOnlineForJobCollections started", LogType.Debug);
            try
            {                
                if (MigrationJobContext.CurrentlyActiveJob == null)
                    return TaskResult.FailedAfterRetries;

                var unitsForMigrate = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);

                _log.WriteLine($"Adding {unitsForMigrate.Count} collections to change stream queue", LogType.Debug);
                _log.ShowInMonitor($"Preparing change stream queue for {unitsForMigrate.Count} collection(s)...");

                int csIndex = 0;
                foreach (var migrationUnit in unitsForMigrate)
                {
                    csIndex++;
                    if (csIndex == 1 || csIndex % 10 == 0 || csIndex == unitsForMigrate.Count)
                    {
                        _log.ShowInMonitor($"Queuing change stream collection {csIndex}/{unitsForMigrate.Count}: {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
                    }

                    if (_migrationCancelled)
                        return TaskResult.Canceled;

                    if (HandleControlPause())
                        return TaskResult.Canceled;
 

                    if (Helper.IsMigrationUnitValid(migrationUnit)|| IsAggrssive)
                    {
                        // Fast-path for completed offline migration units: skip expensive source existence checks
                        // and queue for change stream (gated on pending blocking index builds).
                        if (await TryQueueCompletedUnitForChangeStreamAsync(migrationUnit, "start-online"))
                        {
                            if (clearCache)
                            {
                                try { MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(migrationUnit.Id); }
                                catch (Exception ex) { _log.WriteLine($"Error clearing cache for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}: {ex}", LogType.Error); }
                            }
                            continue;
                        }

                        bool valid;
                        if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                            valid = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                        else
                            valid = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);

                        // Immediate mode requires the unit to have completed offline copy. The fast-path above
                        // already handled completed units, so anything reaching here is not yet ready.
                        if (valid && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate)
                        {
                            _log.WriteLine($"Migration unit {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is not ready for immediate change stream", LogType.Debug);
                            valid = false;
                        }

                        if (valid && processor.AddCollectionToChangeStreamQueue(migrationUnit))
                        {
                            _log.WriteLine($"Added {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} to change stream queue", LogType.Debug);
                        }
                    }

                    if(clearCache)
                    {
                        try
                        {
                            //clear cache to free memory
                            MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(migrationUnit.Id);
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Error clearing cache for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}: {ex}", LogType.Error);
                        }
                    }
                }

                processor.RunChangeStreamProcessorForAllCollections();

                _log.WriteLine("Change stream processor started for all collections", LogType.Debug);
                _log.ShowInMonitor($"Change stream processor started for {unitsForMigrate.Count} collection(s).");

                return TaskResult.Success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in starting online migration. Details: {ex}", LogType.Error);
                return TaskResult.FailedAfterRetries;
            }
        }

        private bool HandleControlPause()
        {
            
            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine("Controlled pause detected, skipping processing..", LogType.Warning);
                _migrationProcessor?.StopChangeStreamProcessor();
                return true;
            }
            return false;
        }

        public async Task StartMigrationAsync(string namespacesToMigrate, JobType jobtype, bool trackChangeStreams)
        {
            try
            {
                

                if (!InitializeJob())
                    return;

                var (sourceConnectionString, targetConnectionString) = PrepareConnectionStrings();
                
                await PopulateMigrationUnitsAsync(namespacesToMigrate, sourceConnectionString);

                if (HandleControlPause())
                    return;

                FlagOfflineCompleteForSyncOnly();

                if (!await EnsureMongoToolsAvailableAsync())
                    return;

                if (HandleControlPause())
                    return;

                if (!await ExecutePrepareForMigrationAsync())
                    return;

                JobStarting = false;

                if (HandleControlPause())
                    return;

                await StartImmediateChangeStreamIfNeededAsync();

                if (HandleControlPause())
                    return;

                // RU jobs keep the original eager partitioning behavior.
                if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                {
                    if (!await ExecutePreparePartitionsAsync())
                        return;

                    if (HandleControlPause())
                        return;
                }

                if (HandleControlPause())
                    return;

                await ExecuteComparisonIfNeededAsync();

                if (HandleControlPause())
                    return;

                await ExecuteMigrationAsync(_syncBack);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Fatal error in StartMigrationAsync: {ex}", LogType.Error);
                StopMigration();
            }
        }

        private bool InitializeJob()
        {
            JobStarting = true;
            if (string.IsNullOrWhiteSpace(MigrationJobContext.CurrentlyActiveJob.Id))
            {
                StopMigration();
                return false;
            }

            // Stop and Clear existing percentage timer
            PercentageUpdater.StopPercentageTimer();

            // StopMigration handles cleanup of previous job
            StopMigration();
            ProcessRunning = true;
            
            // Reset all static state and kill leftover processes
            MigrationJobContext.ResetJobState();
            
            _activeJobId = MigrationJobContext.CurrentlyActiveJob.Id;
            Console.WriteLine($"_activeJobId: {_activeJobId}");
            
            // Reset WorkerPoolCoordinator for the new job
            WorkerPoolCoordinator.Reset(_activeJobId);
            
            MigrationJobContext.MigrationUnitsCache = new ActiveMigrationUnitsCache();
                
            InitializeLogging();
            LoadConfig();
            
            _migrationCancelled = false;
            MigrationJobContext.ResetControlledPause();
            _cts = new CancellationTokenSource();
            
            if (MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics == null)
            {
                MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics = new List<MigrationUnitBasic>();
            }

            // Kill all active mongodump and mongorestore processes
            MigrationJobContext.KillAllMigrationProcesses();

            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            return true;
        }

        private void InitializeLogging()
        {
            string logfile = _log.Init(MigrationJobContext.CurrentlyActiveJob.Id);
            if (logfile != MigrationJobContext.CurrentlyActiveJob.Id)
            {
                _log.WriteLine($"Error in reading log. Orginal log backed up as {logfile}", LogType.Error);
            }
            _log.WriteLine($"Processing Job {MigrationJobContext.CurrentlyActiveJob.Id} (started on {MigrationJobContext.CurrentlyActiveJob.StartedOn} (UTC)) - JobType: {MigrationJobContext.CurrentlyActiveJob.JobType} ", LogType.Warning);
            _log.SetJob(MigrationJobContext.CurrentlyActiveJob);
            _log.WriteLine($"Working folder is {Environment.GetEnvironmentVariable("ResourceDrive")}");
            MigrationJobContext.InitializeLog(_log);
        }

        private (string sourceConnectionString, string targetConnectionString) PrepareConnectionStrings()
        {
            MigrationJobContext.AddVerboseLog("PrepareConnectionStrings: encoding connection strings");
            var sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(
                MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
            var targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(
                MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            targetConnectionString = Helper.UpdateAppName(targetConnectionString, 
                $"MSFTMongoWebMigration-{Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob)}-" + MigrationJobContext.CurrentlyActiveJob.Id);

            // Store the modified connection strings back to MigrationJobContext so they're used throughout the migration
            MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id] = sourceConnectionString;
            MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id] = targetConnectionString;

            return (sourceConnectionString, targetConnectionString);
        }

        private async Task PopulateMigrationUnitsAsync(string namespacesToMigrate, string sourceConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"PopulateMigrationUnitsAsync: namespaces={namespacesToMigrate.Replace(",", ", ")}");

            var unitsToAdd = await Helper.PopulateJobCollectionsAsync(
                MigrationJobContext.CurrentlyActiveJob, 
                namespacesToMigrate, 
                sourceConnectionString);

            Helper.AddMigrationUnits(unitsToAdd, MigrationJobContext.CurrentlyActiveJob, _log);
        }

        /// <summary>
        /// For Sync Only (<see cref="CDCMode.SyncOnly"/>) jobs the offline dump/restore phase is skipped
        /// entirely: every migration unit is flagged as offline-complete and the change stream is anchored
        /// to the user-provided start time so change processing begins immediately when the job runs.
        /// Only applies to DumpAndRestore jobs.
        /// </summary>
        private void FlagOfflineCompleteForSyncOnly()
        {
            var job = MigrationJobContext.CurrentlyActiveJob;
            if (job == null || job.CDCMode != CDCMode.SyncOnly)
                return;

            // Anchor the change stream to the user-provided start time (fallback: now). Stored/consumed as UTC.
            var startTime = job.SyncOnlyChangeStreamStartTime.HasValue
                ? DateTime.SpecifyKind(job.SyncOnlyChangeStreamStartTime.Value, DateTimeKind.Utc)
                : DateTime.UtcNow;

            job.SetChangeStreamStartedOn(false, startTime);
            MigrationJobContext.SaveMigrationJob(job);

            _log.WriteLine(
                $"Job is running in Sync Only mode. The offline data copy (dump & restore) will be skipped and marked complete; change stream processing will start from {startTime:O} (UTC).",
                LogType.Warning);

            if (job.MigrationUnitBasics == null)
                return;

            foreach (var mub in job.MigrationUnitBasics)
            {
                MigrationJobContext.MutateMigrationUnit(mub.Id, m =>
                {
                    m.SourceStatus = CollectionStatus.OK;
                    m.DumpComplete = true;
                    m.DumpPercent = 100;
                    m.RestoreComplete = true;
                    m.RestorePercent = 100;
                    m.IndexBuildComplete = true;
                    m.IndexPercent = 100;

                    if (!m.BulkCopyStartedOn.HasValue || m.BulkCopyStartedOn.Value == DateTime.MinValue)
                        m.BulkCopyStartedOn = startTime;
                    if (!m.BulkCopyEndedOn.HasValue || m.BulkCopyEndedOn.Value == DateTime.MinValue)
                        m.BulkCopyEndedOn = DateTime.UtcNow;

                    m.SetChangeStreamStartedOn(false, startTime);
                    m.UpdateParentJob();
                }, updateParent: true);
            }
        }

        private async Task<bool> EnsureMongoToolsAvailableAsync()
        {
            MigrationJobContext.AddVerboseLog($"EnsureMongoToolsAvailableAsync: JobType={MigrationJobContext.CurrentlyActiveJob.JobType}");
            if (MigrationJobContext.CurrentlyActiveJob.JobType != JobType.DumpAndRestore)
                return true;

            if (!Helper.IsWindows())
            {
                _log.WriteLine("Ensuring MongoDB tools are available for DumpAndRestore job", LogType.Debug);
                if (!await Helper.ValidateMongoToolsAvailableAsync(_log))
                {
                    StopMigration();
                    return false;
                }
                _toolsLaunchFolder = string.Empty;
            }
            else
            {
                _log.WriteLine("Ensuring MongoDB tools are available for DumpAndRestore job", LogType.Debug);
                _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_log, _toolsDestinationFolder, _config!);
                if (string.IsNullOrEmpty(_toolsLaunchFolder))
                {
                    _log.WriteLine("MongoDB tools not available - stopping migration", LogType.Error);
                    StopMigration();
                    return false;
                }
                _log.WriteLine($"MongoDB tools ready at: {_toolsLaunchFolder}", LogType.Debug);
                _log.WriteLine($"Working directory  is {Helper.GetWorkingFolder()}", LogType.Debug);
                _toolsLaunchFolder = $"{_toolsLaunchFolder}\\";
            }
            return true;
        }

        private async Task<bool> ExecutePrepareForMigrationAsync()
        {
            _log.WriteLine("Starting ExecutePrepareForMigrationAsync", LogType.Debug);
            TaskResult result = await new RetryHelper().ExecuteTask(
                () => PrepareForMigration(),
                (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                    ex, attemptCount,
                    "Preperation step", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                _log.WriteLine($"PrepareForMigration returned {result} - stopping migration", LogType.Debug);
                StopMigration();
                return false;
            }
            return true;
        }

        private async Task StartImmediateChangeStreamIfNeededAsync()
        {
            MigrationJobContext.AddVerboseLog("StartImmediateChangeStreamIfNeededAsync invoked");

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return;


            if(MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode ==ChangeStreamMode.Delayed && !Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                return;

            //for delayed mode only, at the start no collections are valid, hence IsOfflineJobCompleted gives false positive
            if (!Helper.AnyValidCollection(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return;

            //for server-level change streams, wait for all collections to complete offline migration before starting CS
            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && !Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                return;


            _log.WriteLine("Starting online change stream processor in background.", LogType.Debug);
            _log.ShowInMonitor("Starting online change stream processor in background; warming up for 30 seconds before resuming migration loop...");
#pragma warning disable CS4014
            StartOnlineForJobCollections(_cts.Token, _migrationProcessor!, MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive, true);
#pragma warning restore CS4014
            await Task.Delay(30000);
            
        }

        private async Task<bool> ExecutePreparePartitionsAsync()
        {
            MigrationJobContext.AddVerboseLog("Starting PreparePartitionsAsync with retry logic");
            bool skipPartitioning = false;

            TaskResult result = await new RetryHelper().ExecuteTask(
                () => PreparePartitionsAsync(_cts.Token, skipPartitioning),
                (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                    ex, attemptCount,
                    "Partition step", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                _log.WriteLine($"PreparePartitionsAsync returned {result} - stopping migration", LogType.Debug);
                StopMigration();
                return false;
            }
            return true;
        }

        private async Task ExecuteComparisonIfNeededAsync()
        {
            if (MigrationJobContext.CurrentlyActiveJob.RunComparison)
            {
                _log.WriteLine("RunComparison flag is set - starting comparison", LogType.Debug);
                var compareHelper = new ComparisonHelper();
                _compare_cts = new CancellationTokenSource();
                await compareHelper.CompareRandomDocumentsAsync(_log, MigrationJobContext.CurrentlyActiveJob, _config!, _compare_cts.Token);
                compareHelper = null;
                MigrationJobContext.CurrentlyActiveJob.RunComparison = false;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                _log.WriteLine("Comparison completed - resuming migration", LogType.Debug);
            }
        }

        private async Task ExecuteMigrationAsync(bool syncBack)
        {
            _log.WriteLine("Starting MigrateJobCollections.", LogType.Debug);
            TaskResult result = await new RetryHelper().ExecuteTask(
                () => MigrateJobCollections(syncBack,_cts.Token),
                (ex, attemptCount, currentBackoff) => MigrateCollections_ExceptionHandler(
                    ex, attemptCount,
                    "Migrate collections", currentBackoff
                ),
                _log
            );

            if (HandleControlPause())
                return;

            if (result == TaskResult.Success || result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                _log.WriteLine($"MigrateJobCollections completed with result: {result}", LogType.Debug);
                if (result == TaskResult.Success)
                {
                    if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                    {
                        if (!MigrationJobContext.ControlledPauseRequested)
                        {
                            MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                            _log.WriteLine("Job marked as completed", LogType.Debug);
                        }
                        MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                    }
                }
                StopMigration();
            }
        }

        private void LoadConfig()
        {
            MigrationJobContext.AddVerboseLog("LoadConfig: loading migration settings");
            if (_config == null)
                _config = new MigrationSettings();
             _config.Load();
        }

        public void SyncBackToSource()
        {
            // Stop the forward change stream processor before starting syncback
            // to prevent echo/feedback loops where changes bounce between directions.
            _migrationProcessor?.StopChangeStreamProcessor();

            var dummySourceClient = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
            _migrationProcessor = new SyncBackProcessor(_log, dummySourceClient, _config!, this);
            MigrationJobContext.ActiveMigrationProcessor = _migrationProcessor;
            _syncBack = true;
            _migrationProcessor.ProcessRunning = true;
            JobStarting = false;
            var dummyUnit = new MigrationUnit(MigrationJobContext.CurrentlyActiveJob, "", "", new List<MigrationChunk>());

            //async  call
            _ = SetSyncBackResumeTokenAsync(_cts.Token);
            


            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            _migrationProcessor.StartProcessAsync(dummyUnit.Id, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]).GetAwaiter().GetResult();
        }

        public async Task SyncBackToSourceAsync(string sourceConnectionString, string targetConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"SyncBackToSource: sourceCS length={sourceConnectionString?.Length}, targetCS length={targetConnectionString?.Length}");
            JobStarting= true;

            if (!InitializeJob())
                return;

            if (string.IsNullOrWhiteSpace(MigrationJobContext.CurrentlyActiveJob.Id))
            {
                StopMigration(); //stop any existing
                return;
            }
            MigrationJobContext.ResetControlledPause();
            ProcessRunning = true;
            
            LoadConfig();

            if(_log==null)
                _log = new Log();
            
            string logfile = _log.Init(MigrationJobContext.CurrentlyActiveJob.Id);
            _log.SetJob(MigrationJobContext.CurrentlyActiveJob); // Set job reference for log level filtering


            MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack = true;
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            MigrationJobContext.MigrationUnitsCache = new ActiveMigrationUnitsCache();
            //_jobList.Save();

            if (_migrationProcessor != null)
                _migrationProcessor.StopProcessing();

            _migrationProcessor = null;
            MigrationJobContext.ActiveMigrationProcessor = null;
            var dummySourceClient = MongoClientFactory.Create(_log, sourceConnectionString);
            _migrationProcessor = new SyncBackProcessor(_log, dummySourceClient, _config!, this);
            MigrationJobContext.ActiveMigrationProcessor = _migrationProcessor;
            _syncBack = true;
            _migrationProcessor.ProcessRunning = true;
            JobStarting = false;
            var dummyUnit = new MigrationUnit(MigrationJobContext.CurrentlyActiveJob,"", "", new List<MigrationChunk>());

            MigrationJobContext.SaveMigrationUnit(dummyUnit,false);

            _cts = new CancellationTokenSource();
            //if run comparison is set by customer.
            if (MigrationJobContext.CurrentlyActiveJob.RunComparison)
            {               
                var compareHelper = new ComparisonHelper();
                compareHelper.CompareRandomDocumentsAsync(_log, MigrationJobContext.CurrentlyActiveJob, _config!, _cts.Token).GetAwaiter().GetResult();
                compareHelper = null;
                MigrationJobContext.CurrentlyActiveJob.RunComparison = false;

                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

                _log.WriteLine("Resuming SyncBack.");
            }

            //async  call
            _=SetSyncBackResumeTokenAsync(_cts.Token);

            _migrationProcessor.StartProcessAsync(dummyUnit.Id, sourceConnectionString, targetConnectionString).GetAwaiter().GetResult();

            MigrationJobContext.AddVerboseLog($"Before WaitForMigrationProcessorCompletionAsync");
            
            WaitForMigrationProcessorCompletionAsync(_cts.Token).GetAwaiter().GetResult();

        }

        private async Task<(List<MigrationChunk>, bool)> PartitionCollectionAsync(string databaseName, string collectionName, CancellationToken cts, MigrationUnit migrationUnit)
        {

            MigrationJobContext.AddVerboseLog($"PartitionCollectionAsync: db={databaseName}, coll={collectionName}");
            try
            {
                _log.WriteLine($"PartitionCollectionAsync started for {databaseName}.{collectionName}", LogType.Debug);
                cts.ThrowIfCancellationRequested();

                _log.WriteLine($"Validating prerequisites for {databaseName}.{collectionName}", LogType.Debug);
                ValidatePartitioningPrerequisites();

                _log.WriteLine($"Getting collection info for {databaseName}.{collectionName}", LogType.Debug);
                var (documentCount, totalCollectionSizeBytes, collection) = await GetCollectionInfoAsync(databaseName, collectionName, cts);
                _log.WriteLine($"Collection info retrieved - docCount: {documentCount}, sizeBytes: {totalCollectionSizeBytes}", LogType.Debug);

                // Phase 1: probe which _id BSON types actually exist in the source. We need
                // this BEFORE planning because a multi-type collection forces a $type $match
                // ahead of $sample, which pushes $sample off MongoDB's fast random-cursor
                // path -- the planner must treat that case the same as a user filter.
                _log.WriteLine($"Probing _id data types for {databaseName}.{collectionName}", LogType.Debug);
                var (dataTypes, forceSkipDataTypeFilter) = DetermineDataTypesForPartitioning(collection, migrationUnit, cts);
                migrationUnit.SkipDataTypeFilterForId = forceSkipDataTypeFilter;
                _log.WriteLine($"Data type probe complete - types: {dataTypes.Count}, forceSkipDataTypeFilter: {forceSkipDataTypeFilter}", LogType.Debug);

                _log.WriteLine($"Calculating partitioning strategy for {databaseName}.{collectionName}", LogType.Debug);
                var (totalChunks, minDocsInChunk) = CalculatePartitioningStrategy(
                    documentCount, databaseName, collectionName, migrationUnit, forceSkipDataTypeFilter);

                _log.WriteLine($"Partitioning strategy: totalChunks={totalChunks}, minDocsInChunk={minDocsInChunk}", LogType.Debug);

                List<MigrationChunk> migrationChunks;
                if (totalChunks > 1)
                {
                    _log.WriteLine($"Creating multiple chunks ({totalChunks}) for {databaseName}.{collectionName}", LogType.Debug);
                    migrationChunks = CreateMultipleChunks(collection, totalChunks, minDocsInChunk, migrationUnit, cts, databaseName, collectionName, dataTypes, forceSkipDataTypeFilter);
                    _log.WriteLine($"CreateMultipleChunks completed - returned {(migrationChunks == null ? "null" : migrationChunks.Count.ToString())} chunks", LogType.Debug);
                }
                else
                {
                    _log.WriteLine($"Creating single chunk for {databaseName}.{collectionName}", LogType.Debug);
                    migrationChunks = CreateSingleChunk(databaseName, collectionName);
                    _log.WriteLine($"CreateSingleChunk completed - returned {migrationChunks.Count} chunk(s)", LogType.Debug);
                }

                _log.WriteLine($"PartitionCollectionAsync completed - {migrationChunks.Count} chunks created for {databaseName}.{collectionName}", LogType.Debug);
                return (migrationChunks, true);
            }
            catch (OperationCanceledException)
            {

                _log.WriteLine($"PartitionCollectionAsync cancelled for {databaseName}.{collectionName}", LogType.Warning);
                return (new List<MigrationChunk>(), false);
            }
            catch (TimeoutException ex)
            {
                _log.WriteLine($"PartitionCollectionAsync timed out for {databaseName}.{collectionName}. {ex.Message}", LogType.Error);
                return (new List<MigrationChunk>(), false);
            }
            catch (Exception ex)
            {

                _log.WriteLine($"Error chunking collection {databaseName}.{collectionName}. Details: {ex}", LogType.Error);
                return (new List<MigrationChunk>(), false);
            }
        }

        private void ValidatePartitioningPrerequisites()
        {
            if (_sourceClient == null || _config == null || MigrationJobContext.CurrentlyActiveJob == null)
                throw new InvalidOperationException("Worker not initialized");
        }

        private async Task<(long documentCount, long totalCollectionSizeBytes, IMongoCollection<BsonDocument> collection)> GetCollectionInfoAsync(
            string databaseName, string collectionName, CancellationToken cancellationToken = default)
        {
            MigrationJobContext.AddVerboseLog($"GetCollectionInfoAsync: db={databaseName}, coll={collectionName}");

            var stats = await MongoHelper.GetCollectionStatsAsync(_sourceClient!, databaseName, collectionName, cancellationToken);
            long documentCount = stats.DocumentCount;
            long totalCollectionSizeBytes = stats.CollectionSizeBytes;

            _log.WriteLine($"{databaseName}.{collectionName} - docCount: {documentCount}, size: {totalCollectionSizeBytes} bytes", LogType.Debug);
            
            var database = _sourceClient!.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            return (documentCount, totalCollectionSizeBytes, collection);
        }

        private (int totalChunks, long minDocsInChunk) CalculatePartitioningStrategy(
            long documentCount, string databaseName, string collectionName, MigrationUnit migrationUnit, bool forceSkipDataTypeFilter)
        {
            MigrationJobContext.AddVerboseLog($"CalculatePartitioningStrategy: docCount={documentCount}, db={databaseName}, coll={collectionName}, forceSkipDataTypeFilter={forceSkipDataTypeFilter}");

            // Small collections under 1M docs: skip partitioning, process as a single chunk.
            if (documentCount < 1_000_000)
            {
                _log.WriteLine($"{databaseName}.{collectionName} has {documentCount} docs (< 1M). Skipping partitioning.", LogType.Debug);
                return (1, documentCount);
            }

            var userFilterDoc = MongoHelper.GetFilterDoc(migrationUnit.UserFilter);
            bool hasUserFilter = userFilterDoc != null && userFilterDoc.ElementCount > 0;
            // A $type predicate on _id (added when more than one _id type is present)
            // is prepended to the $sample pipeline by SamplePartitioner, which has the
            // same fast-path cost as a user filter -- treat both the same for cap math.
            bool hasMatchBeforeSample = hasUserFilter || !forceSkipDataTypeFilter;
            bool useSampleCommand = _config!.ObjectIdPartitioner == PartitionerType.UseSampleCommand;
            bool isMongoDriver = MigrationJobContext.CurrentlyActiveJob!.JobType != JobType.DumpAndRestore;

            // Step 1: total sub-range count for DumpAndRestore (segments = 1).
            //   Sample command  -> capped sample size (3K if a $match precedes $sample, 300K otherwise).
            //   Non-sample      -> doc-count-driven via GetMinDocsPerChunk (same chunk floor
            //                      the sample path bottoms out at, so all four partitioners
            //                      produce the same chunk count for a given docCount).
            long dumpSubRanges;
            if (useSampleCommand)
            {
                dumpSubRanges = SamplePartitioner.GetMaxSamples(hasMatchBeforeSample);
            }
            else
            {
                dumpSubRanges = Math.Max(1L, documentCount / SamplePartitioner.GetMinDocsPerChunk(documentCount));
            }

            // Step 2: derive driver/dump sub-ranges per job type.
            //   DumpAndRestore -> chunks == dumpSubRanges (segments = 1 downstream).
            //   MongoDriver    -> 10x sub-ranges, grouped into chunks of MaxSegments segments.
            //   Exception: useSampleCommand + ($match before $sample) caps $sample size at
            //              MaxSamples(withFilter) = 3,000 (top-k sort bound). We can't get more
            //              usable boundaries than that, so drop the driver multiplier when both
            //              are true.
            const int DriverSubRangeMultiplier = 10;
            bool capSampleAbsolute = useSampleCommand && hasMatchBeforeSample;
            long desiredSubRanges = (isMongoDriver && !capSampleAbsolute)
                ? dumpSubRanges * DriverSubRangeMultiplier
                : dumpSubRanges;

            // Step 3: cap $sample size to 5% of estimated doc count (random-cursor fast path).
            // With 10x oversampling, sampleSize = subRanges * 10, so subRanges <= docCount / 200.
            // Only applies when the partitioner actually issues a $sample.
            long subRangesActual = desiredSubRanges;
            if (useSampleCommand)
            {
                long sampleSizeCap = Math.Max(1L, documentCount / 20);          // 5% of doc count
                long subRangeCap = Math.Max(1L, sampleSizeCap / SamplePartitioner.SampleOversampleFactor); // /10x oversample
                subRangesActual = Math.Min(desiredSubRanges, subRangeCap);
            }

            int totalChunks;
            if (isMongoDriver)
            {
                int maxSegments = SamplePartitioner.GetMaxSegments();
                totalChunks = (int)Math.Max(1, Math.Ceiling((double)subRangesActual / maxSegments));
            }
            else
            {
                totalChunks = (int)Math.Max(1, subRangesActual);
            }

            // Universal chunk floor: docs/chunk >= MinDocsPerChunk (tiered).
            long perChunkFloor = SamplePartitioner.GetMinDocsPerChunk(documentCount);

            // MongoDriver: never drop segment count below MaxSegments just because chunks
            // are too small to hold MaxSegments segments. Instead, raise the per-chunk
            // floor so every chunk can saturate MaxSegments parallel segments at the
            // tier's MinDocsPerSegment minimum. Equivalent to:
            //     docs/chunk >= MaxSegments * MinDocsPerSegment(docCount).
            // DumpAndRestore keeps segments=1 by design, so this constraint doesn't apply.
            if (isMongoDriver)
            {
                long perChunkFloorForFullSegments =
                    (long)SamplePartitioner.GetMaxSegments() * SamplePartitioner.GetMinDocsPerSegment(documentCount);
                perChunkFloor = Math.Max(perChunkFloor, perChunkFloorForFullSegments);
            }

            long maxChunksByMinDocs = Math.Max(1L, documentCount / perChunkFloor);
            totalChunks = (int)Math.Min(totalChunks, maxChunksByMinDocs);

            long minDocsInChunk = documentCount / Math.Max(1, totalChunks);
            return (totalChunks, minDocsInChunk);
        }

        private List<MigrationChunk> CreateSingleChunk(string databaseName, string collectionName)
        {
            _log.WriteLine($"Single chunk (no partitioning) for {databaseName}.{collectionName}", LogType.Debug);
            var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.Other, false, false);
            var migrationChunks = new List<MigrationChunk> { chunk };

            if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.MongoDriver)
            {
                chunk.Segments = new List<Segment>
                {
                    new Segment { Gte = "", Lt = "", IsProcessed = false, Id = "1" }
                };
            }

            return migrationChunks;
        }

        private List<MigrationChunk> CreateMultipleChunks(
            IMongoCollection<BsonDocument> collection,
            int totalChunks,
            long minDocsInChunk,
            MigrationUnit migrationUnit,
            CancellationToken cts,
            string databaseName,
            string collectionName,
            List<DataType> dataTypes,
            bool forceSkipDataTypeFilter)
        {
            
            MigrationJobContext.AddVerboseLog($"Chunking {databaseName}.{collectionName}");
            _log.WriteLine($"CreateMultipleChunks started for {databaseName}.{collectionName} - totalChunks: {totalChunks}, minDocsInChunk: {minDocsInChunk}, dataTypes: {dataTypes.Count}, forceSkipDataTypeFilter: {forceSkipDataTypeFilter}", LogType.Debug);

            bool optimizeForObjectId = forceSkipDataTypeFilter && dataTypes[0] == DataType.ObjectId;
            if (optimizeForObjectId)
            {
                _log.WriteLine("ObjectId optimization enabled", LogType.Debug);
            }

            var migrationChunks = new List<MigrationChunk>();

            int dataTypeIndex = 0;
            foreach (var dataType in dataTypes)
            {
                dataTypeIndex++;
                _log.WriteLine($"Processing data type {dataTypeIndex}/{dataTypes.Count}: {dataType} for {databaseName}.{collectionName}", LogType.Debug);
                
                if (HandleControlPause())
                {
                    _log.WriteLine($"Control pause detected during partition creation for {databaseName}.{collectionName}", LogType.Warning);
                    return null;
                }

                try
                {
                    ProcessDataTypePartitions(collection, totalChunks, minDocsInChunk, dataType,
                        optimizeForObjectId, migrationUnit, cts, migrationChunks, forceSkipDataTypeFilter);
                    _log.WriteLine($"Completed processing data type {dataType} for {databaseName}.{collectionName} - current chunk count: {migrationChunks.Count}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error processing data type {dataType} for {databaseName}.{collectionName}. Details: {ex}", LogType.Error);
                    throw;
                }
            }

            _log.WriteLine($"CreateMultipleChunks completed for {databaseName}.{collectionName} - total chunks: {migrationChunks.Count}", LogType.Debug);
            return migrationChunks;
        }

        private List<DataType> DetermineDataTypes()
        {
            // Always probe every supported _id BSON type. PruneAbsentIdDataTypes will trim the
            // list to what's actually present, so there's no cost to keeping BinData in here.
            var dataTypes = new List<DataType>
            {
                DataType.Int, DataType.Int64, DataType.String, DataType.Object,
                DataType.Decimal128, DataType.Date, DataType.ObjectId, DataType.BinData,
                DataType.Double, DataType.Timestamp
            };
            _log.WriteLine($"Using all DataTypes for partitioning ({dataTypes.Count} types)", LogType.Debug);
            return dataTypes;
        }

        // Resolves the per-collection _id type list (user-pinned override or live probe)
        // and the "skip $type filter" flag, returning both for downstream planning and
        // partitioning. Runs once per collection, before CalculatePartitioningStrategy,
        // so the planner can correctly treat a multi-type collection as "has $match
        // before $sample" for cap purposes.
        private (List<DataType> dataTypes, bool forceSkipDataTypeFilter) DetermineDataTypesForPartitioning(
            IMongoCollection<BsonDocument> collection, MigrationUnit migrationUnit, CancellationToken cts)
        {
            List<DataType> dataTypes;
            if (migrationUnit.DataTypeForId.HasValue)
            {
                // User pinned the _id type; skip probing and use it directly.
                dataTypes = new List<DataType> { migrationUnit.DataTypeForId.Value };
                _log.WriteLine($"User-pinned _id data type {migrationUnit.DataTypeForId.Value} for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
            }
            else
            {
                _log.WriteLine($"Determining data types for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
                dataTypes = DetermineDataTypes();
                _log.WriteLine($"Data types determined - count: {dataTypes.Count}", LogType.Debug);

                // Probe the source so we only spend partitioning effort on _id types that actually exist.
                _log.ShowInMonitor($"Detecting _id data type(s) in use for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
                dataTypes = MongoHelper.PruneAbsentIdDataTypes(_log, collection, dataTypes, MongoHelper.GetFilterDoc(migrationUnit.UserFilter), cts);
            }

            // If exactly one _id type survives, downstream queries can skip the $type predicate entirely.
            bool forceSkipDataTypeFilter = dataTypes.Count == 1;
            return (dataTypes, forceSkipDataTypeFilter);
        }

        private void ProcessDataTypePartitions(
            IMongoCollection<BsonDocument> collection,
            int totalChunks,
            long minDocsInChunk,
            DataType dataType,
            bool optimizeForObjectId,
            MigrationUnit migrationUnit,
            CancellationToken cts,
            List<MigrationChunk> migrationChunks,
            bool forceSkipDataTypeFilter)
        {
           _log.WriteLine($"Calling SamplePartitioner.CreatePartitions for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType}, totalChunks: {totalChunks}", LogType.Debug);
            
            ChunkBoundaries? chunkBoundaries;
            long docCountByType;
            
            try
            {
                chunkBoundaries = SamplePartitioner.CreatePartitions(
                    _log, 
                    MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.DumpAndRestore, 
                    collection, 
                    totalChunks, 
                    dataType, 
                    minDocsInChunk, 
                    cts, 
                    migrationUnit!, 
                    optimizeForObjectId, 
                    _config!, 
                    forceSkipDataTypeFilter,
                    out docCountByType);
                
                _log.WriteLine($"SamplePartitioner.CreatePartitions completed for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType} - docCountByType: {docCountByType}, chunkBoundaries: {(chunkBoundaries == null ? "null" : "not null")}", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in SamplePartitioner.CreatePartitions for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType}. Details: {ex}", LogType.Error);
                throw;
            }

            if (docCountByType == 0 || chunkBoundaries == null)
            {
                _log.WriteLine($"No documents found for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType}", LogType.Debug);
                return;
            }

            if (chunkBoundaries.Boundaries.Count == 0)
            {
                _log.WriteLine($"Empty boundaries for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType} - creating empty boundary chunk", LogType.Debug);
                CreateEmptyBoundaryChunk(migrationChunks, dataType);
            }
            else
            {
                _log.WriteLine($"Creating segments from {chunkBoundaries.Boundaries.Count} boundaries for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType}", LogType.Debug);
#pragma warning disable CS8604
                CreateSegments(chunkBoundaries, migrationChunks, dataType, migrationUnit?.UserFilter);
#pragma warning restore CS8604
                _log.WriteLine($"Segments created for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} DataType: {dataType}", LogType.Debug);
            }

            // Post-partition action: set Lte on the last chunk/segments for this data type using snapshot max _id.
            MongoHelper.PopulateLteForLastChunk(
                _log,
                collection,
                migrationChunks,
                dataType,
                MongoHelper.GetFilterDoc(migrationUnit?.UserFilter),
                forceSkipDataTypeFilter);
        }

        private void CreateEmptyBoundaryChunk(List<MigrationChunk> migrationChunks, DataType dataType)
        {
            _log.WriteLine($"No boundaries created for DataType: {dataType}, creating single chunk", LogType.Debug);
            var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.Other, false, false);
            chunk.Id = migrationChunks.Count.ToString();
            migrationChunks.Add(chunk);

            if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.MongoDriver)
            {
                chunk.Segments = new List<Segment>
                {
                    new Segment { Gte = "", Lt = "", IsProcessed = false, Id = "1" }
                };
            }
        }
              

        private void CreateSegments(ChunkBoundaries chunkBoundaries, List<MigrationChunk> migrationChunks, DataType dataType, string userFilter)
        {
            _log.WriteLine($"CreateSegments started - creating segments for {chunkBoundaries.Boundaries.Count} boundaries", LogType.Debug);
            for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
            {               
                var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i, userFilter, dataType: dataType);
                var chunk = new MigrationChunk(startId, endId, dataType, false, false);
                chunk.Id = migrationChunks.Count.ToString();
                migrationChunks.Add(chunk);

                var boundary = chunkBoundaries.Boundaries[i];
                if (MigrationJobContext.CurrentlyActiveJob != null && MigrationJobContext.CurrentlyActiveJob.JobType == JobType.MongoDriver && (boundary.SegmentBoundaries == null || boundary.SegmentBoundaries.Count == 0))
                {
                    chunk.Segments ??= new List<Segment>();
                    chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false, Id = "1" });
                }

                if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.MongoDriver && boundary.SegmentBoundaries != null && boundary.SegmentBoundaries.Count > 0)
                {
                    int totalSegmentsToCreate = boundary.SegmentBoundaries.Count + 1;
                    _log.WriteLine($"Creating {totalSegmentsToCreate} segments for boundary {i}", LogType.Debug);

                    chunk.Segments ??= new List<Segment>();

                    string chunkStart = chunk.Gte ?? string.Empty;
                    string chunkEnd = chunk.Lt ?? string.Empty;

                    string firstSegmentEnd = SerializeBoundaryValue(boundary.SegmentBoundaries[0].StartId, dataType) ?? chunkEnd;
                    chunk.Segments.Add(new Segment
                    {
                        Gte = chunkStart,
                        Lt = firstSegmentEnd,
                        IsProcessed = false,
                        Id = "1"
                    });

                    for (int j = 0; j < boundary.SegmentBoundaries.Count; j++)
                    {
                        var segmentBoundary = boundary.SegmentBoundaries[j];
                        string segmentStartId = SerializeBoundaryValue(segmentBoundary.StartId, dataType) ?? string.Empty;
                        string segmentEndId = SerializeBoundaryValue(segmentBoundary.EndId, dataType) ?? chunkEnd;

                        chunk.Segments.Add(new Segment
                        {
                            Gte = segmentStartId,
                            Lt = segmentEndId,
                            IsProcessed = false,
                            Id = (j + 2).ToString()
                        });
                    }
                }
            }
            _log.WriteLine($"CreateSegments completed - {migrationChunks.Count} total chunks created", LogType.Debug);
        }
        private Tuple<string, string> GetStartEnd(bool isChunk, Boundary boundary, int totalBoundaries, int currentIndex, string userFilter, DataType dataType, string chunkLt = "", string chunkGte = "")
        {
            string startId;
            string endId;

            if (currentIndex == 0)
            {
                string min = string.Empty;
               
                var filterDoc = MongoHelper.GetFilterDoc(userFilter);
                var minValue = MongoHelper.GetIdRangeMin(filterDoc);

                string minId =string.Empty;

#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                try
                {
                    if (minValue != BsonMinKey.Value)
                        minId = minValue.AsBsonValue.ToString();
                }
                catch
                {
                    minId = string.Empty;
                }

                startId = isChunk ? minId : chunkGte;
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
                endId = SerializeBoundaryValue(boundary.EndId, dataType) ?? "";
            }
            else if (currentIndex == totalBoundaries - 1)
            {
                startId = SerializeBoundaryValue(boundary.StartId, dataType) ?? "";
                endId = isChunk ? "" : chunkLt;
            }
            else
            {
                startId = SerializeBoundaryValue(boundary.StartId, dataType) ?? "";
                endId = SerializeBoundaryValue(boundary.EndId, dataType) ?? "";
            }

            return Tuple.Create(startId, endId);
        }           

        private static string? SerializeBoundaryValue(BsonValue? value, DataType dataType)
        {
            if (value == null)
                return null;

            if (value.IsBsonNull)
                return "BsonNull";

            if (value.IsBsonMaxKey)
                return "BsonMaxKey";

            return dataType switch
            {
                DataType.BinData => value.ToJson(),
                DataType.Object => value.ToJson(),
                DataType.Double => value.AsDouble.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
                DataType.Timestamp => value.AsBsonTimestamp.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                _ => value.ToString()
            };
        }
 
    }
}
