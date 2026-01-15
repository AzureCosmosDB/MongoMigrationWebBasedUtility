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
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using ZstdSharp.Unsafe;


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

        private CancellationTokenSource? _compare_cts;
        private CancellationTokenSource? _cts;

        private bool JobStarting = false;
        
        // Track resume token setup tasks per collection to enable per-collection waiting
        private Dictionary<string, Task> _resumeTokenTasksByCollection = new Dictionary<string, Task>();
       
        public MigrationWorker()
        {            
            _log = new Log();          
            MigrationJobContext.JobList.SetLog(_log);
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
            try
            {
                _activeJobId=string.Empty;
                _log.WriteLine("StopMigration called - cancelling all tokens and stopping processor", LogType.Debug);
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
                }
                
                ProcessRunning = false;
                MigrationJobContext.ResetControlledPause(); // Reset controlled pause flag
                MigrationJobContext.MigrationUnitsCache = null;
                
                // Clear the centralized cache when stopping
                MigrationJobContext.ClearCurrentlyActiveJobCache();

                // Stop percentage timer
                PercentageUpdater.StopPercentageTimer();

                _log.WriteLine("StopMigration completed - all resources released", LogType.Debug);
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
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
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
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
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
                if (MigrationJobContext.CurrentlyActiveJob.AppendMode)
                {
                    _log.WriteLine("Target collections will not be dropped, and no indexes will be modified or created. Only new data will be migrated.", LogType.Warning);
                }
                else
                {
                    if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                    {
                        _log.WriteLine("This migration job will not transfer the indexes to the target collections. Use the schema migration script at https://aka.ms/mongoruschemamigrationscript to create the indexes on the target collections.", LogType.Warning);
                    }
                    else
                    {
                        if (MigrationJobContext.CurrentlyActiveJob.SkipIndexes)
                        {
                            _log.WriteLine("No indexes will be created.", LogType.Warning);
                        }
                    }
                }
            }

            try
            {
                string version = MongoHelper.GetServerVersion(_sourceClient);
            }
            catch
            {
                _log.WriteLine("Failed to connect to source server. Please verify the connection string and network connectivity.", LogType.Error);
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
                //// Connect to the MongoDB server
                var client = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], true, _config.CACertContentsForSourceServer ?? string.Empty);
                var version = MongoHelper.GetServerVersion(client);
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
            
            // Set the delegate to wait for resume token tasks before processing collections
            _migrationProcessor.WaitForResumeTokenTaskDelegate = WaitForResumeTokenTask;
            _log.WriteLine("WaitForResumeTokenTaskDelegate set for migration processor", LogType.Debug);

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

            if (mu.MigrationChunks!=null && mu.MigrationChunks.Count>0)
            {
                _log.WriteLine($"Partitions already exist for {mu.DatabaseName}.{mu.CollectionName} - Count: {mu.MigrationChunks.Count}", LogType.Debug);
                return TaskResult.Success; //partitions already created
            }

            _log.WriteLine($"No existing partitions found, will create new ones for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
			List<MigrationChunk>? chunks = null;

            DateTime currrentTime = DateTime.UtcNow;
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
                mu.ChangeStreamStartedOn = currrentTime;
                _log.WriteLine($"Partitions created successfully - Chunks: {chunks!.Count}, ChangeStreamStartedOn: {currrentTime}", LogType.Debug);
                return TaskResult.Success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in CreatePartitionsAsync for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                throw;
            }
        }

        private async Task<TaskResult> SetCollectionResumeToken(MigrationUnit mu, bool syncBack, CancellationToken _cts, List<Task> resumeTokenTasks)
        {
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
                _log.WriteLine($"Calling SetChangeStreamResumeTokenAsync as async for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                var task = Task.Run(async () =>
                {
                    try
                    {
                        MongoClient mongoClient = new MongoClient();
                        if(syncBack)
                        {
                            mongoClient= MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, string.Empty);
                        }
                        else
                        {
                            if(_sourceClient!=null)
                            {
                                mongoClient = _sourceClient!;
                            }
                            else
                            {
                                mongoClient= MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, string.Empty);
                            }

                        }

                        await MongoHelper.SetChangeStreamResumeTokenAsync(_log, mongoClient, MigrationJobContext.CurrentlyActiveJob, mu, durationSeconds, syncBack, _cts,false);                            
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Error in SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                    }
                });
                    
                // Store task in dictionary by collection key
                _resumeTokenTasksByCollection[collectionKey] = task;
                // Also add to list for tracking
                resumeTokenTasks.Add(task);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error creating async task for SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
            }
            
           
            return TaskResult.Success;
        }

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

            int unitIndex = 0;
            foreach (var mu in unitsForPrep)
            {
                unitIndex++;
                _log.WriteLine($"Entered foreach loop - iteration {unitIndex}", LogType.Debug);
                
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

                    var result = await ProcessMigrationUnitAsync(mu, prepContext, _cts);
                    if (result != TaskResult.Success)
                    {
                        _log.WriteLine($"ProcessMigrationUnitAsync returned {result} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Error);
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

        private async Task<TaskResult> ProcessMigrationUnitAsync(MigrationUnit mu, PartitionPrepContext context, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"ProcessMigrationUnitAsync: mu={mu.DatabaseName}.{mu.CollectionName}");
            _log.WriteLine($"Starting ProcessMigrationUnitAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);

            if (mu.SourceStatus == CollectionStatus.IsView)
            {
                _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is a view - skipping", LogType.Debug);
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
            _log.WriteLine($"Completed ProcessMigrationUnitAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
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
                try
                {
                    var ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient, mu.DatabaseName, mu.CollectionName);
                    isCollection = checkExist && ret.Item2;
                    _log.WriteLine($"CheckIsCollectionAsync result: {isCollection} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error checking if {mu.DatabaseName}.{mu.CollectionName} is a collection. Details: {ex}", LogType.Warning);
                    isCollection = true;
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

            mu.EstimatedDocCount = coll.EstimatedDocumentCount();

            _ = Task.Run(() =>
            {
                long count = MongoHelper.GetActualDocumentCount(coll, mu);
                mu.ActualDocCount = count;
                MigrationJobContext.SaveMigrationUnit(mu, false);
            }, _cts);
        }


        private async Task SetupServerLevelResumeTokenAsync(MigrationUnit mu, PartitionPrepContext context, CancellationToken _cts, bool syncBack)
        {
            MigrationJobContext.AddVerboseLog($"SetupServerLevelResumeTokenAsync: mu={mu.DatabaseName}.{mu.CollectionName}, syncBack={syncBack}");

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) || !context.UseServerLevel)
                return;

            if (context.UseServerLevel && !context.ServerLevelResumeTokenSet)
            {
                if(syncBack)
                    _log.WriteLine($"SyncBack: Setting up server-level change stream resume token.");
                else
                    _log.WriteLine($"Setting up server-level change stream resume token.");


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

                _ = Task.Run(async () =>
                {
                    await MongoHelper.SetChangeStreamResumeTokenAsync(_log, mongoClient, MigrationJobContext.CurrentlyActiveJob, mu, 30, syncBack, _cts,false);
                });

                context.ServerLevelResumeTokenSet = true;
            }
        }

        private async Task<TaskResult> PrepareTargetCollectionAsync(MigrationUnit mu, PartitionPrepContext context, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"PrepareTargetCollectionAsync: mu={mu.DatabaseName}.{mu.CollectionName}");

            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun || MigrationJobContext.CurrentlyActiveJob.AppendMode || mu.TargetCreated)
                return TaskResult.Success;

            var database = _sourceClient!.GetDatabase(mu.DatabaseName);
            var collection = database.GetCollection<BsonDocument>(mu.CollectionName);
            
            if (string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return TaskResult.FailedAfterRetries;
            
            var result = await MongoHelper.DeleteAndCopyIndexesAsync(_log, mu, 
                MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], 
                collection, MigrationJobContext.CurrentlyActiveJob.SkipIndexes);

            if (_cts.IsCancellationRequested)
                return TaskResult.Canceled;

            if (!result)
                return TaskResult.Retry;

            MigrationJobContext.SaveMigrationUnit(mu, false);

            if (MigrationJobContext.CurrentlyActiveJob.SyncBackEnabled && 
                !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && 
                Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && 
                !context.CheckedCS)
            {
                _log.WriteLine("SyncBack: Checking if change stream is enabled on target");
                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, string.Empty, 
                    MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], mu, true);
                
                context.CheckedCS = true;
                
                if (!retValue.IsCSEnabled)
                    return TaskResult.Abort;
            }

            return TaskResult.Success;
        }

        private async Task<TaskResult> HandleMissingCollectionAsync(MigrationUnit mu, CancellationToken _cts)
        {
            MigrationJobContext.AddVerboseLog($"HandleMissingCollectionAsync: mu={mu.DatabaseName}.{mu.CollectionName}");

            if (_cts.IsCancellationRequested)
                return TaskResult.Canceled;

            if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && 
                !MigrationJobContext.CurrentlyActiveJob.AppendMode && 
                !mu.TargetCreated)
            {
                try
                {
                    var database = _sourceClient!.GetDatabase(mu.DatabaseName);
                    var collection = database.GetCollection<BsonDocument>(mu.CollectionName);
                    await MongoHelper.DeleteAndCopyIndexesAsync(_log, mu, 
                        MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], 
                        collection, MigrationJobContext.CurrentlyActiveJob.SkipIndexes);
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
            IsView
        }

        private async Task<TaskResult> MigrateJobCollections(bool syncBack,CancellationToken ctsToken)
        {
            _log.WriteLine("MigrateJobCollections started", LogType.Debug);

            if (MigrationJobContext.CurrentlyActiveJob == null)
                return TaskResult.FailedAfterRetries;

            List<Task> resumeTokenTasks = new List<Task>();
            _log.WriteLine($"Processing {MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics.Count} migration units", LogType.Debug);

            foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
            {
                if (_migrationCancelled)
                    return TaskResult.Canceled;

                if (HandleControlPause())
                    return TaskResult.Canceled;

                MigrationJobContext.AddVerboseLog($"Before ProcessMigrationUnitAsync for {mub.DatabaseName}.{mub.CollectionName}");

                var result = await ProcessMigrationUnitAsync(mub, syncBack, ctsToken, resumeTokenTasks);
                if (result != TaskResult.Success)
                    return result;

                MigrationJobContext.AddVerboseLog($"Before ShouldBreakMigrationLoop, last processed {mub.DatabaseName}.{mub.CollectionName}");

                if (ShouldBreakMigrationLoop())
                {
                    _log.WriteLine("Breaking loop: CS post-processing started and offline job completed", LogType.Debug);
                    break;
                }
            }

            MigrationJobContext.AddVerboseLog($"Before WaitForMigrationProcessorCompletionAsync");
            return await WaitForMigrationProcessorCompletionAsync(ctsToken);
        }

        private async Task<TaskResult> ProcessMigrationUnitAsync(
            MigrationUnitBasic mub,
            bool syncBack,
            CancellationToken ctsToken,
            List<Task> resumeTokenTasks)
        {
            MigrationJobContext.AddVerboseLog($"ProcessMigrationUnitAsync: mub.Id={mub.Id}");
            var migrationUnit = MigrationJobContext.GetMigrationUnit(mub.Id);
            migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;

            if (!Helper.IsMigrationUnitValid(migrationUnit))
                return TaskResult.Success;

            if (migrationUnit.SourceStatus == CollectionStatus.IsView)
                return TaskResult.Success;

            var (exists, isCollection) = await ValidateSourceCollectionAsync(migrationUnit);

            if (!isCollection)
            {
                migrationUnit.SourceStatus = CollectionStatus.IsView;
                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Warning);
                return TaskResult.Success;
            }

            if (!exists)
            {
                migrationUnit.SourceStatus = CollectionStatus.NotFound;
                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source. Created empty collection.", LogType.Warning);
                return TaskResult.Abort;
            }

            await ValidateTargetCollectionExistsAsync(migrationUnit);

            MigrationJobContext.AddVerboseLog($"Before ExecuteMigrationForUnitAsync {migrationUnit.Id}");
            return await ExecuteMigrationForUnitAsync(migrationUnit,  syncBack, ctsToken, resumeTokenTasks);
        }

        private async Task<(bool exists, bool isCollection)> ValidateSourceCollectionAsync(MigrationUnit migrationUnit)
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
                return (false, false);
            }

            bool isCollection = true;
            try
            {
                var ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                isCollection = ret.IsCollection;
            }
            catch
            {
                isCollection = true;
            }

            if (isCollection)
                migrationUnit.SourceStatus = CollectionStatus.OK;

            return (checkExist, isCollection);
        }

        private async Task ValidateTargetCollectionExistsAsync(MigrationUnit migrationUnit)
        {
            MigrationJobContext.AddVerboseLog($"ValidateTargetCollectionExistsAsync: mu={migrationUnit.DatabaseName}.{migrationUnit.CollectionName}");
            if (MigrationJobContext.CurrentlyActiveJob!.IsSimulatedRun)
                return;

            if (string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return;

            MongoClient? targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            bool checkExist;
            if (Helper.IsRU(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
            else
                checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);

            if (checkExist && !MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted)
            {
                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} already exists on the target and is ready.", LogType.Debug);
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

            MigrationJobContext.AddVerboseLog($"Before StartMigrationProcessorAsync {migrationUnit.Id}");
            return await StartMigrationProcessorAsync(migrationUnit);
        }


        private async Task<TaskResult> SetSyncBackResumeTokenAsync(CancellationToken ctsToken)
        {
            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob!))
            {

                MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack = true;
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server)
                {
                    if (MigrationJobContext.CurrentlyActiveJob.SyncBackChangeStreamStartedOn==null||MigrationJobContext.CurrentlyActiveJob.SyncBackChangeStreamStartedOn.HasValue == false)
                        MigrationJobContext.CurrentlyActiveJob.SyncBackChangeStreamStartedOn = DateTime.UtcNow;
                }
                else
                {
                    if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn==null|| MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn.HasValue == false)
                        MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn = DateTime.UtcNow;
                }

                List<Task> resumeTokenTasks = new List<Task>();

                var units = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);

                // Setup server-level resume token if applicable
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && units.Count > 0)
                {
                    PartitionPrepContext ctx= new PartitionPrepContext();
                    ctx.UseServerLevel = true;
                    ctx.ServerLevelResumeTokenSet = false;

                    await SetupServerLevelResumeTokenAsync(units[0], ctx, ctsToken, MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack);
                    return TaskResult.Success;
                }



                foreach (var mub in units)
                {
                    var mu= MigrationJobContext.GetMigrationUnit(mub.Id);

                    if (!mu.SyncBackChangeStreamStartedOn.HasValue)
                    {
                        mu.SyncBackChangeStreamStartedOn = DateTime.UtcNow;
                        mu.CSLastChecked = DateTime.MinValue;
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                        var setResumeResult = await SetCollectionResumeToken(mu, true, ctsToken, resumeTokenTasks);
                        if (setResumeResult != TaskResult.Success)
                            return setResumeResult;
                    }                    
                }
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

        private async Task<TaskResult> WaitForMigrationProcessorCompletionAsync(CancellationToken ctsToken)
        {
            MigrationJobContext.AddVerboseLog("Waiting for migration processor to complete all activities");
            
            // Get the web app base URL from class-level variable
            bool useKeepAlive = !string.IsNullOrEmpty(_webAppBaseUrl);
            
            if (useKeepAlive)
            {
                _log.WriteLine($"Keep-alive mechanism enabled with base URL: {_webAppBaseUrl}", LogType.Debug);
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

                foreach (var migrationUnit in unitsForMigrate)
                {
                    if (_migrationCancelled)
                        return TaskResult.Canceled;

                    if (HandleControlPause())
                        return TaskResult.Canceled;
 

                    if (Helper.IsMigrationUnitValid(migrationUnit)|| IsAggrssive)
                    {
                        bool valid;

                        if (MigrationJobContext.CurrentlyActiveJob.JobType== JobType.RUOptimizedCopy)
                            valid = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                        else
                            valid = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);

                        if (valid && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate)
                        {
                            if(! (migrationUnit.DumpComplete && migrationUnit.RestoreComplete))
                            {
                                _log.WriteLine($"Migration unit {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is not ready for immediate change stream", LogType.Debug);
                                valid = false;
                            }
                            
                        }                        

                        if (valid)
                        {
                            processor.AddCollectionToChangeStreamQueue(migrationUnit);
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
                //_migrationProcessor?.StopProcessing(true);
                //ProcessRunning = false;
                //JobStarting = false;
                //StopMigration();
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

                if (!await ExecutePreparePartitionsAsync())
                    return;

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
            _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} - JobType: {MigrationJobContext.CurrentlyActiveJob.JobType} started on {MigrationJobContext.CurrentlyActiveJob.StartedOn} (UTC)", LogType.Warning);
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
                $"MSFTMongoWebMigration{Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob)}-" + MigrationJobContext.CurrentlyActiveJob.Id);

            return (sourceConnectionString, targetConnectionString);
        }

        private async Task PopulateMigrationUnitsAsync(string namespacesToMigrate, string sourceConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"PopulateMigrationUnitsAsync: namespaces={namespacesToMigrate.Replace(",", ", ")}");

            var unitsToAdd = await Helper.PopulateJobCollectionsAsync(
                MigrationJobContext.CurrentlyActiveJob, 
                namespacesToMigrate, 
                sourceConnectionString, 
                MigrationJobContext.CurrentlyActiveJob.AllCollectionsUseObjectId);

            Helper.AddMigrationUnits(unitsToAdd, MigrationJobContext.CurrentlyActiveJob, _log);
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


            _log.WriteLine("Starting online change stream processor in background.", LogType.Debug);
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

            var dummySourceClient = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
            _migrationProcessor = new SyncBackProcessor(_log, dummySourceClient, _config!, this);
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
            _log.WriteLine($"SyncBack: {MigrationJobContext.CurrentlyActiveJob.Id} started on {MigrationJobContext.CurrentlyActiveJob.StartedOn} (UTC)");

            MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack = true;
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            MigrationJobContext.MigrationUnitsCache = new ActiveMigrationUnitsCache();
            //_jobList.Save();

            if (_migrationProcessor != null)
                _migrationProcessor.StopProcessing();

            _migrationProcessor = null;
            var dummySourceClient = MongoClientFactory.Create(_log, sourceConnectionString);
            _migrationProcessor = new SyncBackProcessor(_log, dummySourceClient, _config!, this);
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
                var (documentCount, totalCollectionSizeBytes, collection) = await GetCollectionInfoAsync(databaseName, collectionName);
                _log.WriteLine($"Collection info retrieved - docCount: {documentCount}, sizeBytes: {totalCollectionSizeBytes}", LogType.Debug);

                _log.WriteLine($"Calculating partitioning strategy for {databaseName}.{collectionName}", LogType.Debug);
                var (totalChunks, minDocsInChunk, targetChunkSizeBytes) = CalculatePartitioningStrategy(
                    documentCount, totalCollectionSizeBytes, databaseName, collectionName);

                _log.WriteLine($"Partitioning strategy: totalChunks={totalChunks}, minDocsInChunk={minDocsInChunk}, chunkSizeBytes={targetChunkSizeBytes}", LogType.Debug);

                List<MigrationChunk> migrationChunks;
                if (totalChunks > 1)
                {
                    _log.WriteLine($"Creating multiple chunks ({totalChunks}) for {databaseName}.{collectionName}", LogType.Debug);
                    migrationChunks = CreateMultipleChunks(collection, totalChunks, minDocsInChunk, migrationUnit, cts, databaseName, collectionName);
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
            string databaseName, string collectionName)
        {
            MigrationJobContext.AddVerboseLog($"GetCollectionInfoAsync: db={databaseName}, coll={collectionName}");

            var stats = await MongoHelper.GetCollectionStatsAsync(_sourceClient!, databaseName, collectionName);
            long documentCount = stats.DocumentCount;
            long totalCollectionSizeBytes = stats.CollectionSizeBytes;

            _log.WriteLine($"{databaseName}.{collectionName} - docCount: {documentCount}, size: {totalCollectionSizeBytes} bytes", LogType.Debug);
            
            var database = _sourceClient!.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            return (documentCount, totalCollectionSizeBytes, collection);
        }

        private (int totalChunks, long minDocsInChunk, long targetChunkSizeBytes) CalculatePartitioningStrategy(
            long documentCount, long totalCollectionSizeBytes, string databaseName, string collectionName)
        {
            MigrationJobContext.AddVerboseLog($"CalculatePartitioningStrategy: docCount={documentCount}, totalSizeBytes={totalCollectionSizeBytes}, db={databaseName}, coll={collectionName}");

            long targetChunkSizeBytes = _config!.ChunkSizeInMb * 1024 * 1024;
            var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);

            int totalChunks;
            long minDocsInChunk;

            if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.DumpAndRestore)
            {
                totalChunks = totalChunksBySize;
                minDocsInChunk = documentCount / (totalChunks == 0 ? 1 : totalChunks);
                _log.WriteLine($"{databaseName}.{collectionName} storage size: {totalCollectionSizeBytes}", LogType.Debug);
            }
            else
            {
                _log.WriteLine($"{databaseName}.{collectionName} estimated document count: {documentCount}", LogType.Debug);
                totalChunks = (int)Math.Min(SamplePartitioner.MaxSamples / SamplePartitioner.MaxSegments, 
                    documentCount / (SamplePartitioner.MaxSamples == 0 ? 1 : SamplePartitioner.MaxSamples));
                totalChunks = Math.Max(1, totalChunks);
                totalChunks = Math.Max(totalChunks, totalChunksBySize);
                totalChunks = Math.Min(totalChunks, SamplePartitioner.MaxSamples);
                minDocsInChunk = documentCount / (totalChunks == 0 ? 1 : totalChunks);
            }

            return (totalChunks, minDocsInChunk, targetChunkSizeBytes);
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
            string collectionName)
        {
            
            MigrationJobContext.AddVerboseLog($"Chunking {databaseName}.{collectionName}");
            _log.WriteLine($"CreateMultipleChunks started for {databaseName}.{collectionName} - totalChunks: {totalChunks}, minDocsInChunk: {minDocsInChunk}", LogType.Debug);

            _log.WriteLine($"Determining data types for {databaseName}.{collectionName}", LogType.Debug);
            var (dataTypes, optimizeForObjectId) = DetermineDataTypes(migrationUnit);
            _log.WriteLine($"Data types determined - count: {dataTypes.Count}, optimizeForObjectId: {optimizeForObjectId}", LogType.Debug);
            
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
                        optimizeForObjectId, migrationUnit, cts, migrationChunks);
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

        private (List<DataType> dataTypes, bool optimizeForObjectId) DetermineDataTypes(MigrationUnit migrationUnit)
        {
            bool optimizeForObjectId = false;
            List<DataType> dataTypes;

            if (migrationUnit?.DataTypeFor_Id.HasValue == true)
            {
                dataTypes = new List<DataType> { migrationUnit.DataTypeFor_Id.Value };
                _log.WriteLine($"Using specified DataType for _id: {migrationUnit.DataTypeFor_Id.Value}", LogType.Debug);

                if (migrationUnit.DataTypeFor_Id.Value == DataType.ObjectId)
                {
                    optimizeForObjectId = true;
                    _log.WriteLine("ObjectId optimization enabled", LogType.Debug);
                }
            }
            else
            {
                dataTypes = new List<DataType> 
                { 
                    DataType.Int, DataType.Int64, DataType.String, DataType.Object, 
                    DataType.Decimal128, DataType.Date, DataType.ObjectId 
                };
                _log.WriteLine($"Using all DataTypes for partitioning ({dataTypes.Count} types)", LogType.Debug);

                if (_config!.ReadBinary)
                {
                    dataTypes.Add(DataType.BinData);
                }
            }

            return (dataTypes, optimizeForObjectId);
        }

        private void ProcessDataTypePartitions(
            IMongoCollection<BsonDocument> collection,
            int totalChunks,
            long minDocsInChunk,
            DataType dataType,
            bool optimizeForObjectId,
            MigrationUnit migrationUnit,
            CancellationToken cts,
            List<MigrationChunk> migrationChunks)
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
                var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i, userFilter);
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
                    _log.WriteLine($"Creating {boundary.SegmentBoundaries.Count} segments for boundary {i}", LogType.Debug);
                    for (int j = 0; j < boundary.SegmentBoundaries.Count; j++)
                    {
                        var segment = boundary.SegmentBoundaries[j];
                        var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, boundary.SegmentBoundaries.Count, j, userFilter, chunk.Lt ?? string.Empty, chunk.Gte ?? string.Empty);

                        chunk.Segments ??= new List<Segment>();
                        chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false, Id = (j + 1).ToString() });
                    }
                }
            }
            _log.WriteLine($"CreateSegments completed - {migrationChunks.Count} total chunks created", LogType.Debug);
        }
        private Tuple<string, string> GetStartEnd(bool isChunk, Boundary boundary, int totalBoundaries, int currentIndex, string userFilter,string chunkLt = "", string chunkGte = "")
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
                endId = boundary.EndId?.ToString() ?? "";
            }
            else if (currentIndex == totalBoundaries - 1)
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = isChunk ? "" : chunkLt;
            }
            else
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = boundary.EndId?.ToString() ?? "";
            }

            return Tuple.Create(startId, endId);
        }           
 
    }
}
