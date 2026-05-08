using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

#if !LEGACY_MONGODB_DRIVER
namespace OnlineMongoMigrationProcessor
{
    public class CollectionLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        
        private MongoClient _changeStreamMongoClient;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _flushLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
        // [RemovingProbe] private static readonly TimeSpan StaleCursorProbeThreshold = TimeSpan.FromHours(48);

        public CollectionLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, targetClient, muCache, config, syncBack, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor: Constructor called, syncBack={syncBack}");
            // Initialize the change stream client based on syncBack mode
            var mj = MigrationJobContext.CurrentlyActiveJob;
            string connectionString = _syncBack
                ? MigrationJobContext.TargetConnectionString[mj.Id]
                : MigrationJobContext.SourceConnectionString[mj.Id];

            _changeStreamMongoClient = MongoClientFactory.Create(
                _log,
                connectionString,
                false,
                _syncBack ? null : _config.CACertContentsForSourceServer);
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog("CollectionLevelChangeStreamProcessor.ProcessChangeStreamsAsync: starting");
            WriteBasicLog();

            int index = 0;
            var sortedKeys = GetSortedCollectionKeys();

            LogProcessingConfiguration(sortedKeys.Count);

            long loops = 0;
            long emptyLoops = 0;
            DateTime lastResumeTokenCheck = DateTime.MinValue;

            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                var totalKeys = sortedKeys.Count;

                // Handle empty sortedKeys case
                if (totalKeys == 0)
                {
                    // Process any pending ResetChangeStream flags before waiting,
                    // otherwise collections stuck with ResetChangeStream=true are
                    // filtered out by GetSortedCollectionKeys and never processed.
                    await ProcessPendingChangeStreamResetsAsync();

                    var result = await HandleEmptyCollectionKeys(emptyLoops, lastResumeTokenCheck, token);
                    sortedKeys = result.sortedKeys;
                    emptyLoops = result.emptyLoops;
                    lastResumeTokenCheck = result.lastResumeTokenCheck;
                    continue;
                }

                // Reset empty loops counter when we have collections to process
                emptyLoops = ResetEmptyLoopsCounterIfNeeded(emptyLoops, totalKeys);

                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                    int seconds = CalculateBatchDuration(batchKeys);

                    var (tasks, collectionProcessed) = await PrepareCollectionTasks(batchKeys, seconds, token);

                    await ExecuteBatchTasks(tasks, collectionProcessed, seconds);

                    CleanupProcessedCollections(batchKeys);

                    index += _concurrentProcessors;

                    // Pause between batches to allow memory recovery and reduce CPU spikes
                    // Increased to 5000ms to address OOM issues and server CPU spikes
                    Thread.Sleep(5000);
                    
                }
                
                loops++;
                LogRoundCompletion(loops, totalKeys);

                // Process any pending ResetChangeStream flags so collections aren't stuck
                await ProcessPendingChangeStreamResetsAsync();

                // Initialize resume tokens for migration units without them, after 1st loop and  then every 4 loops
                if (loops==1||loops % 4 == 0)
                {
                    _ = InitializeResumeTokensForUnsetUnitsAsync(token);
                    lastResumeTokenCheck = DateTime.UtcNow;

                    //cleanup for aggressive CS mode
                    await AggressiveCSCleanupAsync();
                }

                

                index = 0;
                sortedKeys = GetSortedCollectionKeys();
            }
        }

        private async Task<(List<string> sortedKeys, long emptyLoops, DateTime lastResumeTokenCheck)> HandleEmptyCollectionKeys(long emptyLoops, DateTime lastResumeTokenCheck, CancellationToken token)
        {
            emptyLoops++;

            var loopDurationSec=Math.Max(60, _config.ChangeStreamBatchDurationMin);
            _log.ShowInMonitor($"{_syncBackPrefix}No collections with resume tokens found (empty loop #{emptyLoops}). Waiting {loopDurationSec} seconds before rechecking.");

            _ = InitializeResumeTokensForUnsetUnitsAsync(token);

            // Wait for loopDurationSec before checking again
            await Task.Delay(loopDurationSec * 1000, token);
            
            // Recheck for collections with resume tokens
            var sortedKeys = GetSortedCollectionKeys();
            return (sortedKeys, emptyLoops, lastResumeTokenCheck);
        }

        private long ResetEmptyLoopsCounterIfNeeded(long emptyLoops, int totalKeys)
        {
            if (emptyLoops > 0)
            {
                _log.WriteLine($"{_syncBackPrefix}Resuming processing with {totalKeys} collection(s) after {emptyLoops} empty loops", LogType.Info);
                return 0;
            }
            return emptyLoops;
        }      

        private async Task ProcessPendingChangeStreamResetsAsync()
        {
            foreach (var muId in _migrationUnitsToProcess.Keys)
            {
                var mu = MigrationJobContext.GetMigrationUnit(muId);
                if (mu == null || !mu.ResetChangeStream)
                    continue;

                try
                {
                    _log.WriteLine($"{_syncBackPrefix}Processing pending ResetChangeStream for {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
                    await MongoHelper.ResetCS(MigrationJobContext.CurrentlyActiveJob, mu, _syncBack);
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    _log.WriteLine($"{_syncBackPrefix}ResetChangeStream completed for {mu.DatabaseName}.{mu.CollectionName}. Collection will be re-included in next round.", LogType.Warning);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing ResetChangeStream for {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Error);
                }
            }
        }

        private void WriteBasicLog()
        {

            bool isVCore = (_syncBack ? MigrationJobContext.CurrentlyActiveJob.TargetEndpoint : MigrationJobContext.CurrentlyActiveJob.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            _log.WriteLine($"{_syncBackPrefix}Environment detection - IsVCore: {isVCore}, SyncBack: {_syncBack}", LogType.Debug);
        }

        private List<string> GetSortedCollectionKeys()
        {
            return _migrationUnitsToProcess
                .Where(kvp =>
                {
                    var mu = MigrationJobContext.GetMigrationUnit(kvp.Key);
                    if (mu == null)
                        return false;                    
                   
                    
                    // Check cursor timestamp based on syncBack mode
                    bool hasCursorTimestamp = _syncBack 
                        ? mu.SyncBackCursorUtcTimestamp > DateTime.MinValue 
                        : mu.CursorUtcTimestamp > DateTime.MinValue;

                    //for RUOptimizedCopy job type, also check for resume token if cursor timestamp is not set
                    if (!hasCursorTimestamp && MigrationJobContext.CurrentlyActiveJob.JobType==JobType.RUOptimizedCopy)
                    {
                        var muFull = MigrationJobContext.GetMigrationUnit(mu.Id);
                        hasCursorTimestamp =_syncBack
                            ? !string.IsNullOrEmpty(muFull.SyncBackResumeToken)
                            : !string.IsNullOrEmpty(muFull.ResumeToken);
                    }



                    bool isReady=false;
                    if (hasCursorTimestamp)
                    {
                        isReady = !mu.ResetChangeStream && mu.OpLogError == ChangeStreamError.None;
                    }

                    return isReady;
                })
                .OrderByDescending(kvp => kvp.Value) //value is CSNormalizedUpdatesInLastBatch
                .Select(kvp => kvp.Key)
                .ToList();
        }


        // [RemovingProbe] Probe eligibility check removed - always eligible
        // private async Task<TaskResult> EnsureProbeEligibilityAsync(MigrationUnit mu)
        // {
        //     if (mu == null || mu.ResetChangeStream)
        //     {
        //         return TaskResult.Abort;
        //     }
        //
        //     DateTime cursorTimestamp = _syncBack ? mu.SyncBackCursorUtcTimestamp : mu.CursorUtcTimestamp;
        //     string resumeToken = _syncBack ? mu.SyncBackResumeToken ?? string.Empty : mu.ResumeToken ?? string.Empty;
        //
        //     if(cursorTimestamp == DateTime.MinValue)
        //         return TaskResult.Abort;
        //
        //     if (!string.IsNullOrEmpty(resumeToken))
        //         return TaskResult.Success;
        //
        //     if (DateTime.UtcNow - cursorTimestamp.ToUniversalTime()< StaleCursorProbeThreshold)
        //         return TaskResult.Success;
        //
        //     var currentJob = MigrationJobContext.CurrentlyActiveJob;
        //     string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
        //     _log.WriteLine($"{_syncBackPrefix}Stale cursor with empty resume token for {collectionKey}; running probe before processing.", LogType.Debug);
        //
        //     bool probeFoundChange = await MongoHelper.TryInitializeResumeTokenWithIsolatedProbeAsync(
        //         _log,
        //         currentJob,
        //         mu,
        //         _syncBack,
        //         CancellationToken.None,
        //         _syncBack ? null : _config.CACertContentsForSourceServer);
        //
        //     if (!probeFoundChange)
        //     {
        //         _log.ShowInMonitor($"{_syncBackPrefix}Skipping {collectionKey} - stale cursor and probe found no new change.");
        //         return TaskResult.Abort;
        //     }
        //
        //     return TaskResult.Success;
        // }

        private void LogProcessingConfiguration(int collectionCount)
        {
            _log.WriteLine($"{_syncBackPrefix}Starting collection-level change stream processing for {collectionCount} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, collectionCount)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds.", LogType.Info);
        }

        private int CalculateBatchDuration(List<string> batchKeys)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.CalculateBatchDuration: batchKeys.Count={batchKeys.Count}");
            long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value);
            long totalUpdatesInBatch = _migrationUnitsToProcess
                .Where(kvp => batchKeys.Contains(kvp.Key))
                .Sum(kvp => kvp.Value);

            float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;
            
            int seconds = GetBatchDurationInSeconds(timeFactor);

            return seconds;
        }

        private async Task<(List<Task> tasks, List<string> collectionProcessed)> PrepareCollectionTasks(List<string> batchKeys, int seconds, CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.PrepareCollectionTasks: batchKeys.Count={batchKeys.Count}, seconds={seconds}");
            var tasks = new List<Task>();
            var collectionProcessed = new List<string>();

            foreach (var key in batchKeys)
            {
                if (_migrationUnitsToProcess.ContainsKey(key))
                {
                    var mu = MigrationJobContext.GetMigrationUnit(key);
                    var collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

                    // Check if resume token setup is still pending - if so, skip this collection
                    if (!await IsResumeTokenReady(collectionKey))
                    {
                        continue;
                    }

                    // Skip collections with change stream errors
                    if (mu.OpLogError != ChangeStreamError.None)
                    {
                        continue;
                    }

                    collectionProcessed.Add(collectionKey);
                    InitializeAccumulatedChangesTracker(collectionKey);

                    mu.CSLastBatchDurationSeconds = seconds;

                    var task = CreateCollectionProcessingTask(mu, collectionKey, seconds);
                    tasks.Add(task);
                }
            }

            return (tasks, collectionProcessed);
        }

        private async Task<bool> IsResumeTokenReady(string collectionKey)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.IsResumeTokenReady: collectionKey={collectionKey}");
            if (WaitForResumeTokenTaskDelegate != null)
            {
                var checkTask = WaitForResumeTokenTaskDelegate(collectionKey);
                if (!checkTask.IsCompleted)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Skipping collection {collectionKey} - resume token not yet ready");
                    return false;
                }
                await checkTask;
            }
            return true;
        }

       

        private Task CreateCollectionProcessingTask(MigrationUnit mu, string collectionKey, int seconds)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.CreateCollectionProcessingTask: collectionKey={collectionKey}, seconds={seconds}");
            return Task.Run(async () =>
            {
                try
                {
                    // [RemovingProbe] Probe check removed - always proceed with watch
                    await SetChangeStreamOptionandWatch(mu, true, seconds);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Unhandled exception in Task.Run for collection {collectionKey}. Details: {ex}", LogType.Error);
                    throw;
                }
            });
        }

        private async Task ExecuteBatchTasks(List<Task> tasks, List<string> collectionProcessed, int seconds)
        {
            _log.WriteLine($"{_syncBackPrefix}Processing change streams for {collectionProcessed.Count} collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Info);

            try
            {
                await Task.WhenAll(tasks);
                _log.WriteLine($"{_syncBackPrefix}Completed processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Task.WhenAll threw exception. Details: {ex}", LogType.Error);
                LogTaskStates(tasks);
                throw;
            }
        }

        private void LogTaskStates(List<Task> tasks)
        {
            for (int i = 0; i < tasks.Count; i++)
            {
                try
                {
                    var task = tasks[i];

                    if (task.IsFaulted)
                    {
                        var baseEx = task.Exception?.GetBaseException();
                        if (baseEx is TimeoutException)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Task {i} {baseEx?.Message}", LogType.Debug);
                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Task {i} FAULTED: {baseEx?.Message}", LogType.Debug);
                        }
                    }
                    else if (task.IsCanceled)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Task {i} CANCELED", LogType.Warning);
                    }
                }
                catch
                {
                    // Ignore exceptions during logging
                }
            }
        }

        private void CleanupProcessedCollections(List<string> batchKeys)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.CleanupProcessedCollections: batchKeys.Count={batchKeys.Count}");
            foreach (var key in batchKeys)
            {
                MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(key);
            }
        }

        private void LogRoundCompletion(long loops, int totalKeys)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.LogRoundCompletion: loops={loops}, totalKeys={totalKeys}");
            _log.WriteLine($"{_syncBackPrefix}Completed round {loops} of change stream processing for all {totalKeys} collection(s). Starting a new round; collections are sorted by their previous batch change counts.");
        }

        private bool HandleOpLogError(MigrationUnit mu, ChangeStreamError errorType = ChangeStreamError.ResumeTokenExpired)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.HandleOpLogError: muId={mu?.Id}, collection={mu?.DatabaseName}.{mu?.CollectionName}, errorType={errorType}");
            
            try
            {
                if (mu == null)
                    return false;

                mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                mu.OpLogError = errorType;
                string reason = errorType == ChangeStreamError.ResumeTokenExpired 
                    ? "Resume Token expired" 
                    : "Watch failed (cursor creation timed out)";
                _log.WriteLine($"{_syncBackPrefix}{reason} for {mu.DatabaseName}.{mu.CollectionName}. Collection will be excluded from change stream processing.", LogType.Warning);
                _log.ShowInMonitor($"{_syncBackPrefix}{reason} for {mu.DatabaseName}.{mu.CollectionName}. Collection will be excluded from change stream processing.");
                MigrationJobContext.SaveMigrationUnit(mu, true);
                return false;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error in HandleOpLogError for {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Error);
                StopProcessing = true;
                return false;
            }
        }

        private async Task InitializeResumeTokensForUnsetUnitsAsync(CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.InitializeResumeTokensForUnsetUnitsAsync: starting, unitsToProcess={_migrationUnitsToProcess.Count}");
            try
            {
                bool shownlog = false;
                foreach (var unitId in _migrationUnitsToProcess.Keys)
                {
                    if (token.IsCancellationRequested || ExecutionCancelled)
                        break;

                    var mu = MigrationJobContext.GetMigrationUnit(unitId);
                    if (mu == null)
                        continue;

                    // Check if both ResumeToken and OriginalResumeToken are not set
                    bool needToSetToken = false;
                    if (_syncBack)
                        needToSetToken = string.IsNullOrEmpty(mu.SyncBackResumeToken) && !mu.ResetChangeStream;
                    else
                        needToSetToken = string.IsNullOrEmpty(mu.ResumeToken) && !mu.ResetChangeStream;


                    if (needToSetToken)
                    {
                        if (shownlog == false)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Rechecking collections without a resume token; these collections were previously skipped.", LogType.Info);
                            shownlog = true;
                        }

                        MigrationJobContext.AddVerboseLog(($"{_syncBackPrefix}Setting resume token for {mu.DatabaseName}.{mu.CollectionName} (no tokens set)"));

                        try
                        {

                            await MongoHelper.SetChangeStreamResumeTokenAsync(
                                _log,
                                _syncBack ? _targetClient : _sourceClient,
                                MigrationJobContext.CurrentlyActiveJob,
                                mu,
                                30,
                                _syncBack,
                                token);
                        }
                        catch (Exception ex)
                        {
                            // do nothing
                        }

                        mu.CSLastChecked = DateTime.UtcNow;
                        mu.UpdateParentJob();
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                    }

                    //remove from cache
                    MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(mu.Id);
                }

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error in InitializeResumeTokensForUnsetUnitsAsync. Details: {ex}", LogType.Error);
            }
        }
        private async Task SetChangeStreamOptionandWatch(MigrationUnit mu, bool isCSProcessingRun = false, int seconds = 0)
        {

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}SetChangeStreamOptionandWatch started for {collectionKey} - Seconds: {seconds}", LogType.Debug);

            try
            {
                var (changeStreamCollection, targetCollection) = GetCollectionsForChangeStream(mu);
                
                try
                {
                    seconds = CalculateBatchDuration(seconds, collectionKey);
                    var (options, resolvedTargetCollection) = await ConfigureChangeStreamOptionsAsync(mu, seconds, collectionKey, changeStreamCollection, targetCollection);

                    await WatchCollection(mu, options, changeStreamCollection!, resolvedTargetCollection, seconds);
                }
				catch (OperationCanceledException ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}OperationCanceledException in SetChangeStreamOptionandWatch for {collectionKey}.Details: {ex}", LogType.Info);
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {collectionKey}. Details: {ex}", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Oplog is full. Error processing change stream for {collectionKey}. Details: {ex}");
                    //StopProcessing = true;
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {collectionKey}.", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {collectionKey}.");
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                StopProcessing = true;
            }
        }

        private (IMongoCollection<BsonDocument>? changeStreamCollection, IMongoCollection<BsonDocument>? targetCollection) GetCollectionsForChangeStream(MigrationUnit mu)
        {
            string sourceDatabaseName = mu.DatabaseName;
            string sourceCollectionName = mu.CollectionName;
            string targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            string targetCollectionName = mu.GetEffectiveTargetCollectionName();

            IMongoDatabase targetDb;
            IMongoDatabase changeStreamDb;
            IMongoCollection<BsonDocument>? targetCollection = null;
            IMongoCollection<BsonDocument>? changeStreamCollection = null;

            if (!_syncBack)
            {
                changeStreamDb = _changeStreamMongoClient.GetDatabase(sourceDatabaseName);
                changeStreamCollection = changeStreamDb.GetCollection<BsonDocument>(sourceCollectionName);

                if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    targetDb = _targetClient.GetDatabase(targetDatabaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(targetCollectionName);
                }
            }
            else
            {
                targetDb = _sourceClient.GetDatabase(sourceDatabaseName);
                targetCollection = targetDb.GetCollection<BsonDocument>(sourceCollectionName);
                
                changeStreamDb = _changeStreamMongoClient.GetDatabase(targetDatabaseName);
                changeStreamCollection = changeStreamDb.GetCollection<BsonDocument>(targetCollectionName);
            }

            return (changeStreamCollection, targetCollection);
        }

        private int CalculateBatchDuration(int seconds, string collectionKey)
        {
            if (seconds == 0)
                seconds = GetBatchDurationInSeconds(.5f);

            _log.WriteLine($"{_syncBackPrefix}ChangeStream timing - TotalDuration: {seconds}s for {collectionKey}", LogType.Debug);

            return seconds;
        }

        private async Task<(ChangeStreamOptions options, IMongoCollection<BsonDocument> targetCollection)> ConfigureChangeStreamOptionsAsync(MigrationUnit mu, int seconds, string collectionKey, IMongoCollection<BsonDocument> changeStreamCollection, IMongoCollection<BsonDocument>? targetCollection)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ConfigureChangeStreamOptionsAsync: collectionKey={collectionKey}, seconds={seconds}");
            int maxAwaitSeconds = Math.Max(5, (int)(seconds * 0.8));
            ChangeStreamOptions options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };

            var (timeStamp, resumeToken, version, startedOn) = GetResumeParameters(mu);

            await HandleAutoReplayIfNeeded(mu, collectionKey, targetCollection);

            options = DetermineResumeStrategy(mu, timeStamp, resumeToken, version, startedOn, maxAwaitSeconds, collectionKey);

            // In simulated runs, use change stream collection as a placeholder
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && targetCollection == null)
            {
                targetCollection = changeStreamCollection;
            }

            return (options, targetCollection!);
        }

        private (DateTime timeStamp, string resumeToken, string version, DateTime startedOn) GetResumeParameters(MigrationUnit mu)
        {
            DateTime timeStamp;
            string resumeToken;
            string? version;
            DateTime startedOn;

            if (!_syncBack)
            {
                timeStamp = mu.CursorUtcTimestamp;
                resumeToken = mu.ResumeToken ?? string.Empty;
                version = MigrationJobContext.CurrentlyActiveJob.SourceServerVersion;
                startedOn = mu.ChangeStreamStartedOn.HasValue ? mu.ChangeStreamStartedOn.Value : DateTime.MinValue;
            }
            else
            {
                timeStamp = mu.SyncBackCursorUtcTimestamp;
                resumeToken = mu.SyncBackResumeToken ?? string.Empty;
                version = "8";
                startedOn = mu.SyncBackChangeStreamStartedOn.HasValue ? mu.SyncBackChangeStreamStartedOn.Value : DateTime.MinValue;
            }

            return (timeStamp, resumeToken, version!, startedOn);
        }


        private async Task HandleAutoReplayIfNeeded(MigrationUnit mu, string collectionKey, IMongoCollection<BsonDocument>? targetCollection)
        {
            if (!mu.InitialDocumenReplayed && 
                !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && 
                MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive)
            {
                // If ResumeDocumentKey is empty the token came from a postBatchResumeToken
                // (no actual change detected). Nothing to replay — mark as done.
                var documentKey = mu.ResumeDocumentKey ?? mu.ResumeDocumentId;
                if (string.IsNullOrEmpty(documentKey))
                {
                    mu.InitialDocumenReplayed = true;
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    _log.WriteLine($"{_syncBackPrefix}No first change to replay for {collectionKey} (postBatchResumeToken), skipping auto-replay", LogType.Debug);
                    return;
                }

                _log.WriteLine($"{_syncBackPrefix}Auto-replaying first change for {collectionKey} - ResumeDocKey: {mu.ResumeDocumentKey}, Operation: {mu.ResumeTokenOperation}", LogType.Debug);
                
                if (targetCollection == null)
                {
                    var targetDb2 = _targetClient.GetDatabase(mu.GetEffectiveTargetDatabaseName());
                    targetCollection = targetDb2.GetCollection<BsonDocument>(mu.GetEffectiveTargetCollectionName());
                }
                
                var replaySourceClient = _syncBack ? _targetClient : _sourceClient;
                var replaySourceDb = replaySourceClient.GetDatabase(_syncBack ? mu.GetEffectiveTargetDatabaseName() : mu.DatabaseName);
                var replaySourceCollection = replaySourceDb.GetCollection<BsonDocument>(_syncBack ? mu.GetEffectiveTargetCollectionName() : mu.CollectionName);
                
                if (AutoReplayFirstChangeInResumeToken(documentKey, mu.ResumeTokenOperation, replaySourceCollection, targetCollection!, mu))
                {
                    mu.InitialDocumenReplayed = true;
                    mu.CSLastChangeUTCTime = mu.CursorUtcTimestamp;
                    mu.CSLastResumeTokenWithChange = _syncBack ? mu.SyncBackResumeToken : mu.ResumeToken;
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    _log.WriteLine($"{_syncBackPrefix}Auto-replay successful for {collectionKey}, proceeding with change stream", LogType.Debug);
                }
                else
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {collectionKey}. Skipping change stream processing for this collection.", LogType.Error);
                    throw new Exception($"Failed to replay the first change for {collectionKey}. Skipping change stream processing for this collection.");
                }
            }
            else
            {
                // In Aggressive or Simulated mode, auto-replay is skipped — the change stream
                // handles the first change itself.  Mark the flag so the UI doesn't show a
                // misleading "False".
                if (!mu.InitialDocumenReplayed)
                {
                    mu.InitialDocumenReplayed = true;
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    _log.WriteLine($"{_syncBackPrefix}Auto-replay not needed for {collectionKey} (IsSimulated={MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun}, ChangeStreamMode={MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode}), marking InitialDocumenReplayed=true", LogType.Debug);
                }
            }
        }

        private ChangeStreamOptions DetermineResumeStrategy(MigrationUnit mu, DateTime timeStamp, string resumeToken, string version, DateTime startedOn, int maxAwaitSeconds, string collectionKey)
        {
            ChangeStreamOptions options;

            if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && string.IsNullOrEmpty(resumeToken) && 
                !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack))
            {
                var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: StartAtOperationTime - Timestamp: {timeStamp} for {collectionKey}", LogType.Debug);
            }
            else if (!string.IsNullOrEmpty(resumeToken) && !mu.ResetChangeStream)
            {
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken), MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: ResumeAfter token for {collectionKey}", LogType.Debug);
            }
            else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3"))
            {
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: No resume (MongoDB 3.x) for {collectionKey}", LogType.Debug);
            }
            else if (startedOn > DateTime.MinValue && !version.StartsWith("3") && 
                     !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack))
            {
                var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(startedOn);
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: StartAtOperationTime from ChangeStreamStartedOn - StartedOn: {startedOn} for {collectionKey}", LogType.Debug);
                
            }
            else
            {
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
            }

            return options;
        }

        private async Task FlushPendingChangesAsync(MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection, AccumulatedChangesTracker accumulatedChangesInColl, bool isFinalFlush)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.FlushPendingChangesAsync: collection={mu.DatabaseName}.{mu.CollectionName}, totalChanges={accumulatedChangesInColl.TotalChangesCount}, isFinalFlush={isFinalFlush}");
            
            // Get or create a semaphore for this migration unit
            var flushLock = _flushLocks.GetOrAdd(mu.Id, _ => new SemaphoreSlim(1, 1));
            
            // Acquire the lock to ensure only one flush operation per migration unit at a time
            await flushLock.WaitAsync();
            try
            {
                // Flush accumulated changes - convert Dictionary.Values to List for BulkProcessChangesAsync
                await BulkProcessChangesAsync(
                    mu,
                    targetCollection,
                    insertEvents: accumulatedChangesInColl.DocsToBeInserted.Values.ToList(),
                    updateEvents: accumulatedChangesInColl.DocsToBeUpdated.Values.ToList(),
                    deleteEvents: accumulatedChangesInColl.DocsToBeDeleted.Values.ToList(),
                    accumulatedChangesInColl: accumulatedChangesInColl,
                    batchSize: 500);

                // Update resume token after successful flush.
                // Guard with TotalChangesCount > 0: after a Reset(false) the dicts are cleared
                // but LatestResumeToken/LatestTimestamp linger.  Without this guard a second
                // flush (e.g. from ProcessWatchFinallyAsync) would compare the stale tracker
                // timestamp against a mu timestamp that was already advanced by
                // postBatchResumeToken, causing a spurious "Timestamp mismatch" exception.
                if (!string.IsNullOrEmpty(accumulatedChangesInColl.LatestResumeToken) && accumulatedChangesInColl.TotalChangesCount > 0)
                {
                    var (currentTimestamp, currentResumeToken, _, _) = GetResumeParameters(mu);
                    string collectionNamespace = $"{mu.DatabaseName}.{mu.CollectionName}";
                    
                    // We don't allow going backwards in time
                    if (accumulatedChangesInColl.LatestTimestamp - currentTimestamp >= TimeSpan.FromSeconds(0))
                    {
                        SetResumeParameters(mu, accumulatedChangesInColl.LatestTimestamp, accumulatedChangesInColl.LatestResumeToken,_syncBack);
                        mu.CSLastResumeTokenWithChange = accumulatedChangesInColl.LatestResumeToken;
                        mu.CSLastChangeUTCTime = accumulatedChangesInColl.LatestTimestamp;
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                    }
                    else
                    {
                        _log.WriteLine($"Old Token:{currentResumeToken}, New Token:{accumulatedChangesInColl.LatestResumeToken} for {collectionNamespace}", LogType.Error);
                        throw new Exception($"{_syncBackPrefix} Timestamp mismatch Old Value: {currentTimestamp} is newer than New Value: {accumulatedChangesInColl.LatestTimestamp} for {collectionNamespace}");
                    }
                    
                    _resumeTokenCache[$"{targetCollection.CollectionNamespace}"] = accumulatedChangesInColl.LatestResumeToken;
                }

                // Clear collections to free memory
                accumulatedChangesInColl.Reset(isFinalFlush);
            }
            finally
            {
                // Always release the lock
                flushLock.Release();
            }
        }

        

        private async Task WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> changeStreamCollection, IMongoCollection<BsonDocument> targetCollection, int seconds)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}WatchCollection started for {collectionKey} - Duration: {seconds}s, ResumeToken: {(!string.IsNullOrEmpty(mu.ResumeToken) ? "SET" : "NOT SET")}", LogType.Debug);

            BsonDocument userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
                        
            AccumulatedChangesTracker accumulatedChangesInColl;
            InitializeAccumulatedChangesTracker(collectionKey);
            accumulatedChangesInColl = _accumulatedChangesPerCollection[collectionKey];

            //reset latency counters
            
            accumulatedChangesInColl.CSTotalReadDurationInMS = 0;
            accumulatedChangesInColl.CSTotaWriteDurationInMS = 0;
            accumulatedChangesInColl.Reset();


            string currentPos= mu.ResumeToken ;

            // creating the watch cursor
            System.Diagnostics.Stopwatch readStopwatch = new System.Diagnostics.Stopwatch();

            try
            {
                var pipelineArray = CreateChangeStreamPipeline();

                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Starting cursor creation for {collectionKey}");
                readStopwatch.Start();

                IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? cursor = null;
                bool cursorCreationTimedOut = false;
                try
                {
                    // 1. Create cursor with a dedicated 5-minute timeout (independent of batch duration).
                    //    The batch CTS is intentionally NOT linked here — cursor creation
                    //    must get the full 5 minutes even when the batch is shorter (e.g. 30s).
                    using var cursorCreationCts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
                    var cursorCreationSw = System.Diagnostics.Stopwatch.StartNew();
                    try
                    {
                        cursor = await CreateChangeStreamCursorAsync(
                            changeStreamCollection,
                            pipelineArray,
                            options,
                            cursorCreationCts.Token,
                            collectionKey
                        );
                    }
                    catch (OperationCanceledException) when (cursorCreationCts.IsCancellationRequested)
                    {
                        // Cursor creation exceeded 5-minute timeout
                        cursorCreationTimedOut = true;
                        throw;
                    }
                    finally
                    {
                        cursorCreationSw.Stop();
                    }

                    if (cursorCreationSw.Elapsed.TotalSeconds > seconds)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Cursor creation for {collectionKey} took {cursorCreationSw.Elapsed.TotalSeconds:F1}s, exceeding batch duration of {seconds}s", LogType.Warning);
                    }

                    if (cursor == null)
                    {
                        MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Cursor is null for {collectionKey}");
                        return;
                    }

                    MigrationJobContext.AddVerboseLog($"{_syncBackPrefix} Cursor created for {collectionKey} in {cursorCreationSw.Elapsed.TotalSeconds:F1}s. Starting processing...");

                    // 2. Process cursor with a fresh batch-duration CTS that starts NOW
                    //    (after cursor creation), so processing always gets the full batch time.
                    using var batchCts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));

                    await ProcessChangeStreamCursorAsync(
                        cursor,
                        mu,
                        changeStreamCollection,
                        targetCollection,
                        accumulatedChangesInColl,
                        batchCts.Token,
                        seconds,
                        userFilterDoc,
                        readStopwatch
                    );

                    MigrationJobContext.AddVerboseLog($"{_syncBackPrefix} Finished processing for {collectionKey}.");
                }
                catch(Exception ex) when (ex.Message.Contains("CollectionScan died due to position in capped collection being deleted"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Change stream position invalidated for {collectionKey} - oplog position was deleted. Will not be processed for Change stream.", LogType.Warning);
                    HandleOpLogError(mu); 
                }
                catch (Exception ex) when (ex.Message.Contains("Expired resume token or cursor")|| ex.Message.Contains("resume point may no longer be in the oplog"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Expired resume token or cursor for {collectionKey} - oplog position {currentPos} was deleted. Will not be processed for Change stream.", LogType.Warning);
                    HandleOpLogError(mu);
                }
                catch (OperationCanceledException) when (cursorCreationTimedOut)
                {
                    _log.WriteLine($"{_syncBackPrefix}Cursor creation timed out (5 min) for {collectionKey}. Marking as WatchFailed.", LogType.Warning);
                    HandleOpLogError(mu, ChangeStreamError.WatchFailed);
                }
                catch (OperationCanceledException)
                {
                    // Batch-duration CTS expired — normal end of batch
                    _log.WriteLine($"{_syncBackPrefix}Batch duration expired for {collectionKey}.", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Debug);
                }
                finally
                {
                    if (cursor != null)
                    {
                        try { cursor.Dispose(); } catch { /* best-effort */ }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Exception in WatchCollection for {changeStreamCollection!.CollectionNamespace}.Details: {ex}", LogType.Error);
                throw;
            }
            finally
            {
                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}WatchCollection finally block for {collectionKey}");

                readStopwatch.Stop();

                // Capture timestamps before flush resets the tracker
                DateTime firstChangeTs = accumulatedChangesInColl.EarliestTimestamp;
                DateTime lastChangeTs = accumulatedChangesInColl.LatestTimestamp;

                // Note: readStopwatch time is already accumulated in ProcessMongoDB3x/4xChangeStreamAsync
                // No need to accumulate here to avoid double-counting
                await ProcessWatchFinallyAsync(mu, changeStreamCollection, targetCollection, accumulatedChangesInColl, collectionKey,true);

                // Log this watch call after flush so mu.CSUpdatesInLastBatch has the correct total
                if (_config.EnableCSWatchLog)
                {
                    try
                    {
                        CSWatchLogHelper.InsertCSWatchLog(
                            _targetClient,
                            MigrationJobContext.AppId ?? string.Empty,
                            collectionKey,
                            MigrationJobContext.CurrentlyActiveJob?.Id ?? string.Empty,
                            currentPos ?? string.Empty,
                            mu.ResumeToken ?? string.Empty,
                            mu.CSUpdatesInLastBatch,
                            readStopwatch.ElapsedMilliseconds,
                            _syncBack,
                            firstChangeTs,
                            lastChangeTs);
                    }
                    catch { /* best-effort diagnostic logging */ }
                }
            }
        }

        private BsonDocument[] CreateChangeStreamPipeline()
        {
            List<BsonDocument> pipeline;
            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
            {
                pipeline = new List<BsonDocument>()
                {
                    new BsonDocument("$match", new BsonDocument("operationType",
                        new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" }))),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "operationType", 1 },
                        { "_id", 1 },
                        { "fullDocument", 1 },
                        { "ns", 1 },
                        { "documentKey", 1 }
                    })
                };
            }
            else
            {
                pipeline = new List<BsonDocument>();
            }

            return pipeline.ToArray();
        }

        private async Task<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>> CreateChangeStreamCursorAsync(
            IMongoCollection<BsonDocument> changeStreamCollection,
            BsonDocument[] pipelineArray,
            ChangeStreamOptions options,
            CancellationToken cancellationToken,
            string collectionKey)
        {

            return await Task.Run(() =>
            {
                MigrationJobContext.AddVerboseLog(($"Starting Watch() for {collectionKey}..."));
                try
                {
                    var cursor = changeStreamCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);
                    return cursor;
                }
                catch(Exception ex) when (ex is OperationCanceledException || ex is TimeoutException)
                {
                    _log.WriteLine($"Watch() cancelled for {collectionKey}.", LogType.Debug);
                    throw;
                }
                catch (Exception ex) when (ex.Message.Contains("CollectionScan died due to position in capped collection being deleted"))
                {
                    // Don't log here - let outer catch handle it to avoid double logging
                    throw;
                }
                catch (Exception ex) when (ex.Message.Contains("Expired resume token or cursor") || ex.Message.Contains("resume point may no longer be in the oplog"))
                {
                    // Don't log here - let outer catch handle it to avoid double logging
                    throw;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Exception in Watch() for {collectionKey}: {ex}", LogType.Error);
                    throw;
                }
            }, cancellationToken);
        }

        private async Task<bool> ProcessChangeStreamCursorAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            System.Diagnostics.Stopwatch readStopwatch)
        {

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            var sucess = false;
            // Note: cursor disposal is handled in ProcessMongoDB3x/4xChangeStreamAsync methods
            string lastProcessedToken = string.Empty;


            if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
            {
                sucess = await ProcessMongoDB3xChangeStreamAsync(cursor, mu, changeStreamCollection, targetCollection, accumulatedChangesInColl, cancellationToken, userFilterDoc, collectionKey, readStopwatch);
            }
            else
            {
                sucess = await ProcessMongoDB4xChangeStreamAsync(cursor, mu, changeStreamCollection, targetCollection, accumulatedChangesInColl, cancellationToken, seconds, userFilterDoc, collectionKey, readStopwatch);
            }

            return sucess;
        }

        private async Task<bool> ProcessMongoDB3xChangeStreamAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            BsonDocument userFilterDoc,
            string collectionKey,
            System.Diagnostics.Stopwatch readStopwatch)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ProcessMongoDB3xChangeStreamAsync: collectionKey={collectionKey}");

            long flushedCount = 0;
            using (cursor)
            {
                foreach (var change in cursor.ToEnumerable(cancellationToken))
                {
                    // Stop the read stopwatch immediately after getting the change from source
                    readStopwatch.Stop();
                    accumulatedChangesInColl.CSTotalReadDurationInMS += readStopwatch.ElapsedMilliseconds;
                    
                    if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                    {
                        //_log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {changeStreamCollection!.CollectionNamespace}", LogType.Info);
                        break; // Exit loop, let finally block handle cleanup
                    }

                    string lastProcessedToken = string.Empty;
                    _resumeTokenCache.TryGetValue($"{changeStreamCollection!.CollectionNamespace}", out string? token1);
                    lastProcessedToken = token1 ?? string.Empty;

                    if (lastProcessedToken == change.ResumeToken.ToJson())
                    {
                        _log.ShowInMonitor($"{_syncBackPrefix}Skipping already processed change for {changeStreamCollection!.CollectionNamespace}");

                        return true; // Skip processing if the event has already been processed
                    }

                    try
                    {
                        bool result = ProcessCursor(change, cursor, targetCollection, collectionKey, mu, accumulatedChangesInColl, userFilterDoc);
                        if (!result)
                            break; // Exit loop on error, let finally block handle cleanup
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}. Details: {ex}", LogType.Error);
                        break; // Exit loop on exception, let finally block handle cleanup
                    }

                    if((accumulatedChangesInColl.TotalChangesCount - flushedCount) > _config.ChangeStreamMaxDocsInBatch)
                    {
                        flushedCount = flushedCount + accumulatedChangesInColl.TotalChangesCount;
                        MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Flushing accumulated changes - Count: {accumulatedChangesInColl.TotalChangesCount} exceeds max: {_config.ChangeStreamMaxDocsInBatch} for {collectionKey}");
                        
                        try
                        {
                            await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, false);
                        }
                        catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                        {
                            _log.WriteLine($"{_syncBackPrefix}CRITICAL error during flush for {collectionKey}. Details: {ex}", LogType.Error);
                            StopJob($"CRITICAL error during flush. Details: {ex}");
                            throw; // Re-throw to stop processing
                        }

                    }

                    // Restart stopwatch for next read iteration
                    readStopwatch.Restart();
                }
                readStopwatch.Stop();
            } 

            return true;
        }       

        private async Task<bool> ProcessMongoDB4xChangeStreamAsync(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            string collectionKey,
            System.Diagnostics.Stopwatch readStopwatch)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ProcessMongoDB4xChangeStreamAsync: collectionKey={collectionKey}, seconds={seconds}");

            using (cursor)
            {
                try
                {
                    long flushedCount = 0;

                    // Iterate changes detected
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(cancellationToken);
                        if (!hasNext)
                        {
                            readStopwatch.Stop();
                            break; // Stream closed or no more data
                        }

                        // Stop the read stopwatch after reading from source is complete
                        // Only accumulate read time when we actually got data (hasNext == true)
                        readStopwatch.Stop();
                        accumulatedChangesInColl.CSTotalReadDurationInMS += readStopwatch.ElapsedMilliseconds;


                        foreach (var change in cursor.Current)
                        {

                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Change stream processing cancelled for {changeStreamCollection!.CollectionNamespace}");
                                break; // Exit inner loop, outer loop will also break
                            }

                            string lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{changeStreamCollection!.CollectionNamespace}", out string? token2);
                            lastProcessedToken = token2 ?? string.Empty;

                            if (lastProcessedToken == change.ResumeToken.ToJson() && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy)
                                return true; // Skip processing if the event has already been processed                            
                        

                        try
                        {
                            bool result = ProcessCursor(change, cursor, targetCollection, collectionKey, mu, accumulatedChangesInColl, userFilterDoc);
                            if (!result)
                                break; // Exit loop on error, let finally block cleanup
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}. Details: {ex}", LogType.Error);
                            break; // Exit loop on exception, let finally block handle cleanup
                        }                            // Check if we need to flush accumulated changes to prevent memory buildup
                            if ((accumulatedChangesInColl.TotalChangesCount - flushedCount) > _config.ChangeStreamMaxDocsInBatch)
                            {
                                flushedCount = flushedCount + accumulatedChangesInColl.TotalChangesCount;
                                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Flushing accumulated changes - Count: {accumulatedChangesInColl.TotalChangesCount} exceeds max: {_config.ChangeStreamMaxDocsInBatch} for {collectionKey}");
                                
                                try
                                {
                                    await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, false);
                                }
                                catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                                {
                                    _log.WriteLine($"{_syncBackPrefix}CRITICAL error during flush for {collectionKey}. Details: {ex}", LogType.Error);
                                    StopJob($"CRITICAL error during flush. Details: {ex}");
                                    throw; // Re-throw to stop processing
                                }
                            }

                        }
                        
                        // Restart the stopwatch for the next read iteration
                        readStopwatch.Restart();

                    }
                    
                    readStopwatch.Stop();
                }
                catch (OperationCanceledException)
                {
                    // Cancellation requested - exit quietly
                }
                finally
                {
                    readStopwatch.Stop();

                    // Flush pending changes FIRST, while mu still carries the timestamp
                    // from actual change events.  The postBatchResumeToken advancement
                    // below uses DateTime.UtcNow, which would be newer than the change-
                    // event timestamp and cause a "Timestamp mismatch" if flush ran after.
                    try
                    {
                        await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, false);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL error during final flush for {collectionKey}. Details: {ex}", LogType.Error);
                        StopJob($"CRITICAL error during final flush. Details: {ex}");
                        throw; // Re-throw to stop processing
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Error during final flush for {collectionKey}. Details: {ex}", LogType.Error);
                        // Don't throw non-critical errors from finally block
                    }

                    // Advance the resume token using the server's postBatchResumeToken.
                    // TotalEventCount is reset to 0 at the start of each WatchCollection call,
                    // so TotalEventCount == 0 here means no events were read in this batch.
                    // When events were processed, the flush already advanced mu.ResumeToken
                    // to the last change's token. We only use postBatchResumeToken for idle
                    // collections to keep CursorUtcTimestamp current.
                    if (accumulatedChangesInColl.TotalEventCount == 0)
                    {
                        try
                        {
                            var postBatchToken = cursor.GetResumeToken();
                            if (postBatchToken != null)
                            {
                                string tokenJson = postBatchToken.ToJson();
                                var (currentTimestamp, currentResumeToken, _, _) = GetResumeParameters(mu);
                                if (tokenJson != currentResumeToken)
                                {
                                    SetResumeParameters(mu, DateTime.UtcNow, tokenJson, _syncBack);
                                    MigrationJobContext.SaveMigrationUnit(mu, true);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Could not retrieve postBatchResumeToken for {collectionKey}: {ex.Message}", LogType.Debug);
                        }
                    }
                }
            }
            return true;
        }

        private async Task ProcessWatchFinallyAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            string collectionKey,
            bool isFinalFlush)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ProcessWatchFinallyAsync: collectionKey={collectionKey}, isFinalFlush={isFinalFlush}");
            try
            {

                long eventCounter = accumulatedChangesInColl.TotalEventCount;// TotalEventCount will get reset in FlushPendingChangesAsync
                if (eventCounter > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Processing batch for {changeStreamCollection.CollectionNamespace}:{eventCounter} events, {accumulatedChangesInColl.TotalChangesCount} changes (I:{accumulatedChangesInColl.DocsToBeInserted.Count}, U:{accumulatedChangesInColl.DocsToBeUpdated.Count}, D:{accumulatedChangesInColl.DocsToBeDeleted.Count})");
                    MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Final batch processing - Events: {eventCounter} Total: {accumulatedChangesInColl.TotalChangesCount}, Inserts: {accumulatedChangesInColl.DocsToBeInserted.Count}, Updates: {accumulatedChangesInColl.DocsToBeUpdated.Count}, Deletes: {accumulatedChangesInColl.DocsToBeDeleted.Count}");
                }

                try
                {
                    await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, isFinalFlush);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                {
                    _log.WriteLine($"{_syncBackPrefix}CRITICAL error during flush in ProcessWatchFinallyAsync for {collectionKey}. Details: {ex}", LogType.Error);
                    StopJob($"CRITICAL error in ProcessWatchFinallyAsync. Details: {ex}");
                    throw; // Re-throw to stop processing
                }

                mu.CSUpdatesInLastBatch = eventCounter; 
                mu.CSNormalizedUpdatesInLastBatch = (long)(eventCounter / (mu.CSLastBatchDurationSeconds > 0 ? mu.CSLastBatchDurationSeconds : 1));
                mu.CSLastChecked = System.DateTime.UtcNow;

                // Transfer latency metrics from accumulatedChangesInColl to mu
                if (eventCounter > 0)
                {
                    mu.CSAvgReadLatencyInMS = Math.Round((double)accumulatedChangesInColl.CSTotalReadDurationInMS / eventCounter,2);
                    mu.CSAvgWriteLatencyInMS = Math.Round((double)accumulatedChangesInColl.CSTotaWriteDurationInMS / eventCounter,2);

                    // Ensure CSLast* reflects the current resume position whenever
                    // events were processed.  FlushPendingChangesAsync sets these from
                    // the actual change-event token when TotalChangesCount > 0.
                    // Only fall back to mu's current resume parameters when the flush
                    // did NOT set them (e.g. all events had null FullDocument).
                    // This avoids overwriting the real change token with a
                    // postBatchResumeToken which has a different (shorter) format.
                    if (string.IsNullOrEmpty(mu.CSLastResumeTokenWithChange))
                    {
                        var (curTs, curToken, _, _) = GetResumeParameters(mu);
                        if (!string.IsNullOrEmpty(curToken))
                        {
                            mu.CSLastResumeTokenWithChange = curToken;
                            mu.CSLastChangeUTCTime = curTs;
                        }
                    }
                }

                MigrationJobContext.SaveMigrationUnit(mu,true);
                
                // Update the dictionary with the latest CSNormalizedUpdatesInLastBatch for accurate sorting
                if (_migrationUnitsToProcess.ContainsKey(mu.Id))
                {
                    _migrationUnitsToProcess[mu.Id] = mu.CSNormalizedUpdatesInLastBatch;
                }

                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Batch counters updated - CSUpdatesInLastBatch: {eventCounter}, CSNormalizedUpdatesInLastBatch: {mu.CSNormalizedUpdatesInLastBatch} for {collectionKey}");
                

                if (eventCounter > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Watch cycle completed for {changeStreamCollection.CollectionNamespace}: {eventCounter} events processed in batch. Avg Read Latency: {mu.CSAvgReadLatencyInMS} ms | Avg Write Latency: {mu.CSAvgWriteLatencyInMS} ms");
                }
            }
            catch (Exception ex)
            {
                _log.ShowInMonitor($"{_syncBackPrefix}ERROR processing batch for {changeStreamCollection.CollectionNamespace}. Details {ex}");
                _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {changeStreamCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                // On failure, resume token is NOT updated - we will resume from the last successful checkpoint
            }
        }

        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(string? documentKey, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}CollectionLevelChangeStreamProcessor.AutoReplayFirstChangeInResumeToken: documentKey={documentKey}, opType={opType}, collection={sourceCollection.CollectionNamespace}");
            if (documentKey == null || string.IsNullOrEmpty(documentKey))
            {
                _log.WriteLine($"{_syncBackPrefix}Auto replay is empty for {sourceCollection.CollectionNamespace}.", LogType.Debug);
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.ShowInMonitor($"{_syncBackPrefix}Auto replay for {opType} operation with document key {documentKey} in {sourceCollection.CollectionNamespace}.");
            }

            var bsonDoc = BsonDocument.Parse(documentKey);
            var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);
            var sourceRawCollection = sourceCollection.Database.GetCollection<RawBsonDocument>(sourceCollection.CollectionNamespace.CollectionName);
            var targetRawCollection = targetCollection.Database.GetCollection<RawBsonDocument>(targetCollection.CollectionNamespace.CollectionName);
            var renderedFilter = RenderFilterForRawCollection(filter);
            var rawFilter = new BsonDocumentFilterDefinition<RawBsonDocument>(renderedFilter);
            var result = sourceRawCollection.Find(rawFilter).FirstOrDefault(); // Retrieve the document for the resume token

            try
            {
                IncrementEventCounter(mu, opType);
                switch (opType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"{_syncBackPrefix}No document found for insert operation with document key {documentKey} in {sourceCollection.CollectionNamespace}. Skipping insert.", LogType.Warning);
                            return true; // Skip if no document found
                        }
                        targetRawCollection.InsertOne(result);
                        IncrementDocCounter(mu, opType);
                        return true;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Processing {opType} operation for {sourceCollection.CollectionNamespace} with document key {documentKey}. No document found on source, deleting it from target.", LogType.Info);
                            try
                            {
                                // Use DocumentKey-based filter for sharded collections
                                targetRawCollection.DeleteOne(rawFilter);
                                IncrementDocCounter(mu, ChangeStreamOperationType.Delete);
                            }
                            catch
                            { }
                            return true;
                        }
                        else
                        {
                            // Use DocumentKey-based filter for sharded collections with upsert
                            targetRawCollection.ReplaceOne(rawFilter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(mu, opType);
                            return true;
                        }
                    case ChangeStreamOperationType.Delete:
                        // Use DocumentKey-based filter for sharded collections
                        targetRawCollection.DeleteOne(rawFilter);
                        IncrementDocCounter(mu, opType);
                        return true;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {opType}", LogType.Error);
                        return false;
                }
            }
            catch (MongoException mex) when (opType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {opType} on {sourceCollection.CollectionNamespace} with document key {documentKey}. Details: {ex}", LogType.Error);
                return false; // Return false to indicate failure in processing
            }
        }

        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, MigrationUnit mu, AccumulatedChangesTracker accumulatedChangesInColl, BsonDocument userFilterDoc)
        {
           
            try
            {
                //check if user filter condition is met
                if (change.OperationType != ChangeStreamOperationType.Delete)
                {
                    if (userFilterDoc.Elements.Count() > 0
                        && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                        return true;
                }
                DateTime timeStamp = GetChangeTimestampUtc(change);


                bool shouldUpdateUI = Task.Run(() => ShowInMonitor(change, collNameSpace, timeStamp, accumulatedChangesInColl.TotalEventCount+1)).Result;


                ProcessChange(change, targetCollection, collNameSpace, accumulatedChangesInColl, MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun, mu);

                // NOTE: Resume token and timestamp are NOT persisted here anymore
                // They will only be persisted after successful batch write completion
                // This ensures we can recover from the last successful checkpoint

                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing cursor. Details: {ex}", LogType.Error);
                StopProcessing = true;
                return false;
            }
        }



        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, AccumulatedChangesTracker accumulatedChangesInColl, bool isWriteSimulated, MigrationUnit mu)
        {
            BsonValue idValue = BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace}. Change stream event is missing _id in DocumentKey.", LogType.Error);
                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate inserts even in simulation mode so counters get updated
                        if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                            accumulatedChangesInColl.AddInsert(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        IncrementEventCounter(mu, change.OperationType);
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            // Skip actual delete operation in simulation mode
                            if (!isWriteSimulated)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} with _id {idValue}. No document found on source.", LogType.Info);
                             }
                        }
                        else
                        {
                            // Accumulate updates even in simulation mode so counters get updated
                            accumulatedChangesInColl.AddUpdate(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate deletes even in simulation mode so counters get updated
                        accumulatedChangesInColl.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}", LogType.Error);
                        break;
                }

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details: {ex}", LogType.Error);
            }
        }

        private static BsonDocument RenderFilterForRawCollection(FilterDefinition<BsonDocument> filter)
        {
            var serializerRegistry = BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<BsonDocument>();
#if LEGACY_MONGODB_DRIVER
            return filter.Render(documentSerializer, serializerRegistry);
#else
            var renderArgs = new RenderArgs<BsonDocument>(documentSerializer, serializerRegistry);
            return filter.Render(renderArgs);
#endif
        }

    }
}
#endif // !LEGACY_MONGODB_DRIVER
