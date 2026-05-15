using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

#if !LEGACY_MONGODB_DRIVER
namespace OnlineMongoMigrationProcessor
{
    public class ServerLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        // Server-level processors use MigrationJob properties directly for global resume tokens
        protected override bool UseResumeTokenCache => false;
        protected OrderedUniqueList<string> _uniqueCollectionKeys;
        private readonly ConcurrentDictionary<string, BsonDocument> _userFilterCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, IMongoCollection<BsonDocument>> _targetCollectionCache = new(StringComparer.OrdinalIgnoreCase);
        private HashSet<string> _touchedMuIdsInPreviousRound = new(StringComparer.OrdinalIgnoreCase);
        private bool _hasTouchedBaseline = false;
        private bool _namespaceFilterApplied = false;

        private sealed class ServerWatchState
        {
            public long Counter { get; set; }
            public long FlushedCount { get; set; }
            public string CollectionKey { get; set; } = string.Empty;
            public string LatestResumeToken { get; set; } = string.Empty;
            public DateTime LatestTimestamp { get; set; } = DateTime.MinValue;
            public ChangeStreamOperationType LatestOperationType { get; set; } = ChangeStreamOperationType.Insert;
            public string LatestDocumentKey { get; set; } = string.Empty;
            public string LatestCollectionKey { get; set; } = string.Empty;
            public bool HasFailures { get; set; }

            /// <summary>Accumulated per-MU batch stats, applied to MU objects only on the final flush.</summary>
            public Dictionary<string, MuBatchStats> BatchStats { get; } = new(StringComparer.OrdinalIgnoreCase);
        }

        private sealed class MuBatchStats
        {
            public long EventCount { get; set; }
            public string LatestResumeToken { get; set; } = string.Empty;
            public DateTime LatestTimestamp { get; set; } = DateTime.MinValue;
            public long TotalReadDurationMs { get; set; }
            public long TotalWriteDurationMs { get; set; }
            public ChangeStreamOperationType LatestOperationType { get; set; }
            public string LatestDocumentKey { get; set; } = string.Empty;
        }


        private bool _monitorAllCollections = false;
        public ServerLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, targetClient, muCache, config, syncBack, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog($"ServerLevelChangeStreamProcessor: Constructor called, syncBack={syncBack}");
            _uniqueCollectionKeys = new OrderedUniqueList<string>();
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog("ServerLevelChangeStreamProcessor.ProcessChangeStreamsAsync: starting");

            // RUOptimizedCopy jobs should not use server-level change streams
            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
            {
                _log.WriteLine($"{_syncBackPrefix}RUOptimizedCopy jobs do not support server-level change streams. This processor should not be used for such jobs.", LogType.Error);
                return;

            }

            ConfigureMonitoringMode();

            long loop = 0;
            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                try
                {
                    loop++;
                    await ProcessServerLevelRoundAsync(loop);

                }
                catch (OperationCanceledException)
                {
                    // Expected when batch times out, continue to next batch
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error in server-level change stream processing: {ex}", LogType.Error);
                    // Continue processing on errors
                }
            }
        }

        private void ConfigureMonitoringMode()
        {
            bool found = _migrationUnitsToProcess.TryGetValue("DUMMY.DUMMY", out var dummyMu);
            if (_migrationUnitsToProcess.Count == 1 && MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && found && dummyMu != null)
            {
                _monitorAllCollections = true;
                _log.WriteLine($"{_syncBackPrefix}Special mode: Starting server-level change stream processing for all collections.", LogType.Warning);
                return;
            }

            _log.WriteLine($"{_syncBackPrefix}Starting server-level change stream processing for {_migrationUnitsToProcess.Count} collection(s).");
        }

        private async Task ProcessServerLevelRoundAsync(long loop)
        {
            int seconds = GetBatchDurationInSeconds(1.0f); // Use full duration for server-level

            _log.WriteLine($"{_syncBackPrefix}Processing round {loop} for server - level change stream. Batch Duration {seconds} seconds");

            string resumeToken = GetResumeToken();

            if (!string.IsNullOrEmpty(resumeToken))
            {
                var touchedMuIdsInRound = await WatchServerLevelChangeStream();
                SetTouchedCollectionsCSLastChecked(touchedMuIdsInRound);
                await ResetCollectionsUntouchedSincePreviousRoundAsync(touchedMuIdsInRound);
            }
            else
            {
                _log.WriteLine($"{_syncBackPrefix}No resume token found for server-level change stream. Waiting for 60 seconds before retrying.", LogType.Warning);
                using var initCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                await InitializeResumeTokensAsync(initCts.Token);
                await Task.Delay(60 * 1000, initCts.Token);
            }

        }

        private async Task InitializeResumeTokensAsync(CancellationToken token)
        {
            _log.WriteLine($"{_syncBackPrefix}Rechecking server for resume token.", LogType.Info);

            try
            {

                await MongoHelper.SetChangeStreamResumeTokenAsync(
                    _log,
                    _syncBack ? _targetClient : _sourceClient,
                    MigrationJobContext.CurrentlyActiveJob,
                    null,
                    30,
                    _syncBack,
                    token);
            }
            catch (Exception ex)
            {
                // do nothing
            }
        }
        private async Task<HashSet<string>> WatchServerLevelChangeStream()
        {
            MigrationJobContext.AddVerboseLog("ServerLevelChangeStreamProcessor.WatchServerLevelChangeStream: starting");

            var state = new ServerWatchState();
            var batchCountersInitializedInRound = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var touchedMuIdsInRound = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int maxAwaitSeconds = Math.Max(5, (int)(GetBatchDurationInSeconds(1.0f) * 0.8));
            BsonDocument[] pipelineArray = Array.Empty<BsonDocument>();

            try
            {
                // Create pipeline for server-level change stream
                List<BsonDocument> pipeline = new List<BsonDocument>();
                _namespaceFilterApplied = BuildServerLevelNamespaceFilterPipeline(pipeline);

                // Set up options - use global resume token from MigrationJob
                var options = new ChangeStreamOptions
                {
                    BatchSize = 500,
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds)
                };

                // Get resume information from MigrationJob for server-level streams

                string tokenJson = GetResumeToken();

                // Handle initial document replay for server-level streams
                bool initialReplayCompleted = GetInitialDocumentReplayedStatus();
                if (!initialReplayCompleted && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    if (!AutoReplayFirstChangeInResumeToken())
                    {
                        _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for server-level change stream. Skipping server-level processing.", LogType.Error);

                        // Reset CSUpdatesInLastBatch before early return to prevent stale values
                        foreach (var kvp in _migrationUnitsToProcess.Keys)
                        {
                            _migrationUnitsToProcess[kvp] = 0;
                        }
                        
                        return touchedMuIdsInRound;
                    }
                    SetInitialDocumentReplayedStatus(true);
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob); ;
                }

                var resumeToken = BsonSerializer.Deserialize<BsonDocument>(tokenJson);

                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter =resumeToken, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };

                pipelineArray = pipeline.ToArray();

                // Watch at client level (server-level) with a dedicated 10-minute cursor creation timeout
                _log.WriteLine($"{_syncBackPrefix}Creating server-level change stream cursor...", LogType.Debug);
                using var cursorCreationCts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
                IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? cursor = null;
                var cursorCreationSw = Stopwatch.StartNew();
                try
                {
                    cursor = await Task.Run(() =>
                        _sourceClient.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cursorCreationCts.Token),
                        cursorCreationCts.Token);
                }
                catch (OperationCanceledException) when (cursorCreationCts.IsCancellationRequested)
                {
                    _log.WriteLine($"{_syncBackPrefix}Cursor creation timed out (10 min) for server-level change stream.", LogType.Warning);
                    if (_namespaceFilterApplied)
                    {
                        MigrationJobContext.CurrentlyActiveJob.UseClientSideCSFilter = true;
                        MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                        _log.WriteLine($"{_syncBackPrefix}Namespace filter has {pipelineArray.Length} stage(s) with {_migrationUnitsToProcess.Count} collections. Switching to unfiltered server-level watch with client-side filtering.", LogType.Warning);
                    }
                    return touchedMuIdsInRound;
                }
                finally
                {
                    cursorCreationSw.Stop();
                }

                _log.WriteLine($"{_syncBackPrefix}Server-level cursor created in {cursorCreationSw.Elapsed.TotalSeconds:F1}s. Starting processing...", LogType.Debug);

                if (cursorCreationSw.Elapsed.TotalSeconds > GetBatchDurationInSeconds(1.0f))
                {
                    _log.WriteLine($"{_syncBackPrefix}Cursor creation for server-level change stream took {cursorCreationSw.Elapsed.TotalSeconds:F1}s, exceeding batch duration of {GetBatchDurationInSeconds(1.0f)}s", LogType.Warning);
                }

                using (cursor)
                {
                    // Create a fresh batch-duration CTS that starts NOW (after cursor creation),
                    // so processing always gets the full batch time regardless of how long cursor creation took.
                    using var batchCts = new CancellationTokenSource(TimeSpan.FromSeconds(GetBatchDurationInSeconds(1.0f)));
                    var batchToken = batchCts.Token;

                    if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
                    {
                        await ProcessLegacyWatchLoopAsync(cursor, batchToken, state, batchCountersInitializedInRound, touchedMuIdsInRound);
                    }
                    else
                    {
                        await ProcessModernWatchLoopAsync(cursor, batchToken, state, batchCountersInitializedInRound, touchedMuIdsInRound);
                    }

                    // Advance resume token using postBatchResumeToken when all changes
                    // were persisted successfully (or no events were received).
                    // If any write failed, keep the last change-event token so we
                    // re-process from that point on the next round.
                    if (!state.HasFailures)
                    {
                        try
                        {
                            var postBatchToken = cursor.GetResumeToken();
                            if (postBatchToken != null)
                            {
                                state.LatestResumeToken = postBatchToken.ToJson();
                                state.LatestTimestamp = DateTime.UtcNow;
                            }
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Could not retrieve postBatchResumeToken for server-level stream: {ex.Message}", LogType.Debug);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when batch timeout occurs
            }
            catch (MongoCommandException mcex) when (
                mcex.Code == 280 ||
                mcex.Message.Contains("resume token was not found") ||
                mcex.Message.Contains("Resume of change stream was not possible") ||
                mcex.Message.Contains("Expired resume token"))
            {
                _log.WriteLine($"{_syncBackPrefix}Resume token invalid for server-level change stream (Code: {mcex.Code}). Falling back to StartAtOperationTime.", LogType.Warning);

                // Determine the best timestamp to resume from
                DateTime fallbackTime = _syncBack
                    ? MigrationJobContext.CurrentlyActiveJob.SyncBackCursorUtcTimestamp
                    : MigrationJobContext.CurrentlyActiveJob.CursorUtcTimestamp;

                if (fallbackTime <= DateTime.MinValue)
                    fallbackTime = MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn ?? DateTime.MinValue;

                if (fallbackTime <= DateTime.MinValue)
                {
                    _log.WriteLine($"{_syncBackPrefix}No fallback timestamp available. Cannot retry with StartAtOperationTime.", LogType.Error);
                    throw;
                }

                try
                {
                    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(fallbackTime);
                    _log.WriteLine($"{_syncBackPrefix}Retrying server-level change stream with StartAtOperationTime: {fallbackTime} (BsonTimestamp: {bsonTimestamp})", LogType.Warning);

                    var retryOptions = new ChangeStreamOptions
                    {
                        BatchSize = 500,
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        StartAtOperationTime = bsonTimestamp,
                        MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds)
                    };

                    using var retryCursorCreationCts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
                    IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? retryCursor;
                    var retryCursorCreationSw = Stopwatch.StartNew();
                    try
                    {
                        retryCursor = await Task.Run(() =>
                            _sourceClient.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, retryOptions, retryCursorCreationCts.Token),
                            retryCursorCreationCts.Token);
                    }
                    catch (OperationCanceledException) when (retryCursorCreationCts.IsCancellationRequested)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Cursor creation timed out (10 min) during StartAtOperationTime fallback.", LogType.Warning);
                        if (_namespaceFilterApplied)
                        {
                            MigrationJobContext.CurrentlyActiveJob.UseClientSideCSFilter = true;
                            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                            _log.WriteLine($"{_syncBackPrefix}Switching to unfiltered server-level watch with client-side filtering for {_migrationUnitsToProcess.Count} collections.", LogType.Warning);
                        }
                        return touchedMuIdsInRound;
                    }
                    finally
                    {
                        retryCursorCreationSw.Stop();
                    }

                    if (retryCursorCreationSw.Elapsed.TotalSeconds > GetBatchDurationInSeconds(1.0f))
                    {
                        _log.WriteLine($"{_syncBackPrefix}Cursor creation for server-level fallback took {retryCursorCreationSw.Elapsed.TotalSeconds:F1}s, exceeding batch duration of {GetBatchDurationInSeconds(1.0f)}s", LogType.Warning);
                    }
                    using (retryCursor)
                    {
                        using var retryBatchCts = new CancellationTokenSource(TimeSpan.FromSeconds(GetBatchDurationInSeconds(1.0f)));
                        var retryBatchToken = retryBatchCts.Token;

                        if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
                            await ProcessLegacyWatchLoopAsync(retryCursor, retryBatchToken, state, batchCountersInitializedInRound, touchedMuIdsInRound);
                        else
                            await ProcessModernWatchLoopAsync(retryCursor, retryBatchToken, state, batchCountersInitializedInRound, touchedMuIdsInRound);

                        // Advance resume token using postBatchResumeToken when all
                        // changes were persisted successfully (or no events received).
                        if (!state.HasFailures)
                        {
                            try
                            {
                                var postBatchToken = retryCursor.GetResumeToken();
                                if (postBatchToken != null)
                                {
                                    state.LatestResumeToken = postBatchToken.ToJson();
                                    state.LatestTimestamp = DateTime.UtcNow;
                                }
                            }
                            catch (Exception ex)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Could not retrieve postBatchResumeToken for server-level fallback stream: {ex.Message}", LogType.Debug);
                            }
                        }
                    }

                    _log.WriteLine($"{_syncBackPrefix}StartAtOperationTime fallback succeeded.", LogType.Warning);
                }
                catch (OperationCanceledException)
                {
                    // Expected when batch timeout occurs during retry
                }
                catch (Exception retryEx)
                {
                    _log.WriteLine($"{_syncBackPrefix}StartAtOperationTime fallback also failed: {retryEx}", LogType.Error);
                    throw;
                }
            }
            catch (MongoCommandException mcex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error watching server-level change stream: {mcex}", LogType.Error);
                throw;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error watching server-level change stream: {ex}", LogType.Error);
                throw;
            }
            finally
            {
                try
                {
                    
                    if (_accumulatedChangesPerCollection != null)
                    {
                        await FlushAndCheckpointAsync(state, batchCountersInitializedInRound, touchedMuIdsInRound, true);
                    }
                    // Apply accumulated batch stats for MUs that were flushed in earlier
                    // mid-batch cycles and didn't appear in the final flush results.
                    ApplyRemainingBatchStats(state, touchedMuIdsInRound);
                    SetJobResumeToken(state.LatestResumeToken, state.LatestTimestamp, state.LatestOperationType, state.LatestDocumentKey, state.LatestCollectionKey);

                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {state.CollectionKey}. Details: {ex}", LogType.Error);
                }
            }

            return touchedMuIdsInRound;
        }

        private async Task ProcessLegacyWatchLoopAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            CancellationToken cancellationToken,
            ServerWatchState state,
            HashSet<string> batchCountersInitializedInRound,
            HashSet<string> touchedMuIdsInRound)
        {
            foreach (var change in cursor.ToEnumerable(cancellationToken))
            {
                bool shouldContinue = await TryProcessServerChangeAsync(change, cancellationToken, state, readDurationShareMs: 0);
                if (!shouldContinue)
                    break;
            }

            await FlushAndCheckpointIfNeededAsync(state, batchCountersInitializedInRound, touchedMuIdsInRound);
        }

        private async Task ProcessModernWatchLoopAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            CancellationToken cancellationToken,
            ServerWatchState state,
            HashSet<string> batchCountersInitializedInRound,
            HashSet<string> touchedMuIdsInRound)
        {
            var readStopwatch = Stopwatch.StartNew();

            while (true)
            {
                bool hasBatch = await cursor.MoveNextAsync(cancellationToken);

                if (!hasBatch)
                {
                    readStopwatch.Stop();
                    break;
                }

                readStopwatch.Stop();
                int currentBatchCount = cursor.Current?.Count() ?? 0;
                double readDurationShareMs = currentBatchCount > 0
                    ? (double)readStopwatch.ElapsedMilliseconds / currentBatchCount
                    : 0;

                cancellationToken.ThrowIfCancellationRequested();
                if (ExecutionCancelled)
                    break;

                foreach (var change in cursor.Current)
                {
                    bool shouldContinue = await TryProcessServerChangeAsync(change, cancellationToken, state, readDurationShareMs);
                    if (!shouldContinue)
                        break;
                }

                if (ExecutionCancelled)
                    break;

                readStopwatch.Restart();
                await FlushAndCheckpointIfNeededAsync(state, batchCountersInitializedInRound, touchedMuIdsInRound);
            }
        }

        private async Task<bool> TryProcessServerChangeAsync(
            ChangeStreamDocument<BsonDocument> change,
            CancellationToken cancellationToken,
            ServerWatchState state,
            double readDurationShareMs)
        {
            state.LatestResumeToken = change.ResumeToken.ToJson();
            state.LatestTimestamp = GetChangeTimestampUtc(change);
            state.CollectionKey = change.CollectionNamespace.ToString();
            state.LatestOperationType = change.OperationType;
            state.LatestDocumentKey = change.DocumentKey?.ToJson() ?? string.Empty;
            state.LatestCollectionKey = state.CollectionKey;

            if (readDurationShareMs > 0 && TryResolveQueuedMigrationUnit(state.CollectionKey, out var latencyMu) && latencyMu != null)
            {
                var latencyKey = $"{latencyMu.DatabaseName}.{latencyMu.CollectionName}";
                InitializeAccumulatedChangesTracker(latencyKey);
                _accumulatedChangesPerCollection[latencyKey].CSTotalReadDurationInMS += (long)Math.Round(readDurationShareMs);
            }

            if (!(_monitorAllCollections || _namespaceFilterApplied || TryResolveQueuedMigrationUnit(state.CollectionKey, out _)))
                return !ExecutionCancelled;

            cancellationToken.ThrowIfCancellationRequested();
            if (ExecutionCancelled)
                return false;

            var result = await PreProcessChange(change, state.Counter);
            if (!result.success)
                return false;

            state.Counter = result.counter;
            return !ExecutionCancelled;
        }

        private async Task FlushAndCheckpointIfNeededAsync(
            ServerWatchState state,
            HashSet<string> batchCountersInitializedInRound,
            HashSet<string> touchedMuIdsInRound)
        {
            if ((state.Counter - state.FlushedCount) <= _config.ChangeStreamMaxDocsInBatch)
                return;

            await FlushAndCheckpointAsync(state, batchCountersInitializedInRound, touchedMuIdsInRound, false);
            state.FlushedCount = state.Counter;
        }

        private async Task FlushAndCheckpointAsync(
            ServerWatchState state,
            HashSet<string> batchCountersInitializedInRound,
            HashSet<string> touchedMuIdsInRound,
            bool forcePersist)
        {
            try
            {
                await BulkProcessAllChangesAsync(_accumulatedChangesPerCollection, batchCountersInitializedInRound, touchedMuIdsInRound, forcePersist, state);
                SetJobResumeToken(state.LatestResumeToken, state.LatestTimestamp, state.LatestOperationType, state.LatestDocumentKey, state.LatestCollectionKey);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
            {
                _log.WriteLine($"{_syncBackPrefix}CRITICAL error during BulkProcessAllChangesAsync. Details: {ex}", LogType.Error);
                StopJob($"CRITICAL error in BulkProcessAllChangesAsync. Details: {ex}");
                throw;
            }
        }

        private bool BuildServerLevelNamespaceFilterPipeline(List<BsonDocument> pipeline)
        {
            // In monitor-all mode, keep full stream visibility.
            if (_monitorAllCollections)
            {
                return false;
            }

            // If a previous cursor creation timed out with server-side filtering,
            // skip the pipeline and use client-side filtering for subsequent rounds.
            if (MigrationJobContext.CurrentlyActiveJob.UseClientSideCSFilter)
            {
                return false;
            }

            var namespacePairs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var orConditions = new BsonArray();

            foreach (var muId in _migrationUnitsToProcess.Keys)
            {
                var mu = MigrationJobContext.GetMigrationUnit(muId);
                if (mu == null)
                    continue;

                // Server-level forward stream is from source namespaces.
                var ns = $"{mu.DatabaseName}.{mu.CollectionName}";
                if (!namespacePairs.Add(ns))
                    continue;

                orConditions.Add(new BsonDocument
                {
                    { "ns.db", mu.DatabaseName },
                    { "ns.coll", mu.CollectionName }
                });
            }

            if (orConditions.Count == 0)
            {
                return false;
            }

            var matchStage = new BsonDocument("$match", new BsonDocument
            {
                {
                    "operationType",
                    new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" })
                },
                { "$or", orConditions }
            });

            // Protect against MongoDB command document size limits when many namespaces are configured.
            int matchStageSizeBytes = matchStage.ToBson().Length;
            const int stageSafetyLimitBytes = 14 * 1024 * 1024;

            if (matchStageSizeBytes > stageSafetyLimitBytes)
            {
                _log.WriteLine($"{_syncBackPrefix}Namespace filter too large ({matchStageSizeBytes} bytes for {orConditions.Count} namespaces). Falling back to unfiltered server-level watch.", LogType.Warning);
                return false;
            }

            pipeline.Add(matchStage);
            return true;
        }

        private bool TryGetMigrationUnitFromSourceNamespace(string databaseName, string collectionName, out MigrationUnit? migrationUnit)
        {
            migrationUnit = null;
            var sourceId = Helper.GenerateMigrationUnitId(databaseName, collectionName);
            if (!_migrationUnitsToProcess.ContainsKey(sourceId))
                return false;

            migrationUnit = MigrationJobContext.GetMigrationUnit(sourceId);
            if (migrationUnit == null)
                return false;

            migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
            return true;
        }

        private void SetJobResumeToken(string latestResumeToken, DateTime latestTimestamp, ChangeStreamOperationType latestOperationType, string latestDocumentKey, string latestCollectionKey)
        {
            if (string.IsNullOrEmpty(latestResumeToken))
                return;

            UpdateResumeToken(latestResumeToken, latestOperationType, latestDocumentKey, latestCollectionKey);

            if (!_syncBack)
                MigrationJobContext.CurrentlyActiveJob.CursorUtcTimestamp = latestTimestamp;
            else
                MigrationJobContext.CurrentlyActiveJob.SyncBackCursorUtcTimestamp = latestTimestamp;

            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
        }

        private void SetTouchedCollectionsCSLastChecked(HashSet<string> touchedMuIds)
        {
            if (touchedMuIds == null || touchedMuIds.Count == 0)
                return;

            var now = DateTime.UtcNow;
            bool anyUpdated = false;

            foreach (var muId in touchedMuIds)
            {
                var mu = MigrationJobContext.GetMigrationUnit(muId);
                if (mu == null)
                    continue;

                mu.CSLastChecked = now;
                mu.UpdateParentJob();
                MigrationJobContext.SaveMigrationUnit(mu, false);
                anyUpdated = true;
            }

            if (anyUpdated)
            {
                MigrationJobContext.CurrentlyActiveJob.CSLastChecked = now;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            }
        }

        private async Task ResetCollectionsUntouchedSincePreviousRoundAsync(HashSet<string> touchedMuIdsInCurrentRound)
        {
            touchedMuIdsInCurrentRound ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            List<string> candidatesToReset;

            if (!_hasTouchedBaseline)
            {
                // First round bootstrap: previous touched set is empty, so build candidates from all MUs.
                candidatesToReset = MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics
                    .Where(mub => !touchedMuIdsInCurrentRound.Contains(mub.Id))
                    .Select(mub => mub.Id)
                    .ToList();
            }
            else
            {
                // Steady state: only consider collections active in previous round but not current round.
                candidatesToReset = _touchedMuIdsInPreviousRound
                    .Where(muId => !touchedMuIdsInCurrentRound.Contains(muId))
                    .ToList();
            }

            bool anyUpdated = false;

            foreach (var muId in candidatesToReset)
            {
                var mu = MigrationJobContext.GetMigrationUnit(muId);
                if (mu == null)
                    continue;

                if (mu.CSUpdatesInLastBatch == 0 && mu.CSNormalizedUpdatesInLastBatch == 0)
                    continue;

                mu.CSUpdatesInLastBatch = 0;
                mu.CSNormalizedUpdatesInLastBatch = 0;
                mu.UpdateParentJob();
                MigrationJobContext.SaveMigrationUnit(mu, false);
                anyUpdated = true;
            }

            if (anyUpdated)
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

            _touchedMuIdsInPreviousRound = new HashSet<string>(touchedMuIdsInCurrentRound, StringComparer.OrdinalIgnoreCase);
            _hasTouchedBaseline = true;

            await Task.CompletedTask;
        }

        private bool TryResolveQueuedMigrationUnit(string collectionKey, out MigrationUnit? migrationUnit)
        {
            migrationUnit = null;

            var parts = collectionKey.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            migrationUnit = ResolveMigrationUnitFromNamespace(parts[0], parts[1]);
            return migrationUnit != null;
        }

        private BsonDocument GetCachedUserFilterDoc(MigrationUnit migrationUnit)
        {
            return _userFilterCache.GetOrAdd(migrationUnit.Id, _ => MongoHelper.GetFilterDoc(migrationUnit.UserFilter));
        }

        private IMongoCollection<BsonDocument> GetCachedTargetCollection(string databaseName, string collectionName)
        {
            string collectionKey = $"{databaseName}.{collectionName}";
            return _targetCollectionCache.GetOrAdd(collectionKey, _ => GetTargetCollection(databaseName, collectionName));
        }

        private async Task<(bool success, long counter)> PreProcessChange(ChangeStreamDocument<BsonDocument> change, long counter)
        {
            try
            {
                // Extract namespace information
                if (change.CollectionNamespace == null)
                {
                    return (true, counter); // Skip if no namespace info
                }

                var databaseName = change.CollectionNamespace.DatabaseNamespace.DatabaseName;
                var collectionName = change.CollectionNamespace.CollectionName;
                var collectionKey = $"{databaseName}.{collectionName}";

                MigrationUnit migrationUnit=null;

                //if monitoring all collections, use a dummy key to report all changes, no filtering of collections and data
                if (_monitorAllCollections)
                {
                    //add to _allCollectionsAsMigrationUnit dynamically
                    _uniqueCollectionKeys.Add(collectionKey);
                }
                else
                {
                    if (_namespaceFilterApplied)
                    {
                        if (!TryGetMigrationUnitFromSourceNamespace(databaseName, collectionName, out migrationUnit) || migrationUnit == null)
                        {
                            // Defensive fallback: if filtered stream still yields an unexpected namespace.
                            if (!TryResolveQueuedMigrationUnit(collectionKey, out migrationUnit) || migrationUnit == null)
                            {
                                return (true, counter);
                            }
                        }
                    }
                    else if (!TryResolveQueuedMigrationUnit(collectionKey, out migrationUnit) || migrationUnit == null)
                    {
                        return (true, counter); // Skip changes for collections not in our job
                    }

                    // Check user filter condition               
                    var userFilterDoc = GetCachedUserFilterDoc(migrationUnit);

                    if (change.OperationType != ChangeStreamOperationType.Delete)
                    {
                        if (userFilterDoc.Elements.Count() > 0
                            && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                            return (true, counter); // Skip if doesn't match user filter
                    }
                }
                counter++;

                DateTime timeStamp = GetChangeTimestampUtc(change);

                ShowInMonitor(change, collectionKey, timeStamp, counter);


                // Get target collection
                IMongoCollection<BsonDocument> targetCollection = GetCachedTargetCollection(databaseName, collectionName);

                
                if (_monitorAllCollections)
                {
                    migrationUnit = MigrationJobContext.GetMigrationUnit(Helper.GenerateMigrationUnitId("DUMMY", "DUMMY"));
                    migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    
                }

                var keyForUI= $"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}";
                InitializeAccumulatedChangesTracker(keyForUI);
                PreProcessChangeEvent(change, targetCollection, collectionKey, _accumulatedChangesPerCollection[keyForUI], MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun, migrationUnit);

                if (ExecutionCancelled)
                    return (false, counter);

                return (true, counter);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing server-level change: {ex}", LogType.Error);
                return (false, counter);
            }
        }

        private async Task BulkProcessAllChangesAsync(Dictionary<string, AccumulatedChangesTracker> accumulatedChangesInColl, HashSet<string> batchCountersInitializedInRound, HashSet<string> touchedMuIdsInRound, bool forcePersist, ServerWatchState? watchState = null)
        {
            var entriesToProcess = CollectEntriesToProcess(accumulatedChangesInColl);

            if (entriesToProcess.Count == 0)
            {
                return;
            }

            var tasks = CreateBulkWriteTasks(entriesToProcess);

            // Await all parallel writes
            var results = await Task.WhenAll(tasks);

            ApplyBulkWriteResults(results, batchCountersInitializedInRound, touchedMuIdsInRound, forcePersist, watchState);

            // Parent job persistence is handled by SetJobResumeToken after each successful flush checkpoint.
        }

        private List<(string collectionKey, AccumulatedChangesTracker docs, MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection)> CollectEntriesToProcess(Dictionary<string, AccumulatedChangesTracker> accumulatedChangesInColl)
        {
            var entriesToProcess = new List<(string collectionKey, AccumulatedChangesTracker docs, MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection)>();

            foreach (var kvp in accumulatedChangesInColl)
            {
                var collectionKey = kvp.Key;
                var docs = kvp.Value;
                int totalChanges = docs.DocsToBeInserted.Count + docs.DocsToBeUpdated.Count + docs.DocsToBeDeleted.Count;

                if (totalChanges <= 0)
                    continue;

                if (!TryResolveQueuedMigrationUnit(collectionKey, out var mu) || mu == null)
                    continue;

                var targetCollection = GetCachedTargetCollection(mu.DatabaseName, mu.CollectionName);

                if (_monitorAllCollections)
                {
                    var muId = Helper.GenerateMigrationUnitId("DUMMY.DUMMY");
                    mu = MigrationJobContext.GetMigrationUnit(muId);
                    mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                }

                entriesToProcess.Add((collectionKey, docs, mu, targetCollection));
            }

            return entriesToProcess;
        }

        private List<Task<(string collectionKey, MigrationUnit mu, AccumulatedChangesTracker docs, bool success)>> CreateBulkWriteTasks(List<(string collectionKey, AccumulatedChangesTracker docs, MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection)> entriesToProcess)
        {
            int maxParallelWrites = Math.Clamp(Environment.ProcessorCount, 4, 32);
            maxParallelWrites = Math.Min(maxParallelWrites, Math.Max(1, entriesToProcess.Count));
            var semaphore = new SemaphoreSlim(maxParallelWrites, maxParallelWrites);
            var tasks = new List<Task<(string collectionKey, MigrationUnit mu, AccumulatedChangesTracker docs, bool success)>>();

            foreach (var entry in entriesToProcess)
            {
                var (collectionKey, docs, mu, targetCollection) = entry;
                tasks.Add(Task.Run(async () =>
                {
                    await semaphore.WaitAsync();
                    try
                    {
                        await BulkProcessChangesAsync(
                            mu,
                            targetCollection,
                            insertEvents: docs.DocsToBeInserted.Values.ToList(),
                            updateEvents: docs.DocsToBeUpdated.Values.ToList(),
                            deleteEvents: docs.DocsToBeDeleted.Values.ToList(),
                            accumulatedChangesInColl: docs,
                            batchSize: 500);
                        return (collectionKey, mu, docs, success: true);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL error in BulkProcessAllChangesAsync for {collectionKey}.Details: {ex}", LogType.Error);
                        StopJob($"CRITICAL error processing {collectionKey}. Details: {ex}");
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Error processing changes for {collectionKey}. Details: {ex}", LogType.Error);
                        throw;
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            return tasks;
        }

        private void ApplyBulkWriteResults(
            (string collectionKey, MigrationUnit mu, AccumulatedChangesTracker docs, bool success)[] results,
            HashSet<string> batchCountersInitializedInRound,
            HashSet<string> touchedMuIdsInRound,
            bool forcePersist,
            ServerWatchState? watchState = null)
        {
            foreach (var (collectionKey, mu, docs, success) in results)
            {
                if (!success && watchState != null)
                    watchState.HasFailures = true;
                if (success && !string.IsNullOrEmpty(docs.LatestResumeToken))
                {
                    var flushedEventCount = docs.TotalEventCount;
                    touchedMuIdsInRound.Add(mu.Id);

                    UpdateResumeToken(docs.LatestResumeToken, docs.LatestOperationType, docs.LatestDocumentKey, collectionKey);
                    if (!_syncBack)
                        mu.CursorUtcTimestamp = docs.LatestTimestamp;
                    else
                        mu.SyncBackCursorUtcTimestamp = docs.LatestTimestamp;

                    // Accumulate stats locally; they are applied to MU on the final flush.
                    if (watchState != null && flushedEventCount > 0)
                    {
                        if (!watchState.BatchStats.TryGetValue(mu.Id, out var stats))
                        {
                            stats = new MuBatchStats();
                            watchState.BatchStats[mu.Id] = stats;
                        }
                        stats.EventCount += flushedEventCount;
                        stats.LatestResumeToken = docs.LatestResumeToken;
                        stats.LatestTimestamp = docs.LatestTimestamp;
                        stats.TotalReadDurationMs += docs.CSTotalReadDurationInMS;
                        stats.TotalWriteDurationMs += docs.CSTotaWriteDurationInMS;
                        stats.LatestOperationType = docs.LatestOperationType;
                        stats.LatestDocumentKey = docs.LatestDocumentKey;
                    }

                    if (forcePersist)
                    {
                        ApplyBatchStatsToMu(mu, watchState);
                        mu.UpdateParentJob();
                        MigrationJobContext.SaveMigrationUnit(mu, false);
                    }
                }

                if (success)
                {
                    docs.Reset(true);
                }
                else
                {
                    // Track any failure so postBatchResumeToken is not advanced
                    // past events that were not fully persisted.
                }
            }
        }

        private void ApplyBatchStatsToMu(MigrationUnit mu, ServerWatchState? watchState)
        {
            if (watchState == null || !watchState.BatchStats.TryGetValue(mu.Id, out var stats))
            {
                mu.CSUpdatesInLastBatch = 0;
                mu.CSNormalizedUpdatesInLastBatch = 0;
                return;
            }

            mu.CSUpdatesInLastBatch = stats.EventCount;
            mu.CSNormalizedUpdatesInLastBatch = stats.EventCount;
            mu.CSLastResumeTokenWithChange = stats.LatestResumeToken;
            mu.CSLastChangeUTCTime = stats.LatestTimestamp;
            mu.CSAvgReadLatencyInMS = Math.Round((double)stats.TotalReadDurationMs / stats.EventCount, 2);
            mu.CSAvgWriteLatencyInMS = Math.Round((double)stats.TotalWriteDurationMs / stats.EventCount, 2);

            // Mark as applied so ApplyRemainingBatchStats skips it.
            watchState.BatchStats.Remove(mu.Id);
        }

        /// <summary>
        /// Applies accumulated batch stats for MUs that were fully flushed during
        /// mid-batch cycles and did not appear in the final flush results.
        /// </summary>
        private void ApplyRemainingBatchStats(ServerWatchState state, HashSet<string> touchedMuIdsInRound)
        {
            if (state.BatchStats.Count == 0)
                return;

            foreach (var (muId, stats) in state.BatchStats)
            {
                var mu = MigrationJobContext.GetMigrationUnit(muId);
                if (mu == null)
                    continue;

                mu.CSUpdatesInLastBatch = stats.EventCount;
                mu.CSNormalizedUpdatesInLastBatch = stats.EventCount;
                mu.CSLastResumeTokenWithChange = stats.LatestResumeToken;
                mu.CSLastChangeUTCTime = stats.LatestTimestamp;
                mu.CSAvgReadLatencyInMS = Math.Round((double)stats.TotalReadDurationMs / stats.EventCount, 2);
                mu.CSAvgWriteLatencyInMS = Math.Round((double)stats.TotalWriteDurationMs / stats.EventCount, 2);

                mu.UpdateParentJob();
                MigrationJobContext.SaveMigrationUnit(mu, false);
            }

            state.BatchStats.Clear();
        }

        // Server-level equivalent of AutoReplayFirstChangeInResumeToken
        private bool AutoReplayFirstChangeInResumeToken()
        {
            string documentKey = GetResumeDocumentKey();
            ChangeStreamOperationType operationType = GetResumeTokenOperation();
            string collectionKey = GetResumeCollectionKey();

            if (string.IsNullOrEmpty(documentKey))
            {
                _log.WriteLine($"Auto replay is empty for server-level change stream.");
                return true; // Skip if no document ID is provided
            }

            if (string.IsNullOrEmpty(collectionKey))
            {
                _log.WriteLine($"Auto replay collection key is empty for server-level change stream. Cannot determine target collection.");
                return true; // Skip if no collection key is provided
            }

            _log.WriteLine($"Auto replay for {operationType} operation with document key {documentKey} in collection {collectionKey} for server-level change stream.");

            var bsonDoc = BsonDocument.Parse(documentKey);
            var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);

            var parts = collectionKey.Split('.');
            if (parts.Length != 2)
            {
                _log.WriteLine($"Invalid collection key format for server-level auto replay: {collectionKey}. Expected format: database.collection");
                return true;
            }

            if (!TryResolveQueuedMigrationUnit(collectionKey, out var migrationUnit) || migrationUnit == null)
            {
                _log.WriteLine($"Collection {collectionKey} for server-level auto replay is not in migration units. Skipping replay.");
                return true;
            }

            var sourceDatabaseName = parts[0];
            var sourceCollectionName = parts[1];
            var targetDatabaseName = migrationUnit.GetEffectiveTargetDatabaseName();
            var targetCollectionName = migrationUnit.GetEffectiveTargetCollectionName();

            IMongoDatabase sourceDb;
            IMongoDatabase targetDb;
            IMongoCollection<BsonDocument> sourceCollection;
            IMongoCollection<BsonDocument> targetCollection;

            if (!_syncBack)
            {
                sourceDb = _sourceClient.GetDatabase(sourceDatabaseName);
                sourceCollection = sourceDb.GetCollection<BsonDocument>(sourceCollectionName);

                if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    targetDb = _targetClient.GetDatabase(targetDatabaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(targetCollectionName);
                }
                else
                {
                    targetCollection = sourceCollection; // Use source as placeholder for simulated runs
                }
            }
            else
            {
                // For sync back, target is source and vice versa
                targetDb = _sourceClient.GetDatabase(migrationUnit.DatabaseName);
                targetCollection = targetDb.GetCollection<BsonDocument>(migrationUnit.CollectionName);

                sourceDb = _targetClient.GetDatabase(targetDatabaseName);
                sourceCollection = sourceDb.GetCollection<BsonDocument>(targetCollectionName);
            }

            var sourceRawCollection = sourceCollection.Database.GetCollection<RawBsonDocument>(sourceCollection.CollectionNamespace.CollectionName);
            var targetRawCollection = targetCollection.Database.GetCollection<RawBsonDocument>(targetCollection.CollectionNamespace.CollectionName);
            var renderedFilter = RenderFilterForRawCollection(filter);
            var rawFilter = new BsonDocumentFilterDefinition<RawBsonDocument>(renderedFilter);
            var result = sourceRawCollection.Find(rawFilter).FirstOrDefault();

            try
            {
                migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                IncrementEventCounter(migrationUnit, operationType);

                switch (operationType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with document key {documentKey} in {collectionKey}. Skipping insert.");
                            return true;
                        }
                        targetRawCollection.InsertOne(result);
                        IncrementDocCounter(migrationUnit, operationType);
                        return true;

                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {operationType} operation for {collectionKey} with document key {documentKey}. No document found on source, deleting it from target.");
                            try
                            {
                                // Use DocumentKey-based filter for sharded collections
                                targetRawCollection.DeleteOne(rawFilter);
                                IncrementDocCounter(migrationUnit, ChangeStreamOperationType.Delete);
                            }
                            catch { }
                            return true;
                        }
                        else
                        {
                            // Use DocumentKey-based filter for sharded collections with upsert
                            targetRawCollection.ReplaceOne(rawFilter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(migrationUnit, operationType);
                            return true;
                        }

                    case ChangeStreamOperationType.Delete:
                        // Use DocumentKey-based filter for sharded collections
                        targetRawCollection.DeleteOne(rawFilter);
                        IncrementDocCounter(migrationUnit, operationType);
                        return true;

                    default:
                        _log.WriteLine($"Unhandled operation type: {operationType}", LogType.Error);
                        return false;
                }
            }
            catch (MongoException mex) when (operationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {operationType} in server-level auto replay with document key {documentKey}. Details: {ex}", LogType.Error);
                return false;
            }
        }

        private void PreProcessChangeEvent(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, AccumulatedChangesTracker accumulatedChangesInColl, bool isWriteSimulated, MigrationUnit mu)
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
                                _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} with _id {idValue}. No document found on source, deleting it from target.");
                                var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                                try
                                {
                                    targetCollection.DeleteOne(deleteTTLFilter);
                                    IncrementEventCounter(mu, ChangeStreamOperationType.Delete);
                                }
                                catch
                                { }
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

        #region Server-Level Resume Token Management

        private string GetResumeToken()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeToken ?? string.Empty;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeToken ?? string.Empty;
            }
        }

        private bool GetInitialDocumentReplayedStatus()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.InitialDocumenReplayed;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackInitialDocumenReplayed;
            }
        }

        private void SetInitialDocumentReplayedStatus(bool value)
        {
            if (!_syncBack)
            {
                MigrationJobContext.CurrentlyActiveJob.InitialDocumenReplayed = value;
            }
            else
            {
                MigrationJobContext.CurrentlyActiveJob.SyncBackInitialDocumenReplayed = value;
            }
        }

        private ChangeStreamOperationType GetResumeTokenOperation()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeTokenOperation;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeTokenOperation;
            }
        }

        private string GetResumeDocumentKey()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeDocumentKey 
                    ?? MigrationJobContext.CurrentlyActiveJob.ResumeDocumentId // Fallback for backward compatibility
                    ?? string.Empty;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeDocumentKey 
                    ?? MigrationJobContext.CurrentlyActiveJob.SyncBackResumeDocumentId // Fallback for backward compatibility
                    ?? string.Empty;
            }
        }

        private string GetResumeCollectionKey()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeCollectionKey ?? string.Empty;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeCollectionKey ?? string.Empty;
            }
        }

        private void UpdateResumeToken(string resumeToken, ChangeStreamOperationType operationType, string documentId, string collectionKey)
        {
            if (!_syncBack)
            {
                MigrationJobContext.CurrentlyActiveJob.ResumeToken = resumeToken;
                if (string.IsNullOrEmpty(MigrationJobContext.CurrentlyActiveJob.OriginalResumeToken))
                {
                    MigrationJobContext.CurrentlyActiveJob.OriginalResumeToken = resumeToken;
                }
                MigrationJobContext.CurrentlyActiveJob.ResumeTokenOperation = operationType;
                MigrationJobContext.CurrentlyActiveJob.ResumeDocumentId = documentId; // Deprecated - kept for backward compatibility
                MigrationJobContext.CurrentlyActiveJob.ResumeDocumentKey = documentId;
                MigrationJobContext.CurrentlyActiveJob.ResumeCollectionKey = collectionKey;
            }
            else
            {
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeToken = resumeToken;
                if (string.IsNullOrEmpty(MigrationJobContext.CurrentlyActiveJob.SyncBackOriginalResumeToken))
                {
                    MigrationJobContext.CurrentlyActiveJob.SyncBackOriginalResumeToken = resumeToken;
                }
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeTokenOperation = operationType;
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeDocumentId = documentId; // Deprecated - kept for backward compatibility
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeDocumentKey = documentId;
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeCollectionKey = collectionKey;
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

        #endregion
    }
}
#endif // !LEGACY_MONGODB_DRIVER