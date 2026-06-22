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
#if !LEGACY_MONGODB_DRIVER
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
#endif
                
        protected Log _log;
        protected MigrationWorker? _migrationWorker;

        public bool ProcessRunning { get; set; }

        // Set true by SignalStop() so in-flight async continuations in workers
        // (DocumentCopyWorker.UpdateProgress / ProcessSegmentAsync, MigrationWorker.UpdateDocumentCountsAsync,
        // DumpRestore coordinator queues) can skip late writes after Pause was requested.
        public volatile bool StopRequested = false;

        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        public bool IsChangeStreamRunning = false;

#if !LEGACY_MONGODB_DRIVER
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
#endif

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

#if !LEGACY_MONGODB_DRIVER
            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
#endif
        }

        /// <summary>
        /// Signals processor to stop accepting new work but complete current tasks
        /// </summary>
        public virtual void SignalStop()
        {
            StopRequested = true;
            MigrationJobContext.StopRequested = true;
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.SignalStop: StopRequested=true");
        }

        /// <summary>
        /// Stops only the change stream processor, leaving offline workers running.
        /// </summary>
        public void StopChangeStreamProcessor()
        {
#if !LEGACY_MONGODB_DRIVER
            if (_changeStreamProcessor != null)
            {
                _changeStreamProcessor.ExecutionCancelled = true;
                MigrationJobContext.AddVerboseLog("MigrationProcessor.StopChangeStreamProcessor: ExecutionCancelled set to true");
            }
#endif
        }

        /// <summary>
        /// Removes in-memory state for the given migration unit id from any active change-stream
        /// processor. Called by <see cref="MigrationJobContext.PurgeMigrationUnit"/> when a
        /// collection is removed from a running job so a later re-add with the same id starts
        /// from a clean slate.
        /// </summary>
        public virtual void RemoveMigrationUnit(string migrationUnitId)
        {
            if (string.IsNullOrEmpty(migrationUnitId))
                return;

#if !LEGACY_MONGODB_DRIVER
            _changeStreamProcessor?.RemoveMigrationUnit(migrationUnitId);
#endif
        }


        protected ProcessorContext SetProcessorContext(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            var databaseName = mu.DatabaseName;
            var collectionName = mu.CollectionName;
            var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            var targetCollectionName = mu.GetEffectiveTargetCollectionName();
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
                TargetDatabaseName = targetDatabaseName,
                TargetCollectionName = targetCollectionName,
                Database = database!,
                Collection = collection!,
            };

            return context;
        }

        public bool AddCollectionToChangeStreamQueue(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.AddCollectionToChangeStreamQueue: migrationUnitId={mu.Id}");

#if LEGACY_MONGODB_DRIVER
            _log.WriteLine($"Online migration (change streams) is not supported with the legacy MongoDB driver. Skipping {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
            return false;
#else
            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;

            // In Delayed mode, per-collection enqueue (and the side-effect of lazily
            // creating the change-stream processor) must wait until offline migration
            // has completed for ALL collections. Otherwise change streams start
            // running for already-finished collections while others are still in
            // bulk copy, defeating the whole point of Delayed mode.
            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed
                && !Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
            {
                _log.WriteLine($"Deferring change-stream enqueue for {mu.DatabaseName}.{mu.CollectionName}: Delayed mode and offline migration not yet complete for all collections.", LogType.Debug);
                return false;
            }

            if (_targetClient == null)
                _targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config);


            _log.WriteLine($"Adding {mu.DatabaseName}.{mu.CollectionName} to Change Stream processing queue", LogType.Debug);
            _changeStreamProcessor?.AddCollectionsToProcess(mu.Id, _cts);

            return true;
#endif
        }


        public bool RunChangeStreamProcessorForAllCollections()
        {
            MigrationJobContext.AddVerboseLog("MigrationProcessor.RunChangeStreamProcessorForAllCollections: called");

#if LEGACY_MONGODB_DRIVER
            _log.WriteLine("Online migration (change streams) is not supported with the legacy MongoDB driver.", LogType.Warning);
            return false;
        }
#else
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

            //for server-level change streams, wait for all collections to complete offline migration before starting CS
            if (!Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server)
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
                // In Delayed mode AddCollectionToChangeStreamQueue intentionally skips per-unit
                // enqueues until offline migration finishes for all collections. Now that the
                // gates above have passed, populate the queue with every valid unit so CS
                // processing actually has work to do.
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                {
                    foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics ?? new List<MigrationUnitBasic>())
                    {
                        if (!Helper.IsMigrationUnitValid(mub))
                            continue;

                        _changeStreamProcessor.AddCollectionsToProcess(mub.Id, _cts);
                    }
                }

                var result = _changeStreamProcessor.RunChangeStreamProcessorForAllCollections(_cts);
            }
            return true;            
        }
#endif

        
        /// <summary>
        /// Builds non-unique indexes on the target after offline data copy completes.
        /// For blocking mode: waits for index builds to complete, monitoring via currentOp.
        /// For non-blocking mode: starts index builds and monitors progress asynchronously.
        /// Updates mu.IndexPercent and mu.IndexBuildComplete accordingly.
        /// Returns true if change stream can proceed immediately (non-blocking or no indexes needed).
        /// </summary>
        public async Task<bool> BuildNonUniqueIndexesAfterCopyAsync(MigrationUnit mu)
        {
            if (_cts.Token.IsCancellationRequested)
                return false;

            var activeJob = MigrationJobContext.CurrentlyActiveJob;
            if (activeJob == null)
            {
                _log.WriteLine($"Skipping non-unique index build for {mu.DatabaseName}.{mu.CollectionName}: active job context unavailable", LogType.Debug);
                return false;
            }

            // Skip if indexes are not being migrated
            if (!mu.IndexingStrategy.HasValue || mu.IndexingStrategy.Value == IndexingStrategy.DontIndex)
            {
                mu.IndexPercent = 100;
                mu.IndexBuildComplete = true;
                MigrationJobContext.SaveMigrationUnit(mu, true);
                return true;
            }

            // Skip if already complete
            if (mu.IndexBuildComplete)
                return true;

            if (!MigrationJobContext.TargetConnectionString.TryGetValue(activeJob.Id, out var targetConnStr) || string.IsNullOrWhiteSpace(targetConnStr))
            {
                _log.WriteLine($"Skipping non-unique index build for {mu.DatabaseName}.{mu.CollectionName}: target connection string unavailable", LogType.Warning);
                return false;
            }
            var sourceDatabase = _sourceClient!.GetDatabase(mu.DatabaseName);
            var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(mu.CollectionName);
            var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            var targetCollectionName = mu.GetEffectiveTargetCollectionName();
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

            bool isBlocking = mu.IndexingStrategy.Value == IndexingStrategy.SameAsSourceBlocking;

            // Authoritative resume check: if the target already has all expected non-unique
            // index *documents* (listIndexes), the previous run already issued createIndexes.
            // However, listIndexes shows entries from the moment a build starts — the index
            // may still be building in the background. So cross-check currentOp before declaring
            // completion; if builds are still active, hand off to the resume monitor path instead
            // of either declaring complete or re-issuing createIndexes (which would either
            // double-count or fail with IndexOptionsConflict).
            bool resumeMonitorCase = mu.IndexesExpected > 0 && mu.IndexesMigrated >= mu.IndexesExpected;
            if (mu.IndexesExpected > 0)
            {
                try
                {
                    var verifyClient = MongoClientFactory.Create(_log, targetConnStr);
                    int builtOnTarget = await Helpers.Mongo.IndexCopier.CountNonUniqueIndexesOnTargetAsync(verifyClient, targetDatabaseName, targetCollectionName, _log);
                    int activeBuildsAtResume = -1;
                    if (builtOnTarget >= mu.IndexesExpected)
                    {
                        var (active, _) = await MongoHelper.CheckIndexBuildProgressAsync(_log, targetConnStr, targetDatabaseName, targetCollectionName, mu.IndexesExpected);
                        activeBuildsAtResume = active;
                    }
                    if (builtOnTarget >= mu.IndexesExpected && activeBuildsAtResume == 0)
                    {
                        mu.IndexesMigrated = mu.IndexesExpected;
                        mu.IndexPercent = 100;
                        mu.IndexBuildComplete = true;
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                        _log.WriteLine($"Non-unique index builds already complete on target for {namespaceForLog} ({builtOnTarget}/{mu.IndexesExpected})");
                        return true;
                    }
                    if (builtOnTarget >= mu.IndexesExpected)
                    {
                        // Index docs exist (createIndexes was already submitted in a prior run) but
                        // background builds either haven't finished or currentOp couldn't be parsed.
                        // Force the resume-monitor path so it polls and verifies completion.
                        resumeMonitorCase = true;
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Target index verification failed for {namespaceForLog}: {ex.Message}", LogType.Warning);
                }
            }

            // Resume path: a previous run already submitted createIndexes for all expected indexes
            // but server-side builds didn't finish before the pause. Skip re-issuing createIndexes
            // and go straight to monitoring/waiting so we don't double-count IndexesMigrated or
            // hammer the target unnecessarily.
            if (resumeMonitorCase)
            {
                _log.WriteLine($"Resuming index-build monitoring for {namespaceForLog} ({(isBlocking ? "blocking" : "non-blocking")}, IndexesMigrated={mu.IndexesMigrated}/{mu.IndexesExpected})");
                if (isBlocking)
                {
                    if (_cts.Token.IsCancellationRequested)
                        return false;
                    return await WaitForIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName);
                }
                else
                {
                    _ = Task.Run(() => MonitorIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName));
                    return true;
                }
            }

            // Reset transient progress at the start of a new index-build cycle.
            // This avoids showing stale values (for example 99%) from a previous run.
            mu.IndexPercent = 0;
            mu.IndexBuildComplete = false;
            mu.IndexesFailed = 0;
            MigrationJobContext.SaveMigrationUnit(mu, true);

            _log.WriteLine($"Starting non-unique index build ({(isBlocking ? "blocking" : "non-blocking")}) for {namespaceForLog}");

            // Pre-count source non-unique indexes so the UI immediately shows the denominator
            // (e.g. "0/5") instead of "0/0" while createIndexes commands are being submitted.
            int originalIndexesMigrated = mu.IndexesMigrated;
            try
            {
                var preCountCopier = new Helpers.Mongo.IndexCopier();
                int expectedNonUniqueCount = await preCountCopier.CountNonUniqueIndexesAsync(sourceCollection, _log);
                if (expectedNonUniqueCount > 0)
                {
                    mu.IndexesExpected = expectedNonUniqueCount;
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    _log.WriteLine($"Expecting {expectedNonUniqueCount} non-unique indexes for {namespaceForLog}");
                }
            }
            catch { /* non-fatal; UI falls back to "Pending" if this fails */ }

            // Build non-unique indexes. With per-index maxTimeMS the submission loop returns
            // quickly; the dedicated monitor below (blocking or non-blocking) tracks completion.
            int count;
            if (_cts.Token.IsCancellationRequested)
                return false;

            count = await MongoHelper.BuildNonUniqueIndexesAsync(_log, mu, targetConnStr, sourceCollection, isBlocking);

            // BuildNonUniqueIndexesAsync sets IndexesMigrated += actualCount.
            // If the build failed (count < 0), restore the original count.
            if (count < 0)
            {
                mu.IndexesMigrated = originalIndexesMigrated;
                mu.IndexesExpected = 0;
            }

            if (count < 0)
            {
                _log.WriteLine($"Failed to build non-unique indexes for {namespaceForLog}", LogType.Error);
                return !isBlocking; // In non-blocking mode, don't block change stream on failure
            }

            if (count == 0)
            {
                // No non-unique indexes to build
                mu.IndexesExpected = 0;
                mu.IndexPercent = 100;
                mu.IndexBuildComplete = true;
                MigrationJobContext.SaveMigrationUnit(mu, true);
                _log.WriteLine($"No non-unique indexes to build for {namespaceForLog}");
                return true;
            }

            _log.ShowInMonitor($"Submitted {count} non-unique index build(s) for {namespaceForLog}; waiting for builds to complete on target.");

            if (isBlocking)
            {
                if (_cts.Token.IsCancellationRequested)
                    return false;

                // Wait for all index builds to complete by polling currentOp
                _log.WriteLine($"Waiting for blocking index builds to complete on {namespaceForLog}");
                return await WaitForIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName);
            }
            else
            {
                // Non-blocking: start monitoring in background, don't block change stream
                _ = Task.Run(() => MonitorIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName));
                return true;
            }
        }

        /// <summary>
        /// Polls currentOp until all index builds on the collection complete. (Blocking mode)
        /// </summary>
        private async Task<bool> WaitForIndexBuildsAsync(MigrationUnit mu, string targetConnStr, string databaseName, string collectionName)
        {
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, databaseName, collectionName);
            const int pollIntervalMs = 60000;
            const int maxAttempts = 8640; // ~12 hours at 5s intervals
            const int maxStallChecks = 3; // ~3 stall confirmations (~3-6 minutes after first stall) before we unblock
            int consecutiveZeroPolls = 0;
            int stallChecks = 0;
            int lastBuiltOnTarget = -1;

            try
            {
                for (int attempt = 0; attempt < maxAttempts; attempt++)
                {
                    if (_cts.Token.IsCancellationRequested || MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine($"Blocking index build wait exiting for {namespaceForLog} (cancelled={_cts.Token.IsCancellationRequested}, paused={MigrationJobContext.ControlledPauseRequested})", LogType.Debug);
                        return false;
                    }

                    // Use IndexesExpected as the denominator. IndexesMigrated may be 0 after a
                    // resume that skipped re-running createIndexes (target already has the index docs).
                    int denom = mu.IndexesExpected > 0 ? mu.IndexesExpected : mu.IndexesMigrated;
                    var (activeBuilds, progress) = await MongoHelper.CheckIndexBuildProgressAsync(_log, targetConnStr, databaseName, collectionName, denom);

                    // Monotonic guard: progress can momentarily dip when a build finishes and the next
                    // queued build flips to active with terms_progress=0. Never let the bar regress.
                    var clamped = Math.Min(100, Math.Max(0, progress));
                    mu.IndexPercent = Math.Max(mu.IndexPercent, clamped);
                    MigrationJobContext.SaveMigrationUnit(mu, true);

                    if (activeBuilds == 0)
                    {
                        consecutiveZeroPolls++;
                        if (consecutiveZeroPolls >= 2)
                        {
                            // Verify with target listIndexes before declaring done. currentOp can
                            // report zero in-flight ops while builds are still queued / not yet scheduled
                            // (especially right after resume), which previously caused premature completion.
                            int builtOnTarget = -1;
                            try
                            {
                                var verifyClient = MongoClientFactory.Create(_log, targetConnStr);
                                builtOnTarget = await Helpers.Mongo.IndexCopier.CountNonUniqueIndexesOnTargetAsync(verifyClient, databaseName, collectionName, _log);
                            }
                            catch { }
                            if (mu.IndexesExpected <= 0 || (builtOnTarget >= 0 && builtOnTarget >= mu.IndexesExpected))
                            {
                                mu.IndexPercent = 100;
                                mu.IndexBuildComplete = true;
                                MigrationJobContext.SaveMigrationUnit(mu, true);
                                _log.WriteLine($"Blocking index builds completed for {namespaceForLog}");
                                _log.ShowInMonitor($"Index builds completed for {namespaceForLog} ({builtOnTarget}/{mu.IndexesExpected}).");
                                return true;
                            }

                            // Server reports no active builds yet the target is still missing
                            // one or more indexes. Track stall cycles so a failed or skipped build
                            // can't keep us waiting (and blocking the change stream) indefinitely.
                            consecutiveZeroPolls = 0;
                            if (builtOnTarget >= 0)
                            {
                                if (builtOnTarget > lastBuiltOnTarget)
                                {
                                    stallChecks = 0;
                                    lastBuiltOnTarget = builtOnTarget;
                                }
                                else
                                {
                                    stallChecks++;
                                }

                                if (mu.IndexesExpected > 0)
                                {
                                    var partial = Math.Min(99, builtOnTarget * 100.0 / mu.IndexesExpected);
                                    mu.IndexPercent = Math.Max(mu.IndexPercent, partial);
                                    MigrationJobContext.SaveMigrationUnit(mu, true);
                                }

                                if (stallChecks >= maxStallChecks)
                                {
                                    var failedCount = Math.Max(0, mu.IndexesExpected - builtOnTarget);
                                    _log.WriteLine($"Index builds for {namespaceForLog} stalled at {builtOnTarget}/{mu.IndexesExpected} (no active builds, no progress for {maxStallChecks} checks). Unblocking change stream; any missing indexes must be created manually on the target.", LogType.Warning);
                                    _log.ShowInMonitor($"Index builds stalled for {namespaceForLog} at {builtOnTarget}/{mu.IndexesExpected}. {failedCount} index(es) may need to be created manually on the target.", LogType.Warning);
                                    mu.IndexesFailed = failedCount;
                                    mu.IndexBuildComplete = true;
                                    MigrationJobContext.SaveMigrationUnit(mu, true);
                                    return true;
                                }
                            }
                        }
                    }
                    else
                    {
                        consecutiveZeroPolls = 0;
                        stallChecks = 0;
                    }

                    // Log every poll (~1 minute)
                    _log.WriteLine($"Index build in progress for {namespaceForLog}: {activeBuilds} active, {progress:F1}% complete", LogType.Debug);
                    _log.ShowInMonitor($"Index build in progress for {namespaceForLog}: {activeBuilds} active build(s), {progress:F1}% complete.");

                    await Task.Delay(pollIntervalMs, _cts.Token);
                }

                _log.WriteLine($"Index build monitoring timed out for {namespaceForLog}", LogType.Warning);
                _log.ShowInMonitor($"Index build monitoring timed out for {namespaceForLog}; check target manually.", LogType.Warning);
                return false;
            }
            catch (OperationCanceledException)
            {
                // Expected when pause/stop cancels processor token.
                return false;
            }
        }

        /// <summary>
        /// Monitors index builds asynchronously and updates IndexPercent. (Non-blocking mode)
        /// </summary>
        private async Task MonitorIndexBuildsAsync(MigrationUnit mu, string targetConnStr, string databaseName, string collectionName)
        {
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, databaseName, collectionName);
            const int pollIntervalMs = 60000;
            const int maxAttempts = 4320; // ~12 hours at 10s intervals
            const int maxStallChecks = 3; // unblock after repeated stalls so a failed build doesn't keep monitoring forever
            bool seenAnyBuild = false;
            int stallChecks = 0;
            int lastBuiltOnTarget = -1;

            try
            {
                for (int attempt = 0; attempt < maxAttempts; attempt++)
                {
                    if (_cts.Token.IsCancellationRequested || MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine($"Non-blocking index build monitor exiting for {namespaceForLog} (cancelled={_cts.Token.IsCancellationRequested}, paused={MigrationJobContext.ControlledPauseRequested})", LogType.Debug);
                        return;
                    }

                    // Use IndexesExpected as the denominator. IndexesMigrated may be 0 after a
                    // resume that skipped re-running createIndexes (target already has the index docs).
                    int denom = mu.IndexesExpected > 0 ? mu.IndexesExpected : mu.IndexesMigrated;
                    var (activeBuilds, progress) = await MongoHelper.CheckIndexBuildProgressAsync(_log, targetConnStr, databaseName, collectionName, denom);

                    if (activeBuilds > 0)
                    {
                        seenAnyBuild = true;
                        stallChecks = 0;
                    }

                    // Monotonic guard: progress can momentarily dip when a build finishes and the next
                    // queued build flips to active with terms_progress=0. Never let the bar regress.
                    var clamped = Math.Min(100, Math.Max(0, progress));
                    mu.IndexPercent = Math.Max(mu.IndexPercent, clamped);
                    MigrationJobContext.SaveMigrationUnit(mu, true);

                    // activeBuilds < 0 = currentOp parse error; do not treat as complete.
                    // activeBuilds == 0 with seenAnyBuild=false = warm-up window before queued ops appear; keep waiting.
                    if (activeBuilds == 0 && seenAnyBuild)
                    {
                        // Verify with target listIndexes before flipping complete.
                        int builtOnTarget = -1;
                        try
                        {
                            var verifyClient = MongoClientFactory.Create(_log, targetConnStr);
                            builtOnTarget = await Helpers.Mongo.IndexCopier.CountNonUniqueIndexesOnTargetAsync(verifyClient, databaseName, collectionName, _log);
                        }
                        catch { }
                        if (mu.IndexesExpected <= 0 || (builtOnTarget >= 0 && builtOnTarget >= mu.IndexesExpected))
                        {
                            mu.IndexPercent = 100;
                            mu.IndexBuildComplete = true;
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            _log.WriteLine($"Non-blocking index builds completed for {namespaceForLog}");
                            _log.ShowInMonitor($"Index builds completed for {namespaceForLog} ({builtOnTarget}/{mu.IndexesExpected}).");
                            return;
                        }

                        if (builtOnTarget >= 0)
                        {
                            if (builtOnTarget > lastBuiltOnTarget)
                            {
                                stallChecks = 0;
                                lastBuiltOnTarget = builtOnTarget;
                            }
                            else
                            {
                                stallChecks++;
                            }

                            if (mu.IndexesExpected > 0)
                            {
                                var partial = Math.Min(99, builtOnTarget * 100.0 / mu.IndexesExpected);
                                mu.IndexPercent = Math.Max(mu.IndexPercent, partial);
                                MigrationJobContext.SaveMigrationUnit(mu, true);
                            }

                            if (stallChecks >= maxStallChecks)
                            {
                                var failedCount = Math.Max(0, mu.IndexesExpected - builtOnTarget);
                                _log.WriteLine($"Non-blocking index builds for {namespaceForLog} stalled at {builtOnTarget}/{mu.IndexesExpected} (no active builds, no progress for {maxStallChecks} checks). Ending monitor; any missing indexes must be created manually on the target.", LogType.Warning);
                                _log.ShowInMonitor($"Index builds stalled for {namespaceForLog} at {builtOnTarget}/{mu.IndexesExpected}. {failedCount} index(es) may need to be created manually on the target.", LogType.Warning);
                                mu.IndexesFailed = failedCount;
                                mu.IndexBuildComplete = true;
                                MigrationJobContext.SaveMigrationUnit(mu, true);
                                return;
                            }
                        }
                    }

                    // Log every poll (~1 minute)
                    _log.WriteLine($"Index build in progress (non-blocking) for {namespaceForLog}: {activeBuilds} active, {progress:F1}% complete", LogType.Debug);
                    _log.ShowInMonitor($"Index build in progress for {namespaceForLog}: {activeBuilds} active build(s), {progress:F1}% complete.");

                    await Task.Delay(pollIntervalMs, _cts.Token);
                }

                _log.WriteLine($"Non-blocking index build monitoring timed out for {namespaceForLog}", LogType.Warning);
                _log.ShowInMonitor($"Index build monitoring timed out for {namespaceForLog}; check target manually.", LogType.Warning);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error monitoring index builds for {namespaceForLog}: {ex.Message}", LogType.Warning);
            }
        }

        public void StopOfflineOrInvokeChangeStreams()
        {
            // Handle offline completion and post-upload CS logic

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
            {
                // Defer when background post-copy index builds (resumed from a prior pause) are still
                // running; StopProcessing here would cancel _cts and abort them mid-build. The last
                // background task to finish will call back into this method to drive the final stop.
                if (_migrationWorker?.HasPendingIndexBuilds() == true)
                {
                    _log.WriteLine("Deferring offline job completion: background index builds still in progress", LogType.Debug);
                    return;
                }

                // Don't mark as completed if this is a controlled pause
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} Completed from StopOfflineOrInvokeChangeStreams");
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
        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString)
        { return Task.FromResult(TaskResult.Success); }

        /// <summary>
        /// Signals the underlying processor that the orchestrator has finished dispatching every MU.
        /// Default no-op. DumpRestoreProcessor overrides this to close the coordinator's registration phase
        /// so it can self-shutdown safely once queues drain.
        /// </summary>
        public virtual void MarkAllUnitsDispatched() { }
    }
}
