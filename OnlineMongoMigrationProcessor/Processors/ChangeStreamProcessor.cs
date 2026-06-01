using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
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
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Context;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

#if !LEGACY_MONGODB_DRIVER
namespace OnlineMongoMigrationProcessor
{
    public abstract class ChangeStreamProcessor : IDisposable
    {
        protected int _concurrentProcessors;
        protected int _processorRunMaxDurationInSec;
        protected int _processorRunMinDurationInSec;

        protected MongoClient _sourceClient;
        protected MongoClient _targetClient;

        protected MigrationSettings? _config;
        protected bool _syncBack = false;
        protected string _syncBackPrefix = string.Empty;
        protected bool _isCSProcessing = false;
        protected Log _log;

        // Reference to MigrationWorker for coordinated shutdown
        protected MigrationWorker? _migrationWorker;
       
        // Resume token cache - used by collection-level processors to track individual collection resume tokens
        // Server-level processors don't need this as they use MigrationJob properties directly for global tokens
        protected virtual bool UseResumeTokenCache => true; // Override in server-level processor to return false
        protected ConcurrentDictionary<string, string> _resumeTokenCache = new ConcurrentDictionary<string, string>();
        protected ConcurrentDictionary<string, long> _migrationUnitsToProcess = new ConcurrentDictionary<string, long>();

        // Keep temp forensic log lines bounded; emit multiple lines when IDs are large.
        private const int TempLogMaxIdsPerLine = 25;

        protected bool IsCSWatchLogEnabled => _config?.EnableCSWatchLog == true;

        // Reverse lookup: "targetDb.targetCollection" -> MigrationUnit ID, for O(1) resolution in change stream hot path
        protected ConcurrentDictionary<string, string> _targetNamespaceToUnitId = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Negative cache for namespaces not part of current migration units to avoid repeated fallback scans
        protected ConcurrentDictionary<string, byte> _namespaceMissCache = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        private int GetTrackedNamespaceCount()
        {
            // Source IDs are in _migrationUnitsToProcess and target IDs are in _targetNamespaceToUnitId.
            return Math.Max(1, _migrationUnitsToProcess.Count + _targetNamespaceToUnitId.Count);
        }

        private bool ShouldUseNamespaceMissCache()
        {
            // Use miss cache only when it is not larger than tracked namespaces.
            return _namespaceMissCache.Count <= GetTrackedNamespaceCount();
        }

        private void TrimNamespaceMissCacheIfNeeded()
        {
            // If miss cache balloons, clear it and fall back to normal hot path lookups.
            int tracked = GetTrackedNamespaceCount();
            if (_namespaceMissCache.Count > tracked * 2)
            {
                _namespaceMissCache.Clear();
            }
        }

        // Tracking for aggressive cleanup to prevent duplicate executions
        protected ConcurrentDictionary<string, bool> _aggressiveCleanupProcessed = new ConcurrentDictionary<string, bool>();
        protected bool _finalCleanupExecuted = false;

        // Critical failure tracking - shared across server-level and collection-level processors
        protected readonly ConcurrentBag<Task> _backgroundProcessingTasks = new();
        protected volatile bool _criticalFailureDetected = false;
        protected Exception? _criticalFailureException = null;

        protected static readonly object _processingLock = new object();
        protected static readonly object _cleanupLock = new object();

        // Delegate to wait for resume token setup task for a specific collection
        public Func<string, Task>? WaitForResumeTokenTaskDelegate { get; set; }

        // Global backpressure tracking across ALL collections/processors to prevent OOM
        protected static readonly object _pendingWritesLock = new object();

        // UI update throttling to prevent Blazor rendering OOM (max 1 update per second per collection)
        const int GLOBAL_UI_UPDATE_INTERVAL_MS = 500; // 500ms global throttling across all collections
        protected readonly ConcurrentDictionary<string, DateTime> _lastUIUpdateTime = new ConcurrentDictionary<string, DateTime>();

        protected bool StopProcessing = false;

        private bool _disposed = false;
        protected DateTime _lastGlobalUIUpdate = DateTime.MinValue; // Track last global UI update time for 500ms throttling

        protected Dictionary<string, AccumulatedChangesTracker> _accumulatedChangesPerCollection = new Dictionary<string, AccumulatedChangesTracker>();

        protected void InitializeAccumulatedChangesTracker(string collectionKey)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.InitializeAccumulatedChangesTracker: collectionKey={collectionKey}");
            lock (_accumulatedChangesPerCollection)
            {
                if (!_accumulatedChangesPerCollection.ContainsKey(collectionKey))
                {
                    _accumulatedChangesPerCollection[collectionKey] = new AccumulatedChangesTracker(collectionKey);
                }
            }
        }

        protected bool IsOptimizeForLargeDocsEnabled => _config?.OptimizeForLargeDocs == true;

        protected ChangeStreamFullDocumentOption GetFullDocumentOption()
        {
            var opt = IsOptimizeForLargeDocsEnabled
                ? ChangeStreamFullDocumentOption.Default
                : ChangeStreamFullDocumentOption.UpdateLookup;
            _log.WriteLine($"{_syncBackPrefix}[OFLD] GetFullDocumentOption: OptimizeForLargeDocs={IsOptimizeForLargeDocsEnabled}, FullDocument={opt}", LogType.Debug);
            return opt;
        }

        // Pipeline stage used when OptimizeForLargeDocs is enabled. Strips the two
        // fields that blow up the change-stream event size (fullDocument and
        // updateDescription) so the event stays well under the 16 MB getMore cap,
        // while leaving every other field — including driver-internal ones like
        // wallTime, lsid, txnNumber, and the resume-token _id — untouched.
        // The flush path re-fetches the document body from source before bulk writing.
        protected static readonly BsonDocument OptimizeForLargeDocsProjectStage = new BsonDocument(
            "$unset",
            new BsonArray { "fullDocument", "updateDescription" });

        // Cached reflection accessor for BsonDocumentBackedClass.BackingDocument so we
        // can inject a freshly-fetched fullDocument into an existing event without
        // reconstructing the whole ChangeStreamDocument.
        private static readonly System.Reflection.PropertyInfo? _backingDocumentProperty =
            typeof(MongoDB.Bson.Serialization.BsonDocumentBackedClass)
                .GetProperty(
                    "BackingDocument",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.NonPublic);

        protected static BsonDocument? GetBackingDocument(ChangeStreamDocument<BsonDocument> change)
        {
            return _backingDocumentProperty?.GetValue(change) as BsonDocument;
        }

        /// <summary>
        /// When OptimizeForLargeDocs is enabled, the change-stream events arrive with
        /// no fullDocument (we stripped it via $project to dodge the 16 MB getMore
        /// limit). Re-fetch the current source documents by _id and patch them back
        /// into each insert/update event's backing BSON so the downstream bulk-write
        /// path can keep using <c>change.FullDocument</c> unchanged.
        /// Events whose document no longer exists on the source are dropped — a
        /// subsequent delete event will reconcile the target.
        /// </summary>
        protected async Task HydrateFullDocumentsAsync(
            IMongoCollection<BsonDocument> sourceCollection,
            AccumulatedChangesTracker tracker)
        {
            if (!IsOptimizeForLargeDocsEnabled || sourceCollection == null)
                return;

            int insertCount = tracker.DocsToBeInserted.Count;
            int updateCount = tracker.DocsToBeUpdated.Count;
            int deleteCount = tracker.DocsToBeDeleted.Count;
            _log.WriteLine($"{_syncBackPrefix}[OFLD] HydrateFullDocumentsAsync entry ns={sourceCollection.CollectionNamespace}, inserts={insertCount}, updates={updateCount}, deletes={deleteCount}", LogType.Debug);

            await HydrateBucketAsync(sourceCollection, tracker.DocsToBeInserted, "insert");
            await HydrateBucketAsync(sourceCollection, tracker.DocsToBeUpdated, "update");
        }

        private async Task HydrateBucketAsync(
            IMongoCollection<BsonDocument> sourceCollection,
            Dictionary<string, ChangeStreamDocument<BsonDocument>> bucket,
            string bucketName)
        {
            if (bucket.Count == 0)
                return;

            int initialCount = bucket.Count;
            int eventsMissingDocKey = 0;
            int eventsAlreadyHaveFullDoc = 0;

            // Diagnostic: count how many of the events still have a non-null FullDocument
            // (they shouldn't when projection is active; if they do, the projection isn't taking effect).
            foreach (var kvp in bucket)
            {
                if (kvp.Value?.FullDocument != null && !kvp.Value.FullDocument.IsBsonNull)
                    eventsAlreadyHaveFullDoc++;
            }

            // Group by full DocumentKey so we can issue a single $or query per batch.
            // DocumentKey includes the shard key for sharded collections — using only _id
            // is wrong because the same _id can exist on different shards.
            var distinctDocKeys = new Dictionary<string, BsonDocument>(bucket.Count);
            var keysByDocKeyJson = new Dictionary<string, List<string>>(bucket.Count);
            string[]? docKeyFieldNames = null;
            foreach (var kvp in bucket)
            {
                var dk = kvp.Value?.DocumentKey;
                if (dk == null || dk.ElementCount == 0)
                {
                    eventsMissingDocKey++;
                    continue;
                }

                if (docKeyFieldNames == null)
                {
                    docKeyFieldNames = dk.Names.ToArray();
                }

                var dkJson = dk.ToJson();
                if (!keysByDocKeyJson.TryGetValue(dkJson, out var list))
                {
                    list = new List<string>();
                    keysByDocKeyJson[dkJson] = list;
                    distinctDocKeys[dkJson] = dk;
                }
                list.Add(kvp.Key);
            }

            if (distinctDocKeys.Count == 0)
            {
                _log.WriteLine($"{_syncBackPrefix}[OFLD] HydrateBucket bucket={bucketName} ns={sourceCollection.CollectionNamespace}: no DocumentKeys to fetch (bucket={initialCount}, missingDocKey={eventsMissingDocKey})", LogType.Debug);
                return;
            }

            bool isSharded = docKeyFieldNames != null && docKeyFieldNames.Length > 1;

            Dictionary<string, BsonDocument> fetched;
            var fetchSw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                FilterDefinition<BsonDocument> filter;
                if (distinctDocKeys.Count == 1)
                {
                    var dk = distinctDocKeys.Values.First();
                    filter = BuildDocKeyFilter(dk);
                }
                else
                {
                    var subFilters = distinctDocKeys.Values.Select(BuildDocKeyFilter).ToList();
                    filter = Builders<BsonDocument>.Filter.Or(subFilters);
                }

                var cursor = await sourceCollection.FindAsync(filter);
                var docs = await cursor.ToListAsync();
                fetched = new Dictionary<string, BsonDocument>(docs.Count);
                foreach (var doc in docs)
                {
                    // Project the fetched document down to the DocumentKey shape so
                    // we can match it back to the originating change event(s).
                    var dkShape = ExtractDocKeyShape(doc, docKeyFieldNames!);
                    if (dkShape != null)
                    {
                        fetched[dkShape.ToJson()] = doc;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}[OFLD] HydrateBucket FAILED bucket={bucketName} ns={sourceCollection.CollectionNamespace}, docKeysRequested={distinctDocKeys.Count}, sharded={isSharded}. Details: {ex}", LogType.Error);
                return;
            }
            finally
            {
                fetchSw.Stop();
            }

            int patchedCount = 0;
            int reflectionFailedCount = 0;
            var keysToDrop = new List<string>();
            foreach (var pair in keysByDocKeyJson)
            {
                fetched.TryGetValue(pair.Key, out var doc);
                foreach (var trackerKey in pair.Value)
                {
                    if (!bucket.TryGetValue(trackerKey, out var change) || change == null)
                        continue;

                    if (doc == null)
                    {
                        // Document gone on source — a delete event will reconcile.
                        keysToDrop.Add(trackerKey);
                        continue;
                    }

                    var backing = GetBackingDocument(change);
                    if (backing == null)
                    {
                        // Reflection failed (driver shape changed). Drop the event so
                        // we don't push a write with a null FullDocument.
                        reflectionFailedCount++;
                        keysToDrop.Add(trackerKey);
                        continue;
                    }
                    backing["fullDocument"] = doc;
                    patchedCount++;
                }
            }

            foreach (var k in keysToDrop)
                bucket.Remove(k);

            int droppedMissing = keysToDrop.Count - reflectionFailedCount;
            _log.WriteLine(
                $"{_syncBackPrefix}[OFLD] HydrateBucket done bucket={bucketName} ns={sourceCollection.CollectionNamespace}, bucket={initialCount}, docKeysRequested={distinctDocKeys.Count}, sharded={isSharded}, docKeyFields=[{string.Join(",", docKeyFieldNames ?? Array.Empty<string>())}], fetched={fetched.Count}, patched={patchedCount}, droppedMissingOnSource={droppedMissing}, reflectionFailed={reflectionFailedCount}, alreadyHadFullDoc={eventsAlreadyHaveFullDoc}, missingDocKey={eventsMissingDocKey}, fetchMs={fetchSw.ElapsedMilliseconds}",
                LogType.Debug);

            if (reflectionFailedCount > 0)
            {
                _log.WriteLine($"{_syncBackPrefix}[OFLD] WARNING: BackingDocument reflection returned null for {reflectionFailedCount} event(s) — OptimizeForLargeDocs will drop these; check MongoDB.Driver version compatibility.", LogType.Warning);
            }
            if (eventsAlreadyHaveFullDoc > 0)
            {
                _log.WriteLine($"{_syncBackPrefix}[OFLD] NOTE: {eventsAlreadyHaveFullDoc} event(s) already carried a fullDocument before hydration — the $project stage may not be applied on this stream.", LogType.Warning);
            }
        }

        /// <summary>
        /// Builds a Mongo filter that matches a single document by its full change-stream
        /// DocumentKey. For sharded collections DocumentKey contains <c>{_id, &lt;shardKey...&gt;}</c>
        /// so this filter is shard-aware; for unsharded collections it reduces to <c>{_id: ...}</c>.
        /// </summary>
        private static FilterDefinition<BsonDocument> BuildDocKeyFilter(BsonDocument docKey)
        {
            var fb = Builders<BsonDocument>.Filter;
            var parts = new List<FilterDefinition<BsonDocument>>(docKey.ElementCount);
            foreach (var el in docKey.Elements)
            {
                parts.Add(fb.Eq(el.Name, el.Value));
            }
            return parts.Count == 1 ? parts[0] : fb.And(parts);
        }

        /// <summary>
        /// Extracts a projection of <paramref name="doc"/> containing only the fields that
        /// appear in the change-stream DocumentKey (preserving order), so it can be matched
        /// back to the originating change event via canonical JSON.
        /// </summary>
        private static BsonDocument? ExtractDocKeyShape(BsonDocument doc, string[] docKeyFieldNames)
        {
            var shape = new BsonDocument();
            foreach (var name in docKeyFieldNames)
            {
                if (!doc.TryGetValue(name, out var v))
                    return null;
                shape.Add(name, v);
            }
            return shape;
        }

        /// <summary>
        /// Stops the job immediately by setting flags and calling the migration worker's stop method
        /// </summary>
        protected void StopJob(string reason)
        {
            _log.WriteLine($"{_syncBackPrefix}StopJob called - Reason: {reason}", LogType.Error);
            
            // Set flags to stop processing loops
            StopProcessing = true;
            ExecutionCancelled = true;
            
            // Call the MigrationWorker's stop migration to coordinate shutdown
            try
            {
                if (_migrationWorker is not null)
                {
                    _log.WriteLine($"{_syncBackPrefix}Requesting immediate job termination via MigrationWorker", LogType.Error);
                    _migrationWorker.StopMigration();
                }
                else
                {
                    _log.WriteLine($"{_syncBackPrefix}MigrationWorker reference is null, cannot coordinate shutdown", LogType.Warning);
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error calling MigrationWorker.StopMigration. Details: {ex}", LogType.Error);
            }
        }

        public bool ExecutionCancelled { 
            get => _executionCancelled; 
            set 
            {
                if (_executionCancelled != value)
                {
                    //_log.WriteLine($"{_syncBackPrefix}ExecutionCancelled state change - From: {_executionCancelled}, To: {value}", LogType.Debug);
                    _executionCancelled = value;
                }
            }
        }
        private bool _executionCancelled = false;


        public ChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient,  ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
        {
             _log = log;
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            MigrationJobContext.MigrationUnitsCache= muCache;
            _config = config;
            _syncBack = syncBack;
            _migrationWorker = migrationWorker;
            if (_syncBack)
                _syncBackPrefix = "SyncBack: ";

            _concurrentProcessors = _config?.ChangeStreamMaxCollsInBatch ?? 5;
            _processorRunMaxDurationInSec = _config?.ChangeStreamBatchDuration ?? 120;
            _processorRunMinDurationInSec = _config?.ChangeStreamBatchDurationMin ?? 30;
            StopProcessing = false;

        }

        public bool AddCollectionsToProcess(string migrationUnitId, CancellationTokenSource cts)
        {
            //no checks on what collections can be added, caller is responsible for that                        
            var mu=MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics.FirstOrDefault(mub => mub.Id == migrationUnitId);
            //var mu = MigrationJobContext.GetMigrationUnit(migrationUnitId);
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";

            _log.WriteLine($"{_syncBackPrefix} AddCollectionsToProcess invoked for {key}", LogType.Debug);

            if (!_migrationUnitsToProcess.ContainsKey(mu.Id))
            {
                // Load persisted CSNormalizedUpdatesInLastBatch so sorting is correct after a restart
                long initialValue = 0;
                var fullMu = MigrationJobContext.GetMigrationUnit(mu.Id);
                if (fullMu != null)
                    initialValue = fullMu.CSNormalizedUpdatesInLastBatch;

                _migrationUnitsToProcess.TryAdd(mu.Id, initialValue);

                // Populate reverse lookup for O(1) target namespace resolution
                var targetKey = $"{mu.GetEffectiveTargetDatabaseName()}.{mu.GetEffectiveTargetCollectionName()}";
                _targetNamespaceToUnitId.TryAdd(targetKey, mu.Id);

                // Remove from negative cache if this namespace was previously seen as a miss
                var sourceKey = $"{mu.DatabaseName}.{mu.CollectionName}";
                _namespaceMissCache.TryRemove(sourceKey, out _);
                _namespaceMissCache.TryRemove(targetKey, out _);

                _log.WriteLine($"{_syncBackPrefix}Collection added to change stream queue - Key: {key}, DumpComplete: {mu.DumpComplete}, RestoreComplete: {mu.RestoreComplete}", LogType.Debug);
                _log.ShowInMonitor($"Change stream processor added {mu.DatabaseName}.{mu.CollectionName} to the monitoring queue.");
                return true;
            }
            else
            {
                _log.WriteLine($"{_syncBackPrefix} {key} is already added for change stream  processing.", LogType.Debug);
                return false;
            }
        }

        public async Task RunChangeStreamProcessorForAllCollections(CancellationTokenSource cts)
        {

            lock (_processingLock)
            {
                if (_isCSProcessing)
                {
                    return; //already processing    
                }
                _isCSProcessing = true;
            }

            _log.WriteLine($"{_syncBackPrefix} RunChangeStreamProcessorForAllCollections invoked", LogType.Debug);
            try
            {
                cts = new CancellationTokenSource();
                var token = cts.Token;
                await ProcessChangeStreamsAsync(token);

            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"{_syncBackPrefix}Change stream processing was paused.");
            }
            catch (Exception ex) when (ex is TimeoutException)
            {
                _log.WriteLine($"{_syncBackPrefix}Timeout during change stream processing: {ex}", LogType.Debug);
            }
            catch (Exception ex) 
            {
                _log.WriteLine($"{_syncBackPrefix}Error during change stream processing: {ex}", LogType.Debug);
            }
            finally
            {
                lock (_processingLock)
                {
                    _isCSProcessing = false;
                }
            }
        }

        protected abstract Task ProcessChangeStreamsAsync(CancellationToken token);

        protected MigrationUnit? ResolveMigrationUnitFromNamespace(string databaseName, string collectionName)
        {
            var nsKey = $"{databaseName}.{collectionName}";
            bool useMissCache = ShouldUseNamespaceMissCache();

            if (useMissCache && _namespaceMissCache.ContainsKey(nsKey))
            {
                return null;
            }

            var sourceId = Helper.GenerateMigrationUnitId(databaseName, collectionName);
            if (_migrationUnitsToProcess.ContainsKey(sourceId))
            {
                var mu = MigrationJobContext.GetMigrationUnit(sourceId);
                if (mu != null)
                {
                    mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    return mu;
                }
            }

            // O(1) reverse lookup by target namespace instead of iterating all MigrationUnitBasics
            var targetKey = $"{databaseName}.{collectionName}";
            if (_targetNamespaceToUnitId.TryGetValue(targetKey, out var cachedUnitId))
            {
                if (_migrationUnitsToProcess.ContainsKey(cachedUnitId))
                {
                    var mu = MigrationJobContext.GetMigrationUnit(cachedUnitId);
                    if (mu != null)
                    {
                        mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                        return mu;
                    }
                }
            }

            // Fallback: linear scan for edge cases (e.g., collections added outside AddCollectionsToProcess)
            if (MigrationJobContext.CurrentlyActiveJob?.MigrationUnitBasics == null)
            {
                return null;
            }

            foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
            {
                var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
                if (mu == null)
                {
                    continue;
                }

                if (!_migrationUnitsToProcess.ContainsKey(mu.Id))
                {
                    continue;
                }

                if (string.Equals(mu.GetEffectiveTargetDatabaseName(), databaseName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(mu.GetEffectiveTargetCollectionName(), collectionName, StringComparison.OrdinalIgnoreCase))
                {
                    // Cache for future lookups
                    _targetNamespaceToUnitId.TryAdd(targetKey, mu.Id);
                    _namespaceMissCache.TryRemove(nsKey, out _);
                    mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    return mu;
                }
            }

            // Cache miss to avoid repeated fallback scans for ignored namespaces.
            // Only use this optimization when miss cache remains smaller than tracked namespaces.
            if (useMissCache)
            {
                _namespaceMissCache.TryAdd(nsKey, 0);
            }
            else
            {
                TrimNamespaceMissCacheIfNeeded();
            }

            return null;
        }

        
        protected IMongoCollection<BsonDocument> GetTargetCollection(string databaseName, string collectionName)
        {
            var mu = ResolveMigrationUnitFromNamespace(databaseName, collectionName);

            if (!_syncBack)
            {
                if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    var targetDbName = mu?.GetEffectiveTargetDatabaseName() ?? databaseName;
                    var targetCollectionName = mu?.GetEffectiveTargetCollectionName() ?? collectionName;
                    var targetDb = _targetClient.GetDatabase(targetDbName);
                    return targetDb.GetCollection<BsonDocument>(targetCollectionName);
                }
                else
                {
                    // In simulated runs, use source collection as placeholder
                    var sourceDb = _sourceClient.GetDatabase(databaseName);
                    return sourceDb.GetCollection<BsonDocument>(collectionName);
                }
            }
            else
            {
                // For sync back, target is the source
                var targetDbName = mu?.DatabaseName ?? databaseName;
                var targetCollectionName = mu?.CollectionName ?? collectionName;
                var targetDb = _sourceClient.GetDatabase(targetDbName);
                return targetDb.GetCollection<BsonDocument>(targetCollectionName);
            }
        }

        protected int GetBatchDurationInSeconds(float timeFactor = 1)
        {

            // Calculate batch duration with time factor
            int seconds = (int)(_processorRunMaxDurationInSec * timeFactor);
            if (seconds < _processorRunMinDurationInSec)
                seconds = _processorRunMinDurationInSec; // Ensure at least minimum duration
            return seconds;
        }

        protected DateTime GetChangeTimestampUtc(ChangeStreamDocument<BsonDocument> change)
        {
            if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
            {
                return MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
            }

            if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.WallTime != null)
            {
                return change.WallTime.Value;
            }

            return DateTime.MinValue;
        }

        protected void IncrementFailureCounter(MigrationUnit mu, int incrementBy = 1)
        {

            if (!_syncBack)
                mu.CSErrors = mu.CSErrors + incrementBy;
            else
                mu.SyncBackErrors = mu.SyncBackErrors + incrementBy;
        }

        protected void IncrementSkippedCounter(MigrationUnit mu, int incrementBy = 1)
        {

            if (!_syncBack)
                mu.CSDuplicateDocsSkipped = mu.CSDuplicateDocsSkipped + incrementBy;
            else
                mu.SyncBackDuplicateDocsSkipped = mu.SyncBackDuplicateDocsSkipped + incrementBy;
        }

        protected void IncrementDocCounter(MigrationUnit mu, ChangeStreamOperationType op, int incrementBy = 1)
        {

            if (op == ChangeStreamOperationType.Insert)
            {
                if (!_syncBack)
                    mu.CSDocsInserted = mu.CSDocsInserted + incrementBy;
                else
                    mu.SyncBackDocsInserted = mu.SyncBackDocsInserted + incrementBy;
            }
            else if (op == ChangeStreamOperationType.Update || op == ChangeStreamOperationType.Replace)
            {
                if (!_syncBack)
                    mu.CSDocsUpdated = mu.CSDocsUpdated + incrementBy;
                else
                    mu.SyncBackDocsUpdated = mu.SyncBackDocsUpdated + incrementBy;
            }
            else if (op == ChangeStreamOperationType.Delete)
            {
                if (!_syncBack)
                    mu.CSDocsDeleted = mu.CSDocsDeleted + incrementBy;
                else
                    mu.SyncBackDocsDeleted = mu.SyncBackDocsDeleted + incrementBy;
            }
        }

        protected void IncrementEventCounter(MigrationUnit mu, ChangeStreamOperationType op)
        {

            if (op == ChangeStreamOperationType.Insert)
            {
                if (!_syncBack)
                    mu.CSDInsertEvents++;
                else
                    mu.SyncBackInsertEvents++;
            }
            else if (op == ChangeStreamOperationType.Update || op == ChangeStreamOperationType.Replace)
            {
                if (!_syncBack)
                    mu.CSUpdateEvents++;
                else
                    mu.SyncBackUpdateEvents++;
            }
            else if (op == ChangeStreamOperationType.Delete)
            {
                if (!_syncBack)
                    mu.CSDeleteEvents++;
                else
                    mu.SyncBackDeleteEvents++;
            }
        }

        protected string GetChangeDocumentId(ChangeStreamDocument<BsonDocument> change)
        {
            if (change?.DocumentKey != null && change.DocumentKey.TryGetValue("_id", out var documentId))
            {
                return documentId.ToString();
            }

            return "<missing _id>";
        }

        protected sealed class TempRawWatchSummary
        {
            public long Total { get; set; }
            public long Inserts { get; set; }
            public long Updates { get; set; }
            public long Deletes { get; set; }
            public List<string> InsertIds { get; } = new List<string>();
            public List<string> UpdateIds { get; } = new List<string>();
            public List<string> DeleteIds { get; } = new List<string>();
        }

        protected TempRawWatchSummary CreateTempRawWatchSummary()
        {
            return new TempRawWatchSummary();
        }

        protected void AddTempRawReceivedEvent(TempRawWatchSummary summary, ChangeStreamDocument<BsonDocument> change)
        {
            if (!IsCSWatchLogEnabled || summary == null || change == null)
            {
                return;
            }

            summary.Total++;
            string docId = GetChangeDocumentId(change);

            if (change.OperationType == ChangeStreamOperationType.Insert)
            {
                summary.Inserts++;
                summary.InsertIds.Add(docId);
            }
            else if (change.OperationType == ChangeStreamOperationType.Update || change.OperationType == ChangeStreamOperationType.Replace)
            {
                summary.Updates++;
                summary.UpdateIds.Add(docId);
            }
            else if (change.OperationType == ChangeStreamOperationType.Delete)
            {
                summary.Deletes++;
                summary.DeleteIds.Add(docId);
            }
        }

        private void LogTempIdsInChunks(string prefix, string opName, List<string> ids)
        {
            if (ids == null || ids.Count == 0)
            {
                _log.WriteLine($"{prefix}, {opName}Ids=[]", LogType.Debug);
                return;
            }

            int part = 0;
            int totalParts = (ids.Count + TempLogMaxIdsPerLine - 1) / TempLogMaxIdsPerLine;
            for (int i = 0; i < ids.Count; i += TempLogMaxIdsPerLine)
            {
                part++;
                var chunk = ids.Skip(i).Take(TempLogMaxIdsPerLine);
                _log.WriteLine(
                    $"{prefix}, {opName}Ids part={part}/{totalParts}, count={chunk.Count()}, ids=[{string.Join(", ", chunk)}]",
                    LogType.Debug);
            }
        }

        protected void LogTempRawWatchSummary(string watchScope, string collectionKey, TempRawWatchSummary summary)
        {
            if (!IsCSWatchLogEnabled || summary == null || summary.Total == 0)
            {
                return;
            }

            string prefix = $"{_syncBackPrefix}[cswatch] received raw watch scope={watchScope}, ns={collectionKey}, total={summary.Total}, inserts={summary.Inserts}, updates={summary.Updates}, deletes={summary.Deletes}";
            _log.WriteLine(prefix, LogType.Debug);
            LogTempIdsInChunks(prefix, "insert", summary.InsertIds);
            LogTempIdsInChunks(prefix, "update", summary.UpdateIds);
            LogTempIdsInChunks(prefix, "delete", summary.DeleteIds);
        }

        private void LogTempBatchSummary(
            string stage,
            string collectionKey,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            string? status = null)
        {
            if (!IsCSWatchLogEnabled)
            {
                return;
            }

            int total = insertEvents.Count + updateEvents.Count + deleteEvents.Count;
            if (total == 0)
            {
                return;
            }

            var statusText = string.IsNullOrWhiteSpace(status) ? string.Empty : $", status={status}";
            string prefix = $"{_syncBackPrefix}[cswatch] {stage} batch ns={collectionKey}, total={total}, inserts={insertEvents.Count}, updates={updateEvents.Count}, deletes={deleteEvents.Count}{statusText}";
            _log.WriteLine(prefix, LogType.Debug);

            var insertIds = insertEvents.Select(GetChangeDocumentId).Where(id => !string.IsNullOrEmpty(id)).ToList();
            var updateIds = updateEvents.Select(GetChangeDocumentId).Where(id => !string.IsNullOrEmpty(id)).ToList();
            var deleteIds = deleteEvents.Select(GetChangeDocumentId).Where(id => !string.IsNullOrEmpty(id)).ToList();

            LogTempIdsInChunks(prefix, "insert", insertIds);
            LogTempIdsInChunks(prefix, "update", updateIds);
            LogTempIdsInChunks(prefix, "delete", deleteIds);
        }

        

        protected async Task<int> BulkProcessChangesAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            AccumulatedChangesTracker accumulatedChangesInColl,
            int batchSize = 50)
        {
            
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

            LogTempBatchSummary("received", collectionKey, insertEvents, updateEvents, deleteEvents);

            if ((insertEvents.Count + updateEvents.Count + deleteEvents.Count) > 0)
                _log.ShowInMonitor($"{_syncBackPrefix}Flushing Changes for Collection: {collectionKey}, Events: {accumulatedChangesInColl.TotalEventCount}, Inserts: {insertEvents.Count}, Updates: {updateEvents.Count}, Deletes: {deleteEvents.Count}, BatchSize: {batchSize}");

            CounterDelegate<MigrationUnit> counterDelegate = (migrationUnit, counterType, operationType, count) =>
            {
                switch (counterType)
                {
                    case CounterType.Processed:
                        if (operationType.HasValue)
                        {
                            IncrementDocCounter(migrationUnit, operationType.Value, count);
                        }
                        break;
                    case CounterType.Skipped:
                        IncrementSkippedCounter(migrationUnit, count);
                        break;
                }
            };

            try
            {
                // Get context for aggressive change stream functionality
                bool isAggressive = MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive;
                bool isAggressiveComplete = mu.RestoreComplete;
                string jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty;
                bool isSimulatedRun = MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun;

                // Use ParallelWriteHelper for improved performance with retry logic
                var parallelWriteHelper = new ParallelWriteHelper(_log, _syncBackPrefix);

                var result = await parallelWriteHelper.ProcessWritesAsync(
                    mu,
                    collection,
                    insertEvents,
                    updateEvents,
                    deleteEvents,
                    counterDelegate,
                    batchSize,
                    isAggressive,
                    isAggressiveComplete,
                    jobId,
                    _targetClient,
                    isSimulatedRun);

                // Track write latency directly in AccumulatedChangesTracker
                accumulatedChangesInColl.CSTotaWriteDurationInMS += result.WriteLatencyMS;

                if (!result.Success)
                {
                    IncrementFailureCounter(mu, result.Failures);
                    _log.WriteLine($"{_syncBackPrefix}Bulk processing had {result.Failures} failures for {collectionKey}. Failed DocumentKeys: [{string.Join(", ", result.FailedDocumentKeys.Take(50))}]{(result.FailedDocumentKeys.Count > 50 ? $"... (+{result.FailedDocumentKeys.Count - 50} more)" : "")}", LogType.Error);
                    
                    // If there were critical errors that would cause data loss, stop the job
                    if (result.Errors.Any(e => e.Contains("CRITICAL")))
                    {
                        var criticalError = result.Errors.First(e => e.Contains("CRITICAL"));
                        _log.WriteLine($"{_syncBackPrefix}Stopping job due to critical error: {criticalError}", LogType.Error);
                        throw new InvalidOperationException(criticalError);
                    }
                }
                else if (result.Failures > 0)
                {
                    LogTempBatchSummary("flushed", collectionKey, insertEvents, updateEvents, deleteEvents, $"non-critical-failures={result.Failures}");
                    IncrementFailureCounter(mu, result.Failures);
                    _log.WriteLine($"{_syncBackPrefix}Bulk processing had {result.Failures} non-critical failures for {collectionKey}. Failed DocumentKeys: [{string.Join(", ", result.FailedDocumentKeys.Take(50))}]{(result.FailedDocumentKeys.Count > 50 ? $"... (+{result.FailedDocumentKeys.Count - 50} more)" : "")}", LogType.Error);
                }
                else
                {
                    // success - no action needed
                }

                return result.Failures;
                
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL") && ex.Message.Contains("persistent deadlock"))
            {
                // Critical deadlock failure - re-throw to stop the job and prevent data loss
                _log.WriteLine($"{_syncBackPrefix}Stopping job due to persistent deadlock that would cause data loss. Details: {ex}", LogType.Error);
                throw; // Re-throw to stop the entire migration job
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
            {
                // Critical error that would cause data loss - re-throw to stop the job
                _log.WriteLine($"{_syncBackPrefix}Stopping job due to critical error that would cause data loss. Details: {ex}", LogType.Error);
                throw; // Re-throw to stop the entire migration job
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operations for {collection.CollectionNamespace.FullName}. Details: {ex}", LogType.Error);
                throw; // Re-throw all exceptions to ensure they are handled upstream
            }
        }

        protected async Task AggressiveCSCleanupAsync()
        {
            MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.AggressiveCSCleanupAsync invoked");

            //agrressive cleanup is complete
            if (_finalCleanupExecuted)
                return;

            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive)
                return;

            foreach (var muId in _migrationUnitsToProcess.Keys)
            {                
                var mu = MigrationJobContext.GetMigrationUnit(muId);

                MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.AggressiveCSCleanupAsync Checking {mu.DatabaseName}.{mu.CollectionName}, AggressiveCacheDeleted={mu?.AggressiveCacheDeleted}, RestoreComplete={mu?.RestoreComplete}");

                if (mu != null && mu.RestoreComplete && !mu.AggressiveCacheDeleted)
                {
                    MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.AggressiveCSCleanupAsync, CleanupAggressiveCSForCollectionAsync invoked for {mu.DatabaseName}.{mu.CollectionName} ");
                    await CleanupAggressiveCSForCollectionAsync(mu);
                }
            }

            if(Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) )
            {
                MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.AggressiveCSCleanupAsync, CleanupAggressiveTempDBAsync invoked as offline job is completed.");
                await CleanupAggressiveTempDBAsync();
                _finalCleanupExecuted = true;
            }
        }

        private async Task CleanupAggressiveCSForCollectionAsync(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.CleanupAggressiveCSForCollectionAsync: muId={mu.Id}, collection={mu.DatabaseName}.{mu.CollectionName}, RestoreComplete={mu.RestoreComplete}");

            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive || !mu.RestoreComplete || MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                return;

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

            // Check if cleanup has already been processed for this collection
            lock (_cleanupLock)
            {
                if (mu.AggressiveCacheDeleted || _aggressiveCleanupProcessed.ContainsKey(collectionKey))
                {
                    _log.WriteLine($"Aggressive change stream cleanup already processed for {collectionKey}", LogType.Debug);
                    return;
                }

                // Mark as being processed to prevent concurrent execution
                _aggressiveCleanupProcessed.TryAdd(collectionKey, true);
            }

            try
            {
                var aggressiveHelper = new AggressiveChangeStreamHelper(_targetClient, _log, MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty);
                var targetDatabaseName = _syncBack ? mu.DatabaseName : mu.GetEffectiveTargetDatabaseName();
                var targetCollectionName = _syncBack ? mu.CollectionName : mu.GetEffectiveTargetCollectionName();

                _log.WriteLine($"Processing aggressive change stream cleanup for {mu.DatabaseName}.{mu.CollectionName}");

                // First, apply stored inserts and updates
                var (inserted, updated, skipped) = await aggressiveHelper.ApplyStoredChangesAsync(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName, _sourceClient);
                
                if (inserted > 0 || updated > 0)
                {
                    // Update counters
                    if (!_syncBack)
                    {
                        mu.CSDocsInserted += inserted;
                        mu.CSDocsUpdated += updated;
                    }
                    else
                    {
                        mu.SyncBackDocsInserted += inserted;
                        mu.SyncBackDocsUpdated += updated;
                    }

                    _log.WriteLine($"Aggressive change stream applied changes for {mu.DatabaseName}.{mu.CollectionName}: {inserted} inserted, {updated} updated, {skipped} skipped");
                }

                // Then, process deletes
                long deletedCount = await aggressiveHelper.DeleteStoredDocsAsync(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

                // Mark cleanup as completed
                mu.AggressiveCacheDeleted = true;
                mu.AggressiveCacheDeletedOn = DateTime.UtcNow;
                

                // retry deletion in case some documents were added during the first deletion pass
                deletedCount += await aggressiveHelper.DeleteStoredDocsAsync(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

                if (deletedCount > 0)
                {
                    // Update counters
                    if (!_syncBack)
                        mu.CSDocsDeleted += deletedCount;
                    else
                        mu.SyncBackDocsDeleted += deletedCount;

                    _log.WriteLine($"Aggressive change stream cleanup completed for {mu.DatabaseName}.{mu.CollectionName}: {deletedCount} documents deleted");
                }
                else
                {
                    _log.WriteLine($"Aggressive change stream cleanup completed for {mu.DatabaseName}.{mu.CollectionName}: No documents to delete");
                }
                // Save the updated state
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error during aggressive change stream cleanup for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);

                // Remove from processing cache to allow retry
                lock (_cleanupLock)
                {
                    _aggressiveCleanupProcessed.TryRemove(collectionKey, out _);
                }
            }
        }

        public async Task CleanupAggressiveTempDBAsync()
        {
            // Final cleanup of any remaining temp collections
            try
            {
                var aggressiveHelper = new AggressiveChangeStreamHelper(_targetClient, _log, MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty);
                await aggressiveHelper.CleanupTempDatabaseAsync();
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error during final aggressive change stream cleanup. Details: {ex}", LogType.Error);
            }

            _log.WriteLine($"Aggressive change stream cleanup completed.");
        }

        protected bool ShowInMonitor(ChangeStreamDocument<BsonDocument> change, string collNameSpace, DateTime timeStamp, long counter, bool ignoredServerLevelChange=false)
        {
            DateTime now = DateTime.UtcNow;
            bool shouldUpdateUI = false;

            // Check if enough time has passed since last global UI update (500ms throttling across all collections)
            if (_lastGlobalUIUpdate == DateTime.MinValue ||
                (now - _lastGlobalUIUpdate).TotalMilliseconds >= GLOBAL_UI_UPDATE_INTERVAL_MS)
            {
                // Update the global last UI update time and allow UI update
                _lastGlobalUIUpdate = now;
                shouldUpdateUI = true;
            }

            // Show on monitor only if global UI update is allowed (once per 500ms), but always log to file
            if (shouldUpdateUI)
            {
                if(ignoredServerLevelChange)
                {
                     _log.ShowInMonitor($"{_syncBackPrefix}Ignored server-level change: {change.OperationType} operation in {collNameSpace} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                    return shouldUpdateUI;

                }

                if (timeStamp == DateTime.MinValue)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                }
                else
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}. Lag: {Helper.GetTimestampDiff(timeStamp)}");
                }
            }

            return shouldUpdateUI;
        }
        
        #region IDisposable Implementation
        
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                // Dispose managed resources - override in derived classes if needed
                ExecutionCancelled = true;
                _disposed = true;
            }
        }

        ~ChangeStreamProcessor()
        {
            Dispose(false);
        }

        #endregion
    }
}
#endif // !LEGACY_MONGODB_DRIVER
