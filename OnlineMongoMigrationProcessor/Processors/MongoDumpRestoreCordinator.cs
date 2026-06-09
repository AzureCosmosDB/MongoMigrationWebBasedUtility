using MongoDB.Bson;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

// CS4014: Use explicit discards for intentional fire-and-forget tasks.

namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// MongoDumpRestoreCordinator provides centralized static timer-based control over MongoDB dump and restore operations
    /// across multiple migration units. This static coordinator ensures all migration units share the same coordination infrastructure.
    /// 
    /// Architecture:
    ///    - Download Manifest: Tracks chunks pending dump across all migration units
    ///    - Upload Manifest: Tracks chunks pending restore across all migration units
    ///    - Migration Unit Tracker: Tracks overall progress per collection
    ///    - Timer: Polls every 2 seconds to process pending work across all units
    /// 
    /// Benefits:
    ///    - Centralized coordination across multiple migration units
    ///    - Automatic retry logic with configurable limits (max 3 retries)
    ///    - Respects WorkerPoolManager capacity automatically
    ///    - Pause/resume friendly with timer-based processing
    ///    - Clean progress tracking and error handling
    /// 
    /// Example Usage:
    /// <code>
    /// MongoDumpRestoreCordinator.Initialize(jobId, log);
    /// MongoDumpRestoreCordinator.EnqueueMigrationUnit(mu, sourceConn, targetConn);
    /// var stats = MongoDumpRestoreCordinator.GetCoordinatorStats();
    /// </code>
    /// </summary>
    internal class MongoDumpRestoreCordinator
    {
        /// <summary>
        /// Delegate for notifying when a migration unit completes dump/restore processing
        /// </summary>
        public delegate void MigrationUnitCompletedHandler(MigrationUnit mu);
        public delegate void PendingTasksCompletedHandler();

        private readonly object _initLock = new object();
        private string? _jobId;
        private Log? _log;
        private string? _mongoToolsFolder;
        private string? _mongoDumpToolPath;
        private string? _mongoRestoreToolPath;
        private string _processorRunId = string.Empty;

        private string _mongoDumpOutputFolder = Path.Combine(Helper.GetWorkingFolder(), "mongodump");
        private readonly SemaphoreSlim _uploadLock = new(1, 1);

        // Worker pool references (shared across all migration units)
        private WorkerPoolManager? _dumpPool;
        private WorkerPoolManager? _restorePool;

        // Thread-safe locks
        private readonly object _pidLock = new object();
        private readonly object _timerLock = new object();
        private readonly object _diskSpaceCheckLock = new object();
        private readonly object _workingSetLock = new object();

        // Bound the in-memory active manifest so per-context lookups stay O(1) and memory stays flat on jobs with thousands of chunks; overflow lands in the backlog queue.
        private const int MaxManifestWorkingSetSize = 200;
        // Hard cap prevents mongodump query timeouts and OOM on slow/unstable sources; oversize chunks are auto-split before dispatch.
        private const long AbsoluteMaxDocsPerChunk = 25000000;
        // Below this size the dynamic per-MU threshold is skipped — splitting tiny chunks further usually hurts throughput more than a count timeout would.
        private const long DynamicThresholdMinChunkDocs = 5000000;
        private const int MaxActiveMigrationUnitsWorkingSetSize = 200;

        // Coordinated processing infrastructure (shared across all migration units)
        private readonly ConcurrentDictionary<string, DumpRestoreProcessContext> _downloadManifest = new();
        private readonly ConcurrentDictionary<string, DumpRestoreProcessContext> _uploadManifest = new();
        private readonly ConcurrentDictionary<string, MigrationUnitTracker> _activeMigrationUnits = new();
        private readonly ConcurrentQueue<DumpRestoreProcessContext> _downloadBacklog = new();
        private readonly ConcurrentQueue<DumpRestoreProcessContext> _uploadBacklog = new();
        private readonly ConcurrentDictionary<string, byte> _downloadBacklogIndex = new();
        private readonly ConcurrentDictionary<string, byte> _uploadBacklogIndex = new();
        private readonly ConcurrentQueue<ProcessorContext> _pendingMigrationUnits = new();
        private readonly ConcurrentDictionary<string, byte> _pendingMigrationUnitIndex = new();
        // Track all spawned worker tasks so StopCoordinatedProcessing can await them.
        private readonly ConcurrentBag<Task> _activeWorkerTasks = new();
        private System.Timers.Timer? _processTimer;
        private readonly int _timerIntervalMs = 2000; // Check every 2 seconds
        private bool _coordinatorInitialized = false;
        private bool _timerStarted = false;
        private CancellationTokenSource? _processCts;
        private MigrationUnitCompletedHandler? _onMigrationUnitCompleted;
        private PendingTasksCompletedHandler? _onPendingTasksCompleted;

        private bool _processNewTasks = true;
        private volatile bool _stopped = false; // Volatile flag to prevent queued timer callbacks from executing after stop

        // Cached duplicate settings (read once at Initialize)
        private bool _ignoreDuplicatesAndContinueRestore = false;
        private TimeSpan _continuousDuplicateThreshold = TimeSpan.FromMinutes(5);

        private DateTime _downLoadPausedTill = DateTime.MinValue;
        private DateTime _lastDiskSpaceCheckedAtUtc = DateTime.MinValue;
        private bool _lastDiskSpaceCheckResult = true;
        private const int DiskSpaceCheckCacheSeconds = 30;

        // Per-chunk merge lock: prevents concurrent merge/cleanup on the same chunk when a
        // paused worker's finally-block DROP races with a newly-dispatched worker's CREATE.
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _chunkMergeLocks = new();

        private MongoDumpRestoreBehavior GetMongoDumpRestoreBehavior()
        {
            var config = new MigrationSettings();
            config.Load();
            return config.MongoDumpRestoreBehavior;
        }

        private static void ClearQueue<T>(ConcurrentQueue<T> queue)
        {
            while (queue.TryDequeue(out _)) { }
        }

        private bool TryQueuePendingMigrationUnit(ProcessorContext context)
        {
            if (string.IsNullOrEmpty(context.MigrationUnitId))
                return false;

            lock (_workingSetLock)
            {
                if (!_pendingMigrationUnitIndex.TryAdd(context.MigrationUnitId, 0))
                    return false;

                _pendingMigrationUnits.Enqueue(context);
                return true;
            }
        }

        private bool TryAddDownloadContext(DumpRestoreProcessContext context)
        {
            lock (_workingSetLock)
            {
                if (_downloadManifest.ContainsKey(context.Id) || _downloadBacklogIndex.ContainsKey(context.Id))
                    return false;

                if (_downloadManifest.Count < MaxManifestWorkingSetSize)
                    return _downloadManifest.TryAdd(context.Id, context);

                _downloadBacklog.Enqueue(context);
                _downloadBacklogIndex[context.Id] = 0;
                return true;
            }
        }

        private bool TryAddUploadContext(DumpRestoreProcessContext context)
        {
            lock (_workingSetLock)
            {
                if (_uploadManifest.ContainsKey(context.Id) || _uploadBacklogIndex.ContainsKey(context.Id))
                    return false;

                if (_uploadManifest.Count < MaxManifestWorkingSetSize)
                    return _uploadManifest.TryAdd(context.Id, context);

                _uploadBacklog.Enqueue(context);
                _uploadBacklogIndex[context.Id] = 0;
                return true;
            }
        }

        private void RefillDownloadWorkingSet()
        {
            lock (_workingSetLock)
            {
                while (_downloadManifest.Count < MaxManifestWorkingSetSize && _downloadBacklog.TryDequeue(out var context))
                {
                    _downloadBacklogIndex.TryRemove(context.Id, out _);

                    if (_downloadManifest.ContainsKey(context.Id))
                        continue;

                    _downloadManifest.TryAdd(context.Id, context);
                }
            }
        }

        private void RefillUploadWorkingSet()
        {
            lock (_workingSetLock)
            {
                while (_uploadManifest.Count < MaxManifestWorkingSetSize && _uploadBacklog.TryDequeue(out var context))
                {
                    _uploadBacklogIndex.TryRemove(context.Id, out _);

                    if (_uploadManifest.ContainsKey(context.Id))
                        continue;

                    _uploadManifest.TryAdd(context.Id, context);
                }
            }
        }

        private void RefillManifestWorkingSets()
        {
            RefillDownloadWorkingSet();
            RefillUploadWorkingSet();
        }

        private bool TryStartMigrationUnit(ProcessorContext ctx)
        {
            var mu = MigrationJobContext.GetMigrationUnit(ctx.MigrationUnitId);

            if (mu == null)
            {
                throw new InvalidOperationException($"MigrationUnit {ctx.MigrationUnitId} not found in context");
            }

            if (mu.MigrationChunks == null || mu.MigrationChunks.Count == 0)
            {
                _log?.WriteLine($"Cannot start coordinated process for {mu.DatabaseName}.{mu.CollectionName} - no chunks available (may have failed during partitioning)", LogType.Warning);
                return false;
            }

            var tracker = new MigrationUnitTracker
            {
                MigrationUnitId = ctx.MigrationUnitId,
                TotalChunks = mu.MigrationChunks.Count,
                DownloadedChunks = mu.MigrationChunks.Count(c => c.IsDownloaded == true),
                RestoredChunks = mu.MigrationChunks.Count(c => c.IsUploaded == true),
                AddedAt = DateTime.UtcNow
            };

            bool trackerAdded;
            lock (_workingSetLock)
            {
                if (_activeMigrationUnits.ContainsKey(mu.Id))
                    return true;

                if (_activeMigrationUnits.Count >= MaxActiveMigrationUnitsWorkingSetSize)
                {
                    TryQueuePendingMigrationUnit(ctx);
                    return false;
                }

                trackerAdded = _activeMigrationUnits.TryAdd(mu.Id, tracker);
            }

            if (!trackerAdded)
                return false;

            _log?.WriteLine($"Started coordinated processing for {mu.DatabaseName}.{mu.CollectionName} " +
                          $"(Downloaded: {tracker.DownloadedChunks}/{tracker.TotalChunks}, " +
                          $"Restored: {tracker.RestoredChunks}/{tracker.TotalChunks})", LogType.Info);
            _log?.WriteLine($"TryStartMigrationUnit: SEED tracker muId={mu.Id} ({mu.DatabaseName}.{mu.CollectionName}) total={tracker.TotalChunks}, seedDownloaded={tracker.DownloadedChunks}, seedRestored={tracker.RestoredChunks}", LogType.Debug);

            PrepareDownloadList(mu, ctx.SourceConnectionString, ctx.TargetConnectionString);
            PrepareRestoreList(mu, ctx.SourceConnectionString, ctx.TargetConnectionString);
            RefillManifestWorkingSets();

            return true;
        }

        private void TryActivatePendingMigrationUnits()
        {
            if (!_processNewTasks || MigrationJobContext.ControlledPauseRequested)
                return;

            while (true)
            {
                ProcessorContext? queuedContext = null;

                lock (_workingSetLock)
                {
                    // Bound the active tracker so memory stays flat; queued MUs get retried on the next timer tick after slots free up.
                    if (_activeMigrationUnits.Count >= MaxActiveMigrationUnitsWorkingSetSize)
                        break;

                    if (!_pendingMigrationUnits.TryDequeue(out var nextContext))
                        break;

                    _pendingMigrationUnitIndex.TryRemove(nextContext.MigrationUnitId, out _);

                    if (_activeMigrationUnits.ContainsKey(nextContext.MigrationUnitId))
                        continue;

                    queuedContext = nextContext;
                }

                if (queuedContext == null)
                    continue;

                try
                {
                    TryStartMigrationUnit(queuedContext);
                }
                catch (Exception ex)
                {
                    _log?.WriteLine($"Error activating queued migration unit {queuedContext.MigrationUnitId}: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                }
            }
        }

        // Work item class for chunk processing
        private class ChunkWorkItem : IComparable<ChunkWorkItem>
        {
            public int ChunkIndex { get; set; }
            public MigrationUnit MigrationUnit { get; set; } = null!;
            public MigrationChunk Chunk { get; set; } = null!;
            public DateTime QueuedAt { get; set; }

            public int CompareTo(ChunkWorkItem? other)
            {
                if (other == null) return 1;
                int result = ChunkIndex.CompareTo(other.ChunkIndex);
                if (result == 0)
                    return QueuedAt.CompareTo(other.QueuedAt);
                return result;
            }
        }

        // Migration unit tracker for coordinated processing
        private class MigrationUnitTracker
        {
            public string MigrationUnitId { get; set; } = null!;
            public int TotalChunks;  // Field instead of property for Interlocked.Add support
            public int DownloadedChunks;  // Field instead of property for Interlocked.Add support
            public int RestoredChunks;  // Field instead of property for Interlocked.Add support
            public DateTime AddedAt { get; set; }
            public bool AllDownloadsCompleted => DownloadedChunks >= TotalChunks;
            public bool AllRestoresCompleted => RestoredChunks >= TotalChunks;
        }

        /// <summary>
        /// Initializes the static coordinator with job-specific configuration
        /// </summary>
        /// <param name="jobId">The migration job ID</param>
        /// <param name="log">Logger instance</param>
        /// <param name="mongoToolsFolder">Optional path to mongo tools folder</param>
        /// <param name="onMigrationUnitCompleted">Optional callback invoked when a migration unit completes</param>
        public void Initialize(string jobId, Log log, string? mongoToolsFolder = null, string? mongoDumpToolPath = null, string? mongoRestoreToolPath = null, string? processorRunId = null, MigrationUnitCompletedHandler? onMigrationUnitCompleted = null, PendingTasksCompletedHandler? onPendingTasksCompleted=null)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.Initialize: jobId={jobId}, mongoToolsFolder={mongoToolsFolder}");
            try
            {
                Reset();

                lock (_initLock)
                {
                    if (_coordinatorInitialized)
                        return;

                    _jobId = jobId;
                    _log = log;
                    _mongoToolsFolder = mongoToolsFolder;
                    _mongoDumpToolPath = mongoDumpToolPath;
                    _mongoRestoreToolPath = mongoRestoreToolPath;
                    // Empty string (not null) keeps PrepareDownloadList/PrepareRestoreList free of null-coalesce on every context they create.
            _processorRunId = string.IsNullOrWhiteSpace(processorRunId) ? string.Empty : processorRunId;
                    _onMigrationUnitCompleted = onMigrationUnitCompleted;
                    _onPendingTasksCompleted = onPendingTasksCompleted;
                    _processNewTasks = true;
                    _stopped = false;

                    // Resolve worker counts (use persisted value if set, otherwise auto-calculate)
                    int maxDumpWorkers, maxRestoreWorkers;
                    if (MigrationJobContext.CurrentlyActiveJob.EnableParallelProcessing)
                    {
                        maxDumpWorkers = WorkerCountHelper.CalculateOptimalConcurrency(
                            MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers,
                            isDump: true
                        );

                        maxRestoreWorkers = WorkerCountHelper.CalculateOptimalConcurrency(
                            MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers,
                            isDump: false
                        );

                        log.WriteLine($"Resolved dump concurrency: {maxDumpWorkers},  restore concurrency: {maxRestoreWorkers}", LogType.Info);
                    }
                    else
                    {
                        maxDumpWorkers = 1;
                        maxRestoreWorkers = 1;
                    }

                    var behavior = GetMongoDumpRestoreBehavior();
                    if (behavior == MongoDumpRestoreBehavior.DumpOnly)
                    {
                        maxRestoreWorkers = 0;
                        log.WriteLine($"MongoDumpRestoreBehavior=DumpOnly. Running in dump-only mode", LogType.Warning);
                    }
                    else if (behavior == MongoDumpRestoreBehavior.RestoreOnly)
                    {
                        maxDumpWorkers = 0;
                        log.WriteLine($"MongoDumpRestoreBehavior=RestoreOnly. Running in restore-only mode", LogType.Warning);
                    }

                    // Get or create shared worker pools
                    _dumpPool = WorkerPoolCoordinator.GetOrCreateDumpPool(jobId, log, maxDumpWorkers);
                    _restorePool = WorkerPoolCoordinator.GetOrCreateRestorePool(jobId, log, maxRestoreWorkers);

                    // Store initial values in MigrationJobContext.CurrentlyActiveJob for UI monitoring
                    MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers = maxDumpWorkers;
                    MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers = maxRestoreWorkers;

                    if (maxDumpWorkers == 0)
                    {
                        _log?.WriteLine("Dump is paused", LogType.Warning);
                    }

                    if (maxRestoreWorkers == 0)
                    {
                        _log?.WriteLine("Restore is paused", LogType.Warning);
                    }

                    if (!MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers.HasValue)
                    {
                        MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = WorkerCountHelper.CalculateDefaultInsertionWorkers();
                    }
                    else
                    {
                        MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = WorkerCountHelper.ValidateWorkerCount(
                            MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers.Value
                        );
                    }

                    // Initialize cancellation token source
                    _processCts = new CancellationTokenSource();

                    // Initialize timer
                    _processTimer = new System.Timers.Timer(_timerIntervalMs);
                    _processTimer.Elapsed += OnTimerTick;
                    _processTimer.AutoReset = true;

                    _coordinatorInitialized = true;

                    // Cache duplicate settings once
                    try
                    {
                        var dupSettings = new MigrationSettings();
                        dupSettings.Load();
                        _ignoreDuplicatesAndContinueRestore = dupSettings.IgnoreDuplicatesAndContinueRestore;
                        _continuousDuplicateThreshold = dupSettings.ContinuousDuplicateThresholdInSeconds > 0
                            ? TimeSpan.FromSeconds(dupSettings.ContinuousDuplicateThresholdInSeconds)
                            : TimeSpan.FromMinutes(5);
                    }
                    catch
                    {
                        _ignoreDuplicatesAndContinueRestore = false;
                        _continuousDuplicateThreshold = TimeSpan.FromMinutes(5);
                    }

                    // Reset any previously skipped collections to allow retry on job start/resume
                    ResetSkippedCollectionFlags();

                    log.WriteLine($"MongoDumpRestore Cordinator initialized", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                log?.WriteLine($"Error initializing MongoDumpRestoreCordinator: {ex}", LogType.Error);
                throw;
            }
        }

        /// <summary>
        /// Resets the SkippedDueToMaxRetries flag on all collections to allow retry on job restart.
        /// Call this when starting or resuming a job to enable retrying previously failed collections.
        /// </summary>
        public void ResetSkippedCollectionFlags()
        {
            MigrationJobContext.AddVerboseLog("MongoDumpRestoreCordinator.ResetSkippedCollectionFlags: resetting skip flags on all collections");
            try
            {
                if (MigrationJobContext.CurrentlyActiveJob == null || MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics == null)
                {
                    return;
                }

                _log?.WriteLine("Resetting skip flags on all collections", LogType.Info);

                int resetCount = 0;
                foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
                {
                    if (mub.SkippedDueToMaxRetries)
                    {
                        mub.SkippedDueToMaxRetries = false;
                        mub.FailedOperation = null;
                        resetCount++;
                        _log?.WriteLine($"Reset skip flag for {mub.DatabaseName}.{mub.CollectionName}", LogType.Info);
                        
                        // Atomically reset attempts and skip flags
                        MigrationJobContext.MutateMigrationUnit(mub.Id, m =>
                        {
                            foreach (var chunk in m.MigrationChunks)
                            {
                                chunk.Attempt = 0;
                            }
                            m.SkippedDueToMaxRetries = false;
                            m.FailedOperation = null;
                        }, updateParent: true);
                    }
                }

                if (resetCount > 0)
                {
                    _log?.WriteLine($"Reset SkippedDueToMaxRetries flag on {resetCount} collection(s)", LogType.Info);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error resetting skipped collection flags: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime. Thread-safe.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.AdjustDumpWorkers: newCount={newCount}");
            try
            {
                if (_dumpPool == null)
                {
                    _log?.WriteLine("Dump pool not initialized - cannot adjust workers");
                    return;
                }

                if (GetMongoDumpRestoreBehavior() == MongoDumpRestoreBehavior.RestoreOnly)
                {
                    _log?.WriteLine("MongoDumpRestoreBehavior=RestoreOnly. Dump workers forced to 0", LogType.Warning);
                    newCount = 0;
                }

                int validatedCount = WorkerCountHelper.ValidateDumpRestoreWorkerCount(newCount);
                _dumpPool.AdjustPoolSize(validatedCount);

                // Update current value in context for UI monitoring and persist
                MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers = validatedCount;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

                if (validatedCount == 0)
                {
                    _log?.WriteLine("Dump is paused", LogType.Warning);
                }

                _log?.WriteLine($"Dump workers adjusted to {validatedCount}");
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error adjusting dump workers: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime. Thread-safe.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.AdjustRestoreWorkers: newCount={newCount}");
            try
            {
                if (_restorePool == null)
                {
                    _log?.WriteLine("Restore pool not initialized - cannot adjust workers");
                    return;
                }

                if (GetMongoDumpRestoreBehavior() == MongoDumpRestoreBehavior.DumpOnly)
                {
                    _log?.WriteLine("MongoDumpRestoreBehavior=DumpOnly. Restore workers forced to 0", LogType.Warning);
                    newCount = 0;
                }

                int validatedCount = WorkerCountHelper.ValidateDumpRestoreWorkerCount(newCount);
                _restorePool.AdjustPoolSize(validatedCount);

                // Update current value in context for UI monitoring and persist
                MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers = validatedCount;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

                if (validatedCount == 0)
                {
                    _log?.WriteLine("Restore is paused", LogType.Warning);
                }

                _log?.WriteLine($"Restore workers adjusted to {validatedCount}");
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error adjusting restore workers: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.AdjustInsertionWorkers: newCount={newCount}");
            try
            {
                WorkerCountHelper.AdjustInsertionWorkers(newCount, _log);
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error adjusting insertion workers: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        #region Coordinated Dump/Restore Infrastructure

        /// <summary>
        /// Gets coordinator statistics for monitoring
        /// </summary>
        public (int pendingDownloads, int pendingRestores, int activeMUs) GetCoordinatorStats()
        {
            MigrationJobContext.AddVerboseLog("MongoDumpRestoreCordinator.GetCoordinatorStats: called");
            try
            {
                return (
                    _downloadManifest.Count(kvp => kvp.Value.State == ProcessState.Pending) + _downloadBacklogIndex.Count,
                    _uploadManifest.Count(kvp => kvp.Value.State == ProcessState.Pending) + _uploadBacklogIndex.Count,
                    _activeMigrationUnits.Count
                );
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error getting coordinator stats: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return (0, 0, 0);
            }
        }

        /// <summary>
        /// Checks if the entire job (all migration units) has completed dump/restore processing
        /// </summary>
        /// <returns>True if all migration units are complete, false otherwise</returns>
        public bool IsJobComplete()
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.IsJobComplete: activeMUs count={_activeMigrationUnits.Count}");
            try
            {
                return _activeMigrationUnits.IsEmpty &&
                       _pendingMigrationUnitIndex.IsEmpty &&
                       _downloadManifest.IsEmpty &&
                       _uploadManifest.IsEmpty &&
                       _downloadBacklogIndex.IsEmpty &&
                       _uploadBacklogIndex.IsEmpty;
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error checking job completion: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return false;
            }
        }

        /// <summary>
        /// Checks if a specific migration unit has completed dump/restore processing
        /// </summary>
        /// <param name="migrationUnitId">The ID of the migration unit to check</param>
        /// <returns>True if the migration unit is complete (not in active tracking), false otherwise</returns>
        public bool IsMigrationUnitCompleted(string migrationUnitId)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.IsMigrationUnitCompleted: migrationUnitId={migrationUnitId}");
            try
            {
                if (_activeMigrationUnits.ContainsKey(migrationUnitId))
                    return false;

                if (_pendingMigrationUnitIndex.ContainsKey(migrationUnitId))
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error checking migration unit completion: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return false;
            }
        }

        /// <summary>
        /// Resets all static state to prepare for a new job.
        /// IMPORTANT: Call this before starting a new migration job to prevent leftover state from previous jobs.
        /// </summary>
        public void Reset()
        {
            MigrationJobContext.AddVerboseLog("MongoDumpRestoreCordinator.Reset: resetting coordinator");
            try
            {
                lock (_initLock)
                {
                    _log?.WriteLine("Resetting MongoDumpRestoreCordinator for new job", LogType.Info);

                    // Stop and dispose timer
                    if (_processTimer != null)
                    {
                        _processTimer.Stop();
                        _processTimer.Elapsed -= OnTimerTick;
                        _processTimer.Dispose();
                        _processTimer = null;
                    }

                    // Cancel any ongoing operations
                    _processCts?.Cancel();
                    _processCts?.Dispose();
                    _processCts = null;

                    // Clear all manifests and tracking
                    _downloadManifest.Clear();
                    _uploadManifest.Clear();
                    _activeMigrationUnits.Clear();
                    _downloadBacklogIndex.Clear();
                    _uploadBacklogIndex.Clear();
                    _pendingMigrationUnitIndex.Clear();
                    ClearQueue(_downloadBacklog);
                    ClearQueue(_uploadBacklog);
                    ClearQueue(_pendingMigrationUnits);

                    // Clear tracked worker tasks
                    while (_activeWorkerTasks.TryTake(out _)) { }

                    // Dispose worker pools
                    _dumpPool?.Dispose();
                    _dumpPool = null;
                    _restorePool?.Dispose();
                    _restorePool = null;

                    // Clear callbacks
                    _onMigrationUnitCompleted = null;
                    _onPendingTasksCompleted = null;

                    // Reset state flags
                    _coordinatorInitialized = false;
                    _timerStarted = false;

                    // Clear job-specific data
                    _jobId = null;
                    _log = null;
                    _mongoToolsFolder = null;
                    _mongoDumpToolPath = null;
                    _mongoRestoreToolPath = null;

                    _downLoadPausedTill=DateTime.MinValue;
                    _lastDiskSpaceCheckedAtUtc = DateTime.MinValue;
                    _lastDiskSpaceCheckResult = true;

                    _log?.WriteLine("MongoDumpRestoreCordinator reset complete", LogType.Info);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error resetting MongoDumpRestoreCordinator: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Timer tick handler - processes pending dumps and restores
        /// </summary>
        private void OnTimerTick(object? sender, System.Timers.ElapsedEventArgs e)
        {
            // Fast exit for already-stopped coordinator. This catches queued thread pool
            // callbacks that fire after Stop() has been called on the timer.
            if (_stopped)
                return;

            // gets called often, avoid detailed logs
            // Prevent re-entrant calls
            if (!Monitor.TryEnter(_timerLock))
            {
                _log?.WriteLine("Timer tick skipped - previous tick still processing", LogType.Debug);
                return;
            }

            try
            {
                // Double-check after acquiring the lock
                if (_stopped)
                    return;

                // Check for cancellation, pause, or job stopped
                if (_processCts?.Token.IsCancellationRequested == true ||
                    MigrationJobContext.ControlledPauseRequested ||
                    MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true ||
                    MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                {
                    if (_processTimer != null && _timerStarted && _processNewTasks)
                    {
                        _processNewTasks = false;

                        if (MigrationJobContext.ControlledPauseRequested)
                        {
                            _log?.WriteLine("Controlled pause detected - stopped processing new tasks.", LogType.Warning);
                        }
                        else
                        {
                            // Stop the timer immediately for non-controlled pause/cancel
                            _stopped = true;
                            _processTimer.Stop();
                            _timerStarted = false;
                            _log?.WriteLine("Job paused/cancelled - timer stopped immediately.", LogType.Warning);
                            return;
                        }
                    }
                    else if (!MigrationJobContext.ControlledPauseRequested)
                    {
                        // Non-controlled pause/cancel but _processNewTasks already false — nothing left to do
                        return;
                    }
                }

                if (_processNewTasks)
                { 
                    TryActivatePendingMigrationUnits();
                    RefillManifestWorkingSets();
                    ProcessPendingDumps();
                    ProcessPendingRestores();
                }

                CheckForCompletedMigrationUnits();

                // Stop timer if all work is done
                _log?.WriteLine($"TimerTick: activeMUs={_activeMigrationUnits.Count}, dlManifest={_downloadManifest.Count}, ulManifest={_uploadManifest.Count}, dlBacklog={_downloadBacklogIndex.Count}, ulBacklog={_uploadBacklogIndex.Count}, pendMU={_pendingMigrationUnitIndex.Count}, processNewTasks={_processNewTasks}", LogType.Debug);
                if (IsAllWorkComplete())
                {
                    _log?.WriteLine($"IsAllWorkComplete=true, invoking _onPendingTasksCompleted and stopping", LogType.Debug);
                    _onPendingTasksCompleted?.Invoke();
                    StopCoordinatedProcessing();                   
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in timer tick: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
            finally
            {
                Monitor.Exit(_timerLock);
            }
        }

        /// <summary>
        /// Starts coordinated processing for a migration unit.
        /// Thread-safe static method that coordinates dump/restore operations.
        /// </summary>
        /// <param name="ctx">Processor context containing migration unit and connection details</param>
        public void StartCoordinatedProcess(ProcessorContext ctx)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.StartCoordinatedProcess: migrationUnitId={ctx.MigrationUnitId}");
            try
            {
                if (!_coordinatorInitialized)
                {
                    throw new InvalidOperationException("Coordinator must be initialized before starting coordinated process. Call Initialize() first.");
                }

                if (_activeMigrationUnits.ContainsKey(ctx.MigrationUnitId) || _pendingMigrationUnitIndex.ContainsKey(ctx.MigrationUnitId))
                {
                    _log?.WriteLine($"Migration unit already active or queued: {ctx.MigrationUnitId}", LogType.Debug);
                    return;
                }

                if (!TryStartMigrationUnit(ctx))
                {
                    if (_pendingMigrationUnitIndex.ContainsKey(ctx.MigrationUnitId))
                    {
                        _log?.WriteLine($"Active migration unit working set is full ({MaxActiveMigrationUnitsWorkingSetSize}). Queued {ctx.MigrationUnitId}.", LogType.Debug);
                    }
                    return;
                }

                // Start timer if not already running
                lock (_timerLock)
                {
                    if (_processTimer != null && !_timerStarted)
                    {
                        _processTimer.Start();
                        _timerStarted = true;
                        _log?.WriteLine("Started coordination timer", LogType.Debug);
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error starting coordinated process: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                throw;
            }
        }

        /// <summary>
        /// Prepares the download manifest for a migration unit
        /// </summary>
        private void PrepareDownloadList(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            //gets called very often hence removing detailed logging

            try
            {
                // Skip if collection was marked as failed due to max retries
                if (mu.SkippedDueToMaxRetries)
                {
                    _log?.WriteLine($"Skipping download preparation for {mu.DatabaseName}.{mu.CollectionName} - marked as failed due to max retries", LogType.Debug);
                    return;
                }

                int addedCount = 0;
                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    var chunk = mu.MigrationChunks[i];

                    // Only add if not downloaded and not already in manifest
                    if (chunk.IsDownloaded != true)
                    {
                        string contextId = $"{mu.Id}_{i}";
                        var context = new DumpRestoreProcessContext
                        {
                            Id = contextId,
                            ProcessorRunId = _processorRunId,
                            MigrationUnitId = mu.Id,
                            ChunkIndex = i,
                            State = ProcessState.Pending,
                            QueuedAt = DateTime.UtcNow,
                            RetryCount = 0,
                            SourceConnectionString = sourceConnectionString,
                            TargetConnectionString = targetConnectionString
                        };

                        if (TryAddDownloadContext(context))
                        {
                            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName}[{i}] accepted for download processing", LogType.Debug);
                            addedCount++;
                        }
                    }
                }

                if (addedCount > 0)
                {
                    _log?.WriteLine($"Added {addedCount} chunks to download manifest for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error preparing download list for {mu.DatabaseName}.{mu.CollectionName}: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Prepares the download manifest for specific chunk indices of a migration unit
        /// </summary>
        private void UpdateDownloadList(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, int startIndex, int count)
        {
            try
            {
                int addedCount = 0;
                int endIndex = Math.Min(startIndex + count, mu.MigrationChunks.Count);
                for (int i = startIndex; i < endIndex; i++)
                {
                    var chunk = mu.MigrationChunks[i];

                    // Only add if not downloaded and not already in manifest
                    if (chunk.IsDownloaded != true)
                    {
                        string contextId = $"{mu.Id}_{i}";
                        var context = new DumpRestoreProcessContext
                        {
                            Id = contextId,
                            ProcessorRunId = _processorRunId,
                            MigrationUnitId = mu.Id,
                            ChunkIndex = i,
                            State = ProcessState.Pending,
                            QueuedAt = DateTime.UtcNow,
                            RetryCount = 0,
                            SourceConnectionString = sourceConnectionString,
                            TargetConnectionString = targetConnectionString
                        };

                        if (TryAddDownloadContext(context))
                        {
                            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName}[{i}] accepted for download processing", LogType.Debug);
                            addedCount++;
                        }
                    }
                }

                if (addedCount > 0)
                {
                    _log?.WriteLine($"Added {addedCount} chunks to download manifest for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error updating download list for {mu.DatabaseName}.{mu.CollectionName}: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }


        private string GetDumpFilePath(MigrationUnit mu, int chunkIndex, bool overwrite = false)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.GetDumpFilePath: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}, overwrite={overwrite}");
            return GetDumpFilePath(mu.DatabaseName, mu.CollectionName, chunkIndex, overwrite);
        }

        private string GetDumpFilePath(string databaseName, string collectionName, int chunkIndex, bool overwrite = false)
        {
            string dumpFilePath = Path.Combine(PrepareDumpFolder(databaseName, collectionName), $"{chunkIndex}.bson");
            if (overwrite)
            {
                //Ensure previous dump file(if any) is removed before fresh dump
                try { StorageStreamFactory.DeleteIfExists(dumpFilePath); } catch { }
            }

            return dumpFilePath;
        }

        private bool CheckDumpDownloaded(MigrationUnit mu, int chunkIndex)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.CheckDumpDownloaded: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            string dumpFilePath = GetDumpFilePath(mu, chunkIndex);
            return StorageStreamFactory.Exists(dumpFilePath);
        }

        /// <summary>
        /// Prepares the restore manifest for a migration unit
        /// </summary>
        private void PrepareRestoreList(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            //gets called very often hence removing detailed logging
            try
            {
                // Skip if collection was marked as failed due to max retries
                if (mu.SkippedDueToMaxRetries)
                {
                    _log?.WriteLine($"Skipping restore preparation for {mu.DatabaseName}.{mu.CollectionName} - marked as failed due to max retries", LogType.Debug);
                    return;
                }

                int addedCount = 0;
                string folder = PrepareDumpFolder(mu.DatabaseName, mu.CollectionName);
                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    var chunk = mu.MigrationChunks[i];

                    // Only add if downloaded but not restored
                    if (chunk.IsDownloaded == true && chunk.IsUploaded != true)
                    {
                        string contextId = $"{mu.Id}_{i}";
                        var context = new DumpRestoreProcessContext
                        {
                            Id = contextId,
                            ProcessorRunId = _processorRunId,
                            MigrationUnitId = mu.Id,
                            ChunkIndex = i,
                            State = ProcessState.Pending,
                            QueuedAt = DateTime.UtcNow,
                            RetryCount = 0,
                            SourceConnectionString = sourceConnectionString,
                            TargetConnectionString = targetConnectionString
                        };

                        // Validate dump file exists
                        if (!ValidateDumpFileExists(context))
                        {
                            // Get dump folder and file path                        
                            string dumpFilePath = GetDumpFilePath(mu, i);
                            _log.WriteLine($"Dump file missing for restore context {contextId} at {dumpFilePath}. Marking chunk as not downloaded.", LogType.Warning);
                            int chunkIdx = i;
                            MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
                            {
                                m.MigrationChunks[chunkIdx].IsDownloaded = false;
                                m.DumpComplete = false;
                            }, updateParent: true);
                            continue;
                        }

                        bool added = TryAddUploadContext(context);
                        if (added)
                        {
                            addedCount++;
                            _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName}[{i}] accepted for restore processing{(GetChunkNeedsCleanup(context) ? " (needs merge)" : "")}", LogType.Debug);
                        }
                        _log?.WriteLine($"PrepareRestoreList: chunk [{i}] ctxId={contextId} eligible (IsDown=true, IsUp=false), needsCleanup={GetChunkNeedsCleanup(context)}, addedToUploadManifest={added} (alreadyInManifest={!added})", LogType.Debug);
                    }
                }

                if (addedCount > 0)
                {
                    _log?.WriteLine($"Added {addedCount} chunks to restore manifest for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
                int alreadyUploaded = mu.MigrationChunks.Count(c => c.IsUploaded == true);
                int eligible = mu.MigrationChunks.Count(c => c.IsDownloaded == true && c.IsUploaded != true);
                _log?.WriteLine($"PrepareRestoreList: SUMMARY muId={mu.Id} total={mu.MigrationChunks.Count}, alreadyUploaded={alreadyUploaded}, eligibleForRestore={eligible}, addedNow={addedCount}", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error preparing restore list for {mu.DatabaseName}.{mu.CollectionName}: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }        

        /// <summary>
        /// Processes pending dump contexts using available workers
        /// </summary>
        private void ProcessPendingDumps()
        {
            //gets called very often hence removing detailed logging
            try
            {
                if (_dumpPool == null)
                {
                    _log?.WriteLine("[ProcessPendingDumps] Dump pool is null - skipping", LogType.Debug);
                    return;
                }

                // Check for pause/cancel before spawning any workers
                if (_stopped || MigrationJobContext.ControlledPauseRequested ||
                    MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true ||
                    MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                {
                    _log?.WriteLine("[ProcessPendingDumps] Pause/cancel detected - skipping dump processing", LogType.Debug);
                    return;
                }

                // Get available worker capacity
                int availableWorkers = _dumpPool.CurrentAvailable;
                int totalWorkers = _dumpPool.MaxWorkers;
                int busyWorkers = totalWorkers - availableWorkers;


                if (availableWorkers <= 0)
                {
                    return; // No workers available
                }

                //commented out the disk space check for now, as it can cause issues with large jobs and blob storage scenarios. Will revisit if needed.
                //if (!HasSufficientDiskSpace())
                //{
                //    // Skip this cycle and retry on next timer tick
                //    return;
                //}

                // Find pending dump contexts (not already processing)
                var pendingContexts = _downloadManifest.Values
                    .Where(ctx => ctx.State == ProcessState.Pending)
                    .OrderBy(ctx => ctx.QueuedAt)
                    .Take(availableWorkers)
                    .ToList();

                if (pendingContexts.Count > 0)
                    MigrationJobContext.AddVerboseLog($"[ProcessPendingDumps] Found {pendingContexts.Count} pending contexts to process (capacity: {availableWorkers})");

                int spawned = 0;
                foreach (var context in pendingContexts)
                {
                    // Check for pause/cancel before spawning each worker
                    if (_stopped || MigrationJobContext.ControlledPauseRequested ||
                        MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true ||
                        MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                    {
                        _log?.WriteLine("[ProcessPendingDumps] Pause/cancel detected - skipping dump processing", LogType.Debug);
                        return;
                    }

                    // Check if the collection is marked as skipped due to max retries
                    var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (mu != null && mu.SkippedDueToMaxRetries)
                    {
                        _log?.WriteLine($"[ProcessPendingDumps] Skipping chunk {context.ChunkIndex} for {mu.DatabaseName}.{mu.CollectionName} - collection marked as failed due to max retries", LogType.Debug);
                        _downloadManifest.TryRemove(context.Id, out _);
                        continue;
                    }

                    // Try to acquire a worker slot
                    if (_dumpPool.TryAcquire())
                    {
                        //initating timer for status  tracking
                        PercentageUpdater.AddToPercentageTracker(context.MigrationUnitId, false, _log);                        // Mark as processing
                        context.State = ProcessState.Processing;
                        context.StartedAt = DateTime.UtcNow;
                        spawned++;

                        _log?.WriteLine($"[ProcessPendingDumps] Spawning dump worker for {mu?.DatabaseName}.{mu?.CollectionName}[{context.ChunkIndex}] (worker {spawned}/{availableWorkers})", LogType.Debug);                        // Spawn worker task
                        var cancellationToken = _processCts?.Token ?? CancellationToken.None;

                        //Updating BulkCopyStartedOn timestamp, set it before the first dump starts
                        if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                        {
                            MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                            {
                                if (!m.BulkCopyStartedOn.HasValue || m.BulkCopyStartedOn == DateTime.MinValue)
                                    m.BulkCopyStartedOn = DateTime.UtcNow;
                            }, updateParent: true);
                        }

                        var workerTask = Task.Run(async () => await ProcessChunkForDownload(context), cancellationToken);
                        _activeWorkerTasks.Add(workerTask);
                    }
                    else
                    {
                        _log?.WriteLine($"[ProcessPendingDumps] Failed to acquire worker slot after spawning {spawned} workers - stopping", LogType.Debug);
                        break; // No more workers available
                    }
                }

                if (spawned > 0)
                {
                    _log?.WriteLine($"[ProcessPendingDumps] Successfully spawned {spawned} dump worker(s)", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error processing pending dumps: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        private bool HasSufficientDiskSpace()
        {
            // When using Azure Blob Storage with Entra ID, skip disk space check
            // as blob storage has virtually unlimited capacity
            if (StorageStreamFactory.UseBlobStorage)
                return true;

            lock (_diskSpaceCheckLock)
            {
                //check if current time is less than paused till
                if (_downLoadPausedTill > DateTime.Now)
                    return false;

                if (_lastDiskSpaceCheckedAtUtc != DateTime.MinValue &&
                    DateTime.UtcNow - _lastDiskSpaceCheckedAtUtc < TimeSpan.FromSeconds(DiskSpaceCheckCacheSeconds))
                {
                    return _lastDiskSpaceCheckResult;
                }

                string folder = Helper.GetWorkingFolder();
                MigrationSettings config = new MigrationSettings();
                config.Load();
               

                //checking if there are too many downloads or disk full. Caused by limited uploads.
                bool continueDownlods;
                double pendingUploadsGB = 0;
                double freeSpaceGB = 0;
                
                continueDownlods = Helper.CanProceedWithDownloads(folder, config.ChunkSizeInMb * 2, out pendingUploadsGB, out freeSpaceGB);
                _lastDiskSpaceCheckResult = continueDownlods;
                _lastDiskSpaceCheckedAtUtc = DateTime.UtcNow;

                if (!continueDownlods)
                {
                    _log.WriteLine($"Disk space is running low, with only {freeSpaceGB}GB available. Free up disk space by deleting unwanted jobs. Will recheck in 15 minutes...", LogType.Warning);
                    _downLoadPausedTill = DateTime.Now.AddMinutes(15);

                }

                return continueDownlods;
            }
        }

        /// <summary>
        /// Processes pending restore contexts using available workers
        /// </summary>
        private void ProcessPendingRestores()
        {
            try
            {
                if (_restorePool == null)
                {
                    _log?.WriteLine("[ProcessPendingRestores] Restore pool is null - skipping", LogType.Debug);
                    return;
                }

                // Check for pause/cancel before spawning any workers
                if (_stopped || MigrationJobContext.ControlledPauseRequested ||
                    MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true ||
                    MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                {
                    _log?.WriteLine("[ProcessPendingRestores] Pause/cancel detected - skipping restore processing", LogType.Debug);
                    return;
                }

                // Get available worker capacity
                int availableWorkers = _restorePool.CurrentAvailable;
                int totalWorkers = _restorePool.MaxWorkers;
                if (availableWorkers <= 0)
                {
                    return; // No workers available
                }

                var readyToDispatch = _uploadManifest.Values
                    .Where(ctx => ctx.State == ProcessState.Pending)
                    .OrderBy(ctx => ctx.QueuedAt)
                    .ToList();

                if (readyToDispatch.Count == 0)
                {
                    return;
                }

                // Split into fresh (Attempt=0) and retry-ready (Attempt>0, cleanup done).
                var fresh = readyToDispatch.Where(ctx => !IsRetryContext(ctx)).ToList();
                var retryReady = readyToDispatch.Where(ctx => IsRetryContext(ctx)).ToList();

                int retrySlotCap;
                if (fresh.Count > 0)
                {
                    int processingRetries = _uploadManifest.Values
                        .Count(ctx => ctx.State == ProcessState.Processing && IsRetryContext(ctx));
                    // Cap retries at ~25% of capacity so a flood of fresh chunks can't starve in-flight merge/cleanup; retries finish faster on small slices.
                    int maxRetryWorkers = Math.Max(1, (int)Math.Floor(totalWorkers * 0.25));
                    retrySlotCap = Math.Max(0, maxRetryWorkers - processingRetries);
                }
                else
                {
                    // If all remaining chunks are retries, allow full worker usage.
                    retrySlotCap = availableWorkers;
                }

                // Reserve retry slots first, then fill the rest with fresh chunks.
                // This guarantees mixed processing when both sets are available.
                int retryTarget = Math.Min(retryReady.Count, Math.Min(retrySlotCap, availableWorkers));
                int freshTarget = Math.Min(fresh.Count, Math.Max(0, availableWorkers - retryTarget));

                var toDispatch = new List<DumpRestoreProcessContext>(availableWorkers);
                for (int i = 0; i < freshTarget; i++)
                {
                    toDispatch.Add(fresh[i]);
                }

                for (int i = 0; i < retryTarget; i++)
                {
                    toDispatch.Add(retryReady[i]);
                }

                // Backfill any remaining capacity after initial target allocation.
                // Prefer remaining fresh first, then retries.
                for (int i = freshTarget; i < fresh.Count && toDispatch.Count < availableWorkers; i++)
                {
                    toDispatch.Add(fresh[i]);
                }
                for (int i = retryTarget; i < retryReady.Count && toDispatch.Count < availableWorkers; i++)
                {
                    toDispatch.Add(retryReady[i]);
                }

                if (toDispatch.Count > 0)
                {
                    MigrationJobContext.AddVerboseLog(
                        $"[ProcessPendingRestores] Dispatching {toDispatch.Count} restore context(s). Fresh={fresh.Count}, RetryReady={retryReady.Count}, RetrySlots={retrySlotCap}, Capacity={availableWorkers}");
                }

                int spawned = 0;
                foreach (var context in toDispatch)
                {
                    // Check for pause/cancel before spawning each worker
                    if (_stopped || MigrationJobContext.ControlledPauseRequested ||
                        MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true ||
                        MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                    {
                        _log?.WriteLine("[ProcessPendingRestores] Pause/cancel detected - skipping restore processing", LogType.Debug);
                        return;
                    }

                    // Check if the collection is marked as skipped due to max retries
                    var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (mu != null && mu.SkippedDueToMaxRetries)
                    {
                        _log?.WriteLine($"[ProcessPendingRestores] Skipping chunk {context.ChunkIndex} for {mu.DatabaseName}.{mu.CollectionName} - collection marked as failed due to max retries", LogType.Debug);
                        _uploadManifest.TryRemove(context.Id, out _);
                        continue;
                    }

                    // Try to acquire a worker slot
                    if (_restorePool.TryAcquire())
                    {
                        //initating timer for status  tracking
                        PercentageUpdater.AddToPercentageTracker(context.MigrationUnitId, true, _log);                        // Mark as processing
                        context.State = ProcessState.Processing;
                        context.StartedAt = DateTime.UtcNow;
                        spawned++;

                        _log?.WriteLine($"[ProcessPendingRestores] Spawning restore worker for {mu?.DatabaseName}.{mu?.CollectionName}[{context.ChunkIndex}] (worker {spawned}/{availableWorkers})", LogType.Debug);                        // Spawn worker task
                        var cancellationToken = _processCts?.Token ?? CancellationToken.None;
                        var workerTask = Task.Run(async () => await ProcessChunkForRestore(context), cancellationToken);
                        _activeWorkerTasks.Add(workerTask);
                    }
                    else
                    {
                        _log?.WriteLine($"[ProcessPendingRestores] Failed to acquire worker slot after spawning {spawned} workers - stopping", LogType.Debug);
                        break; // No more workers available
                    }
                }

                if (spawned > 0)
                {
                    _log?.WriteLine($"[ProcessPendingRestores] Successfully spawned {spawned} restore worker(s)", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error processing pending restores: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Processes a single chunk dump
        /// </summary>
        private async Task ProcessChunkForDownload(DumpRestoreProcessContext context)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ProcessChunkForDownload: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);

            if (mu == null)
            {
                _log?.WriteLine($"Coordinator: MigrationUnit not found in cache for context {context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Warning);
                HandleDumpFailure(context, TaskResult.Retry);
                _dumpPool?.Release();
                return;
            }

            

            string dbName = mu.DatabaseName;
            string colName = mu.CollectionName;
            int chunkIndex = context.ChunkIndex;

            try
            {
                _log?.WriteLine($"Coordinator: Starting dump for {dbName}.{colName}[{chunkIndex}]", LogType.Debug);

                // Check cancellation or job stopped
                if (_processCts?.Token.IsCancellationRequested == true ||
                    _stopped ||
                    MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                {
                    HandleDumpFailure(context, TaskResult.Canceled);
                    return;
                }

                // Prepare dump environment                
                string dumpFilePath = GetDumpFilePath(dbName, colName, chunkIndex, true);
                MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator GetDumpFilePath={dumpFilePath}");

                // Build dump arguments with query
                var dumpArgs = await BuildDumpArgumentsAsync(
                    mu,
                    chunkIndex,
                    context.SourceConnectionString,
                    context.TargetConnectionString,
                    dbName,
                    colName
                );
                MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator DumpArgs={Helper.RedactPii(dumpArgs.args)} Count={dumpArgs.docCount}");


                // Execute dump
                bool success = await ExecuteDumpProcessAsync(
                    mu,
                    chunkIndex,
                    dumpArgs.args,
                    dumpArgs.docCount,
                    dumpFilePath
                );

                if (success)
                {
                    HandleDumpSuccess(context);
                    _log?.WriteLine($"Coordinator: Completed dump {dbName}.{colName}[{chunkIndex}]", LogType.Debug);
                }
                else
                {
                    HandleDumpFailure(context, TaskResult.Retry);
                }
            }
            catch (OperationCanceledException)
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine($"Coordinator: Dump cancelled for {dbName}.{colName}[{chunkIndex}]", LogType.Debug);
                }
                HandleDumpFailure(context, TaskResult.Canceled);
            }
            catch (Exception ex)
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine($"Coordinator: Error dumping {dbName}.{colName}[{chunkIndex}]: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                }
                HandleDumpFailure(context, TaskResult.Retry, ex);
            }
            finally
            {
                // Always release worker slot
                _dumpPool?.Release();
            }
        }

        /// <summary>
        /// Prepares the dump folder for a collection
        /// </summary>
        private string PrepareDumpFolder(string dbName, string colName)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.PrepareDumpFolder: database={dbName}, collection={colName}");
            string folder = Path.Combine(_mongoDumpOutputFolder, _jobId ?? "", Helper.EncodeStoragePathSegment($"{dbName}.{colName}"));
            StorageStreamFactory.EnsureDirectoryExists(folder);
            return folder;
        }


        /// <summary>
        /// Builds complete dump arguments including query filters
        /// </summary>
        private async Task<(string args, long docCount)> BuildDumpArgumentsAsync(
            MigrationUnit mu,
            int chunkIndex,
            string sourceConnectionString,
            string targetConnectionString,
            string dbName,
            string colName)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.BuildDumpArgumentsAsync: collection={dbName}.{colName}, chunkIndex={chunkIndex}");
            // Build base dump arguments
            string args;

            //3.6 doesn't like --db when filter is also there
            if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
            {
                var embeddedConnStr = Helper.EmbedDatabaseNameInConnectionString(sourceConnectionString, dbName);
                args = $" --uri={QuoteMongoToolArgument(embeddedConnStr)} --gzip --collection={QuoteMongoToolArgument(colName)} --archive";
            }
            else
            {
                args = $" --uri={QuoteMongoToolArgument(sourceConnectionString)} --gzip --db={QuoteMongoToolArgument(dbName)} --collection={QuoteMongoToolArgument(colName)} --archive";
            }


            // Build query and get doc count
            var queryResult = await BuildDumpQueryAsync(mu, chunkIndex, args, sourceConnectionString, targetConnectionString);

            return (queryResult.args, queryResult.docCount);
        }

        /// <summary>
        /// Executes the mongodump process
        /// </summary>
        private async Task<bool> ExecuteDumpProcessAsync(
            MigrationUnit mu,
            int chunkIndex,
            string args,
            long docCount,
            string dumpFilePath)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ExecuteDumpProcessAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}, docCount={docCount}");
            // Calculate progress factors
            //double initialPercent = ((double)100 / mu.MigrationChunks.Count) * chunkIndex;
            //double contributionFactor = 1.0 / mu.MigrationChunks.Count;

            // Execute dump process
            var processExecutor = new ProcessExecutor(_log);
            var dumpToolPath = GetMongoToolPath("mongodump");
            bool success = await Task.Run(() => processExecutor.Execute(
                mu,
                mu.MigrationChunks[chunkIndex],
                chunkIndex,                
                docCount,
                dumpToolPath,
                args,
                dumpFilePath,
                _processCts?.Token ?? CancellationToken.None,
                _ignoreDuplicatesAndContinueRestore,
                _continuousDuplicateThreshold,
                onProcessStarted: pid => MigrationJobContext.ActiveDumpProcessIds.Add(pid),
                onProcessEnded: pid => MigrationJobContext.ActiveDumpProcessIds.Remove(pid)
            ), _processCts?.Token ?? CancellationToken.None);

            return success;
        }

        /// <summary>
        /// Handles successful dump completion
        /// </summary>
        private void HandleDumpSuccess(DumpRestoreProcessContext context)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.HandleDumpSuccess: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);

            if (CheckDumpDownloaded(mu, context.ChunkIndex) == false)
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine($"Dump file not found after dump for {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]", LogType.Warning);
                    HandleDumpFailure(context, TaskResult.Retry);
                }
                return;
            }
            _log.WriteLine($"Dump file verified for {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]", LogType.Debug);
            int chunkIndex = context.ChunkIndex;

            // Mark chunk as completed
            context.State = ProcessState.Completed;
            context.CompletedAt = DateTime.UtcNow;

            // Atomically read-modify-write to avoid lost updates from concurrent workers.
            // Capture prior state so we only increment the tracker on a real transition (a chunk
            // dumped twice via retries must not double-count, or the tracker hits TotalChunks
            // prematurely and the job thinks all downloads are done).
            bool wasAlreadyDownloaded = false;
            var updatedMu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
            {
                wasAlreadyDownloaded = m.MigrationChunks[chunkIndex].IsDownloaded == true;
                m.MigrationChunks[chunkIndex].IsDownloaded = true;
            }, updateParent: true);

            bool isDownPersistedD = updatedMu?.MigrationChunks[chunkIndex].IsDownloaded == true;
            bool isUpPersistedD = updatedMu?.MigrationChunks[chunkIndex].IsUploaded == true;
            _log?.WriteLine($"HandleDumpSuccess: chunk [{chunkIndex}] ctxId={context.Id} wasAlreadyDownloaded={wasAlreadyDownloaded}, persisted IsDownloaded={isDownPersistedD}, IsUploaded={isUpPersistedD}", LogType.Debug);

            if (!wasAlreadyDownloaded)
            {
                UpdateMigrationUnitTracker(mu.Id, downloadIncrement: 1, caller: "HandleDumpSuccess", chunkIndex: chunkIndex);
            }
            else
            {
                _log?.WriteLine($"HandleDumpSuccess: SKIP tracker increment for chunk [{chunkIndex}] - already downloaded", LogType.Debug);
            }

            // Remove from download manifest
            _downloadManifest.TryRemove(context.Id, out _);

            if(MigrationJobContext.ControlledPauseRequested)
            {
                return; // Skip preparing restore list during controlled pause
            }
            // Prepare restore list after successful dump
            PrepareRestoreList(updatedMu ?? mu, context.SourceConnectionString, context.TargetConnectionString);
        }

        /// <summary>
        /// Builds the dump query and returns document count and updated arguments
        /// </summary>
        private async Task<(long docCount, string args)> BuildDumpQueryAsync(MigrationUnit mu, int chunkIndex, string baseArgs, string sourceConnectionString, string targetConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.BuildDumpQueryAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            try
            {
                if (mu.MigrationChunks.Count > 1)
                {
                    return await BuildMultiChunkDumpQueryAsync(mu, chunkIndex, baseArgs, sourceConnectionString, targetConnectionString);
                }
                else if (mu.MigrationChunks.Count == 1 && !string.IsNullOrEmpty(mu.UserFilter))
                {
                    return await BuildSingleChunkWithFilterDumpQueryAsync(mu, chunkIndex, baseArgs, sourceConnectionString);
                }
                else
                {
                    return BuildSingleChunkFullDumpQuery(mu, chunkIndex, baseArgs);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error building dump query for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                throw;
            }
        }

        /// <summary>
        /// Builds dump query for multi-chunk scenario with chunk bounds
        /// </summary>
        /// <summary>
        /// Attempts to get document count with retry logic.
        /// </summary>
        /// <param name="collection">The MongoDB collection</param>
        /// <param name="gte">Greater than or equal bound</param>
        /// <param name="lt">Less than bound</param>
        /// <param name="dataType">The data type of the _id field</param>
        /// <param name="userFilterDoc">Optional user filter</param>
        /// <param name="skipDataTypeFilter">Whether to skip data type filtering</param>
        /// <param name="maxRetries">Maximum number of retry attempts (default: 3)</param>
        /// <returns>Tuple of (success, docCount) - if success is false, docCount is -1</returns>
        private (bool success, long docCount) TryGetDocumentCountWithRetry(
            IMongoCollection<BsonDocument> collection,
            int chunkIndex,
            BsonValue gte,
            BsonValue lt,
            BsonValue lte,
            DataType dataType,
            BsonDocument? userFilterDoc,
            bool skipDataTypeFilter,
            int maxRetries = 3)
        {
            MigrationJobContext.AddVerboseLog($"TryGetDocumentCountWithRetry: collection={collection.CollectionNamespace}[{chunkIndex}], maxRetries={maxRetries}");

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    _log?.WriteLine($"GetDocumentCount for {collection.CollectionNamespace}[{chunkIndex}] attempt {attempt}/{maxRetries} for {collection.CollectionNamespace}", LogType.Debug);
                    long count = MongoHelper.GetDocumentCount(
                        collection,
                        gte,
                        lt,
                        lte,
                        dataType,
                        userFilterDoc,
                        skipDataTypeFilter
                    );
                    return (true, count);
                }
                catch (Exception ex)
                {
                    _log?.WriteLine($"GetDocumentCount for {collection.CollectionNamespace}[{chunkIndex}] attempt {attempt}/{maxRetries} threw error: {Helper.RedactPii(ex.Message)}", LogType.Warning);
                    if (attempt == maxRetries)
                    {
                        _log?.WriteLine($"GetDocumentCount for {collection.CollectionNamespace}[{chunkIndex}] failed after {maxRetries} attempts.", LogType.Warning);
                        return (false, -1);
                    }
                    Thread.Sleep(Helper.GetRetryDelayMs(attempt));
                }               
            }

            return (false, -1);
        }

        /// <summary>
        /// Replaces a chunk at the specified index with multiple sub-chunks in the MigrationChunks array.
        /// </summary>
        /// <param name="mu">The migration unit</param>
        /// <param name="chunkIndex">Index of the chunk to replace</param>
        /// <param name="subChunks">List of sub-chunks to insert</param>
        /// <returns>The number of new chunks added (subChunks.Count - 1)</returns>
        private int ReplaceChunkWithSubChunks(MigrationUnit mu, int chunkIndex, List<MigrationChunk> subChunks)
        {
            MigrationJobContext.AddVerboseLog($"ReplaceChunkWithSubChunks: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}, subChunkCount={subChunks.Count}");

            if (subChunks.Count <= 1)
            {
                return 0; // No replacement needed
            }

            // Preserve the original chunk's ID for the first sub-chunk — chunk IDs are referenced by manifests/queues and dump file paths, so reusing it lets in-flight contexts keep resolving without a rename pass.
            var originalChunk = mu.MigrationChunks[chunkIndex];
            var originalId = originalChunk.Id;

            // Find the maximum existing ID to avoid duplicates
            int maxExistingId = 0;
            foreach (var chunk in mu.MigrationChunks)
            {
                if (int.TryParse(chunk.Id, out int chunkIdNum) && chunkIdNum > maxExistingId)
                {
                    maxExistingId = chunkIdNum;
                }
            }

            // Update the original chunk in-place with the first sub-chunk's values (retains original ID)
            originalChunk.Gte = subChunks[0].Gte;
            originalChunk.Lt = subChunks[0].Lt;
            originalChunk.Lte = subChunks[0].Lte;
            originalChunk.DataType = subChunks[0].DataType;
            originalChunk.IsDownloaded = subChunks[0].IsDownloaded;
            originalChunk.IsUploaded = subChunks[0].IsUploaded;

            // Assign IDs to remaining sub-chunks and add them to the end
            int nextId = maxExistingId + 1;
            for (int i = 1; i < subChunks.Count; i++)
            {
                subChunks[i].Id = nextId.ToString();
                mu.MigrationChunks.Add(subChunks[i]);
                nextId++;
            }

            _log?.WriteLine($"Updated chunk at index {chunkIndex} and added {subChunks.Count - 1} new sub-chunks to {mu.DatabaseName}.{mu.CollectionName}", LogType.Info);

            return subChunks.Count - 1;
        }

        /// <summary>
        /// Handles count timeout by splitting ObjectId chunks into smaller sub-chunks and retrying.
        /// </summary>
        /// <param name="mu">The migration unit</param>
        /// <param name="chunkIndex">Index of the chunk that timed out</param>
        /// <param name="sourceCollection">The source MongoDB collection</param>
        /// <param name="userFilterDoc">Optional user filter document</param>
        /// <param name="sourceConnectionString">Source connection string for PrepareDownloadList</param>
        /// <param name="targetConnectionString">Target connection string for PrepareDownloadList</param>
        /// <returns>Tuple containing (docCount, gte bound, lt bound, lte bound, query string)</returns>
        private async Task<(long docCount, BsonValue gte, BsonValue lt, BsonValue lte, string query)> HandleCountTimeoutWithChunkSplitAsync(
            MigrationUnit mu,
            int chunkIndex,
            IMongoCollection<BsonDocument> sourceCollection,
            BsonDocument? userFilterDoc,
            string sourceConnectionString,
            string targetConnectionString,
            bool isTimeout)
        {
            MigrationJobContext.AddVerboseLog($"HandleCountTimeoutWithChunkSplitAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");

            var originalChunk = mu.MigrationChunks[chunkIndex];
            var dataType = originalChunk.DataType;
            var isObjectId = dataType == DataType.ObjectId;
            var strategyName = isObjectId ? "ObjectId splitting" : "sample-based splitting";

            _logSplittingStrategy(mu, chunkIndex, dataType, isTimeout, strategyName);

            try
            {
                // Use appropriate splitting strategy based on data type
                var subChunks = isObjectId
                    ? await MongoObjectIdSampler.SplitObjectIdChunkIntoSubChunksAsync(originalChunk, sourceCollection, 10)
                    : await SplitNonObjectIdChunkUsingSampleAsync(originalChunk, sourceCollection, mu.DatabaseName, mu.CollectionName, userFilterDoc, 10);

                // Process the split result (common logic for both strategies)
                return await ProcessChunkSplitResultAsync(
                    mu,
                    chunkIndex,
                    subChunks,
                    sourceCollection,
                    userFilterDoc,
                    sourceConnectionString,
                    targetConnectionString,
                    dataType,
                    strategyName);
            }
            catch (Exception ex) when (!(ex is InvalidOperationException))
            {
                var errorMessage = $"Error in {strategyName} for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]: {ex.Message}";
                _log?.WriteLine(errorMessage, LogType.Error);
                throw new InvalidOperationException(errorMessage, ex);
            }
        }
        /// <summary>
        /// Handles the result of chunk splitting - common logic for both ObjectId and sample-based strategies
        /// </summary>
        private async Task<(long docCount, BsonValue gte, BsonValue lt, BsonValue lte, string query)> ProcessChunkSplitResultAsync(
            MigrationUnit mu,
            int chunkIndex,
            List<MigrationChunk> subChunks,
            IMongoCollection<BsonDocument> sourceCollection,
            BsonDocument? userFilterDoc,
            string sourceConnectionString,
            string targetConnectionString,
            DataType dataType,
            string strategyName)
        {
            MigrationJobContext.AddVerboseLog($"ProcessChunkSplitResultAsync: collection={mu.DatabaseName}.{mu.CollectionName}, subChunkCount={subChunks.Count}, strategy={strategyName}");

            if (subChunks.Count < 2)
            {
                throw new InvalidOperationException(
                    $"Failed to split chunk using {strategyName} for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]. " +
                    $"Unable to create multiple sub-chunks (got {subChunks.Count}).");
            }

            // Replace original chunk with sub-chunks atomically on the persisted MU
            int addedChunks = 0;
            var updatedMu = MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
            {
                addedChunks = ReplaceChunkWithSubChunks(m, chunkIndex, subChunks);
            }, updateParent: true);
            if (updatedMu != null) mu = updatedMu;

            // Update tracker for newly added sub-chunks
            if (addedChunks > 0 && _activeMigrationUnits.TryGetValue(mu.Id, out var tracker))
            {
                Interlocked.Add(ref tracker.TotalChunks, addedChunks);
                _log?.WriteLine($"Updated tracker TotalChunks to {tracker.TotalChunks} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
            }

            // Update manifests for new sub-chunks
            if (subChunks.Count > 1)
            {
                int newChunksStartIndex = mu.MigrationChunks.Count - (subChunks.Count - 1);
                UpdateDownloadList(mu, sourceConnectionString, targetConnectionString, newChunksStartIndex, subChunks.Count - 1);
            }

            // Re-process the first sub-chunk (updated in-place at the same index)
            var bounds = SamplePartitioner.GetChunkBounds(
                mu.MigrationChunks[chunkIndex].Gte!,
                mu.MigrationChunks[chunkIndex].Lt ?? "",
                mu.MigrationChunks[chunkIndex].Lte ?? "",
                dataType
            );
            var gte = bounds.gte;
            var lt = bounds.lt;
            var lte = bounds.lte;
            var query = MongoHelper.GenerateQueryString(gte, lt, lte, dataType, userFilterDoc, mu);

            // Try count again on the smaller sub-chunk
            var (retrySuccess, retryCount) = TryGetDocumentCountWithRetry(
                sourceCollection,
                chunkIndex,
                gte,
                lt,
                lte,
                dataType,
                userFilterDoc,
                mu.SkipDataTypeFilterForId
            );

            long docCount = retrySuccess ? retryCount : 0;


            // Validate that the split actually reduced the count for the first sub-chunk
            long maxDocsPerChunk = GetEffectiveMaxDocsPerChunk(mu, retryCount);
           
           
            if (retrySuccess && retryCount > maxDocsPerChunk)
            {
                _log?.WriteLine($"First sub-chunk still has {retryCount} docs (max: {maxDocsPerChunk}). Recursively splitting again.", LogType.Warning);
                var recursiveResult = await HandleCountTimeoutWithChunkSplitAsync(
                    mu, chunkIndex, sourceCollection, userFilterDoc,
                    sourceConnectionString, targetConnectionString, false);
                return recursiveResult;
            }

            return (docCount, gte, lt, lte, query);
        }

        private long GetMaxDocsPerChunk(MigrationUnit mu)
        {
            long maxDocsPerChunk = 0;
            if (mu.MaxDocsPerChunk > 0)
            {
                maxDocsPerChunk = mu.MaxDocsPerChunk;
            }
            else
            {
                // Dynamic threshold baseline: (EstimatedDocCount / InitialChunkCount) * 3, capped at 25M.
                // Whether this threshold is applied is decided per chunk by GetEffectiveMaxDocsPerChunk.
                maxDocsPerChunk = Math.Min((mu.EstimatedDocCount / mu.MigrationChunks.Count) * 3, AbsoluteMaxDocsPerChunk);

                long capturedMax = maxDocsPerChunk;
                MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
                {
                    m.MaxDocsPerChunk = capturedMax;
                }, updateParent: true);
            }

            return maxDocsPerChunk;
        }

        private long GetEffectiveMaxDocsPerChunk(MigrationUnit mu, long chunkDocCount)
        {
            // Apply dynamic threshold only when this chunk itself is > 5M docs.
            if (chunkDocCount <= DynamicThresholdMinChunkDocs)
            {
                return AbsoluteMaxDocsPerChunk;
            }

            return GetMaxDocsPerChunk(mu);
        }

        /// <summary>
        /// Logs the chunk splitting strategy being used
        /// </summary>
        private void _logSplittingStrategy(MigrationUnit mu, int chunkIndex, DataType dataType, bool isTimeout, string strategy)
        {
            string message = isTimeout
                ? $"Count timed out for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]. Using {strategy}."
                : $"{mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] is too large. Using {strategy}.";
            _log?.WriteLine(message, LogType.Info);
        }

        /// <summary>
        /// Splits a non-ObjectId chunk using MongoDB $sample aggregation to find boundary values.
        /// This is modular and handles all non-ObjectId data types.
        /// </summary>
        private async Task<List<MigrationChunk>> SplitNonObjectIdChunkUsingSampleAsync(
            MigrationChunk originalChunk,
            IMongoCollection<BsonDocument> collection,
            string databaseName,
            string collectionName,
            BsonDocument? userFilterDoc,
            int targetSubChunkCount = 10)
        {
            MigrationJobContext.AddVerboseLog($"SplitNonObjectIdChunkUsingSampleAsync: collection={databaseName}.{collectionName}, targetSubChunkCount={targetSubChunkCount}, dataType={originalChunk.DataType}");

            try
            {
                // Get sample documents to determine split points
                var sampleDocs = await SamplePartitioner.SampleCollectionForSplitPointAsync(
                    collection,
                    originalChunk,
                    userFilterDoc,
                    targetSubChunkCount + 1); // Get one more for boundary

                if (sampleDocs.Count < 2)
                {
                    _log?.WriteLine($"Insufficient sample size ({sampleDocs.Count}) for splitting {databaseName}.{collectionName}. Will keep chunk as-is.", LogType.Warning);
                    return new List<MigrationChunk> { originalChunk };
                }

                // Create sub-chunks from sample boundaries
                var subChunks = await SamplePartitioner.CreateSubChunksFromSampleAsync(
                    originalChunk,
                    sampleDocs,
                    databaseName,
                    collectionName);

                _log?.WriteLine($"Created {subChunks.Count} sub-chunks via sampling for {databaseName}.{collectionName}[{originalChunk.Id}]", LogType.Info);
                return subChunks;
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error in SplitNonObjectIdChunkUsingSampleAsync for {databaseName}.{collectionName}: {ex.Message}", LogType.Error);
                throw;
            }
        }



        private async Task<(long docCount, string args)> BuildMultiChunkDumpQueryAsync(
            MigrationUnit mu,
            int chunkIndex,
            string baseArgs,
            string sourceConnectionString,
            string targetConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.BuildMultiChunkDumpQueryAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            // Get chunk bounds
            var bounds = SamplePartitioner.GetChunkBounds(
                mu.MigrationChunks[chunkIndex].Gte!,
                mu.MigrationChunks[chunkIndex].Lt ?? "",
                mu.MigrationChunks[chunkIndex].Lte ?? "",
                mu.MigrationChunks[chunkIndex].DataType
            );
            var gte = bounds.gte;
            var lt = bounds.lt;
            var lte = bounds.lte;

            // Get source collection
            var sourceCollection = GetSourceCollection(sourceConnectionString, mu.DatabaseName, mu.CollectionName);

            // Build query and get count with retry logic
            BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
            string query = MongoHelper.GenerateQueryString(gte, lt, lte, mu.MigrationChunks[chunkIndex].DataType, userFilterDoc, mu);

            // Try to get document count with retry
            var (countSuccess, docCount) = TryGetDocumentCountWithRetry(
                sourceCollection,
                chunkIndex,
                gte,
                lt,
                lte,
                mu.MigrationChunks[chunkIndex].DataType,
                userFilterDoc,
                mu.SkipDataTypeFilterForId
            );

            // Handle timeout by splitting chunk if needed
            long maxDocsPerChunk = countSuccess
                ? GetEffectiveMaxDocsPerChunk(mu, docCount)
                : AbsoluteMaxDocsPerChunk;

            bool countTimedOut = !countSuccess;
            bool chunkTooLarge = countSuccess && docCount > maxDocsPerChunk;
            bool shouldSplitLargeChunk = true;

            if (chunkTooLarge)
            {
                var config = new MigrationSettings();
                config.Load();
                shouldSplitLargeChunk = config.LargePartitionsShouldBeSplit;

                if (!shouldSplitLargeChunk)
                {
                    _log?.WriteLine(
                        $"{mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] is too large ({docCount} docs, max {maxDocsPerChunk}) but LargePartitionsShouldBeSplit is disabled. Continuing without split.",
                        LogType.Debug);
                }
            }

            if (countTimedOut || (chunkTooLarge && shouldSplitLargeChunk))
            {
                var result = await HandleCountTimeoutWithChunkSplitAsync(
                    mu,
                    chunkIndex,
                    sourceCollection,
                    userFilterDoc,
                    sourceConnectionString,
                    targetConnectionString,
                    countTimedOut);
                docCount = result.docCount;
                gte = result.gte;
                lt = result.lt;
                lte = result.lte;
                query = result.query;
            }

            mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
            _log?.WriteLine($"Count for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] is {docCount}", LogType.Debug);

            // Convert query for mongodump
            string extendedQuery = MongoQueryConverter.ConvertMondumpFilter(query, gte, lt, lte, mu.MigrationChunks[chunkIndex].DataType);
            string args = $"{baseArgs} --query=\"{extendedQuery}\"";

            return (docCount, args);
        }

        /// <summary>
        /// Builds dump query for single chunk with user filter
        /// </summary>
        private async Task<(long docCount, string args)> BuildSingleChunkWithFilterDumpQueryAsync(
            MigrationUnit mu,
            int chunkIndex,
            string baseArgs,
            string sourceConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.BuildSingleChunkWithFilterDumpQueryAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            // Get source collection
            var sourceCollection = GetSourceCollection(sourceConnectionString, mu.DatabaseName, mu.CollectionName);

            // Build query with user filter
            BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
            long docCount = MongoHelper.GetActualDocumentCount(sourceCollection, mu);
            string query = MongoHelper.GenerateQueryString(userFilterDoc);
            string args = $"{baseArgs} --query=\"{query}\"";

            mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;

            return (docCount, args);
        }

        /// <summary>
        /// Builds dump query for single chunk without filter (full collection dump)
        /// </summary>
        private (long docCount, string args) BuildSingleChunkFullDumpQuery(
            MigrationUnit mu,
            int chunkIndex,
            string baseArgs)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.BuildSingleChunkFullDumpQuery: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            // Single chunk without filter - dump entire collection
            long docCount = Helper.GetMigrationUnitDocCount(mu);
            mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;

            return (docCount, baseArgs);
        }

        /// <summary>
        /// Gets source MongoDB collection
        /// </summary>
        private IMongoCollection<BsonDocument> GetSourceCollection(string sourceConnectionString, string dbName, string colName)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.GetSourceCollection: database={dbName}, collection={colName}");
            var sourceClient = MongoClientFactory.Create(_log, sourceConnectionString);
            var sourceDb = sourceClient.GetDatabase(dbName);
            return sourceDb.GetCollection<BsonDocument>(colName);
        }

        /// <summary>
        /// Processes a single chunk restore
        /// </summary>
        private async Task ProcessChunkForRestore(DumpRestoreProcessContext context)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ProcessChunkForRestore: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);

            if (mu == null)
            {
                // MigrationUnit not yet registered - reset to Pending and let it retry naturally
                HandleRestoreFailure(context, TaskResult.Retry);
                _restorePool?.Release();
                return;
            }

            string sourceDbName = mu.DatabaseName;
            string sourceColName = mu.CollectionName;
            string targetDbName = mu.GetEffectiveTargetDatabaseName();
            string targetColName = mu.GetEffectiveTargetCollectionName();
            int chunkIndex = context.ChunkIndex;
            string chunkId = (chunkIndex >= 0 && chunkIndex < mu.MigrationChunks.Count)
                ? (mu.MigrationChunks[chunkIndex].Id ?? string.Empty)
                : string.Empty;

            try
            {
                _log?.WriteLine($"Coordinator: Starting restore for {Log.FormatNamespaceForLog(sourceDbName, sourceColName, targetDbName, targetColName)}[{chunkIndex}] (ChunkId={chunkId}, ContextId={context.Id}, ProcessorRunId={_processorRunId})", LogType.Debug);

                // Check cancellation or job stopped
                if (_processCts?.Token.IsCancellationRequested == true ||
                    _stopped ||
                    MigrationJobContext.CurrentlyActiveJob?.IsStarted == false)
                {
                    HandleRestoreFailure(context, TaskResult.Canceled);
                    return;
                }

                // Check for simulation mode
                if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.SimulateRestoreChunk: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
                    SimulateRestoreChunk(context);
                    return;
                }

                // If chunk needs merge (prior restore left partial data), run merge flow
                if (GetChunkNeedsCleanup(context))
                {
                    _log?.WriteLine($"ProcessChunkForRestore: chunk [{chunkIndex}] needs merge, entering RunCleanupForRestoreRetryAsync", LogType.Debug);
                    var cancellationToken = _processCts?.Token ?? CancellationToken.None;
                    bool mergeNeedsRetry = await RunCleanupForRestoreRetryAsync(context, cancellationToken);
                    _log?.WriteLine($"ProcessChunkForRestore: chunk [{chunkIndex}] merge returned mergeNeedsRetry={mergeNeedsRetry}", LogType.Debug);
                    if (mergeNeedsRetry)
                    {
                        HandleRestoreFailure(context, TaskResult.Retry);
                    }
                    // Success/cancel paths are handled inside RunCleanupForRestoreRetryAsync
                    return;
                }

                // Get dump folder and file path
                var dumpFilePath = GetDumpFilePath(sourceDbName, sourceColName, chunkIndex);


                // Validate dump file exists
                if (!ValidateDumpFileExists(context))
                {
                    MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ValidateDumpFileExists: Failed for muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
                    _log.WriteLine($"Dump file missing before executing restore at {dumpFilePath}. Marking chunk as not downloaded.", LogType.Warning);
                    // Note: ValidateDumpFileExists already cleared IsDownloaded and DumpComplete atomically and called HandleRestoreFailure.
                    return;
                }
                else
                {
                    // Build restore arguments
                    var restoreArgs = BuildRestoreArguments(
                        mu,
                        chunkIndex,
                        context.TargetConnectionString,
                        sourceDbName,
                        sourceColName,
                        targetDbName,
                        targetColName
                    );

                    // Fire-and-forget warm-up: a single findOne against the target primes the driver's connection pool and TLS handshake so mongorestore's first batch doesn't pay that latency on every chunk.
                    _ = WarmUpTargetConnectionAsync(context.TargetConnectionString, targetDbName, targetColName);

                    // Execute restore
                    bool success = await ExecuteRestoreProcessAsync(
                        mu,
                        chunkIndex,
                        restoreArgs.args,
                        restoreArgs.docCount,
                        dumpFilePath
                    );

                    if (success)
                    {
                        await HandleRestoreSuccessAsync(context, dumpFilePath);
                        _log?.WriteLine($"Coordinator: Completed restore {Log.FormatNamespaceForLog(sourceDbName, sourceColName, targetDbName, targetColName)}[{chunkIndex}]", LogType.Debug);
                    }
                    else
                    {
                        // Check if already uploaded (idempotency) - re-read from storage to avoid stale cache
                        var freshMu = MigrationJobContext.GetMigrationUnitFromStorage(MigrationJobContext.CurrentlyActiveJob?.Id, context.MigrationUnitId);
                        if (freshMu != null && freshMu.MigrationChunks[chunkIndex].IsUploaded == true)
                        {
                            ProcessRestoreSuccess(context);
                        }
                        else
                        {
                            HandleRestoreFailure(context, TaskResult.Retry);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine($"Coordinator: Restore cancelled for {sourceDbName}.{sourceColName}[{chunkIndex}]", LogType.Debug);
                }
                HandleRestoreFailure(context, TaskResult.Canceled);
            }
            catch (Exception ex)
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine($"Coordinator: Error restoring {Log.FormatNamespaceForLog(sourceDbName, sourceColName, targetDbName, targetColName)}[{chunkIndex}]: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                }
                HandleRestoreFailure(context, TaskResult.Retry, ex);
            }
            finally
            {
                // Always release worker slot
                _restorePool?.Release();
            }
        }

        /// <summary>
        /// Simulates restore for a chunk in simulation mode
        /// </summary>
        private void SimulateRestoreChunk(DumpRestoreProcessContext context)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.SimulateRestoreChunk: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
            int chunkIndex = context.ChunkIndex;

            // Calculate progress
            //double initialPercent = ((double)100 / mu.MigrationChunks.Count) * chunkIndex;
            //double contributionFactor = (double)mu.MigrationChunks[chunkIndex].DumpQueryDocCount / Helper.GetMigrationUnitDocCount(mu);
            //if (mu.MigrationChunks.Count == 1) contributionFactor = 1;

            // Simulate successful restore
            MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
            {
                m.MigrationChunks[chunkIndex].RestoredSuccessDocCount = m.MigrationChunks[chunkIndex].DumpQueryDocCount;
                m.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
                m.MigrationChunks[chunkIndex].IsUploaded = true;
                m.RestorePercent = 100;
            }, updateParent: true);

            _log?.WriteLine($"Simulation mode: Chunk {chunkIndex} restore simulated - 100.00% complete");

            // Small delay to simulate processing time
            try { Task.Delay(50, _processCts?.Token ?? CancellationToken.None).Wait(); } catch { }

            ProcessRestoreSuccess(context);
        }

        /// <summary>
        /// Validates that the dump file exists before attempting restore
        /// </summary>
        private bool ValidateDumpFileExists(DumpRestoreProcessContext context)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ValidateDumpFileExists: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);

            var dumpFilePath = GetDumpFilePath(mu, context.ChunkIndex);
            if (!StorageStreamFactory.Exists(dumpFilePath))
            {
                int chunkIndex = context.ChunkIndex;

                _log?.WriteLine($"Chunk file missing for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] during restore.", LogType.Warning);

                bool wasDown = false;
                bool wasUp = false;
                MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                {
                    wasDown = m.MigrationChunks[chunkIndex].IsDownloaded == true;
                    wasUp = m.MigrationChunks[chunkIndex].IsUploaded == true;
                    // Don't corrupt a chunk that has already been successfully uploaded. The
                    // dump file is intentionally deleted by FinalizeRestoreChunk after a
                    // successful upload, so a later redundant restore context for the same
                    // chunk would otherwise wipe IsDownloaded for a fully done chunk.
                    if (!wasUp)
                    {
                        m.MigrationChunks[chunkIndex].IsDownloaded = false;
                        m.DumpComplete = false;
                    }
                }, updateParent: true);

                if (wasUp)
                {
                    _log?.WriteLine($"ValidateDumpFileExists: chunk [{chunkIndex}] already uploaded; treating restore context as success (ctxId={context.Id}).", LogType.Debug);
                    ProcessRestoreSuccess(context);
                    return true; // chunk is already done
                }

                if (_activeMigrationUnits.TryGetValue(mu.Id, out var trk))
                {
                    _log?.WriteLine($"ValidateDumpFileExists: cleared IsDownloaded for chunk [{chunkIndex}] ctxId={context.Id} (wasDown={wasDown}, wasUp={wasUp}). Tracker NOT decremented. Current dl={trk.DownloadedChunks}/{trk.TotalChunks}, res={trk.RestoredChunks}/{trk.TotalChunks}", LogType.Debug);
                }
                else
                {
                    _log?.WriteLine($"ValidateDumpFileExists: cleared IsDownloaded for chunk [{chunkIndex}] ctxId={context.Id} (wasDown={wasDown}, wasUp={wasUp}). MU not in active tracker.", LogType.Debug);
                }

                HandleRestoreFailure(context, TaskResult.Canceled);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Builds restore arguments including drop/index options and insertion workers
        /// </summary>
        private (string args, long docCount) BuildRestoreArguments(
            MigrationUnit mu,
            int chunkIndex,
            string targetConnectionString,
            string sourceDatabaseName,
            string sourceCollectionName,
            string targetDatabaseName,
            string targetCollectionName)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.BuildRestoreArguments: source={sourceDatabaseName}.{sourceCollectionName}, target={targetDatabaseName}.{targetCollectionName}, chunkIndex={chunkIndex}");
            // Always --noIndexRestore: collection + indexes are pre-created upfront so mongorestore stays a pure data path; lets every chunk run in parallel without index-build contention on the target.
            string args = $" --uri={QuoteMongoToolArgument(targetConnectionString)} --gzip --archive --noIndexRestore";
            args = $"{args} --nsFrom={QuoteMongoToolArgument($"{sourceDatabaseName}.{sourceCollectionName}")} --nsTo={QuoteMongoToolArgument($"{targetDatabaseName}.{targetCollectionName}")}";

            //removed as we built indexes and collections earlier
            /*
            // Handle drop and index restore for first chunk
            if (chunkIndex == 0 && !MigrationJobContext.CurrentlyActiveJob.AppendMode)
            {
                args = $"{args} --drop"; 
                if (MigrationJobContext.CurrentlyActiveJob.SkipIndexes)
                {
                    args = $"{args} --noIndexRestore";
                }
            }
            else
            {
                // No index restore for subsequent chunks
                args = $"{args} --noIndexRestore";
            }
            */

            // Calculate doc count
            long docCount = (mu.MigrationChunks.Count > 1)
                ? mu.MigrationChunks[chunkIndex].DumpQueryDocCount
                : Helper.GetMigrationUnitDocCount(mu);

            // Configure insertion workers
            int insertionWorkers = GetInsertionWorkersCount();
            _log?.WriteLine($"Restore will use {insertionWorkers} insertion worker(s) for {Log.FormatNamespaceForLog(sourceDatabaseName, sourceCollectionName, targetDatabaseName, targetCollectionName)}[{chunkIndex}] ({docCount} docs)", LogType.Debug);

            if (insertionWorkers > 1)
            {
                args = $"{args} --numInsertionWorkersPerCollection={insertionWorkers}";
            }

            return (args, docCount);
        }

        private static string QuoteMongoToolArgument(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return "\"\"";
            }

            return $"\"{value.Replace("\\", "\\\\").Replace("\"", "\\\"")}\"";
        }

        /// <summary>
        /// Gets the configured insertion workers count
        /// </summary>
        private int GetInsertionWorkersCount()
        {
            return WorkerCountHelper.GetInsertionWorkersCount(
                MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers,
                WorkerCountHelper.CalculateDefaultInsertionWorkers()
            );
        }

        /// <summary>
        /// Warms up connection to the target collection by performing an async findOne operation.
        /// This runs in the background and does not block the restore operation.
        /// </summary>
        private async Task WarmUpTargetConnectionAsync(string targetConnectionString, string dbName, string colName)
        {
            try
            {
                var targetClient = MongoClientFactory.Create(_log, targetConnectionString);
                var targetDb = targetClient.GetDatabase(dbName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(colName);
                
                // Perform a findOne to warm up the connection
                await targetCollection.Find(new BsonDocument()).Limit(1).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                // Log but don't fail - this is just a warm-up operation
                _log?.WriteLine($"WarmUp findOne for {dbName}.{colName} failed: {Helper.RedactPii(ex.Message)}", LogType.Debug);
            }
        }

        /// <summary>
        /// Executes the mongorestore process
        /// </summary>
        private async Task<bool> ExecuteRestoreProcessAsync(
            MigrationUnit mu,
            int chunkIndex,
            string args,
            long docCount,
            string dumpFilePath,
            MigrationChunk? chunkOverride = null)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ExecuteRestoreProcessAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}, docCount={docCount}");
            // Calculate progress factors
            //double initialPercent = ((double)100 / mu.MigrationChunks.Count) * chunkIndex;
            //double contributionFactor = (double)mu.MigrationChunks[chunkIndex].DumpQueryDocCount / Helper.GetMigrationUnitDocCount(mu);
            //if (mu.MigrationChunks.Count == 1) contributionFactor = 1;

            // Execute restore process
            var processExecutor = new ProcessExecutor(_log);
            var restoreToolPath = GetMongoToolPath("mongorestore");
            // Merge restore-to-temp passes a throwaway chunk clone so ProcessExecutor's per-line
            // RestoredSuccessDocCount/RestoredFailedDocCount writes don't trample the live cached chunk.
            var chunkForExec = chunkOverride ?? mu.MigrationChunks[chunkIndex];
            bool success = await Task.Run(() => processExecutor.Execute(
                mu,
                chunkForExec,
                chunkIndex,                
                docCount,
                restoreToolPath,
                args,
                dumpFilePath,
                _processCts?.Token ?? CancellationToken.None,
                _ignoreDuplicatesAndContinueRestore,
                _continuousDuplicateThreshold,
                onProcessStarted: pid => MigrationJobContext.ActiveRestoreProcessIds.Add(pid),
                onProcessEnded: pid => MigrationJobContext.ActiveRestoreProcessIds.Remove(pid)
            ), _processCts?.Token ?? CancellationToken.None);

            return success;
        }

        private string GetMongoToolPath(string toolName)
        {
            bool canUseConfiguredToolPath = Helper.IsWindows();

            if (canUseConfiguredToolPath && toolName.Equals("mongodump", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(_mongoDumpToolPath))
            {
                return _mongoDumpToolPath;
            }

            if (canUseConfiguredToolPath && toolName.Equals("mongorestore", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(_mongoRestoreToolPath))
            {
                return _mongoRestoreToolPath;
            }

            if (string.IsNullOrWhiteSpace(_mongoToolsFolder))
            {
                return toolName;
            }

            string toolFileName = toolName;
            if (Helper.IsWindows() && !toolName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase))
            {
                toolFileName = toolName + ".exe";
            }

            string toolsFolder = _mongoToolsFolder.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            return Path.Combine(toolsFolder, toolFileName);
        }

        /// <summary>
        /// Handles successful restore completion with validation
        /// </summary>
        private async Task HandleRestoreSuccessAsync(DumpRestoreProcessContext context, string dumpFilePath)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.HandleRestoreSuccessAsync: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
            int chunkIndex = context.ChunkIndex;
            var chunk = mu.MigrationChunks[chunkIndex];

            // Validate restored document count if there were failures
            if (chunk.RestoredFailedDocCount > 0)
            {
                bool shouldRetry = await ValidateRestoredChunkDocumentCountAsync(mu, chunkIndex, context.TargetConnectionString);
                if (shouldRetry)
                {
                    HandleRestoreFailure(context, TaskResult.Retry);
                    return;
                }
            }

            // mongorestore doesn't always report doc count, calculate from target count percent
            if (chunk.RestoredSuccessDocCount == 0)
            {
                long docCount = (mu.MigrationChunks.Count > 1)
                    ? mu.MigrationChunks[chunkIndex].DumpQueryDocCount
                    : Helper.GetMigrationUnitDocCount(mu);

                chunk.RestoredSuccessDocCount = docCount - chunk.RestoredFailedDocCount;
            }

            _log?.WriteLine($"{mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] restore processing completed", LogType.Debug);

            // Finalize restore chunk
            FinalizeRestoreChunk(mu, chunkIndex, dumpFilePath);

            ProcessRestoreSuccess(context);
        }

        /// <summary>
        /// Validates restored chunk document count against target collection
        /// </summary>
        private async Task<bool> ValidateRestoredChunkDocumentCountAsync(
            MigrationUnit mu,
            int chunkIndex,
            string targetConnectionString)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ValidateRestoredChunkDocumentCountAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            try
            {
                // Get target collection
                var targetClient = MongoClientFactory.Create(_log, targetConnectionString);
                var targetDbName = mu.GetEffectiveTargetDatabaseName();
                var targetCollectionName = mu.GetEffectiveTargetCollectionName();
                var targetDb = targetClient.GetDatabase(targetDbName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(targetCollectionName);

                // Get chunk bounds and count in target
                var bounds = SamplePartitioner.GetChunkBounds(
                    mu.MigrationChunks[chunkIndex].Gte!,
                    mu.MigrationChunks[chunkIndex].Lt ?? "",
                    mu.MigrationChunks[chunkIndex].Lte ?? "",
                    mu.MigrationChunks[chunkIndex].DataType
                );
                var gte = bounds.gte;
                var lt = bounds.lt;
                var lte = bounds.lte;

                mu.MigrationChunks[chunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(
                    targetCollection,
                    gte,
                    lt,
                    lte,
                    mu.MigrationChunks[chunkIndex].DataType,
                    MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!),
                    mu.SkipDataTypeFilterForId
                );

                // Check if counts match
                if (mu.MigrationChunks[chunkIndex].DocCountInTarget >= mu.MigrationChunks[chunkIndex].DumpQueryDocCount)
                {
                    _log?.WriteLine($"Restore for {Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDbName, targetCollectionName)}[{chunkIndex}] No documents missing, count in Target: {mu.MigrationChunks[chunkIndex].DocCountInTarget}", LogType.Info);
                    var countInTargetA = mu.MigrationChunks[chunkIndex].DocCountInTarget;
                    var dupCountA = mu.MigrationChunks[chunkIndex].RestoredFailedDocCount;
                    MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
                    {
                        m.MigrationChunks[chunkIndex].DocCountInTarget = countInTargetA;
                        m.MigrationChunks[chunkIndex].SkippedAsDuplicateCount = dupCountA;
                        m.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
                    }, updateParent: true);
                    return false; // Don't retry
                }
                else
                {
                    _log?.WriteLine($"Restore for {Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDbName, targetCollectionName)}[{chunkIndex}] Documents missing, Chunk will be reprocessed", LogType.Error);
                    var countInTargetB = mu.MigrationChunks[chunkIndex].DocCountInTarget;
                    MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
                    {
                        m.MigrationChunks[chunkIndex].DocCountInTarget = countInTargetB;
                    }, updateParent: true);
                    return true; // Retry
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Restore for {Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, mu.GetEffectiveTargetDatabaseName(), mu.GetEffectiveTargetCollectionName())}[{chunkIndex}] encountered error while counting documents on target. Chunk will be reprocessed. Details: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return true; // Retry on error
            }
        }

        /// <summary>
        /// Finalizes restore chunk by marking as uploaded and deleting dump file
        /// </summary>
        private void FinalizeRestoreChunk(MigrationUnit mu, int chunkIndex, string dumpFilePath)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.FinalizeRestoreChunk: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}");
            MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
            {
                m.MigrationChunks[chunkIndex].IsUploaded = true;
            }, updateParent: true);

            // Delete dump file
            try
            {
                StorageStreamFactory.DeleteIfExists(dumpFilePath);
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Failed to delete dump file {dumpFilePath}: {Helper.RedactPii(ex.ToString())}", LogType.Debug);
            }
        }

        /// <summary>
        /// Handles successful restore completion
        /// </summary>
        private void ProcessRestoreSuccess(DumpRestoreProcessContext context)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ProcessRestoreSuccess: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
            int chunkIndex = context.ChunkIndex;

            // Mark chunk as completed
            context.State = ProcessState.Completed;
            context.CompletedAt = DateTime.UtcNow;

            bool wasAlreadyUploaded = false;

            // Atomically read-modify-write to avoid lost updates from concurrent workers
            var mu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
            {
                wasAlreadyUploaded = m.MigrationChunks[chunkIndex].IsUploaded == true;
                m.MigrationChunks[chunkIndex].IsUploaded = true;
            }, updateParent: true);

            _log?.WriteLine($"ProcessRestoreSuccess: chunk [{chunkIndex}] ctxId={context.Id} wasAlreadyUploaded={wasAlreadyUploaded}, ctxState={context.State}, retryCount={context.RetryCount}", LogType.Debug);

            bool isUpPersisted = mu?.MigrationChunks[chunkIndex].IsUploaded == true;
            bool isDownPersisted = mu?.MigrationChunks[chunkIndex].IsDownloaded == true;
            _log?.WriteLine($"ProcessRestoreSuccess: chunk [{chunkIndex}] ctxId={context.Id} persisted IsUploaded={isUpPersisted}, IsDownloaded={isDownPersisted}", LogType.Debug);

            // Update tracker only if this is a transition (not a duplicate success for an already-uploaded chunk)
            if (!wasAlreadyUploaded)
            {
                UpdateMigrationUnitTracker(context.MigrationUnitId, restoreIncrement: 1, caller: "ProcessRestoreSuccess", chunkIndex: chunkIndex);
            }
            else
            {
                _log?.WriteLine($"ProcessRestoreSuccess: SKIP tracker increment for chunk [{chunkIndex}] - already uploaded", LogType.Debug);
            }

            // Remove from restore manifest
            _uploadManifest.TryRemove(context.Id, out _);

            LogRestoreTerminalOutcome(context, mu, "completed");
        }

        private void LogRestoreTerminalOutcome(DumpRestoreProcessContext context, MigrationUnit? mu, string outcome)
        {
            try
            {
                var chunk = (mu != null && context.ChunkIndex >= 0 && context.ChunkIndex < mu.MigrationChunks.Count)
                    ? mu.MigrationChunks[context.ChunkIndex]
                    : null;

                string chunkId = chunk?.Id ?? string.Empty;
                bool isUploaded = chunk?.IsUploaded == true;
                bool needsCleanup = chunk?.NeedsCleanup == true;
                long restoredSuccess = chunk?.RestoredSuccessDocCount ?? 0;
                long restoredFailed = chunk?.RestoredFailedDocCount ?? 0;

                _log?.WriteLine(
                    $"RestoreTerminalOutcome: Outcome={outcome}; ProcessorRunId={context.ProcessorRunId}; ContextId={context.Id}; MigrationUnitId={context.MigrationUnitId}; ChunkIndex={context.ChunkIndex}; ChunkId={chunkId}; RetryCount={context.RetryCount}; State={context.State}; NeedsCleanup={needsCleanup}; IsUploaded={isUploaded}; RestoredSuccessDocCount={restoredSuccess}; RestoredFailedDocCount={restoredFailed}",
                    LogType.Debug);
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error logging restore terminal outcome for ContextId={context.Id}: {Helper.RedactPii(ex.ToString())}", LogType.Debug);
            }
        }

        /// <summary>
        /// Handles download failure with retry logic
        /// </summary>
        private void HandleDumpFailure(DumpRestoreProcessContext context, TaskResult result, Exception? ex = null)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.HandleDumpFailure: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}, result={result}");

            if (MigrationJobContext.ControlledPauseRequested)
            {
                _downloadManifest.TryRemove(context.Id, out _);
                return;
            }

            // If coordinator is stopped (e.g. job being stopped/paused), don't retry.
            // Without this check, killed processes would be re-enqueued during the
            // window between KillAllMigrationProcesses and StopCoordinatedProcessing.
            if (_stopped || _processCts?.Token.IsCancellationRequested == true)
            {
                _downloadManifest.TryRemove(context.Id, out _);
                return;
            }

            try
            {
                context.LastError = ex;
                context.RetryCount++;

                const int MaxRetries = 3;

                if (context.RetryCount >= MaxRetries || result == TaskResult.Abort)
                {
                    context.State = ProcessState.Failed;
                    var mu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                    {
                        // Mark collection as skipped due to max retries instead of pausing
                        // Do NOT mark as complete - the collection actually failed
                        m.SkippedDueToMaxRetries = true;
                        m.FailedOperation = "Dump";
                    }, updateParent: true);
                    if (mu != null)
                    {
                        _log.WriteLine($"Max retries reached for dump: {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]. Collection marked as skipped. User can restart the job to retry.", LogType.Error);
                    }
                    else
                    {
                        _log.WriteLine($"Max retries for download : MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Error);
                    }
                }
                else
                {
                    // Reset to pending for retry
                    context.State = ProcessState.Pending;
                    var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (mu != null)
                    {
                        _log.WriteLine($"Download will retry ({context.RetryCount}/{MaxRetries}): {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]", LogType.Warning);
                    }
                    else
                    {
                        _log.WriteLine($"Download will retry ({context.RetryCount}/{MaxRetries}): MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Warning);
                    }
                }
            }
            catch (Exception e)
            {
                _log?.WriteLine($"Error handling download failure: {Helper.RedactPii(e.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Handles restore failure with retry logic
        /// </summary>
        private void HandleRestoreFailure(DumpRestoreProcessContext context, TaskResult result, Exception? ex = null)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.HandleRestoreFailure: muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}, result={result}");

            if (MigrationJobContext.ControlledPauseRequested)
            {
                // If an in-flight restore was cancelled during immediate/controlled pause,
                // persist cleanup intent so resume can clean and retry safely.
                if (result == TaskResult.Canceled || context.State == ProcessState.Processing)
                {
                    try
                    {
                        int chIdx = context.ChunkIndex;
                        var pausedMu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                        {
                            if (chIdx >= 0 && chIdx < m.MigrationChunks.Count)
                            {
                                m.MigrationChunks[chIdx].Attempt++;
                                m.MigrationChunks[chIdx].NeedsCleanup = true;
                            }
                        }, updateParent: true);
                        if (pausedMu != null && chIdx >= 0 && chIdx < pausedMu.MigrationChunks.Count)
                        {
                            _log?.WriteLine(
                                $"Restore cancelled due to controlled pause for {pausedMu.DatabaseName}.{pausedMu.CollectionName}[{chIdx}]. Marked for merge and retry.",
                                LogType.Warning);
                        }
                    }
                    catch (Exception pauseEx)
                    {
                        _log?.WriteLine($"Error persisting paused restore merge state: {Helper.RedactPii(pauseEx.ToString())}", LogType.Error);
                    }
                }

                var pausedMuForLog = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                LogRestoreTerminalOutcome(context, pausedMuForLog, "canceled-controlled-pause");

                _uploadManifest.TryRemove(context.Id, out _);
                return;
            }

            // If coordinator is stopped (e.g. job being stopped/paused), don't retry.
            if (_stopped || _processCts?.Token.IsCancellationRequested == true)
            {
                var stoppedMuForLog = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                LogRestoreTerminalOutcome(context, stoppedMuForLog, "canceled-stop");
                _uploadManifest.TryRemove(context.Id, out _);
                return;
            }

            try
            {
                context.LastError = ex;
                context.RetryCount++;

                const int MaxRetries = 3;

                if (context.RetryCount >= MaxRetries || result == TaskResult.Abort)
                {
                    context.State = ProcessState.Failed;
                    var mu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                    {
                        // Mark collection as skipped due to max retries instead of pausing
                        // Do NOT mark as complete - the collection actually failed
                        m.SkippedDueToMaxRetries = true;
                        m.FailedOperation = "Restore";
                    }, updateParent: true);
                    if (mu != null)
                    {
                        _log.WriteLine($"Max retries reached for restore: {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]. Collection marked as skipped. User can restart the job to retry.", LogType.Error);
                    }
                    else
                    {
                        _log.WriteLine($"Max retries for restore complete: MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Error);
                    }

                    LogRestoreTerminalOutcome(context, mu, result == TaskResult.Abort ? "failed-abort" : "failed-max-retries");
                }
                else
                {
                    // Reset to pending for retry
                    int chIdxR = context.ChunkIndex;
                    int retryCountR = context.RetryCount;
                    var mu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                    {
                        if (chIdxR >= 0 && chIdxR < m.MigrationChunks.Count)
                        {
                            // Keep chunk attempt in sync with context retries; ProcessExecutor may have
                            // already incremented Attempt for threshold-triggered duplicate-key failures.
                            if (m.MigrationChunks[chIdxR].Attempt < retryCountR)
                            {
                                m.MigrationChunks[chIdxR].Attempt = retryCountR;
                            }
                            m.MigrationChunks[chIdxR].NeedsCleanup = true;
                        }
                    }, updateParent: true);
                    if (mu != null)
                    {
                        if (chIdxR >= 0 && chIdxR < mu.MigrationChunks.Count)
                        {
                            _log.WriteLine(
                                $"Restore will retry ({context.RetryCount}/{MaxRetries}): {mu.DatabaseName}.{mu.CollectionName}[{chIdxR}]. Merge queued.",
                                LogType.Warning);
                        }
                        else
                        {
                            _log.WriteLine($"Restore will retry ({context.RetryCount}/{MaxRetries}): {mu.DatabaseName}.{mu.CollectionName}[{chIdxR}]", LogType.Warning);
                        }
                    }
                    else
                    {
                        _log.WriteLine($"Restore will retry ({context.RetryCount}/{MaxRetries}): MU:{context.MigrationUnitId}[{chIdxR}]", LogType.Warning);
                    }

                    context.State = ProcessState.Pending;
                    _log?.WriteLine($"HandleRestoreFailure: chunk [{context.ChunkIndex}] set Pending, retryCount={context.RetryCount}, NeedsCleanup=true, contextId={context.Id}, inUploadManifest={_uploadManifest.ContainsKey(context.Id)}", LogType.Debug);
                }
            }
            catch (Exception handlerEx)
            {
                _log?.WriteLine($"Error handling restore failure: {Helper.RedactPii(handlerEx.ToString())}", LogType.Error);
            }
        }

        private bool GetChunkNeedsCleanup(DumpRestoreProcessContext context)
        {
            try
            {
                var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                if (mu == null || context.ChunkIndex < 0 || context.ChunkIndex >= mu.MigrationChunks.Count)
                {
                    return false;
                }

                return mu.MigrationChunks[context.ChunkIndex].NeedsCleanup;
            }
            catch
            {
                return false;
            }
        }

        private bool IsRetryContext(DumpRestoreProcessContext context)
        {
            // Attempt > 0 means the persisted chunk has already failed at least once; used by the scheduler to reserve a 25% retry slot pool so retries don't starve.
            try
            {
                var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                if (mu == null || context.ChunkIndex < 0 || context.ChunkIndex >= mu.MigrationChunks.Count)
                {
                    return false;
                }

                return mu.MigrationChunks[context.ChunkIndex].Attempt > 0;
            }
            catch
            {
                return false;
            }
        }

        private async Task<bool> RunCleanupForRestoreRetryAsync(DumpRestoreProcessContext context, CancellationToken cancellationToken)
        {
            // Acquire per-chunk lock to prevent concurrent merge/cleanup on the same chunk.
            // This avoids WriteConflict when an old attempt's finally-block DROP races with
            // a newly-dispatched attempt's mongorestore CREATE on the same temp collection.
            string chunkKey = $"{context.MigrationUnitId}_{context.ChunkIndex}";
            var chunkLock = _chunkMergeLocks.GetOrAdd(chunkKey, _ => new SemaphoreSlim(1, 1));

            await chunkLock.WaitAsync(cancellationToken);

            try
            {
                return await RunCleanupForRestoreRetryCoreAsync(context, cancellationToken);
            }
            finally
            {
                chunkLock.Release();
            }
        }

        private async Task<bool> RunCleanupForRestoreRetryCoreAsync(DumpRestoreProcessContext context, CancellationToken cancellationToken)
        {
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
            if (mu == null)
            {
                context.State = ProcessState.Failed;
                _log?.WriteLine($"[Merge] Failed: migration unit not found for MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Debug);
                return false;
            }

            int chunkIndex = context.ChunkIndex;
            if (chunkIndex < 0 || chunkIndex >= mu.MigrationChunks.Count)
            {
                context.State = ProcessState.Failed;
                _log?.WriteLine($"[Merge] Failed: invalid chunk index for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]", LogType.Debug);
                return false;
            }

            string sourceDbName = mu.DatabaseName;
            string sourceColName = mu.CollectionName;
            string targetDbName = mu.GetEffectiveTargetDatabaseName();
            string targetColName = mu.GetEffectiveTargetCollectionName();
            // Per-chunk temp name keeps concurrent retries on different chunks of the same collection isolated; if a previous attempt crashed mid-merge, restoring into a fresh-but-deterministic temp lets the new attempt drop+recreate without touching the live target.
            string tempColName = $"{targetColName}.temp4merge_{chunkIndex}";
            string nsLog = Log.FormatNamespaceForLog(sourceDbName, sourceColName, targetDbName, targetColName);
            string nsChunkLog = $"{nsLog}[{chunkIndex}]";

            _log?.ShowInMonitor($"[Merge] Starting: {nsChunkLog}");
            _log?.WriteLine($"[Merge] Starting for {nsChunkLog}, tempCollection={targetDbName}.{tempColName}", LogType.Info);

            try
            {
                if (MigrationJobContext.ControlledPauseRequested || MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true)
                {
                    throw new OperationCanceledException(cancellationToken);
                }

                var targetClient = MongoClientFactory.Create(_log, context.TargetConnectionString);
                var targetDb = targetClient.GetDatabase(targetDbName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(targetColName);
                var tempCollection = targetDb.GetCollection<BsonDocument>(tempColName);

                // Step 1: Drop temp collection if it exists (leftover from a previous failed attempt)
                await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, cancellationToken);

                // Step 2: Restore chunk to the temp collection via mongorestore
                _log?.ShowInMonitor($"[Merge] Restoring to temp: {nsChunkLog}");

                // Check if dump file exists before attempting restore.
                // If missing, clear NeedsCleanup so the normal dump dispatcher can re-download.
                string dumpFilePathCheck = GetDumpFilePath(sourceDbName, sourceColName, chunkIndex);
                if (!StorageStreamFactory.Exists(dumpFilePathCheck))
                {
                    bool wasDownM = false;
                    bool wasUpM = false;
                    // Peek state without mutating so we don't corrupt an already-finalized chunk.
                    // FinalizeRestoreChunk deletes the dump file after a successful upload, so a
                    // stale merge context arriving after that point would otherwise wipe state
                    // on a chunk that's fully done.
                    var snap = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (snap?.MigrationChunks != null && chunkIndex < snap.MigrationChunks.Count)
                    {
                        wasDownM = snap.MigrationChunks[chunkIndex].IsDownloaded == true;
                        wasUpM = snap.MigrationChunks[chunkIndex].IsUploaded == true;
                    }

                    if (wasUpM)
                    {
                        _log?.WriteLine($"[Merge] Dump file missing for {nsChunkLog} but chunk already uploaded; treating as success and discarding stale merge context (ctxId={context.Id}).", LogType.Info);
                        _uploadManifest.TryRemove(context.Id, out _);
                        await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, cancellationToken);
                        return false; // chunk is done; don't requeue, don't re-download
                    }

                    _log?.WriteLine($"[Merge] Dump file missing for {nsChunkLog}. Clearing merge state for re-download.", LogType.Warning);
                    mu = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                    {
                        m.MigrationChunks[chunkIndex].IsDownloaded = false;
                        m.MigrationChunks[chunkIndex].NeedsCleanup = false;
                        m.MigrationChunks[chunkIndex].Attempt = 0;
                        m.DumpComplete = false;
                    }, updateParent: true);
                    if (mu != null && _activeMigrationUnits.TryGetValue(mu.Id, out var trkM))
                    {
                        _log?.WriteLine($"MergeDumpMissing: cleared IsDownloaded for chunk [{chunkIndex}] ctxId={context.Id} (wasDown={wasDownM}, wasUp={wasUpM}). Tracker NOT decremented. Current dl={trkM.DownloadedChunks}/{trkM.TotalChunks}, res={trkM.RestoredChunks}/{trkM.TotalChunks}", LogType.Debug);
                    }
                    context.State = ProcessState.Pending;
                    _uploadManifest.TryRemove(context.Id, out _);
                    await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, cancellationToken);

                    // Re-add chunk to download pipeline so the dump dispatcher picks it up
                    PrepareDownloadList(mu, context.SourceConnectionString, context.TargetConnectionString);

                    return false; // don't requeue — let dump dispatcher re-download
                }

                bool restoreSuccess = await RestoreChunkToTempAsync(mu, chunkIndex, context, targetDbName, tempColName, cancellationToken);
                if (!restoreSuccess)
                {
                    _log?.WriteLine($"[Merge] Restore to temp failed for {nsChunkLog}. Will re-queue.", LogType.Warning);
                    await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, cancellationToken);
                    return true; // requeue
                }

                // Step 3: Remove docs from temp that already exist in target (Removing duplicates)
                int pageSize = GetCleanupDeletePageSize();
                _log?.ShowInMonitor($"[Merge] Removing duplicates: {nsChunkLog}");

                var chunk = mu.MigrationChunks[chunkIndex];
                var bounds = SamplePartitioner.GetChunkBounds(
                    chunk.Gte ?? "", chunk.Lt ?? "", chunk.Lte ?? "", chunk.DataType);
                var userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
                var chunkFilter = MongoHelper.GenerateQueryFilter(
                    bounds.gte, bounds.lt, bounds.lte, chunk.DataType, userFilterDoc,
                    mu.SkipDataTypeFilterForId);

                long preDeduped = await MongoHelper.RemoveDuplicatesFromTempAsync(
                    tempCollection, targetCollection, chunkFilter, pageSize, nsChunkLog, _log, cancellationToken);

                // Step 4: Insert remaining docs from temp to target in parallel
                _log?.ShowInMonitor($"[Merge] Inserting to target: {nsChunkLog}");
                int parallelThreads = GetInsertionWorkersCount()*4;
                var (totalInserted, totalSkipped) = await MongoHelper.InsertTempToTargetInParallelAsync(
                    tempCollection, targetCollection, parallelThreads, pageSize, nsChunkLog, _log, cancellationToken);
                totalSkipped += preDeduped;

                _log?.WriteLine($"[Merge] {nsChunkLog}: newDocsAdded={totalInserted}, duplicatesSkipped={totalSkipped} (preDeduped={preDeduped}, skippedDuringInsert={totalSkipped - preDeduped})", LogType.Info);

                // Step 4: Drop temp collection
                await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, cancellationToken);

                // Step 5: Finalize - mark chunk as successfully restored
                long restoredCount;
                long localTotalInserted = totalInserted;
                long localTotalSkipped = totalSkipped;

                var muLocal = MigrationJobContext.MutateMigrationUnit(context.MigrationUnitId, m =>
                {
                    m.MigrationChunks[chunkIndex].NeedsCleanup = false;
                    m.MigrationChunks[chunkIndex].Attempt = 0;
                    // Use DumpQueryDocCount when available; fall back to actual inserted + skipped count
                    long rc = m.MigrationChunks[chunkIndex].DumpQueryDocCount;
                    if (rc == 0)
                        rc = localTotalInserted + localTotalSkipped;
                    m.MigrationChunks[chunkIndex].RestoredSuccessDocCount = rc;
                    m.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
                    m.MigrationChunks[chunkIndex].SkippedAsDuplicateCount = localTotalSkipped;
                    m.MigrationChunks[chunkIndex].IsUploaded = true;
                }, updateParent: true);
                restoredCount = muLocal?.MigrationChunks[chunkIndex].RestoredSuccessDocCount ?? (localTotalInserted + localTotalSkipped);
                mu = muLocal ?? mu;

                // Update tracker and remove from manifest (same as ProcessRestoreSuccess)
                context.State = ProcessState.Completed;
                context.CompletedAt = DateTime.UtcNow;
                UpdateMigrationUnitTracker(mu.Id, restoreIncrement: 1, caller: "MergeSuccess", chunkIndex: chunkIndex);
                _uploadManifest.TryRemove(context.Id, out _);

                // Delete dump file now that data is in target
                try
                {
                    string dumpFilePath = GetDumpFilePath(sourceDbName, sourceColName, chunkIndex);
                    StorageStreamFactory.DeleteIfExists(dumpFilePath);
                }
                catch (Exception ex)
                {
                    _log?.WriteLine($"[Merge] Failed to delete dump file for {nsChunkLog}: {Helper.RedactPii(ex.Message)}", LogType.Debug);
                }

                _log?.ShowInMonitor($"[Merge] Completed: {nsChunkLog}, newDocsAdded={totalInserted}, duplicatesSkipped={totalSkipped}");
                _log?.WriteLine($"[Merge] Completed: {nsChunkLog}, newDocsAdded={totalInserted}, duplicatesSkipped={totalSkipped}", LogType.Info);
                return false; // success, don't requeue
            }
            catch (OperationCanceledException)
            {
                context.State = ProcessState.CleaningUp;
                // Best-effort drop of temp collection on cancellation
                try
                {
                    var targetClient = MongoClientFactory.Create(_log, context.TargetConnectionString);
                    var targetDb = targetClient.GetDatabase(targetDbName);
                    await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, CancellationToken.None);
                }
                catch { /* best effort */ }

                _log?.WriteLine($"[Merge] Canceled for {nsChunkLog} due to pause/cancellation.", LogType.Debug);
                return false;
            }
            catch (Exception ex)
            {
                context.State = ProcessState.CleaningUp;
                // Best-effort drop of temp collection on failure
                try
                {
                    var targetClient = MongoClientFactory.Create(_log, context.TargetConnectionString);
                    var targetDb = targetClient.GetDatabase(targetDbName);
                    await DropTempCollectionSafeAsync(targetDb, tempColName, nsChunkLog, CancellationToken.None);
                }
                catch { /* best effort */ }

                _log?.WriteLine($"[Merge] Exception for {nsChunkLog} (will re-queue): {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return true; // requeue for retry
            }
        }

        /// <summary>
        /// Restores a chunk to a temporary collection using mongorestore.
        /// </summary>
        private async Task<bool> RestoreChunkToTempAsync(
            MigrationUnit mu,
            int chunkIndex,
            DumpRestoreProcessContext context,
            string targetDbName,
            string tempColName,
            CancellationToken cancellationToken)
        {
            string sourceDbName = mu.DatabaseName;
            string sourceColName = mu.CollectionName;
            string dumpFilePath = GetDumpFilePath(sourceDbName, sourceColName, chunkIndex);

            if (!StorageStreamFactory.Exists(dumpFilePath))
            {
                // Peek state first. FinalizeRestoreChunk deletes the dump file after a successful
                // upload, so for an already-uploaded chunk this is expected, not an error — do not
                // clear IsDownloaded or it will corrupt the persisted state.
                bool wasUpR = false;
                var snapR = MigrationJobContext.GetMigrationUnit(mu.Id);
                if (snapR?.MigrationChunks != null && chunkIndex < snapR.MigrationChunks.Count)
                {
                    wasUpR = snapR.MigrationChunks[chunkIndex].IsUploaded == true;
                }
                if (wasUpR)
                {
                    _log?.WriteLine($"[Merge] Dump file missing at {dumpFilePath} but chunk already uploaded; skipping state reset.", LogType.Info);
                    return false;
                }
                _log?.WriteLine($"[Merge] Dump file missing at {dumpFilePath}. Marking chunk as not downloaded.", LogType.Warning);
                MigrationJobContext.MutateMigrationUnit(mu.Id, m =>
                {
                    m.MigrationChunks[chunkIndex].IsDownloaded = false;
                    m.DumpComplete = false;
                }, updateParent: true);
                return false;
            }

            // Build restore args targeting the temp collection
            var restoreArgs = BuildRestoreArguments(
                mu, chunkIndex, context.TargetConnectionString,
                sourceDbName, sourceColName,
                targetDbName, tempColName);

            // Pass a throwaway snapshot of the chunk to the executor. ProcessExecutor's restore
            // stdout parser writes RestoredSuccessDocCount/RestoredFailedDocCount on the chunk
            // reference per line, which would corrupt the live cached chunk's already-final
            // counts and crash the percent calc (download/upload % drop). The snapshot keeps
            // the live chunk untouched; the authoritative IsUploaded flip happens later under
            // MutateMigrationUnit on the real chunk.
            var realChunk = mu.MigrationChunks[chunkIndex];
            var chunkSnapshot = new MigrationChunk
            {
                Id = realChunk.Id,
                Gte = realChunk.Gte,
                Lt = realChunk.Lt,
                Lte = realChunk.Lte,
                DataType = realChunk.DataType,
                DumpQueryDocCount = realChunk.DumpQueryDocCount,
                DumpResultDocCount = realChunk.DumpResultDocCount,
            };

            bool success = await ExecuteRestoreProcessAsync(
                mu, chunkIndex, restoreArgs.args, restoreArgs.docCount, dumpFilePath, chunkSnapshot);

            return success;
        }

        /// <summary>
        /// Drops a temporary collection, logging but not throwing on failure.
        /// Includes a brief post-drop delay to let the server release the namespace catalog lock,
        /// preventing WriteConflict when mongorestore immediately tries to CREATE the same namespace.
        /// </summary>
        private async Task DropTempCollectionSafeAsync(
            IMongoDatabase targetDb, string tempColName, string nsChunkLog, CancellationToken cancellationToken)
        {
            try
            {
                await targetDb.DropCollectionAsync(tempColName, cancellationToken);
                await Task.Delay(3000, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Let cancellation propagate naturally
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"[Merge] Failed to drop temp collection {tempColName} for {nsChunkLog}: {Helper.RedactPii(ex.Message)}", LogType.Debug);
            }
        }

        private int GetCleanupDeletePageSize()
        {
            try
            {
                var config = new MigrationSettings();
                config.Load();

                // Keep cleanup paging aligned with regular Mongo copy page sizing.
                if (config.MongoCopyPageSize > 0)
                {
                    return config.MongoCopyPageSize;
                }
                else
                {
                    return 100;
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"[Merge] Failed to load MongoCopyPageSize from settings. Using default 500. Details: {Helper.RedactPii(ex.Message)}", LogType.Debug);
            }

            return 500;
        }

        /// <summary>
        /// Updates the migration unit tracker with progress
        /// </summary>
        private void UpdateMigrationUnitTracker(string muId, int downloadIncrement = 0, int restoreIncrement = 0, string caller = "", int chunkIndex = -1)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.UpdateMigrationUnitTracker: muId={muId}, downloadIncrement={downloadIncrement}, restoreIncrement={restoreIncrement}");
            try
            {
                if (_activeMigrationUnits.TryGetValue(muId, out var tracker))
                {
                    int beforeDown = tracker.DownloadedChunks;
                    int beforeRes = tracker.RestoredChunks;
                    // Use Interlocked for thread-safe updates since multiple chunks may complete in parallel
                    if (downloadIncrement != 0)
                        Interlocked.Add(ref tracker.DownloadedChunks, downloadIncrement);
                    if (restoreIncrement != 0)
                        Interlocked.Add(ref tracker.RestoredChunks, restoreIncrement);

                    _log?.WriteLine($"UpdateTracker: caller={caller}, muId={muId}, chunk=[{chunkIndex}], dlInc={downloadIncrement}, resInc={restoreIncrement}, downloaded={beforeDown}=>{tracker.DownloadedChunks}/{tracker.TotalChunks}, restored={beforeRes}=>{tracker.RestoredChunks}/{tracker.TotalChunks}, allDown={tracker.AllDownloadsCompleted}, allRes={tracker.AllRestoresCompleted}", LogType.Debug);
                }
                else
                {
                    _log?.WriteLine($"UpdateTracker: caller={caller}, muId={muId} chunk=[{chunkIndex}] NOT FOUND in _activeMigrationUnits (dlInc={downloadIncrement}, resInc={restoreIncrement})", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error updating migration unit tracker: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Checks for completed migration units and finalizes them
        /// </summary>
        private void CheckForCompletedMigrationUnits()
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.CheckForCompletedMigrationUnits");
            try
            {
                // Resync tracker counters from authoritative MU state. The in-memory increment
                // counters can fall behind reality when chunks are marked IsUploaded via the
                // idempotency path (ProcessRestoreSuccess after success==false but freshMu shows
                // the chunk already uploaded) which intentionally skips the tracker increment to
                // avoid double-counting concurrent successes.
                foreach (var t in _activeMigrationUnits.Values)
                {
                    var muSnap = MigrationJobContext.GetMigrationUnit(t.MigrationUnitId);
                    if (muSnap?.MigrationChunks != null && muSnap.MigrationChunks.Count > 0)
                    {
                        int actualDown = muSnap.MigrationChunks.Count(c => c.IsDownloaded == true);
                        int actualUp = muSnap.MigrationChunks.Count(c => c.IsUploaded == true);
                        if (actualDown > t.DownloadedChunks) Interlocked.Exchange(ref t.DownloadedChunks, actualDown);
                        if (actualUp > t.RestoredChunks) Interlocked.Exchange(ref t.RestoredChunks, actualUp);
                    }
                    _log?.WriteLine($"CheckCompleted: muId={t.MigrationUnitId}, downloaded={t.DownloadedChunks}/{t.TotalChunks}, restored={t.RestoredChunks}/{t.TotalChunks}, allDown={t.AllDownloadsCompleted}, allRes={t.AllRestoresCompleted}", LogType.Debug);
                }

                var completedUnits = _activeMigrationUnits.Values
                .Where(tracker => tracker.AllDownloadsCompleted && tracker.AllRestoresCompleted)
                .ToList();

                foreach (var tracker in completedUnits)
                {
                    string muId = tracker.MigrationUnitId;
                    string targetConnectionString = string.Empty;

                    // Mark migration unit as complete atomically
                    var mu = MigrationJobContext.MutateMigrationUnit(muId, m =>
                    {
                        m.DumpComplete = true;
                        m.DumpPercent = 100;
                        m.RestoreComplete = true;
                        m.RestorePercent = 100;
                        m.UpdateParentJob();

                        if (!m.BulkCopyEndedOn.HasValue || m.BulkCopyEndedOn.Value == DateTime.MinValue)
                        {
                            m.BulkCopyEndedOn = DateTime.UtcNow;
                        }
                    }, updateParent: true);

                    // Remove from active tracking
                    _activeMigrationUnits.TryRemove(muId, out _);

                    if (mu != null)
                    {
                        _log.WriteLine($"Migration unit completed: {mu.DatabaseName}.{mu.CollectionName}", LogType.Info);
                    }

                    // Notify via delegate

                    _onMigrationUnitCompleted?.Invoke(mu);
                }

                TryActivatePendingMigrationUnits();
                RefillManifestWorkingSets();
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error checking for completed migration units: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        /// <summary>
        /// Checks if all coordinated work is complete
        /// </summary>
        private bool IsAllWorkComplete()
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.IsAllWorkComplete called");
            try
            {
                if (!_processNewTasks)
                {
                    int totalDumpProcessing = _downloadManifest.Count(kvp => kvp.Value.State == ProcessState.Processing);
                    int totalRestoreProcessing = _uploadManifest.Count(kvp => kvp.Value.State == ProcessState.Processing);

                    bool result = (totalDumpProcessing + totalRestoreProcessing) == 0;
                    if (result)
                        _log?.WriteLine($"IsAllWorkComplete=true (processNewTasks=false, dumpProcessing={totalDumpProcessing}, restoreProcessing={totalRestoreProcessing})", LogType.Debug);
                    return result;
                }
                else
                {
                    bool activeMUEmpty = _activeMigrationUnits.IsEmpty;
                    bool dlManEmpty = _downloadManifest.IsEmpty;
                    bool ulManEmpty = _uploadManifest.IsEmpty;
                    bool pendMUEmpty = _pendingMigrationUnitIndex.IsEmpty;
                    bool dlBacklogEmpty = _downloadBacklogIndex.IsEmpty;
                    bool ulBacklogEmpty = _uploadBacklogIndex.IsEmpty;
                    bool result = activeMUEmpty && dlManEmpty && ulManEmpty && pendMUEmpty && dlBacklogEmpty && ulBacklogEmpty;
                    if (result)
                        _log?.WriteLine($"IsAllWorkComplete=true (all queues empty)", LogType.Debug);
                    return result;
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error checking if offline is complete: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return false;
            }
        }

        /// <summary>
        /// Stops the coordinator timer. Thread-safe.
        /// </summary>
        /// <summary>
        /// Signals the coordinator to stop accepting/spawning new work immediately.
        /// Call this BEFORE killing processes so that HandleDumpFailure/HandleRestoreFailure
        /// don't re-enqueue killed chunks for retry.
        /// </summary>
        public void SignalStop()
        {
            _stopped = true;
            _processNewTasks = false;
            _processCts?.Cancel();
        }

        public void StopCoordinatedProcessing()
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.StopCoordinatedProcessing");
            try
            {
                // Set stopped flag first - volatile write ensures any in-flight or
                // queued OnTimerTick callbacks exit immediately without doing work.
                _stopped = true;

                // Signal cancellation so any in-flight async work sees it too.
                _processCts?.Cancel();

                // Stop the timer outside the lock to prevent new ticks from firing
                // while we wait for an in-flight tick to release _timerLock.
                _processTimer?.Stop();

                lock (_timerLock)
                {
                    if (_processTimer != null)
                    {
                        if (_timerStarted)
                        {
                            _processTimer.Stop();
                            _timerStarted = false;
                        }
                        
                        // Properly dispose of the timer to stop all callbacks
                        _processTimer.Elapsed -= OnTimerTick;
                        _processTimer.Dispose();
                        _processTimer = null;
                        
                        _log?.WriteLine("Offline processing terminated and timer disposed.", LogType.Info);
                    }

                    // Clear manifests
                    _downloadManifest.Clear();
                    _uploadManifest.Clear();
                    _activeMigrationUnits.Clear();
                    _downloadBacklogIndex.Clear();
                    _uploadBacklogIndex.Clear();
                    _pendingMigrationUnitIndex.Clear();
                    ClearQueue(_downloadBacklog);
                    ClearQueue(_uploadBacklog);
                    ClearQueue(_pendingMigrationUnits);
                }

                // After the timer lock is released no new tasks can be spawned.
                // Drain and await all in-flight worker tasks so zombie threads cannot
                // outlive the coordinator and corrupt persisted state on resume.
                var pendingTasks = new List<Task>();
                while (_activeWorkerTasks.TryTake(out var t))
                    pendingTasks.Add(t);

                if (pendingTasks.Count > 0)
                {
                    _log?.WriteLine($"Waiting for {pendingTasks.Count} in-flight worker(s) to finish...", LogType.Info);
                    try
                    {
                        Task.WaitAll(pendingTasks.ToArray(), TimeSpan.FromSeconds(30));
                    }
                    catch (AggregateException)
                    {
                        // Expected — cancelled or failed worker tasks
                    }
                    _log?.WriteLine("All in-flight workers completed.", LogType.Info);

                    // Safety net: kill any mongodump/mongorestore processes that a zombie
                    // worker may have spawned while we were waiting for tasks to finish.
                    MigrationJobContext.KillAllMigrationProcesses();
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error stopping coordinated processing: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }


        /// <summary>
        /// Statistics about the coordinator's current state
        /// </summary>
        public class CoordinatorStats
        {
            public int ActiveMigrationUnits { get; set; }
            public int PendingDownloads { get; set; }
            public int ProcessingDownloads { get; set; }
            public int CompletedDownloads { get; set; }
            public int FailedDownloads { get; set; }
            public int PendingRestores { get; set; }
            public int ProcessingRestores { get; set; }
            public int CompletedRestores { get; set; }
            public int FailedRestores { get; set; }
            public int DumpPoolBusy { get; set; }
            public int DumpPoolTotal { get; set; }
            public int RestorePoolBusy { get; set; }
            public int RestorePoolTotal { get; set; }
        }
    }
}

#endregion
