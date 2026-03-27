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

        private const int MaxManifestWorkingSetSize = 200;
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
        // Serial cleanup queue - restore failures enqueue here; one loop processes cleanup in order.
        private readonly ConcurrentQueue<DumpRestoreProcessContext> _cleanupQueue = new();
        private readonly ConcurrentDictionary<string, byte> _cleanupQueueIndex = new();
        private int _cleanupLoopRunning = 0; // 0 = idle, 1 = running
        private System.Timers.Timer? _processTimer;
        private readonly int _timerIntervalMs = 2000; // Check every 2 seconds
        private bool _coordinatorInitialized = false;
        private bool _timerStarted = false;
        private CancellationTokenSource? _processCts;
        private MigrationUnitCompletedHandler? _onMigrationUnitCompleted;
        private PendingTasksCompletedHandler? _onPendingTasksCompleted;

        private bool _processNewTasks = true;

        // Cached duplicate settings (read once at Initialize)
        private bool _ignoreDuplicatesAndContinueRestore = false;
        private TimeSpan _continuousDuplicateThreshold = TimeSpan.FromMinutes(5);

        private DateTime _downLoadPausedTill = DateTime.MinValue;
        private DateTime _lastDiskSpaceCheckedAtUtc = DateTime.MinValue;
        private bool _lastDiskSpaceCheckResult = true;
        private const int DiskSpaceCheckCacheSeconds = 30;

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
            var needsCleanup = new List<DumpRestoreProcessContext>();

            lock (_workingSetLock)
            {
                while (_uploadManifest.Count < MaxManifestWorkingSetSize && _uploadBacklog.TryDequeue(out var context))
                {
                    _uploadBacklogIndex.TryRemove(context.Id, out _);

                    if (_uploadManifest.ContainsKey(context.Id))
                        continue;

                    if (_uploadManifest.TryAdd(context.Id, context) && GetChunkNeedsCleanup(context))
                    {
                        needsCleanup.Add(context);
                    }
                }
            }

            // Start cleanup processing for restored backlog entries that already require cleanup.
            foreach (var context in needsCleanup)
            {
                EnqueueCleanup(context);
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
        public void Initialize(string jobId, Log log, string? mongoToolsFolder = null, string? mongoDumpToolPath = null, string? mongoRestoreToolPath = null, MigrationUnitCompletedHandler? onMigrationUnitCompleted = null, PendingTasksCompletedHandler? onPendingTasksCompleted=null)
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
                    _onMigrationUnitCompleted = onMigrationUnitCompleted;
                    _onPendingTasksCompleted = onPendingTasksCompleted;
                    _processNewTasks = true;

                    // Calculate optimal concurrency
                    int maxDumpWorkers, maxRestoreWorkers;
                    if (MigrationJobContext.CurrentlyActiveJob.EnableParallelProcessing)
                    {
                        maxDumpWorkers = WorkerCountHelper.CalculateOptimalConcurrency(
                            MigrationJobContext.CurrentlyActiveJob.MaxParallelDumpProcesses,
                            isDump: true
                        );

                        maxRestoreWorkers = WorkerCountHelper.CalculateOptimalConcurrency(
                            MigrationJobContext.CurrentlyActiveJob.MaxParallelRestoreProcesses,
                            isDump: false
                        );

                        log.WriteLine($"Calculated dump concurrency: {maxDumpWorkers},  restore concurrency: {maxRestoreWorkers}", LogType.Info);
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

                    if (!MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection.HasValue)
                    {
                        MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = WorkerCountHelper.CalculateDefaultInsertionWorkers();
                    }
                    else
                    {
                        MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = WorkerCountHelper.ValidateWorkerCount(
                            MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection.Value
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
                        
                        // Get full migration unit and save immediately
                        var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
                        if (mu != null)
                        {
                            foreach (var chunk in mu.MigrationChunks)
                            {
                                chunk.Attempt = 0;
                            }
                            mu.SkippedDueToMaxRetries = false;
                            mu.FailedOperation = null;                            
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                        }
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

                // Update current value in context for UI monitoring
                MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers = validatedCount;

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

                // Update current value in context for UI monitoring
                MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers = validatedCount;

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
                    ClearQueue(_cleanupQueue);
                    _cleanupQueueIndex.Clear();
                    _cleanupLoopRunning = 0;

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
            // gets called often, avoid detailed logs
            // Prevent re-entrant calls
            if (!Monitor.TryEnter(_timerLock))
            {
                _log?.WriteLine("Timer tick skipped - previous tick still processing", LogType.Debug);
                return;
            }

            try
            {
                // Check for cancellation or pause
                if (_processCts?.Token.IsCancellationRequested == true || MigrationJobContext.ControlledPauseRequested)
                {
                    if (_processTimer != null && _timerStarted && _processNewTasks)
                    {
                        _processNewTasks = false;

                        _log?.WriteLine("Controlled pause detected - stopped processing new tasks.", LogType.Warning);
                    }
                    //return;
                }

                if (_processNewTasks)
                { 
                    TryActivatePendingMigrationUnits();
                    RefillManifestWorkingSets();
                    QueuePendingCleanupContexts();
                    ProcessPendingDumps();
                    ProcessPendingRestores();
                }

                CheckForCompletedMigrationUnits();

                // Stop timer if all work is done
                if (IsAllWorkComplete())
                {
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
                            mu.MigrationChunks[i].IsDownloaded = false;
                            mu.DumpComplete = false;

                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            continue;
                        }

                        if (TryAddUploadContext(context))
                        {
                            addedCount++;
                            if (GetChunkNeedsCleanup(context))
                            {
                                EnqueueCleanup(context);
                            }
                            else
                            {
                                _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName}[{i}] accepted for restore processing", LogType.Debug);
                            }
                        }
                    }
                }

                if (addedCount > 0)
                {
                    _log?.WriteLine($"Added {addedCount} chunks to restore manifest for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
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

                // Check for controlled pause before spawning any workers
                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine("[ProcessPendingDumps] Controlled pause detected - skipping dump processing", LogType.Debug);
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
                    // Check for controlled pause before spawning any workers
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        _log?.WriteLine("[ProcessPendingDumps] Controlled pause detected - skipping dump processing", LogType.Debug);
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
                            mu.BulkCopyStartedOn = DateTime.UtcNow;

                        MigrationJobContext.SaveMigrationUnit(mu, true);

                        _ = Task.Run(async () => await ProcessChunkForDownload(context), cancellationToken);
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

                // Check for controlled pause before spawning any workers
                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log?.WriteLine("[ProcessPendingRestores] Controlled pause detected - skipping restore processing", LogType.Debug);
                    return;
                }

                // Get available worker capacity
                int availableWorkers = _restorePool.CurrentAvailable;
                int totalWorkers = _restorePool.MaxWorkers;
                if (availableWorkers <= 0)
                {
                    return; // No workers available
                }

                // Only dispatch contexts whose pre-restore cleanup has completed.
                var readyToDispatch = _uploadManifest.Values
                    .Where(ctx => ctx.State == ProcessState.Pending && !GetChunkNeedsCleanup(ctx))
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
                    // Check for controlled pause before spawning any workers
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        _log?.WriteLine("[ProcessPendingRestores] Controlled pause detected - skipping restore processing", LogType.Debug);
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
                        _ = Task.Run(async () => await ProcessChunkForRestore(context), cancellationToken);
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

                // Check cancellation
                if (_processCts?.Token.IsCancellationRequested == true)
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

            // Update migration unit
            mu.MigrationChunks[chunkIndex].IsDownloaded = true;
            MigrationJobContext.SaveMigrationUnit(mu, true);

            // Update tracker
            UpdateMigrationUnitTracker(mu.Id, downloadIncrement: 1);

            // Remove from download manifest
            _downloadManifest.TryRemove(context.Id, out _);

            if(MigrationJobContext.ControlledPauseRequested)
            {
                return; // Skip preparing restore list during controlled pause
            }
            // Prepare restore list after successful dump
            PrepareRestoreList(mu, context.SourceConnectionString, context.TargetConnectionString);
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

            // Preserve the original chunk's ID for the first sub-chunk
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
        /// <returns>Tuple containing (docCount, gte bound, lt bound, query string)</returns>
        private async Task<(long docCount, BsonValue gte, BsonValue lt, string query)> HandleCountTimeoutWithChunkSplitAsync(
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
        private async Task<(long docCount, BsonValue gte, BsonValue lt, string query)> ProcessChunkSplitResultAsync(
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

            // Replace original chunk with sub-chunks
            int addedChunks = ReplaceChunkWithSubChunks(mu, chunkIndex, subChunks);

            // Update tracker for newly added sub-chunks
            if (addedChunks > 0 && _activeMigrationUnits.TryGetValue(mu.Id, out var tracker))
            {
                Interlocked.Add(ref tracker.TotalChunks, addedChunks);
                _log?.WriteLine($"Updated tracker TotalChunks to {tracker.TotalChunks} for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
            }

            // Save migration unit with updated chunks
            MigrationJobContext.SaveMigrationUnit(mu, true);

            // Update manifests for new sub-chunks
            if (subChunks.Count > 1)
            {
                int newChunksStartIndex = mu.MigrationChunks.Count - (subChunks.Count - 1);
                UpdateDownloadList(mu, sourceConnectionString, targetConnectionString, newChunksStartIndex, subChunks.Count - 1);
            }

            // Re-process the first sub-chunk (updated in-place at the same index)
            var bounds = SamplePartitioner.GetChunkBounds(
                mu.MigrationChunks[chunkIndex].Gte!,
                mu.MigrationChunks[chunkIndex].Lt!,
                dataType
            );
            var gte = bounds.gte;
            var lt = bounds.lt;
            var query = MongoHelper.GenerateQueryString(gte, lt, dataType, userFilterDoc, mu);

            // Try count again on the smaller sub-chunk
            var (retrySuccess, retryCount) = TryGetDocumentCountWithRetry(
                sourceCollection,
                chunkIndex,
                gte,
                lt,
                dataType,
                userFilterDoc,
                mu.DataTypeFor_Id.HasValue
            );

            long docCount = retrySuccess ? retryCount : 0;


            // Validate that the split actually reduced the count for the first sub-chunk
            long maxDocsPerChunk= GetMaxDocsPerChunk(mu);
           
           
            if (retrySuccess && retryCount > maxDocsPerChunk)
            {
                _log?.WriteLine($"First sub-chunk still has {retryCount} docs (max: {maxDocsPerChunk}). Recursively splitting again.", LogType.Warning);
                var recursiveResult = await HandleCountTimeoutWithChunkSplitAsync(
                    mu, chunkIndex, sourceCollection, userFilterDoc,
                    sourceConnectionString, targetConnectionString, false);
                return recursiveResult;
            }

            return (docCount, gte, lt, query);
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
                // Calculate max docs per chunk: (EstimatedDocCount / ChunkCount) * 3, capped at 25M
                maxDocsPerChunk = Math.Min((mu.EstimatedDocCount / mu.MigrationChunks.Count) * 3, 25000000);
                mu.MaxDocsPerChunk = maxDocsPerChunk;
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }

            return maxDocsPerChunk;
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
                mu.MigrationChunks[chunkIndex].Lt!,
                mu.MigrationChunks[chunkIndex].DataType
            );
            var gte = bounds.gte;
            var lt = bounds.lt;

            // Get source collection
            var sourceCollection = GetSourceCollection(sourceConnectionString, mu.DatabaseName, mu.CollectionName);

            // Build query and get count with retry logic
            BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
            string query = MongoHelper.GenerateQueryString(gte, lt, mu.MigrationChunks[chunkIndex].DataType, userFilterDoc, mu);

            // Try to get document count with retry
            var (countSuccess, docCount) = TryGetDocumentCountWithRetry(
                sourceCollection,
                chunkIndex,
                gte,
                lt,
                mu.MigrationChunks[chunkIndex].DataType,
                userFilterDoc,
                mu.DataTypeFor_Id.HasValue
            );

            // Handle timeout by splitting chunk if needed
            long maxDocsPerChunk = GetMaxDocsPerChunk(mu);

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
                query = result.query;
            }

            mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
            _log?.WriteLine($"Count for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] is {docCount}", LogType.Debug);

            // Convert query for mongodump
            string extendedQuery = MongoQueryConverter.ConvertMondumpFilter(query, gte, lt, mu.MigrationChunks[chunkIndex].DataType);
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

            try
            {
                _log?.WriteLine($"Coordinator: Starting restore for {Log.FormatNamespaceForLog(sourceDbName, sourceColName, targetDbName, targetColName)}[{chunkIndex}]", LogType.Debug);

                // ===== TEMPORARY TEST CODE - REMOVE THIS BLOCK AFTER TESTING =====
                // Simulate restore failure for chunk index 1 (2nd chunk) to test skip logic
                //if (chunkIndex == 1 && sourceColName== "smalldocs_22M"  && context.RetryCount<2)
                //{
                //    _log?.WriteLine($"[TEST] Simulating restore failure for chunk 1: {Log.FormatNamespaceForLog(sourceDbName, sourceColName, targetDbName, targetColName)}[1]", LogType.Warning);
                //    HandleRestoreFailure(context, TaskResult.Retry);
                //    _restorePool?.Release();
                //    return;
                //}
                // ===== END TEMPORARY TEST CODE =====

                // Check cancellation
                if (_processCts?.Token.IsCancellationRequested == true)
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

                // Get dump folder and file path
                var dumpFilePath = GetDumpFilePath(sourceDbName, sourceColName, chunkIndex);


                // Validate dump file exists
                if (!ValidateDumpFileExists(context))
                {
                    MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ValidateDumpFileExists: Failed for muId={context.MigrationUnitId}, chunkIndex={context.ChunkIndex}");
                    _log.WriteLine($"Dump file missing before executing restore at {dumpFilePath}. Marking chunk as not downloaded.", LogType.Warning);
                    mu.MigrationChunks[chunkIndex].IsDownloaded = false;
                    mu.DumpComplete = false;
                    HandleRestoreFailure(context, TaskResult.Retry);

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

                    // Warm up connection to target collection with async findOne
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
                        // Check if already uploaded (idempotency)
                        if (mu.MigrationChunks[chunkIndex].IsUploaded == true)
                        {
                            MigrationJobContext.SaveMigrationUnit(mu, true);
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
            mu.MigrationChunks[chunkIndex].RestoredSuccessDocCount = mu.MigrationChunks[chunkIndex].DumpQueryDocCount;
            mu.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
            mu.MigrationChunks[chunkIndex].IsUploaded = true;

            // Update progress
            //double progress = initialPercent + (contributionFactor * 100);
            mu.RestorePercent = 100;

            _log?.WriteLine($"Simulation mode: Chunk {chunkIndex} restore simulated - {mu.RestorePercent:F2}% complete");

            MigrationJobContext.SaveMigrationUnit(mu, true);

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

                mu.MigrationChunks[chunkIndex].IsDownloaded = false;
                mu.DumpComplete = false;

                MigrationJobContext.SaveMigrationUnit(mu, true);

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
                MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection,
                MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers
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
            string dumpFilePath)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.ExecuteRestoreProcessAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}, docCount={docCount}");
            // Calculate progress factors
            //double initialPercent = ((double)100 / mu.MigrationChunks.Count) * chunkIndex;
            //double contributionFactor = (double)mu.MigrationChunks[chunkIndex].DumpQueryDocCount / Helper.GetMigrationUnitDocCount(mu);
            //if (mu.MigrationChunks.Count == 1) contributionFactor = 1;

            // Execute restore process
            var processExecutor = new ProcessExecutor(_log);
            var restoreToolPath = GetMongoToolPath("mongorestore");
            bool success = await Task.Run(() => processExecutor.Execute(
                mu,
                mu.MigrationChunks[chunkIndex],
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
            if (toolName.Equals("mongodump", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(_mongoDumpToolPath))
            {
                return _mongoDumpToolPath;
            }

            if (toolName.Equals("mongorestore", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(_mongoRestoreToolPath))
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
                    mu.MigrationChunks[chunkIndex].Lt!,
                    mu.MigrationChunks[chunkIndex].DataType
                );
                var gte = bounds.gte;
                var lt = bounds.lt;

                mu.MigrationChunks[chunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(
                    targetCollection,
                    gte,
                    lt,
                    mu.MigrationChunks[chunkIndex].DataType,
                    MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!),
                    mu.DataTypeFor_Id.HasValue
                );

                // Check if counts match
                if (mu.MigrationChunks[chunkIndex].DocCountInTarget >= mu.MigrationChunks[chunkIndex].DumpQueryDocCount)
                {
                    _log?.WriteLine($"Restore for {Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDbName, targetCollectionName)}[{chunkIndex}] No documents missing, count in Target: {mu.MigrationChunks[chunkIndex].DocCountInTarget}", LogType.Info);
                    mu.MigrationChunks[chunkIndex].SkippedAsDuplicateCount = mu.MigrationChunks[chunkIndex].RestoredFailedDocCount;
                    mu.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    return false; // Don't retry
                }
                else
                {
                    _log?.WriteLine($"Restore for {Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDbName, targetCollectionName)}[{chunkIndex}] Documents missing, Chunk will be reprocessed", LogType.Error);
                    MigrationJobContext.SaveMigrationUnit(mu, true);
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
            mu.MigrationChunks[chunkIndex].IsUploaded = true;
            MigrationJobContext.SaveMigrationUnit(mu, true);

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
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
            int chunkIndex = context.ChunkIndex;

            // Mark chunk as completed
            context.State = ProcessState.Completed;
            context.CompletedAt = DateTime.UtcNow;

            // Update migration unit
            mu.MigrationChunks[chunkIndex].IsUploaded = true;
            MigrationJobContext.SaveMigrationUnit(mu, true);

            // Update tracker
            UpdateMigrationUnitTracker(mu.Id, restoreIncrement: 1);

            // Remove from restore manifest
            _uploadManifest.TryRemove(context.Id, out _);
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

            try
            {
                context.LastError = ex;
                context.RetryCount++;

                const int MaxRetries = 3;

                if (context.RetryCount >= MaxRetries || result == TaskResult.Abort)
                {
                    context.State = ProcessState.Failed;
                    var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (mu != null)
                    {
                        // Mark collection as skipped due to max retries instead of pausing
                        // Do NOT mark as complete - the collection actually failed
                        mu.SkippedDueToMaxRetries = true;
                        mu.FailedOperation = "Dump";
                        
                        MigrationJobContext.SaveMigrationUnit(mu, true);
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
                        var pausedMu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                        if (pausedMu != null && context.ChunkIndex >= 0 && context.ChunkIndex < pausedMu.MigrationChunks.Count)
                        {
                            pausedMu.MigrationChunks[context.ChunkIndex].Attempt++;
                            pausedMu.MigrationChunks[context.ChunkIndex].NeedsCleanup = true;
                            MigrationJobContext.SaveMigrationUnit(pausedMu, true);
                            _log?.WriteLine(
                                $"Restore cancelled due to controlled pause for {pausedMu.DatabaseName}.{pausedMu.CollectionName}[{context.ChunkIndex}]. Marked for cleanup and retry.",
                                LogType.Warning);
                        }
                    }
                    catch (Exception pauseEx)
                    {
                        _log?.WriteLine($"Error persisting paused restore cleanup state: {Helper.RedactPii(pauseEx.ToString())}", LogType.Error);
                    }
                }

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
                    var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (mu != null)
                    {
                        // Mark collection as skipped due to max retries instead of pausing
                        // Do NOT mark as complete - the collection actually failed
                        mu.SkippedDueToMaxRetries = true;
                        mu.FailedOperation = "Restore";
                        
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                        _log.WriteLine($"Max retries reached for restore: {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]. Collection marked as skipped. User can restart the job to retry.", LogType.Error);
                    }
                    else
                    {
                        _log.WriteLine($"Max retries for restore complete: MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Error);
                    }
                }
                else
                {
                    // Reset to pending for retry
                    var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
                    if (mu != null)
                    {
                        if (context.ChunkIndex >= 0 && context.ChunkIndex < mu.MigrationChunks.Count)
                        {
                            // Keep chunk attempt in sync with context retries; ProcessExecutor may have
                            // already incremented Attempt for threshold-triggered duplicate-key failures.
                            if (mu.MigrationChunks[context.ChunkIndex].Attempt < context.RetryCount)
                            {
                                mu.MigrationChunks[context.ChunkIndex].Attempt = context.RetryCount;
                            }
                            mu.MigrationChunks[context.ChunkIndex].NeedsCleanup = true;
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            _log.WriteLine(
                                $"Restore will retry ({context.RetryCount}/{MaxRetries}): {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]. Cleanup queued.",
                                LogType.Warning);
                        }
                        else
                        {
                            _log.WriteLine($"Restore will retry ({context.RetryCount}/{MaxRetries}): {mu.DatabaseName}.{mu.CollectionName}[{context.ChunkIndex}]", LogType.Warning);
                        }
                    }
                    else
                    {
                        _log.WriteLine($"Restore will retry ({context.RetryCount}/{MaxRetries}): MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Warning);
                    }

                    context.State = ProcessState.Pending;
                    EnqueueCleanup(context);
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

        private void QueuePendingCleanupContexts()
        {
            try
            {
                // Queue cleanup proactively for persisted retry chunks at startup/resume,
                // instead of waiting for a new restore failure event.
                var candidates = _uploadManifest.Values
                    .Concat(_uploadBacklog.ToArray())
                    .Where(ctx => (ctx.State == ProcessState.Pending || ctx.State == ProcessState.CleaningUp) && GetChunkNeedsCleanup(ctx))
                    .OrderBy(ctx => ctx.QueuedAt)
                    .ToList();

                foreach (var context in candidates)
                {
                    EnqueueCleanup(context);
                }
            }
            catch (Exception ex)
            {
                _log?.WriteLine($"Error queueing pending cleanup contexts: {Helper.RedactPii(ex.ToString())}", LogType.Error);
            }
        }

        private void EnqueueCleanup(DumpRestoreProcessContext context)
        {
            if (!GetChunkNeedsCleanup(context))
            {
                return;
            }

            if (_cleanupQueueIndex.TryAdd(context.Id, 0))
            {
                context.State = ProcessState.CleaningUp;
                _cleanupQueue.Enqueue(context);
                _log?.WriteLine($"[Cleanup to avoid duplicates] Queued: MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Debug);
            }

            if (Interlocked.CompareExchange(ref _cleanupLoopRunning, 1, 0) == 0)
            {
                var ct = _processCts?.Token ?? CancellationToken.None;
                _ = Task.Run(() => RunCleanupLoopAsync(ct), ct);
            }
        }

        private async Task RunCleanupLoopAsync(CancellationToken cancellationToken)
        {
            int maxParallelCleanupWorkers = GetCleanupParallelWorkerCount();
            var inFlightCleanupTasks = new List<Task>(maxParallelCleanupWorkers);

            try
            {
                while (!cancellationToken.IsCancellationRequested &&
                       !MigrationJobContext.ControlledPauseRequested &&
                       MigrationJobContext.CurrentlyActiveJob?.IsCancelled != true)
                {
                    while (inFlightCleanupTasks.Count < maxParallelCleanupWorkers &&
                           _cleanupQueue.TryDequeue(out var context))
                    {
                        inFlightCleanupTasks.Add(ProcessCleanupContextAsync(context, cancellationToken));
                    }

                    if (inFlightCleanupTasks.Count == 0)
                    {
                        break;
                    }

                    var completedCleanupTask = await Task.WhenAny(inFlightCleanupTasks);
                    inFlightCleanupTasks.Remove(completedCleanupTask);
                    await completedCleanupTask;
                }

                // Wait for in-flight cleanup tasks to observe cancellation/pause and unwind.
                if (inFlightCleanupTasks.Count > 0)
                {
                    await Task.WhenAll(inFlightCleanupTasks);
                }
            }
            finally
            {
                Interlocked.Exchange(ref _cleanupLoopRunning, 0);

                // Restart loop if an item was queued after TryDequeue failed but before we reset running flag.
                if (!cancellationToken.IsCancellationRequested &&
                    !MigrationJobContext.ControlledPauseRequested &&
                    MigrationJobContext.CurrentlyActiveJob?.IsCancelled != true &&
                    !_cleanupQueue.IsEmpty &&
                    Interlocked.CompareExchange(ref _cleanupLoopRunning, 1, 0) == 0)
                {
                    _ = Task.Run(() => RunCleanupLoopAsync(cancellationToken), cancellationToken);
                }
            }
        }

        private async Task ProcessCleanupContextAsync(DumpRestoreProcessContext context, CancellationToken cancellationToken)
        {
            bool requeue = false;

            try
            {
                // Each cleanup run already performs internal retries.
                // If it still fails, requeue at tail so other pending chunks can run first.
                requeue = await RunCleanupForRestoreRetryAsync(context, cancellationToken);
            }
            catch (Exception ex)
            {
                context.State = ProcessState.CleaningUp;
                requeue = true;
                _log?.WriteLine($"[Cleanup to avoid duplicates] Unexpected exception for MU:{context.MigrationUnitId}[{context.ChunkIndex}] (will re-queue): {Helper.RedactPii(ex.ToString())}", LogType.Debug);
            }
            finally
            {
                // Keep the index entry for the full cleanup run so periodic scans
                // do not enqueue duplicate work while this context is in progress.
                _cleanupQueueIndex.TryRemove(context.Id, out _);
            }

            if (requeue &&
                !cancellationToken.IsCancellationRequested &&
                !MigrationJobContext.ControlledPauseRequested &&
                MigrationJobContext.CurrentlyActiveJob?.IsCancelled != true)
            {
                EnqueueCleanup(context);
            }
        }

        private int GetCleanupParallelWorkerCount()
        {
            // Match dump/restore default worker math: one worker per 2.5 CPU cores.
            int calculatedWorkers = Math.Max(1, (int)(Environment.ProcessorCount / 2.5));
            return WorkerCountHelper.ValidateDumpRestoreWorkerCount(calculatedWorkers);
        }

        private async Task<bool> RunCleanupForRestoreRetryAsync(DumpRestoreProcessContext context, CancellationToken cancellationToken)
        {
            var mu = MigrationJobContext.GetMigrationUnit(context.MigrationUnitId);
            if (mu == null)
            {
                context.State = ProcessState.Failed;
                _log?.WriteLine($"[Cleanup to avoid duplicates] Failed: migration unit not found for MU:{context.MigrationUnitId}[{context.ChunkIndex}]", LogType.Debug);
                return false;
            }

            int chunkIndex = context.ChunkIndex;
            if (chunkIndex < 0 || chunkIndex >= mu.MigrationChunks.Count)
            {
                context.State = ProcessState.Failed;
                _log?.WriteLine($"[Cleanup to avoid duplicates] Failed: invalid chunk index for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]", LogType.Debug);
                return false;
            }

            string targetDbName = mu.GetEffectiveTargetDatabaseName();
            string targetCollectionName = mu.GetEffectiveTargetCollectionName();
            int deletePageSize = GetCleanupDeletePageSize();

            _log?.WriteLine($"[Cleanup to avoid duplicates] Starting: {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]", LogType.Info);

            try
            {
                if (MigrationJobContext.ControlledPauseRequested || MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true)
                {
                    throw new OperationCanceledException(cancellationToken);
                }

                var targetClient = MongoClientFactory.Create(_log, context.TargetConnectionString);
                var targetDb = targetClient.GetDatabase(targetDbName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(targetCollectionName);

                var cleanupResult = await MongoHelper.DeletePotentialDuplicateDocsForRestoreRetryAsync(
                    mu,
                    chunkIndex,
                    targetCollection,
                    targetDbName,
                    targetCollectionName,
                    deletePageSize,
                    _log,
                    cancellationToken);

                bool cleanupSucceeded = cleanupResult.Succeeded;

                if (cleanupSucceeded)
                {
                    mu.MigrationChunks[chunkIndex].NeedsCleanup = false;
                    mu.MigrationChunks[chunkIndex].Attempt = 0; // Reset attempt so that any future failure will trigger retries up to the max threshold again
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    context.State = ProcessState.Pending;
                    _log?.WriteLine($"[Cleanup to avoid duplicates] Completed: {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}], totalDeleted={cleanupResult.TotalDeleted}", LogType.Info);
                    return false;
                }
                else
                {
                    context.State = ProcessState.CleaningUp;
                    _log?.WriteLine($"[Cleanup to avoid duplicates] Incomplete after internal retries. Re-queueing: {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]", LogType.Debug);
                    return true;
                }
            }
            catch (OperationCanceledException)
            {
                context.State = ProcessState.CleaningUp;
                if (cancellationToken.IsCancellationRequested || MigrationJobContext.ControlledPauseRequested || MigrationJobContext.CurrentlyActiveJob?.IsCancelled == true)
                {
                    _log?.WriteLine($"[Cleanup to avoid duplicates] Canceled: {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] due to pause/cancellation.", LogType.Debug);
                    return false;
                }

                _log?.WriteLine($"[Cleanup to avoid duplicates] Canceled: {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] by cleanup token.", LogType.Debug);
                return false;
            }
            catch (Exception ex)
            {
                context.State = ProcessState.CleaningUp;
                _log?.WriteLine($"[Cleanup to avoid duplicates] Exception for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] (will re-queue): {Helper.RedactPii(ex.ToString())}", LogType.Debug);
                return true;
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
                _log?.WriteLine($"[Cleanup to avoid duplicates] Failed to load MongoCopyPageSize from settings. Using default 500. Details: {Helper.RedactPii(ex.Message)}", LogType.Debug);
            }

            return 500;
        }

        /// <summary>
        /// Updates the migration unit tracker with progress
        /// </summary>
        private void UpdateMigrationUnitTracker(string muId, int downloadIncrement = 0, int restoreIncrement = 0)
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.UpdateMigrationUnitTracker: muId={muId}, downloadIncrement={downloadIncrement}, restoreIncrement={restoreIncrement}");
            try
            {
                if (_activeMigrationUnits.TryGetValue(muId, out var tracker))
                {
                    // Use Interlocked for thread-safe updates since multiple chunks may complete in parallel
                    if (downloadIncrement != 0)
                        Interlocked.Add(ref tracker.DownloadedChunks, downloadIncrement);
                    if (restoreIncrement != 0)
                        Interlocked.Add(ref tracker.RestoredChunks, restoreIncrement);

                    // Note: MigrationUnit doesn't have DownloadPercent/RestorePercent properties
                    // Progress tracking is handled through tracker object
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
                var completedUnits = _activeMigrationUnits.Values
                .Where(tracker => tracker.AllDownloadsCompleted && tracker.AllRestoresCompleted)
                .ToList();

                foreach (var tracker in completedUnits)
                {
                    string muId = tracker.MigrationUnitId;
                    string targetConnectionString = string.Empty;


                    var mu = MigrationJobContext.GetMigrationUnit(muId);

                    // Mark migration unit as complete
                    mu.DumpComplete = true;
                    mu.DumpPercent = 100;
                    mu.RestoreComplete = true;
                    mu.RestorePercent = 100;
                    mu.UpdateParentJob();

                    if (!mu.BulkCopyEndedOn.HasValue || mu.BulkCopyEndedOn.Value == DateTime.MinValue)
                    {
                        mu.BulkCopyEndedOn = DateTime.UtcNow;
                    }

                    MigrationJobContext.SaveMigrationUnit(mu, true);

                    // Remove from active tracking
                    _activeMigrationUnits.TryRemove(muId, out _);

                    _log.WriteLine($"Migration unit completed: {mu.DatabaseName}.{mu.CollectionName}", LogType.Info);

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

                    if(totalDumpProcessing +totalRestoreProcessing > 0)
                        return false;
                    else
                        return true;
                }
                else
                {
                    return _activeMigrationUnits.IsEmpty &&
                           _downloadManifest.IsEmpty &&
                           _uploadManifest.IsEmpty &&
                           _pendingMigrationUnitIndex.IsEmpty &&
                           _downloadBacklogIndex.IsEmpty &&
                           _uploadBacklogIndex.IsEmpty;
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
        public void StopCoordinatedProcessing()
        {
            MigrationJobContext.AddVerboseLog($"MongoDumpRestoreCordinator.StopCoordinatedProcessing");
            try
            {
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