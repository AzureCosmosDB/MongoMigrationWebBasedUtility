using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Centralized coordinator for managing shared worker pools for the currently active migration job.
    /// Only tracks one job at a time - call Reset() when starting a new job.
    /// </summary>
    internal static class WorkerPoolCoordinator
    {
        private static readonly object _lock = new object();
        
        // Current active job ID
        private static string _currentJobId = string.Empty;
        
        // Shared worker pools for the active job
        private static WorkerPoolManager? _dumpPool = null;
        private static WorkerPoolManager? _restorePool = null;
        
        // Track active workers for the active job
        private static List<WorkerReference> _activeDumpWorkers = new();
        private static List<WorkerReference> _activeRestoreWorkers = new();
        
        /// <summary>
        /// Represents a reference to an active worker task
        /// </summary>
        private class WorkerReference
        {
            public string CollectionKey { get; set; } = string.Empty;
            public int WorkerId { get; set; }
            public DateTime StartedAt { get; set; }
            public bool IsCompleted { get; set; }
        }
        
        /// <summary>
        /// Validates that the coordinator is tracking the specified job.
        /// Returns false and logs a warning if job IDs don't match.
        /// </summary>
        private static bool ValidateJobId(string jobId, Log log, string operation)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.ValidateJobId: jobId={jobId}, operation={operation}");
            if (_currentJobId != jobId)
            {
                log.WriteLine($"WARNING: {operation} for job {jobId} but coordinator is tracking {_currentJobId}", LogType.Error);
                return false;
            }
            return true;
        }
        
        /// <summary>
        /// Resets the coordinator for a new job. Call this when starting a new migration job.
        /// </summary>
        public static void Reset(string jobId)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.Reset: jobId={jobId}");
            lock (_lock)
            {
                // Cleanup previous job if exists
                if (_dumpPool != null)
                {
                    _dumpPool.Dispose();
                    _dumpPool = null;
                }
                
                if (_restorePool != null)
                {
                    _restorePool.Dispose();
                    _restorePool = null;
                }
                
                _activeDumpWorkers.Clear();
                _activeRestoreWorkers.Clear();
                _currentJobId = jobId;

            }
        }
        
        /// <summary>
        /// Gets or creates a dump worker pool for the active job
        /// </summary>
        public static WorkerPoolManager GetOrCreateDumpPool(string jobId, Log log, int initialMaxWorkers)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.GetOrCreateDumpPool: jobId={jobId}, initialMaxWorkers={initialMaxWorkers}");
            lock (_lock)
            {
                if (_currentJobId != jobId)
                {
                    throw new InvalidOperationException($"WorkerPoolCoordinator is tracking job {_currentJobId}, cannot get pool. Call Reset() first.");
                }
                
                if (_dumpPool == null)
                {
                    _dumpPool = new WorkerPoolManager(log, $"Dump-{jobId}", initialMaxWorkers);
                    log.WriteLine($"Created shared dump pool with {initialMaxWorkers} workers");
                }
                
                return _dumpPool;
            }
        }
        
        /// <summary>
        /// Gets or creates a restore worker pool for the active job
        /// </summary>
        public static WorkerPoolManager GetOrCreateRestorePool(string jobId, Log log, int initialMaxWorkers)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.GetOrCreateRestorePool: jobId={jobId}, initialMaxWorkers={initialMaxWorkers}");
            lock (_lock)
            {
                if (_currentJobId != jobId)
                {
                    throw new InvalidOperationException($"WorkerPoolCoordinator is tracking job {_currentJobId}, cannot get pool. Call Reset() first.");
                }
                
                if (_restorePool == null)
                {
                    _restorePool = new WorkerPoolManager(log, $"Restore-{jobId}", initialMaxWorkers);
                    log.WriteLine($"Created shared restore pool with {initialMaxWorkers} workers");
                }
                
                return _restorePool;
            }
        }
        
        /// <summary>
        /// Registers a dump worker as active
        /// </summary>
        public static void RegisterDumpWorker(string jobId, string collectionKey, int workerId, Log log)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.RegisterDumpWorker: jobId={jobId}, collectionKey={collectionKey}, workerId={workerId}");
            lock (_lock)
            {
                if (!ValidateJobId(jobId, log, "Attempt to register dump worker"))
                    return;
                
                _activeDumpWorkers.Add(new WorkerReference
                {
                    CollectionKey = collectionKey,
                    WorkerId = workerId,
                    StartedAt = DateTime.UtcNow,
                    IsCompleted = false
                });
                
                int totalActive = _activeDumpWorkers.Count(w => !w.IsCompleted);
                MigrationJobContext.AddVerboseLog($"Registered dump worker: {collectionKey} Worker#{workerId} (Total active: {totalActive})");
            }
        }
        
        /// <summary>
        /// Registers a restore worker as active
        /// </summary>
        public static void RegisterRestoreWorker(string jobId, string collectionKey, int workerId, Log log)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.RegisterRestoreWorker: jobId={jobId}, collectionKey={collectionKey}, workerId={workerId}");
            lock (_lock)
            {
                if (!ValidateJobId(jobId, log, "Attempt to register restore worker"))
                    return;
                
                _activeRestoreWorkers.Add(new WorkerReference
                {
                    CollectionKey = collectionKey,
                    WorkerId = workerId,
                    StartedAt = DateTime.UtcNow,
                    IsCompleted = false
                });
                
                int totalActive = _activeRestoreWorkers.Count(w => !w.IsCompleted);
                MigrationJobContext.AddVerboseLog($"Registered restore worker: {collectionKey} Worker#{workerId} (Total active: {totalActive})");
            }
        }
        
        /// <summary>
        /// Marks a dump worker as completed
        /// </summary>
        public static void MarkDumpWorkerCompleted(string jobId, string collectionKey, int workerId, Log log)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolCoordinator.MarkDumpWorkerCompleted: jobId={jobId}, collectionKey={collectionKey}, workerId={workerId}");
            lock (_lock)
            {
                if (!ValidateJobId(jobId, log, "Attempt to mark dump worker completed"))
                    return;
                
                var worker = _activeDumpWorkers.FirstOrDefault(w => w.WorkerId == workerId && w.CollectionKey == collectionKey);
                if (worker != null)
                {
                    worker.IsCompleted = true;
                    var duration = DateTime.UtcNow - worker.StartedAt;
                    int totalActive = _activeDumpWorkers.Count(w => !w.IsCompleted);
                    log.WriteLine($"Completed dump worker: {collectionKey} Worker#{workerId} (Duration: {duration.TotalSeconds:F1}s, Remaining active: {totalActive})", LogType.Debug);
                }
                else
                {
                    MigrationJobContext.AddVerboseLog($"WARNING: Could not find dump worker to mark completed: {collectionKey} Worker#{workerId}");
                }
            }
        }
        
        /// <summary>
        /// Marks a restore worker as completed
        /// </summary>
        public static void MarkRestoreWorkerCompleted(string jobId, string collectionKey, int workerId, Log log)
        {
            lock (_lock)
            {
                if (!ValidateJobId(jobId, log, "Attempt to mark restore worker completed"))
                    return;
                
                var worker = _activeRestoreWorkers.FirstOrDefault(w => w.WorkerId == workerId && w.CollectionKey == collectionKey);
                if (worker != null)
                {
                    worker.IsCompleted = true;
                    var duration = DateTime.UtcNow - worker.StartedAt;
                    int totalActive = _activeRestoreWorkers.Count(w => !w.IsCompleted);
                    MigrationJobContext.AddVerboseLog($"Completed restore worker: {collectionKey} Worker#{workerId} (Duration: {duration.TotalSeconds:F1}s, Remaining active: {totalActive})");
                }
                else
                {
                    log.WriteLine($"WARNING: Could not find restore worker to mark completed: {collectionKey} Worker#{workerId}", LogType.Warning);
                }
            }
        }
        
        /// <summary>
        /// Gets the count of active (running) dump workers
        /// </summary>
        public static int GetActiveDumpWorkerCount(string jobId, Log log = null)
        {
            lock (_lock)
            {
                if (_currentJobId != jobId)
                {
                    return 0;
                }
                
                int beforeCleanup = _activeDumpWorkers.Count;
                // Clean up completed workers
                _activeDumpWorkers.RemoveAll(w => w.IsCompleted);
                int afterCleanup = _activeDumpWorkers.Count;
                
                if (log != null && beforeCleanup != afterCleanup)
                {
                    MigrationJobContext.AddVerboseLog($"Cleaned up {beforeCleanup - afterCleanup} completed dump workers. Active: {afterCleanup}");
                }
                
                return _activeDumpWorkers.Count;
            }
        }
        
        /// <summary>
        /// Gets the count of active (running) restore workers
        /// </summary>
        public static int GetActiveRestoreWorkerCount(string jobId, Log log = null)
        {
            lock (_lock)
            {
                if (_currentJobId != jobId)
                {
                    return 0;
                }
                
                int beforeCleanup = _activeRestoreWorkers.Count;
                // Clean up completed workers
                _activeRestoreWorkers.RemoveAll(w => w.IsCompleted);
                int afterCleanup = _activeRestoreWorkers.Count;
                
                if (log != null && beforeCleanup != afterCleanup)
                {
                    MigrationJobContext.AddVerboseLog($"Cleaned up {beforeCleanup - afterCleanup} completed restore workers. Active: {afterCleanup}");
                }
                
                return _activeRestoreWorkers.Count;
            }
        }
        
        /// <summary>
        /// Adjusts the number of dump workers
        /// </summary>
        public static int AdjustDumpWorkers(string jobId, int newCount, Log log)
        {
            MigrationJobContext.AddVerboseLog($"Adjusting dump workers to {newCount}");
            lock (_lock)
            {
                if (!ValidateJobId(jobId, log, "Cannot adjust dump workers"))
                    return 0;
                
                if (_dumpPool == null)
                {
                    log.WriteLine($"No dump pool found for active job {jobId}", LogType.Warning);
                    return 0;
                }
                
                // Use the pool's actual active worker count (from semaphore state)
                int currentActiveCount = _dumpPool.CurrentActive;
                return WorkerCountHelper.AdjustDumpWorkers(newCount, currentActiveCount, _dumpPool, log);
            }
        }

        /// <summary>
        /// Adjusts the number of restore workers
        /// </summary>
        public static int AdjustRestoreWorkers(string jobId, int newCount, Log log)
        {
            MigrationJobContext.AddVerboseLog($"Adjusting restore workers to {newCount}");
            lock (_lock)
            {
                if (!ValidateJobId(jobId, log, "Cannot adjust restore workers"))
                    return 0;
                
                if (_restorePool == null)
                {
                    log.WriteLine($"No restore pool found for active job {jobId}", LogType.Warning);
                    return 0;
                }
                
                // Use the pool's actual active worker count (from semaphore state)
                int currentActiveCount = _restorePool.CurrentActive;
                return WorkerCountHelper.AdjustRestoreWorkers(newCount, currentActiveCount, _restorePool, log);
            }
        }
        
        /// <summary>
        /// Gets statistics for the active job's worker pools
        /// </summary>
        public static (int activeDump, int activeRestore, int maxDump, int maxRestore) GetJobStats(string jobId)
        {
            lock (_lock)
            {
                if (_currentJobId != jobId)
                {
                    return (0, 0, 0, 0);
                }
                
                int activeDump = GetActiveDumpWorkerCount(jobId);
                int activeRestore = GetActiveRestoreWorkerCount(jobId);
                int maxDump = _dumpPool?.MaxWorkers ?? 0;
                int maxRestore = _restorePool?.MaxWorkers ?? 0;
                
                return (activeDump, activeRestore, maxDump, maxRestore);
            }
        }
    }
}
