using System;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Context;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Static helper class for managing worker count calculations and validations
    /// for dump, restore, and insertion worker processes.
    /// </summary>
    public static class WorkerCountHelper
    {
        private const int MIN_DUMP_RESTORE_WORKERS = 0;
        private const int MIN_WORKERS = 1;
        private const int MAX_WORKERS = 16;
        private const double CORES_PER_WORKER = 2.5;

        /// <summary>
        /// Validates and clamps worker count to acceptable range.
        /// </summary>
        /// <param name="count">Requested worker count</param>
        /// <returns>Validated worker count between MIN_WORKERS and MAX_WORKERS</returns>
        public static int ValidateWorkerCount(int count)
        {
            MigrationJobContext.AddVerboseLog($"WorkerCountHelper.ValidateWorkerCount: count={count}");
            if (count < MIN_WORKERS) return MIN_WORKERS;
            if (count > MAX_WORKERS) return MAX_WORKERS;
            return count;
        }

        /// <summary>
        /// Validates and clamps dump/restore worker count to acceptable range.
        /// Allows zero to support paused dump/restore pipelines.
        /// </summary>
        /// <param name="count">Requested worker count</param>
        /// <returns>Validated worker count between 0 and MAX_WORKERS</returns>
        public static int ValidateDumpRestoreWorkerCount(int count)
        {
            MigrationJobContext.AddVerboseLog($"WorkerCountHelper.ValidateDumpRestoreWorkerCount: count={count}");
            if (count < MIN_DUMP_RESTORE_WORKERS) return MIN_DUMP_RESTORE_WORKERS;
            if (count > MAX_WORKERS) return MAX_WORKERS;
            return count;
        }

        /// <summary>
        /// Calculates optimal concurrency based on configuration or system resources.
        /// </summary>
        /// <param name="configOverride">Optional configuration override value</param>
        /// <param name="isDump">True for dump operations, false for restore operations</param>
        /// <returns>Optimal worker count</returns>
        public static int CalculateOptimalConcurrency(int? configOverride, bool isDump)
        {
            MigrationJobContext.AddVerboseLog($"WorkerCountHelper.CalculateOptimalConcurrency: configOverride={configOverride}, isDump={isDump}");
            // User override takes precedence
            if (configOverride.HasValue)
            {
                return ValidateDumpRestoreWorkerCount(configOverride.Value);
            }

            // Base calculation: 1 instance per 2.5 cores
            int baseConcurrency = Math.Max(MIN_WORKERS, (int)(Environment.ProcessorCount / CORES_PER_WORKER));

            // Memory safety check (500MB per process) - simplified, assume 8GB if we can't check
            int memorySafeConcurrency = baseConcurrency; // Simplified for now

            // Final calculation
            int finalConcurrency = Math.Min(baseConcurrency, memorySafeConcurrency);

            return ValidateDumpRestoreWorkerCount(finalConcurrency);
        }

        /// <summary>
        /// Gets the insertion worker count based on configuration or calculated default.
        /// </summary>
        /// <param name="configuredMax">Optional configured maximum insertion workers</param>
        /// <param name="currentDefault">Current default insertion workers</param>
        /// <returns>Insertion worker count to use</returns>
        public static int GetInsertionWorkersCount(int? configuredMax, int currentDefault)
        {
            MigrationJobContext.AddVerboseLog($"WorkerCountHelper.GetInsertionWorkersCount: configuredMax={configuredMax}, currentDefault={currentDefault}");
            if (configuredMax.HasValue)
            {
                return ValidateWorkerCount(configuredMax.Value);
            }
            
            return ValidateWorkerCount(currentDefault);
        }

        /// <summary>
        /// Calculates the default insertion workers based on processor count.
        /// </summary>
        /// <returns>Default insertion worker count</returns>
        public static int CalculateDefaultInsertionWorkers()
        {
            MigrationJobContext.AddVerboseLog("WorkerCountHelper.CalculateDefaultInsertionWorkers: calculating default");
            int calculated = Math.Min(Environment.ProcessorCount / 2, 8);
            return ValidateWorkerCount(calculated);
        }

        /// <summary>
        /// Adjusts the number of dump workers. Returns the number of new workers to spawn (positive) or capacity reduction (negative/zero).
        /// </summary>
        /// <param name="newCount">Desired worker count</param>
        /// <param name="currentActiveCount">Current number of active dump workers</param>
        /// <param name="poolManager">Worker pool manager instance</param>
        /// <param name="log">Log instance for output</param>
        /// <returns>Number of workers to spawn (positive), 0 if no change, or negative if reducing</returns>
        internal static int AdjustDumpWorkers(int newCount, int currentActiveCount, WorkerPoolManager poolManager, Log log)
        {
            MigrationJobContext.AddVerboseLog($"WorkerCountHelper.AdjustDumpWorkers: newCount={newCount}, currentActiveCount={currentActiveCount}");
            int originalCount = newCount;
            newCount = ValidateDumpRestoreWorkerCount(newCount);
            
            int currentPoolCapacity = poolManager.MaxWorkers;
            
            log.WriteLine($"AdjustDumpWorkers: requested={originalCount}, adjusted={newCount}, current={currentActiveCount}, pool_capacity={currentPoolCapacity}");
            
            // Check if we're trying to increase or decrease pool capacity
            if (newCount > currentPoolCapacity)
            {
                // We can spawn new workers
                int difference = poolManager.AdjustPoolSize(newCount);
                
                if (difference > 0)
                {
                    MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers = newCount;
                    MigrationJobContext.CurrentlyActiveJob.MaxParallelDumpProcesses = newCount;
                    log.WriteLine($"Increased dump workers to {newCount} (spawning {difference} new workers)");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                    return difference; // Return number of new workers to spawn
                }
                
                log.WriteLine($"AdjustDumpWorkers: No change after pool adjustment");
                return 0; // No change
            }
            else if (newCount < currentPoolCapacity)
            {
                // Trying to decrease - just update the pool size, don't spawn workers
                // The count will naturally come down as workers complete
                int difference = poolManager.AdjustPoolSize(newCount);
                
                log.WriteLine($"AdjustDumpWorkers: Decreasing, poolManager.AdjustPoolSize returned difference={difference}");
                
                if (difference != 0)
                {
                    MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers = newCount;
                    MigrationJobContext.CurrentlyActiveJob.MaxParallelDumpProcesses = newCount;
                    log.WriteLine($"Reduced dump worker capacity to {newCount}. Active workers will complete naturally.");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
                else
                {
                    log.WriteLine($"AdjustDumpWorkers: No pool size change (already at {newCount})");
                }
                
                return 0; // Don't spawn new workers when decreasing
            }
            
            // newCount == currentPoolCapacity, no change needed
            log.WriteLine($"AdjustDumpWorkers: No change needed. Pool capacity already at {newCount}");
            return 0;
        }

        /// <summary>
        /// Adjusts the number of restore workers. Returns the number of new workers to spawn (positive) or capacity reduction (negative/zero).
        /// </summary>
        /// <param name="newCount">Desired worker count</param>
        /// <param name="currentActiveCount">Current number of active restore workers</param>
        /// <param name="poolManager">Worker pool manager instance</param>
        /// <param name="log">Log instance for output</param>
        /// <returns>Number of workers to spawn (positive), 0 if no change, or negative if reducing</returns>
        internal static int AdjustRestoreWorkers(int newCount, int currentActiveCount, WorkerPoolManager poolManager, Log log)
        {
            int originalCount = newCount;
            newCount = ValidateDumpRestoreWorkerCount(newCount);
            
            int currentPoolCapacity = poolManager.MaxWorkers;
            
            log.WriteLine($"AdjustRestoreWorkers: requested={originalCount}, adjusted={newCount}, current={currentActiveCount}, pool_capacity={currentPoolCapacity}");
            
            // Check if we're trying to increase or decrease pool capacity
            if (newCount > currentPoolCapacity)
            {
                // We can spawn new workers
                int difference = poolManager.AdjustPoolSize(newCount);
                
                if (difference > 0)
                {
                    MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers = newCount;
                    MigrationJobContext.CurrentlyActiveJob.MaxParallelRestoreProcesses = newCount;
                    log.WriteLine($"Increased restore workers to {newCount} (spawning {difference} new workers)");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                    return difference; // Return number of new workers to spawn
                }
                
                log.WriteLine($"AdjustRestoreWorkers: No change after pool adjustment" );
                return 0; // No change
            }
            else if (newCount < currentPoolCapacity)
            {
                // Trying to decrease - just update the pool size, don't spawn workers
                // The count will naturally come down as workers complete
                int difference = poolManager.AdjustPoolSize(newCount);
                
                log.WriteLine($"AdjustRestoreWorkers: Decreasing, poolManager.AdjustPoolSize returned difference={difference}");
                
                if (difference != 0)
                {
                    MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers = newCount;
                    MigrationJobContext.CurrentlyActiveJob.MaxParallelRestoreProcesses = newCount;
                    log.WriteLine($"Reduced restore worker capacity to {newCount}. Active workers will complete naturally.");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
                else
                {
                    log.WriteLine($"AdjustRestoreWorkers: No pool size change (already at {newCount})");
                }
                
                return 0; // Don't spawn new workers when decreasing
            }
            
            // newCount == currentPoolCapacity, no change needed
            log.WriteLine($"AdjustRestoreWorkers: No change needed. Pool capacity already at {newCount}");
            return 0;
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore.
        /// </summary>
        /// <param name="newCount">Desired insertion worker count</param>
        /// <param name="log">Log instance for output</param>
        internal static void AdjustInsertionWorkers(int newCount, Log log)
        {
            newCount = ValidateWorkerCount(newCount);
            
            MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = newCount;
            MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection = newCount;
            
            log.WriteLine($"Set insertion workers per collection to {newCount}. Will apply to new restore operations.");
            
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
        }
    }
}
