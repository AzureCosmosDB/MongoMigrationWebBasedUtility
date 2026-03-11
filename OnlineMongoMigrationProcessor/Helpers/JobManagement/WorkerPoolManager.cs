using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OnlineMongoMigrationProcessor.Context;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Manages a pool of workers with dynamic scaling capabilities.
    /// Uses simple counter-based concurrency control for reliability.
    /// </summary>
    internal class WorkerPoolManager
    {
        private readonly Log _log;
        private readonly string _poolName;
        private readonly object _lock = new object();
        private int _maxWorkers;
        private int _activeWorkers = 0;
        
        public int MaxWorkers 
        { 
            get { lock (_lock) { return _maxWorkers; } }
        }
        
        public int CurrentActive 
        { 
            get { lock (_lock) { return _activeWorkers; } }
        }
        
        public int CurrentAvailable 
        { 
            get { lock (_lock) { return Math.Max(0, _maxWorkers - _activeWorkers); } }
        }
        
        public int CurrentInUse 
        { 
            get { lock (_lock) { return _activeWorkers; } }
        }

        public WorkerPoolManager(Log log, string poolName, int initialMaxWorkers)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolManager: Constructor called, poolName={poolName}, initialMaxWorkers={initialMaxWorkers}");
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _poolName = poolName ?? throw new ArgumentNullException(nameof(poolName));
            _maxWorkers = Math.Max(0, initialMaxWorkers);
        }

        /// <summary>
        /// Adjusts the worker pool size. Returns the difference (positive = increased, negative = decreased).
        /// </summary>
        /// <param name="newCount">New maximum worker count (will be clamped to 1-16)</param>
        /// <returns>Number of workers added (positive) or removed (negative)</returns>
        public int AdjustPoolSize(int newCount)
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolManager.AdjustPoolSize: poolName={_poolName}, newCount={newCount}");
            // Safety limits
            if (newCount < 0) newCount = 0;
            if (newCount > 16) newCount = 16;
            
            lock (_lock)
            {
                int oldCount = _maxWorkers;
                int difference = newCount - oldCount;
                
                if (difference == 0) return 0;
                
                _maxWorkers = newCount;
                
                string poolType = _poolName.StartsWith("Dump") ? "Dump" : "Restore";
                
                if (difference > 0)
                {
                    _log.WriteLine($"{poolType} worker pool capacity increased: {oldCount} → {newCount} (currently {_activeWorkers} workers active)", LogType.Debug);
                }
                else
                {
                    _log.WriteLine($"{poolType} worker pool capacity reduced: {oldCount} → {newCount} (currently {_activeWorkers} workers active, will naturally decrease)", LogType.Debug);
                }
                
                return difference;
            }
        }

        /// <summary>
        /// Tries to acquire a worker slot. Returns true if successful, false if pool is at capacity.
        /// </summary>
        public bool TryAcquire()
        {
            lock (_lock)
            {
                if (_activeWorkers < _maxWorkers)
                {
                    _activeWorkers++;
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Releases a worker slot.
        /// </summary>
        public void Release()
        {
            MigrationJobContext.AddVerboseLog($"WorkerPoolManager.Release: poolName={_poolName}");
            lock (_lock)
            {
                if (_activeWorkers > 0)
                {
                    _activeWorkers--;
                }
            }
        }

        /// <summary>
        /// Waits asynchronously until a worker slot becomes available or cancellation is requested.
        /// </summary>
        public async Task WaitAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (TryAcquire())
                {
                    return;
                }
                
                // Wait a bit before checking again
                await Task.Delay(50, cancellationToken);
            }
            
            cancellationToken.ThrowIfCancellationRequested();
        }

        /// <summary>
        /// Releases a worker slot. For compatibility with existing code.
        /// </summary>
        public bool TryRelease()
        {
            Release();
            return true;
        }

        /// <summary>
        /// Disposes resources.
        /// </summary>
        public void Dispose()
        {
            // Nothing to dispose with counter-based approach
        }
    }
}
