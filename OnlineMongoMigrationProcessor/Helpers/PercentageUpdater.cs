using OnlineMongoMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public static class PercentageUpdater
    {
        private const int PERCENTAGE_UPDATE_INTERVAL_MS = 5000; // 5 seconds
        private static SafeDictionary<string, bool> _activeTrackers = new SafeDictionary<string, bool>();

        private static List<string> _trackersToRemove = new List<string>();

        private static System.Timers.Timer _timer =new System.Timers.Timer(PERCENTAGE_UPDATE_INTERVAL_MS);
        
        private static Log _log;

        public static void Initialize()
        {
            MigrationJobContext.AddVerboseLog($"PercentageUpdater Initialize Invoked");
            try
            {
                _activeTrackers = new SafeDictionary<string, bool>();
                _trackersToRemove = new List<string>();
                _timer.Stop();

            }
            finally
            {
            }
        }

        /// <summary>
        /// Ensures a timer is running for the given migration mu to periodically recalculate percentages.
        /// Timer runs every 5 seconds and stops when all chunks are complete.
        /// </summary>
        public static void AddToPercentageTracker(string id, bool isRestore, Log log)
        {
            MigrationJobContext.AddVerboseLog($"PercentageUpdater.AddToPercentageTracker: id={id}, isRestore={isRestore}");
            _log = log;
            var key= $"{id}_{isRestore}";
            if (!_activeTrackers.ContainsKey(key)) {
                _activeTrackers.AddOrUpdate(key, isRestore);
            }

            if (!_timer.Enabled)
            {                
                _timer.Elapsed += (sender, e) =>
                {
                    TimerTick();
                };
                _timer.Start();
            }            
        }

        public static void RemovePercentageTracker(string id, bool isRestore, Log log)
        {
            MigrationJobContext.AddVerboseLog($"PercentageUpdater.RemovePercentageTracker: id={id}, isRestore={isRestore}");
            _log = log;
            var key = $"{id}_{isRestore}";

            MigrationJobContext.AddVerboseLog($"PercentageUpdater _trackersToRemove added {key}");
            _trackersToRemove.Add(key);            
        }

        private  static void TimerTick()
        {
            foreach (var kvp in _activeTrackers.GetAll())
            {                
                bool isRestore = kvp.Value;
                string id = kvp.Key.Split("_")[0];
                
                //cleanup if marked for removal
                if (_trackersToRemove.Contains(kvp.Key))
                {
                    MigrationJobContext.AddVerboseLog($"PercentageUpdater _activeTrackers.Remove({kvp.Key})  _activeTrackers.Count={_activeTrackers.Count}");
                    _activeTrackers.Remove(kvp.Key);

                    if (_activeTrackers.Count == 0)
                    {
                        _timer.Stop();
                        return;
                    }
                    _trackersToRemove.Remove(kvp.Key);
                }

                ProcessMigrationUnitProgress(id, isRestore);
            }
        }


        private static bool ProcessMigrationUnitProgress(string id, bool isRestore)
        {
            MigrationJobContext.AddVerboseLog($"ProcessMigrationUnitProgress mu={id} IsRestore={isRestore}");

            var mu = MigrationJobContext.GetMigrationUnit(id);
            if (mu == null)
            {
                MigrationJobContext.AddVerboseLog($"ProcessMigrationUnitProgress exited as MigrationUnit not found");
                return false; // Migration unit not found
            }

            bool hasActiveChunks = false;
            bool stateCorrected = false;

            bool allDumpChunksDownloaded = mu.MigrationChunks.All(c => c.IsDownloaded == true);
            bool allRestoreChunksUploaded = mu.MigrationChunks.All(c => c.IsUploaded == true);

            // Self-heal stale flags from older runs where percent reached 100 before all chunks finished.
            if (mu.DumpComplete && !allDumpChunksDownloaded)
            {
                mu.DumpComplete = false;
                stateCorrected = true;
            }

            if (mu.RestoreComplete && !allRestoreChunksUploaded)
            {
                mu.RestoreComplete = false;
                stateCorrected = true;
            }

            if (stateCorrected)
            {
                mu.UpdateParentJob();
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }

            if (isRestore && mu.RestoreComplete)
                return true;

            

            if (isRestore)
            {

                // Check for active or pending restore chunks
                foreach (var chunk in mu.MigrationChunks)
                {
                    if (chunk.IsUploaded != true && (chunk.RestoredSuccessDocCount > 0 || chunk.IsDownloaded == true))
                    {
                        hasActiveChunks = true;
                        break;
                    }
                }
                if (hasActiveChunks)
                {
                    // Recalculate overall restore percent atomically on persisted MU so concurrent
                    // worker saves don't wipe the in-memory percent update.
                    bool reachedComplete = false;
                    MigrationJobContext.MutateMigrationUnit(id, m =>
                    {
                        m.RestorePercent = CalculateOverallPercentFromAllChunks(m, isRestore: true, log: _log);
                        bool allUploaded = m.MigrationChunks.All(c => c.IsUploaded == true);
                        if (m.RestorePercent >= 99.99 && allUploaded)
                        {
                            m.RestoreComplete = true;
                            reachedComplete = true;
                        }
                    }, updateParent: true);
                    if (reachedComplete)
                    {
                        RemovePercentageTracker(id, isRestore, _log);
                    }
                }
            }
            else // MongoDump
            {
                if (mu.DumpComplete)
                    return true;

                // Check for active or pending dump chunks
                foreach (var chunk in mu.MigrationChunks)
                {
                    if (chunk.IsDownloaded != true && chunk.DumpQueryDocCount > 0)
                    {
                        hasActiveChunks = true;
                        break;
                    }
                }
                if (hasActiveChunks)
                {
                    // Recalculate overall dump percent atomically on persisted MU so concurrent
                    // worker saves don't wipe the in-memory percent update.
                    bool reachedComplete = false;
                    MigrationJobContext.MutateMigrationUnit(id, m =>
                    {
                        m.DumpPercent = CalculateOverallPercentFromAllChunks(m, isRestore: false, log: _log);
                        bool allDownloaded = m.MigrationChunks.All(c => c.IsDownloaded == true);
                        if (m.DumpPercent >= 99.99 && allDownloaded)
                        {
                            m.DumpComplete = true;
                            reachedComplete = true;
                        }
                    }, updateParent: true);
                    if (reachedComplete)
                    {
                        RemovePercentageTracker(id, isRestore, _log);
                    }
                }
            }
            return true;
        }
        /// <summary>
        /// Calculates overall percent from all chunks by checking their current state.
        /// Used by timer to recalculate overall progress for dump or restore operations.
        /// </summary>
        public static double CalculateOverallPercentFromAllChunks(MigrationUnit mu, bool isRestore, Log log)
        {
            MigrationJobContext.AddVerboseLog($"PercentageUpdater.CalculateOverallPercentFromAllChunks: mu={mu.DatabaseName}.{mu.CollectionName}, isRestore={isRestore} isDumpComplete={mu.DumpComplete} isRestoreComplete={mu.RestoreComplete} dumpPercent={mu.DumpPercent} restorePercent={mu.RestorePercent}");
            double totalPercent = 0;

            // Calculate effective doc count per chunk: use DumpQueryDocCount, falling back to
            // RestoredSuccessDocCount or DumpResultDocCount for chunks that completed without
            // a persisted DumpQueryDocCount (e.g. paused before save on a prior run).
            long totalDocsFromChunks = 0;
            long chunksWithDocCount = 0;
            if (mu.MigrationChunks != null)
            {
                foreach (var c in mu.MigrationChunks)
                {
                    long eff = c.DumpQueryDocCount;
                    if (eff == 0)
                        eff = Math.Max(c.RestoredSuccessDocCount, c.DumpResultDocCount);
                    if (eff > 0)
                    {
                        chunksWithDocCount++;
                        totalDocsFromChunks += eff;
                    }
                }
            }
            long totalDocsFromUnit = Helper.GetMigrationUnitDocCount(mu);

            // Use the collection-level doc count when not all chunks have DumpQueryDocCount set yet.
            // Otherwise the percentage inflates to 100% based on only the started chunks.
            long totalDocs;
            if (chunksWithDocCount > 0 && chunksWithDocCount < (mu.MigrationChunks?.Count ?? 0))
            {
                totalDocs = Math.Max(totalDocsFromChunks, totalDocsFromUnit);
            }
            else if (totalDocsFromChunks > 0)
            {
                totalDocs = totalDocsFromChunks;
            }
            else
            {
                totalDocs = totalDocsFromUnit;
            }

            if (totalDocs == 0) return 0;

            string strLog;
            if (isRestore)
            {
                strLog = "RestoredSuccessDocCount/DumpQueryDocCount - ChunkPercent - Contrib - TotalPercent";
            }
            else
            {
                strLog = "DumpResultDocCount/DumpQueryDocCount - ChunkPercent - Contrib - TotalPercent";
            }

            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                var c = mu.MigrationChunks[i];

                // Determine effective doc count for this chunk.
                // DumpQueryDocCount can be 0 if the chunk was dumped/restored by a prior run
                // whose DumpQueryDocCount was never persisted (e.g. paused before save).
                // Fall back to RestoredSuccessDocCount or DumpResultDocCount so completed
                // chunks still contribute to the overall percentage.
                long effectiveDocCount = c.DumpQueryDocCount;
                if (effectiveDocCount == 0)
                {
                    effectiveDocCount = Math.Max(c.RestoredSuccessDocCount, c.DumpResultDocCount);
                }

                if (effectiveDocCount == 0)
                {
                    strLog = $"{strLog}\n [{i}] Empty";
                    continue;
                }

                double chunkContrib = (double)effectiveDocCount / totalDocs;
                double chunkPercent = 0;

                if (isRestore)
                {
                    if (c.IsUploaded == true)
                    {
                        // Completed chunk: 100%
                        totalPercent += 100 * chunkContrib;
                        chunkPercent = 100;
                    }
                    else if (c.RestoredSuccessDocCount > 0)
                    {
                        // In-progress chunk: calculate from restored count
                        long chunkTarget = Math.Min(effectiveDocCount, c.DumpResultDocCount > 0 ? c.DumpResultDocCount : effectiveDocCount);
                        chunkPercent = Math.Min(100, (double)c.RestoredSuccessDocCount / chunkTarget * 100);
                        totalPercent += chunkPercent * chunkContrib;
                    }
                    // else: not started, contributes 0%

                    strLog = $"{strLog}\n [{i}] {c.RestoredSuccessDocCount}/{effectiveDocCount} - {chunkPercent:F2} - {chunkContrib:F4} - {totalPercent:F2}";
                }
                else // Dump
                {
                    if (c.IsDownloaded == true)
                    {
                        // Completed chunk: 100%
                        totalPercent += 100 * chunkContrib;
                        chunkPercent = 100;
                    }
                    else if (c.DumpResultDocCount > 0)
                    {
                        // In-progress chunk: calculate from dumped count
                        chunkPercent = Math.Min(100, (double)c.DumpResultDocCount / effectiveDocCount * 100);
                        totalPercent += chunkPercent * chunkContrib;
                    }
                    // else: not started, contributes 0%

                    strLog = $"{strLog}\n [{i}] {c.DumpResultDocCount}/{effectiveDocCount} - {chunkPercent:F2} - {chunkContrib:F4} - {totalPercent:F2}";
                }
            }

            string operationType = isRestore ? "Restore" : "Dump";
            // Uncomment the line below to enable detailed logging of percentage calculations for each chunk
            //MigrationJobContext.AddVerboseLog($"{mu.DatabaseName}.{mu.CollectionName} {operationType} Total: {totalPercent:F2}%\n{strLog}");

            return Math.Min(100, totalPercent);
        }


        /// <summary>
        /// Stops and cleans up all percentage calculation timers.
        /// Call this when stopping a migration job to prevent timers from previous jobs
        /// from interfering with new jobs for the same collections.
        /// </summary>
        public static void StopPercentageTimer()
        {

            if(_timer!=null && _timer.Enabled)
                _timer.Stop();

            _activeTrackers.Clear();
        }

    }
}
