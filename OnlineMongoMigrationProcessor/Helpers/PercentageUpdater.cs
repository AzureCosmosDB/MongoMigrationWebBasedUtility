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
            {
                return true;
            }

            

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
                {
                    return true;
                }

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

            int totalChunks = mu.MigrationChunks?.Count ?? 0;
            if (totalChunks == 0) return 0;

            // Tally how many chunks have a usable document count and their running total.
            long totalDocsFromChunks = 0;
            long chunksWithDocCount = 0;
            foreach (var c in mu.MigrationChunks)
            {
                long eff = GetEffectiveDocCount(c);
                if (eff > 0)
                {
                    chunksWithDocCount++;
                    totalDocsFromChunks += eff;
                }
            }

            // When not every chunk has reported a document count yet, the doc-count denominator is
            // incomplete. Doc-weighting would then be non-monotonic — it can momentarily reach 100%
            // using only the chunks started so far, then regress as later chunks report their sizes
            // (common on filtered migrations where ActualDocCount is 0 and many chunks fall outside
            // the filter range). Fall back to equal-weight, chunk-count based progress, which climbs
            // monotonically. This applies equally to dump (download) and restore (upload).
            if (chunksWithDocCount < totalChunks)
            {
                return CalculateChunkWeightedPercent(mu, isRestore);
            }

            // Every chunk has a document count: weight by real document counts for an accurate
            // percentage. (totalDocsFromChunks is guaranteed > 0 here since every chunk has eff > 0.)
            return CalculateDocWeightedPercent(mu, isRestore, totalDocsFromChunks);
        }

        /// <summary>
        /// Effective document count for a chunk: the queried count, falling back to the restored or
        /// dumped count for chunks that completed on a prior run without persisting DumpQueryDocCount.
        /// </summary>
        private static long GetEffectiveDocCount(MigrationChunk c)
        {
            long eff = c.DumpQueryDocCount;
            if (eff == 0)
                eff = Math.Max(c.RestoredSuccessDocCount, c.DumpResultDocCount);
            return eff;
        }

        /// <summary>
        /// Whether a chunk has finished the given operation (restore = uploaded, dump = downloaded).
        /// </summary>
        private static bool IsChunkComplete(MigrationChunk c, bool isRestore)
            => isRestore ? c.IsUploaded == true : c.IsDownloaded == true;

        /// <summary>
        /// Progress of a single chunk (0-100) for the given operation. Completed chunks return 100,
        /// in-progress chunks are prorated by processed/effective document count, and not-started or
        /// empty chunks return 0.
        /// </summary>
        private static double GetChunkProgressPercent(MigrationChunk c, bool isRestore)
        {
            if (IsChunkComplete(c, isRestore))
                return 100;

            long effectiveDocCount = GetEffectiveDocCount(c);
            if (effectiveDocCount == 0)
                return 0;

            if (isRestore)
            {
                if (c.RestoredSuccessDocCount <= 0)
                    return 0;
                // Restore target is bounded by what was actually dumped for this chunk.
                long chunkTarget = Math.Min(effectiveDocCount, c.DumpResultDocCount > 0 ? c.DumpResultDocCount : effectiveDocCount);
                return Math.Min(100, (double)c.RestoredSuccessDocCount / chunkTarget * 100);
            }

            if (c.DumpResultDocCount <= 0)
                return 0;
            return Math.Min(100, (double)c.DumpResultDocCount / effectiveDocCount * 100);
        }

        /// <summary>
        /// Equal-weight, chunk-count based progress: each chunk contributes 1/totalChunks of the
        /// overall percentage. Used when the document-count denominator is incomplete so the result
        /// stays monotonic. Works for both dump and restore via <paramref name="isRestore"/>.
        /// </summary>
        private static double CalculateChunkWeightedPercent(MigrationUnit mu, bool isRestore)
        {
            int totalChunks = mu.MigrationChunks?.Count ?? 0;
            if (totalChunks == 0) return 0;

            double totalPercent = 0;
            foreach (var c in mu.MigrationChunks)
            {
                totalPercent += GetChunkProgressPercent(c, isRestore) / totalChunks;
            }
            return Math.Min(100, totalPercent);
        }

        /// <summary>
        /// Document-weighted progress: each chunk contributes its share of the total document count.
        /// Used when every chunk has reported a document count so the denominator is complete and the
        /// percentage is accurate. Works for both dump and restore via <paramref name="isRestore"/>.
        /// </summary>
        private static double CalculateDocWeightedPercent(MigrationUnit mu, bool isRestore, long totalDocs)
        {
            if (totalDocs <= 0 || mu.MigrationChunks == null) return 0;

            double totalPercent = 0;
            foreach (var c in mu.MigrationChunks)
            {
                long effectiveDocCount = GetEffectiveDocCount(c);
                if (effectiveDocCount == 0)
                    continue; // genuinely empty chunk: contributes no documents

                double chunkContrib = (double)effectiveDocCount / totalDocs;
                totalPercent += GetChunkProgressPercent(c, isRestore) * chunkContrib;
            }
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
