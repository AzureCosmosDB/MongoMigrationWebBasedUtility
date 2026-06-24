using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Linq;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public static class ChangeStreamTransitionHelper
    {
        public static bool TryTransitionCollectionToServerResumeCheckpoint(Log log, bool syncBack)
        {
            var job = MigrationJobContext.CurrentlyActiveJob;
            if (job == null || job.ChangeStreamLevel != ChangeStreamLevel.Server)
                return false;

            if (!string.IsNullOrEmpty(job.GetResumeToken(syncBack)))
                return false;

            var units = Helper.GetMigrationUnitsToMigrate(job)
                .Where(Helper.IsMigrationUnitValid)
                .ToList();

            if (units.Count == 0)
                return false;

            // For sync-back the anchor is when sync-back was enabled, not when the original
            // forward migration started. The job's SyncBackChangeStreamStartedOn is stamped in
            // SetSyncBackResumeTokenAsync the first time sync-back runs; fall back to UtcNow
            // (never job.StartedOn, which would replay forward-direction history into the source).
            var transitionStartedOn = syncBack
                ? (job.SyncBackChangeStreamStartedOn?.ToUniversalTime() ?? DateTime.UtcNow)
                : (job.StartedOn?.ToUniversalTime() ?? DateTime.UtcNow);

            job.SetResumeToken(syncBack, null);
            job.SetOriginalResumeToken(syncBack, null);
            job.SetInitialDocumenReplayed(syncBack, false);
            job.SetChangeStreamStartedOn(syncBack, transitionStartedOn);
            job.SetTransitionBootstrapPending(syncBack, true);

            foreach (var unit in units)
            {
                unit.SetResumeToken(syncBack, null);
                unit.SetOriginalResumeToken(syncBack, null);
                unit.SetCursorUtcTimestamp(syncBack, DateTime.MinValue);
                // Per-unit ChangeStreamStartedOn is preserved across this transition - the
                // SetChangeStreamStartedOn helper is set-when-empty, so any seed call would
                // be a no-op once a value exists.
                unit.SetCSLastChange(syncBack, null, null);
                unit.ClearResumeDocumentInfo(syncBack);
                unit.ResetChangeStreamCounters(syncBack);
                unit.CSLastChecked = DateTime.MinValue;
                unit.ResetChangeStream = false;

                MigrationJobContext.SaveMigrationUnit(unit, false);
            }

            MigrationJobContext.SaveMigrationJob(job);

            var syncBackPrefix = syncBack ? "SyncBack: " : string.Empty;
            bool wasReset = job.GetServerLevelChangeStreamResetPending(syncBack);

            var anchorLabel = syncBack ? "sync-back enabled time" : "job StartedOn";
            if (wasReset)
            {
                log.WriteLine(
                    $"{syncBackPrefix}Server-level change stream reset is being applied. " +
                    $"Server-level change stream start time set to {anchorLabel} at {transitionStartedOn:O}.",
                    LogType.Warning);
                log.WriteLine(
                    $"{syncBackPrefix}Server-level change stream will replay from {anchorLabel} ({transitionStartedOn:O}) following the reset.",
                    LogType.Warning);

                job.SetServerLevelChangeStreamResetPending(syncBack, false);
                MigrationJobContext.SaveMigrationJob(job);
            }
            else
            {
                log.WriteLine(
                    $"{syncBackPrefix} Change stream scope transition detected (Collection -> Server). " +
                    $"Resetting server-level change stream start time to {anchorLabel} at {transitionStartedOn:O}.",
                    LogType.Warning);
                log.WriteLine(
                    $"{syncBackPrefix}Change stream transitioned to server-level and was pushed back to earliest change since {anchorLabel} ({transitionStartedOn:O}).",
                    LogType.Warning);
            }

            return true;
        }

        /// <summary>
        /// Resets the server-level change stream checkpoint for the currently active job
        /// (user-initiated, e.g. from the "Reset Change Stream" UI for server-level jobs).
        /// Clears job- and unit-level resume tokens/timestamps and marks a reset as pending
        /// so subsequent bootstrap log messages reflect a reset rather than a
        /// Collection -> Server transition. Returns true if the reset was applied.
        /// </summary>
        public static bool ResetServerLevelChangeStream(Log log, MigrationJob job, bool syncBack)
        {
            if (job == null || job.ChangeStreamLevel != ChangeStreamLevel.Server)
                return false;

            var units = Helper.GetMigrationUnitsToMigrate(job)
                .Where(Helper.IsMigrationUnitValid)
                .ToList();

            // For sync-back the anchor is when sync-back was enabled (never job.StartedOn).
            var resetStartedOn = syncBack
                ? (job.SyncBackChangeStreamStartedOn?.ToUniversalTime() ?? DateTime.UtcNow)
                : (job.StartedOn?.ToUniversalTime() ?? DateTime.UtcNow);

            job.SetResumeToken(syncBack, null);
            job.SetOriginalResumeToken(syncBack, null);
            job.SetInitialDocumenReplayed(syncBack, false);
            // Route through SetChangeStreamStartedOn so the set-when-empty guard preserves any
            // existing anchor. A reset must not update ChangeStreamStartedOn when one is already
            job.SetTransitionBootstrapPending(syncBack, true);
            job.SetServerLevelChangeStreamResetPending(syncBack, true);

            foreach (var unit in units)
            {
                unit.SetResumeToken(syncBack, null);
                unit.SetCursorUtcTimestamp(syncBack, DateTime.MinValue);
                
                unit.SetCSLastChange(syncBack, null, null);
                unit.ClearResumeDocumentInfo(syncBack);
                unit.ResetChangeStreamCounters(syncBack);
                unit.CSLastChecked = DateTime.MinValue;
                unit.ResetChangeStream = false;

                MigrationJobContext.SaveMigrationUnit(unit, false);
            }

            MigrationJobContext.SaveMigrationJob(job);

            if (log != null)
            {
                var syncBackPrefix = syncBack ? "SyncBack: " : string.Empty;
                var effectiveStartedOn = job.GetChangeStreamStartedOn(syncBack);
                log.WriteLine(
                    $"{syncBackPrefix}Server-level change stream reset requested. " +
                    $"Server-level change stream start time preserved at {effectiveStartedOn:O} (reset does not update ChangeStreamStartedOn).",
                    LogType.Warning);
            }

            return true;
        }

        /// <summary>
        /// Handles the Server -> Collection change stream scope transition for an existing
        /// job. Clears any server-level resume state on the job and performs a per-collection
        /// change stream reset (equivalent to calling <see cref="Helpers.Mongo.MongoHelper.ResetCS"/>
        /// for each migration unit) so that collection-level processing restarts cleanly
        /// from the job's StartedOn time.
        /// </summary>
        public static bool TransitionServerToCollectionResumeCheckpoints(Log log, MigrationJob job, bool syncBack)
        {
            if (job == null || job.ChangeStreamLevel != ChangeStreamLevel.Collection)
                return false;

            var units = Helper.GetMigrationUnitsToMigrate(job)
                .Where(Helper.IsMigrationUnitValid)
                .ToList();

            // Sync-back uses the time sync-back was enabled as the per-unit anchor; there is no
            // forward bulk copy in the reverse direction. Forward direction handles its anchor
            // per-unit below (prefer unit.ChangeStreamStartedOn, fall back to BulkCopyStartedOn - 4h),
            // so no job-wide value is needed there.
            DateTime? syncBackAnchor = syncBack
                ? (job.SyncBackChangeStreamStartedOn?.ToUniversalTime() ?? DateTime.UtcNow)
                : (DateTime?)null;

            // Clear job-level (server-level) change stream state - it no longer applies.
            job.SetResumeToken(syncBack, null);
            job.SetOriginalResumeToken(syncBack, null);
            job.SetInitialDocumenReplayed(syncBack, false);
            job.SetCursorUtcTimestamp(syncBack, DateTime.MinValue);
            job.SetTransitionBootstrapPending(syncBack, false);
            job.SetServerLevelChangeStreamResetPending(syncBack, false);
            if (!syncBack)
                job.CSLastChecked = DateTime.MinValue;

            // Reset change stream for each collection (equivalent to MongoHelper.ResetCS per unit).
            // SetChangeStreamStartedOn is set-when-empty, so existing per-unit anchors are
            // preserved unconditionally. The calls below only seed an anchor for legacy MUs
            foreach (var unit in units)
            {
                unit.SetResumeToken(syncBack, null);
                unit.SetOriginalResumeToken(syncBack, null);
                unit.SetCursorUtcTimestamp(syncBack, DateTime.MinValue);
                //when ChangeStreamStartedOn is empty seed with BulkCopyStartedOn - 4h so the bootstrap opens just before
                //    this collection's bulk copy began (avoids the "Timestamp mismatch: Old is
                //    newer than New" guard that a UtcNow fallback would cause).
                if (unit.BulkCopyStartedOn.HasValue && unit.BulkCopyStartedOn.Value != DateTime.MinValue)
                {
                    unit.SetChangeStreamStartedOn(syncBack, unit.BulkCopyStartedOn.Value.ToUniversalTime().AddHours(-4));
                }
                unit.SetCSLastChange(syncBack, null, null);
                unit.ClearResumeDocumentInfo(syncBack);
                unit.ResetChangeStreamCounters(syncBack);
                unit.CSLastChecked = DateTime.MinValue;
                unit.ResetChangeStream = true;

                MigrationJobContext.SaveMigrationUnit(unit, false);
            }

            MigrationJobContext.SaveMigrationJob(job);

            if (log != null)
            {
                var syncBackPrefix = syncBack ? "SyncBack: " : string.Empty;
                var anchorLabel = syncBack
                    ? $"sync-back enabled time ({syncBackAnchor:O})"
                    : "its existing ChangeStreamStartedOn (or BulkCopyStartedOn - 4h fallback)";
                log.WriteLine(
                    $"{syncBackPrefix}Change stream scope transition detected (Server -> Collection). " +
                    $"Cleared server-level change stream state and reset change stream for {units.Count} collection(s). " +
                    $"Each collection-level change stream will resume from {anchorLabel}.",
                    LogType.Warning);
            }

            return true;
        }
    }
}
