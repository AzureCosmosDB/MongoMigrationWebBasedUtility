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

            // If a previous invocation already performed the bootstrap reset and we are
            // still waiting for the first event (no resume token yet), skip the reset
            // and the log. The retry loop in InitializeResumeTokensAsync would otherwise
            // re-run this every 60s and spam identical messages.
            // Exception: a pending server-level reset (ResetServerLevelChangeStream) must
            // still be honored on first call so its specific log is emitted; the pending
            // bit is cleared in that branch below.
            if (job.GetTransitionBootstrapPending(syncBack)
                && !job.GetServerLevelChangeStreamResetPending(syncBack))
                return false;

            var units = Helper.GetMigrationUnitsToMigrate(job)
                .Where(Helper.IsMigrationUnitValid)
                .ToList();

            if (units.Count == 0)
                return false;

            // Prefer the explicit ChangeStreamStartedOn anchor when present (e.g. when the
            // user flips forward <-> sync-back, ResetForwardChangeStreamForReEnable /
            // ResetSyncBackChangeStreamForReEnable directly assign UtcNow and we must honor
            // that). Fall back to job.StartedOn (forward) or UtcNow (sync-back) for the
            // original Collection -> Server transition at job start where the anchor was
            // not pre-set. Never use job.StartedOn for sync-back - that would replay
            // forward-direction history into the source.
            var transitionStartedOn = job.GetChangeStreamStartedOn(syncBack)?.ToUniversalTime()
                ?? (syncBack
                    ? DateTime.UtcNow
                    : (job.StartedOn?.ToUniversalTime() ?? DateTime.UtcNow));

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

            var anchorLabel = syncBack ? "sync-back change stream start time" : "change stream start time";
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
                // Bootstrap context comes from CSLastAction, set at the actual user/system
                // action site (DraftOption save, forward<->sync-back flip). Falls back to
                // a neutral "initialized" message for the original at-job-start path where
                // no explicit action was recorded.
                string contextMsg;
                switch (job.CSLastAction)
                {
                    case ChangeStreamAction.CollectionToServer:
                        contextMsg = "Change stream scope transitioned from Collection to Server.";
                        break;
                    case ChangeStreamAction.ForwardSyncEnabled:
                        contextMsg = "Forward sync re-enabled by user.";
                        break;
                    case ChangeStreamAction.SyncBackEnabled:
                        contextMsg = "Sync-back enabled by user.";
                        break;
                    default:
                        contextMsg = "Server-level change stream initialized.";
                        break;
                }

                log.WriteLine(
                    $"{syncBackPrefix}{contextMsg} Bootstrapping from {anchorLabel} at {transitionStartedOn:O}.",
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
            job.CSLastAction = ChangeStreamAction.ServerToCollection;
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

        /// <summary>
        /// Forces a re-enable of the forward change stream after sync-back was running.
        /// Overrides the set-when-empty guard on ChangeStreamStartedOn (direct field
        /// assignment) and clears all forward-direction resume tokens / cursor state on
        /// the job and every valid migration unit so the forward change stream restarts
        /// from <see cref="DateTime.UtcNow"/>. Sync-back state is left intact.
        /// </summary>
        public static bool ResetForwardChangeStreamForReEnable(Log log, MigrationJob job)
        {
            if (job == null)
                return false;

            var units = Helper.GetMigrationUnitsToMigrate(job)
                .Where(Helper.IsMigrationUnitValid)
                .ToList();

            var now = DateTime.UtcNow;

            job.ForceResetChangeStreamStartedOn(false, now);
            job.SetOriginalResumeToken(false, null);
            job.SetInitialDocumenReplayed(false, false);
#pragma warning disable CS0618 // ResumeDocumentId is obsolete but may still hold legacy stored data
            job.ResumeDocumentId = null;
#pragma warning restore CS0618
            job.ResumeDocumentKey = null;
            job.ResumeCollectionKey = null;
            job.SetCursorUtcTimestamp(false, DateTime.MinValue);
            job.SetTransitionBootstrapPending(false, false);
            job.SetServerLevelChangeStreamResetPending(false, false);
            job.CSPostProcessingStarted = false;
            job.CSLastChecked = DateTime.MinValue;
            job.CSLastAction = ChangeStreamAction.ForwardSyncEnabled;

            foreach (var unit in units)
            {
                unit.ForceResetChangeStreamStartedOn(false, now);
                unit.SetOriginalResumeToken(false, null);
                unit.SetCursorUtcTimestamp(false, DateTime.MinValue);
                unit.SetCSLastChange(false, null, null);
                unit.ClearResumeDocumentInfo(false);
                unit.ResetChangeStreamCounters(false);
                unit.CSLastChecked = DateTime.MinValue;
                unit.ResetChangeStream = false;

                MigrationJobContext.SaveMigrationUnit(unit, false);
            }

            MigrationJobContext.SaveMigrationJob(job);

            log?.WriteLine(
                $"Forward change stream re-enabled by user request. " +
                $"ChangeStreamStartedOn forced to {now:O} for job and {units.Count} collection(s); " +
                $"forward resume tokens cleared.",
                LogType.Warning);

            return true;
        }

        /// <summary>
        /// Forces a re-initialization of the sync-back change stream when the user
        /// flips forward -> sync-back. Overrides the set-when-empty guard on
        /// SyncBackChangeStreamStartedOn (direct field assignment) and clears all
        /// sync-back-direction resume tokens / cursor state on the job and every
        /// valid migration unit so sync-back replays from <see cref="DateTime.UtcNow"/>.
        /// Forward state is left intact. Do NOT call from the resume-after-restart
        /// path - that path must preserve existing sync-back resume tokens.
        /// </summary>
        public static bool ResetSyncBackChangeStreamForReEnable(Log log, MigrationJob job)
        {
            if (job == null)
                return false;

            var units = Helper.GetMigrationUnitsToMigrate(job)
                .Where(Helper.IsMigrationUnitValid)
                .ToList();

            var now = DateTime.UtcNow;

            job.ForceResetChangeStreamStartedOn(true, now);
            job.SetOriginalResumeToken(true, null);
            job.SetInitialDocumenReplayed(true, false);
#pragma warning disable CS0618 // SyncBackResumeDocumentId is obsolete but may still hold legacy stored data
            job.SyncBackResumeDocumentId = null;
#pragma warning restore CS0618
            job.SyncBackResumeDocumentKey = null;
            job.SyncBackResumeCollectionKey = null;
            job.SetCursorUtcTimestamp(true, DateTime.MinValue);
            job.SetTransitionBootstrapPending(true, false);
            job.SetServerLevelChangeStreamResetPending(true, false);
            job.CSLastAction = ChangeStreamAction.SyncBackEnabled;

            foreach (var unit in units)
            {
                unit.ForceResetChangeStreamStartedOn(true, now);
                unit.SetOriginalResumeToken(true, null);
                unit.SetCursorUtcTimestamp(true, DateTime.MinValue);
                unit.SetCSLastChange(true, null, null);
                unit.ClearResumeDocumentInfo(true);
                unit.ResetChangeStreamCounters(true);
                unit.CSLastChecked = DateTime.MinValue;

                MigrationJobContext.SaveMigrationUnit(unit, false);
            }

            MigrationJobContext.SaveMigrationJob(job);

            log?.WriteLine(
                $"SyncBack: change stream re-initialized by user request. " +
                $"SyncBackChangeStreamStartedOn forced to {now:O} for job and {units.Count} collection(s); " +
                $"sync-back resume tokens cleared.",
                LogType.Warning);

            return true;
        }
    }
}
