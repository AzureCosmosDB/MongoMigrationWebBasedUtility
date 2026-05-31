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

            var transitionStartedOn = job.StartedOn?.ToUniversalTime() ?? DateTime.UtcNow;

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
                unit.SetChangeStreamStartedOn(syncBack, null);
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

            if (wasReset)
            {
                log.WriteLine(
                    $"{syncBackPrefix}Server-level change stream reset is being applied. " +
                    $"Server-level change stream start time set to job StartedOn at {transitionStartedOn:O}.",
                    LogType.Warning);
                log.WriteLine(
                    $"{syncBackPrefix}Server-level change stream will replay from job StartedOn ({transitionStartedOn:O}) following the reset.",
                    LogType.Warning);

                job.SetServerLevelChangeStreamResetPending(syncBack, false);
                MigrationJobContext.SaveMigrationJob(job);
            }
            else
            {
                log.WriteLine(
                    $"{syncBackPrefix} Change stream scope transition detected (Collection -> Server). " +
                    $"Resetting server-level change stream start time to job StartedOn at {transitionStartedOn:O}.",
                    LogType.Warning);
                log.WriteLine(
                    $"{syncBackPrefix}Change stream transitioned to server-level and was pushed back to earliest change since job creation ({transitionStartedOn:O}).",
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

            var resetStartedOn = job.StartedOn?.ToUniversalTime() ?? DateTime.UtcNow;

            job.SetResumeToken(syncBack, null);
            job.SetOriginalResumeToken(syncBack, null);
            job.SetInitialDocumenReplayed(syncBack, false);
            job.SetChangeStreamStartedOn(syncBack, resetStartedOn);
            job.SetTransitionBootstrapPending(syncBack, true);
            job.SetServerLevelChangeStreamResetPending(syncBack, true);

            foreach (var unit in units)
            {
                unit.SetResumeToken(syncBack, null);
                unit.SetOriginalResumeToken(syncBack, null);
                unit.SetCursorUtcTimestamp(syncBack, DateTime.MinValue);
                unit.SetChangeStreamStartedOn(syncBack, null);
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
                log.WriteLine(
                    $"{syncBackPrefix}Server-level change stream reset requested. " +
                    $"Server-level change stream start time set to job StartedOn at {resetStartedOn:O}.",
                    LogType.Warning);
            }

            return true;
        }
    }
}
