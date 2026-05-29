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
                unit.CSLastChecked = DateTime.MinValue;
                unit.ResetChangeStream = false;

                MigrationJobContext.SaveMigrationUnit(unit, false);
            }

            MigrationJobContext.SaveMigrationJob(job);

            var syncBackPrefix = syncBack ? "SyncBack: " : string.Empty;
            log.WriteLine(
                $"{syncBackPrefix} Change stream scope transition detected (Collection -> Server). " +
                $"Resetting server-level change stream start time to job StartedOn at {transitionStartedOn:O}.",
                LogType.Warning);
            log.WriteLine(
                $"{syncBackPrefix}Change stream transitioned to server-level and was pushed back to earliest change since job creation ({transitionStartedOn:O}).",
                LogType.Warning);

            return true;
        }
    }
}
