using OnlineMongoMigrationProcessor.Context;
using System;
using System.Collections.Concurrent;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    public class ActiveMigrationUnitsCache
    {
        private readonly ConcurrentDictionary<string, MigrationUnit> _migrationUnits;

        private static string BuildCacheKey(string migrationUnitId, string jobId) => $"{jobId}::{migrationUnitId}";

        public ActiveMigrationUnitsCache()
        {
            _migrationUnits = new ConcurrentDictionary<string, MigrationUnit>();
        }


        public MigrationUnit GetMigrationUnit(string migrationUnitId, string JobId=null)
        {
            //MigrationJobContext.AddVerboseLog($"ActiveMigrationUnitsCache.GetMigrationUnit: migrationUnitId={migrationUnitId}, cacheCount={_migrationUnits.Count}");

            if (string.IsNullOrEmpty(JobId))
            {
                JobId = MigrationJobContext.CurrentlyActiveJob?.Id;
                if (string.IsNullOrEmpty(JobId))
                    return null;
            }

            var cacheKey = BuildCacheKey(migrationUnitId, JobId);

            if (_migrationUnits.TryGetValue(cacheKey, out MigrationUnit? cachedMigrationUnit) && cachedMigrationUnit != null)
                return cachedMigrationUnit;

            // Atomic load: under concurrent misses, the factory may run more than once,
            // but only one MigrationUnit instance ever survives in the dictionary. Every caller
            // — past, present, and future — sees the same shared instance for this key.
            var mu = _migrationUnits.GetOrAdd(cacheKey, _ =>
                MigrationJobContext.GetMigrationUnitFromStorage(JobId, migrationUnitId));

            if (mu == null)
            {
                // Don't keep a null entry around; it would mask future loads.
                _migrationUnits.TryRemove(new System.Collections.Generic.KeyValuePair<string, MigrationUnit>(cacheKey, null!));
                return null;
            }

            return mu;
        }


        public  bool UpdateMigrationUnit(MigrationUnit migrationUnit)
        {
            MigrationJobContext.AddVerboseLog($"ActiveMigrationUnitsCache.UpdateMigrationUnit: migrationUnitId={migrationUnit.Id}");

            if (migrationUnit == null || string.IsNullOrEmpty(migrationUnit.Id) || string.IsNullOrEmpty(migrationUnit.JobId))
                return false;

            var cacheKey = BuildCacheKey(migrationUnit.Id, migrationUnit.JobId);
            _migrationUnits[cacheKey] = migrationUnit;
            return true;
        }

        public void RemoveMigrationUnit(string migrationUnitId)
        {
            MigrationJobContext.AddVerboseLog($"ActiveMigrationUnitsCache.RemoveMigrationUnit: migrationUnitId={migrationUnitId}");

            if (string.IsNullOrEmpty(migrationUnitId))
                return;

            foreach (var key in _migrationUnits.Keys)
            {
                if (key.EndsWith($"::{migrationUnitId}", StringComparison.Ordinal))
                    _migrationUnits.TryRemove(key, out _);
            }
        }
    }
}
