using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Models;

namespace MongoMigrationWebApp.Helpers
{
    // Single source of truth for the per-collection option conditional matrix used in
    // ManageCollections (Add form, Edit form, Bulk toolbar, AddDraft, SaveEdit,
    // BulkSetOverwrite, RenderOptionsBadges). Rules:
    //   - Overwrite = FALSE  -> Indexing, Sharding, Move-to are not meaningful (target is not recreated).
    //   - Sharding != DontShard -> Move-to is not meaningful.
    //   - Simulated run       -> Indexing, Sharding, Move-to are forced off (server double-checks too).
    // Any caller that needs to read/write/display these fields must go through Normalize + the
    // IsXxxLocked helpers so the behaviour stays consistent.
    public static class DraftOptionRules
    {
        public readonly record struct Normalized(
            bool? Overwrite,
            IndexingStrategy? Indexing,
            ShardingStrategy? Sharding,
            string? MoveToShard);

        public static Normalized Normalize(
            bool? overwrite,
            IndexingStrategy? indexing,
            ShardingStrategy? sharding,
            string? moveToShard,
            bool isSimulatedRun)
        {
            if (isSimulatedRun || overwrite == false)
            {
                return new Normalized(overwrite, null, null, null);
            }

            if (sharding != ShardingStrategy.DontShard)
            {
                moveToShard = null;
            }

            return new Normalized(overwrite, indexing, sharding, moveToShard);
        }

        public static bool IsIndexingLocked(bool? overwrite, bool isSimulatedRun)
            => isSimulatedRun || overwrite == false;

        public static bool IsShardingLocked(bool? overwrite, bool isSimulatedRun)
            => isSimulatedRun || overwrite == false;

        public static bool IsMoveToShardLocked(bool? overwrite, ShardingStrategy? sharding, bool isSimulatedRun)
            => isSimulatedRun || overwrite == false || sharding != ShardingStrategy.DontShard;
    }
}
