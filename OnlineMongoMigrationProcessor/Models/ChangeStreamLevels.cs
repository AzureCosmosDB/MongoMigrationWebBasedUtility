using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    public enum ChangeStreamLevel
    {
        Collection = 0,
        Server = 1
    }

    /// <summary>
    /// Records a user / system action affecting change-stream state that is queued for the
    /// next change-stream worker bootstrap. Set at the user-action site (UI handlers,
    /// scope-transition save) or by <see cref="Helpers.ChangeStreamTransitionHelper"/>
    /// reset helpers, and cleared once the new worker has bootstrapped and emitted the
    /// contextual warning. Persisted on <see cref="MigrationJob"/> so the queued action
    /// survives an application restart and can be resumed automatically.
    /// </summary>
    public enum PendingChangeStreamAction
    {
        None = 0,
        CollectionToServer = 1,
        ServerToCollection = 2,
        SyncBackEnabled = 3,
        ForwardSyncEnabled = 4,
        Cutover = 5
    }
}