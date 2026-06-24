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
    /// Records the most recent user / system action that affected change stream state.
    /// Consumed by <see cref="Helpers.ChangeStreamTransitionHelper"/> to produce accurate
    /// bootstrap log messages and avoid claiming a scope transition when none occurred.
    /// </summary>
    public enum ChangeStreamAction
    {
        None = 0,
        CollectionToServer = 1,
        ServerToCollection = 2,
        SyncBackEnabled = 3,
        ForwardSyncEnabled = 4
    }
}