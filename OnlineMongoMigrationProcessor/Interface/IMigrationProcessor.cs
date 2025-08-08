using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor
{
    public interface IMigrationProcessor
    {

        // Methods
        void StopProcessing(bool updateStatus=true);
        Task StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id");
        bool ProcessRunning { get; set; }

    }
}
