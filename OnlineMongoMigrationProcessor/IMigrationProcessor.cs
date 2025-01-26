using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor
{
    public interface IMigrationProcessor
    {
        // Properties
        bool ProcessRunning { get; set; }

        // Methods
        void StopProcessing();
        void Migrate(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id");        
        //void Upload(MigrationUnit item, string targetConnectionString);
    }
}
