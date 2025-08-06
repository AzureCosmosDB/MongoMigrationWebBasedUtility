using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    internal class CopyProcessContext
    {
        public MigrationUnit Item { get; set; }
        public string SourceConnectionString { get; set; }
        public string TargetConnectionString { get; set; }
        public string JobId { get; set; }
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public int MaxRetries { get; set; }
        public IMongoDatabase Database { get; set; }
        public IMongoCollection<BsonDocument> Collection { get; set; }
        public DateTime MigrationJobStartTime { get; set; }
        public long DownloadCount { get; set; }


    }
}
