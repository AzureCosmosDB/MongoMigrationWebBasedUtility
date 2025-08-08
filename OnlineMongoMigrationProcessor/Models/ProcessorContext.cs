using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    internal class ProcessorContext
    {
        public required MigrationUnit Item { get; set; }
        public required string SourceConnectionString { get; set; }
        public required string TargetConnectionString { get; set; }
        public required string JobId { get; set; }
        public required string DatabaseName { get; set; }
        public required string CollectionName { get; set; }
        public required IMongoDatabase Database { get; set; }
        public required IMongoCollection<BsonDocument> Collection { get; set; }
        public long DownloadCount { get; set; }

    }
}
