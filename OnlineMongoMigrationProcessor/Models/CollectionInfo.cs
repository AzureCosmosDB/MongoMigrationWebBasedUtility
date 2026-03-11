using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    public class CollectionInfo
    {
        public required string CollectionName { get; set; }
        public required string DatabaseName { get; set; }
        public string? TargetCollectionName { get; set; }
        public string? TargetDatabaseName { get; set; }
        public string? Filter { get; set; }
        public string? DataTypeFor_Id { get; set; }
    }
}
