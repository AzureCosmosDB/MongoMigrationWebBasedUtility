using MongoDB.Bson;
using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor
{
    public class Boundary
    {
        public long LSN { get; set; }
        public string? Rid { get; set; }
        public BsonValue? StartId { get; set; }
        public BsonValue? EndId { get; set; }
        public List<Boundary> SegmentBoundaries { get; set; } = new();
    }
}