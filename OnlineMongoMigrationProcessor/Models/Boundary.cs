using MongoDB.Bson;
using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor
{
    public class Boundary
    {
        public BsonValue? StartId { get; set; }
        public BsonValue? EndId { get; set; }
    public List<Boundary> SegmentBoundaries { get; set; } = new();
    }
}