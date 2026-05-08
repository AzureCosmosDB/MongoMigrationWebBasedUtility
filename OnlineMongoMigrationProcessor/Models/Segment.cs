namespace OnlineMongoMigrationProcessor
{
    public class Segment
    {
        public string Lt { get; set; } = string.Empty;
        public string Lte { get; set; } = string.Empty;
        public string Gte { get; set; } = string.Empty;
        public bool? IsProcessed { get; set; }
        public long QueryDocCount { get; set; }
        public long ResultDocCount { get; set; }
        public string Id { get; set; } = string.Empty;
    }
}