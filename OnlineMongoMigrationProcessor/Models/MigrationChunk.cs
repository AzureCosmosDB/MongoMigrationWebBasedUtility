using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor
{
    public class MigrationChunk
    {
        public string? Lt { get; set; }
        public string? Gte { get; set; }
        public bool? IsDownloaded { get; set; }
        public bool? IsUploaded { get; set; }
        public long DumpQueryDocCount { get; set; }
        public long DumpResultDocCount { get; set; }
        public long RestoredSuccessDocCount { get; set; }
        public long RestoredFailedDocCount { get; set; }
        public long DocCountInTarget { get; set; }
        public long SkippedAsDuplicateCount { get; set; }
        public DataType DataType { get; set; }
        public List<Segment> Segments { get; set; } = new();
        public string RUPartitionResumeToken { get; set; } = string.Empty;
        public string RUStopToken { get; set; } = string.Empty;
        public string Id { get; set; } = string.Empty;

        public MigrationChunk() { } 

        public MigrationChunk(string startId, string endId, DataType dataType, bool? downloaded, bool? uploaded)
        {
            Gte = startId;
            Lt = endId;
            IsDownloaded = downloaded;
            IsUploaded = uploaded;
            DataType = dataType;
        }

        public MigrationChunk(string id, string partitonKey_RU, string stopToken_RU)
        {
            Id = id;
            RUStopToken = stopToken_RU;
            RUPartitionResumeToken = partitonKey_RU;
            IsUploaded = false;
            IsDownloaded = false;
        }
    }
}