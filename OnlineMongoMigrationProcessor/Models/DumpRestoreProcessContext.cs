using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    public class DumpRestoreProcessContext
    {
        public string Id { get; set; } = string.Empty; // Format: "{MU.Id}_{ChunkIndex}"
        public string ProcessorRunId { get; set; } = string.Empty;
        public string MigrationUnitId { get; set; } = string.Empty;
        public int ChunkIndex { get; set; }
        public ProcessState State { get; set; }
        public DateTime QueuedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public int RetryCount { get; set; }
        public Exception? LastError { get; set; }
        public string SourceConnectionString { get; set; } = string.Empty;
        public string TargetConnectionString { get; set; } = string.Empty;
    }
}
