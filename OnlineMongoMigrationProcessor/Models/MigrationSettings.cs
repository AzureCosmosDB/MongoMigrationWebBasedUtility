using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Context;
using System;
using System.IO;

namespace OnlineMongoMigrationProcessor
{
    public class MigrationSettings : ICloneable
    {
      
        public string? CACertContentsForSourceServer { get; set; }
        public string? MongoToolsDownloadUrl { get; set; }
        public bool ReadBinary { get; set; }
        public int LogPageSize { get; set; }
        public long ChunkSizeInMb { get; set; }
        public int ChangeStreamMaxDocsInBatch { get; set; }
		public int ChangeStreamBatchDuration { get; set; }
        public int ChangeStreamBatchDurationMin { get; set; }
        public int ChangeStreamMaxCollsInBatch { get; set; }
		public int MongoCopyPageSize { get; set; }
        public int CompareSampleSize { get; set; }
        public PartitionerType ObjectIdPartitioner { get; set; }
        
        private string _filePath = string.Empty;

        public MigrationSettings()
        {
            _filePath = $"migrationjobs\\config.json";
        }

        public object Clone()
        {
            // Deep clone using JSON serialization
            var json = JsonConvert.SerializeObject(this);
            return JsonConvert.DeserializeObject<MigrationSettings>(json) ?? new MigrationSettings();
        }
   
        public void Load()
        {
            bool initialized = false;
            if (MigrationJobContext.Store.DocumentExists(_filePath))
            {
                //string json = File.ReadAllText(_filePath);
                string json= MigrationJobContext.Store.ReadDocument(_filePath);
                var loadedObject = JsonConvert.DeserializeObject<MigrationSettings>(json);
                if (loadedObject != null)
                {
                    ReadBinary = loadedObject.ReadBinary;
                    MongoToolsDownloadUrl = loadedObject.MongoToolsDownloadUrl;
                    ChunkSizeInMb = loadedObject.ChunkSizeInMb;
					ChangeStreamMaxDocsInBatch = loadedObject.ChangeStreamMaxDocsInBatch == 0 ? 10000 : loadedObject.ChangeStreamMaxDocsInBatch;
					ChangeStreamBatchDuration = loadedObject.ChangeStreamBatchDuration == 0 ? 120 : loadedObject.ChangeStreamBatchDuration;
                    ChangeStreamBatchDurationMin = loadedObject.ChangeStreamBatchDurationMin == 0 ? 30 : loadedObject.ChangeStreamBatchDurationMin;
                    ChangeStreamMaxCollsInBatch = loadedObject.ChangeStreamMaxCollsInBatch == 0 ? 5 : loadedObject.ChangeStreamMaxCollsInBatch;
                    MongoCopyPageSize = loadedObject.MongoCopyPageSize;
                    CompareSampleSize = loadedObject.CompareSampleSize == 0 ? 50 : loadedObject.CompareSampleSize;
                    LogPageSize = loadedObject.LogPageSize == 0 ? 5000 : loadedObject.LogPageSize;
                    CACertContentsForSourceServer = loadedObject.CACertContentsForSourceServer;
                    ObjectIdPartitioner = loadedObject.ObjectIdPartitioner;
                    
                    initialized = true;
                    if (ChangeStreamMaxDocsInBatch > 10000)
                        ChangeStreamMaxDocsInBatch = 10000;
                    if (ChangeStreamBatchDuration < 20)
                        ChangeStreamBatchDuration = 120;
                    if (LogPageSize < 1000)
                        LogPageSize = 1000;
                    if (LogPageSize > 100000)
                        LogPageSize = 100000;
                }
            }
            if (!initialized)
            {
                ReadBinary = false;
                MongoToolsDownloadUrl = "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-windows-x86_64-100.10.0.zip";
                ChunkSizeInMb = 512;
				MongoCopyPageSize = 500;
				ChangeStreamMaxDocsInBatch = 10000;
                ChangeStreamBatchDuration = 120;
                ChangeStreamBatchDurationMin = 30;
                ChangeStreamMaxCollsInBatch = 5;
                CACertContentsForSourceServer = string.Empty;
                CompareSampleSize = 50;
                LogPageSize = 5000;
                ObjectIdPartitioner = PartitionerType.UseTimeBoundaries;
            }
        }

        public bool Save(out string errorMessage)
        {
            try
            {
                string json = JsonConvert.SerializeObject(this);
                MigrationJobContext.Store.UpsertDocument(_filePath, json);

                //File.WriteAllText(_filePath, json);
                errorMessage=string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                errorMessage= $"Error saving data: {ex}";    
                return false;
            }
        }

        
    }
}