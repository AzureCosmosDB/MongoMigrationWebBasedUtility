using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.IO;

namespace OnlineMongoMigrationProcessor
{
    public class MigrationSettings : ICloneable
    {
      
        public string? CACertContentsForSourceServer { get; set; }
        public string? MongoToolsDownloadUrl { get; set; }
        public bool ReadBinary { get; set; }
        public long ChunkSizeInMb { get; set; }
        public int ChangeStreamMaxDocsInBatch { get; set; }
		public int ChangeStreamBatchDuration { get; set; }
        public int ChangeStreamBatchDurationMin { get; set; }
        public int ChangeStreamMaxCollsInBatch { get; set; }
		public int MongoCopyPageSize { get; set; }
        public int CompareSampleSize { get; set; }
        private string _filePath = string.Empty;

        public MigrationSettings()
        {
            _filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\config.json";
        }

        public object Clone()
        {
            // Deep clone using JSON serialization
            var json = JsonConvert.SerializeObject(this);
            return JsonConvert.DeserializeObject<MigrationSettings>(json);
        }
   
        public void Load()
        {
            bool initialized = false;
            if (File.Exists(_filePath))
            {
                string json = File.ReadAllText(_filePath);
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
                    CACertContentsForSourceServer = loadedObject.CACertContentsForSourceServer;
                    initialized = true;
                    if (ChangeStreamMaxDocsInBatch > 10000)
                        ChangeStreamMaxDocsInBatch = 10000;
                    if (ChangeStreamBatchDuration < 30)
                        ChangeStreamBatchDuration = 120;
                }
            }
            if (!initialized)
            {
                ReadBinary = false;
                MongoToolsDownloadUrl = "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-windows-x86_64-100.10.0.zip";
                ChunkSizeInMb = 5120;
				MongoCopyPageSize = 500;
				ChangeStreamMaxDocsInBatch = 10000;
                ChangeStreamBatchDuration = 120;
                ChangeStreamBatchDurationMin = 30;
                ChangeStreamMaxCollsInBatch = 5;
                CACertContentsForSourceServer = string.Empty;
                CompareSampleSize = 50;
            }
        }

        public bool Save(out string errorMessage)
        {
            try
            {
                string json = JsonConvert.SerializeObject(this);
                File.WriteAllText(_filePath, json);
                errorMessage=string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                errorMessage= $"Error saving data: {ex.ToString()}";    
                return false;
            }
        }

        
    }
}