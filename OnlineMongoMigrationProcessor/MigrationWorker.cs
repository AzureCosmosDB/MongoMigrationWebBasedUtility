using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Processors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning disable CS8629
#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8625
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

namespace OnlineMongoMigrationProcessor
{


    public class MigrationWorker
    {
        private string _toolsDestinationFolder = $"{Helper.GetWorkingFolder()}mongo-tools";
        private string _toolsLaunchFolder = string.Empty;
        private bool _migrationCancelled = false;
        private JobList? _jobList;
        private MigrationJob? _job;
        private Log _log;
        private MongoClient? _sourceClient;
        private IMigrationProcessor _migrationProcessor;
        public MigrationSettings? _config;
        public bool ProcessRunning { get; set; }

        //public MigrationSettings? _config { get; set; }

        public MigrationWorker(JobList jobList)
        {            
            _log = new Log();
            _jobList = jobList;
            jobList.SetLog(_log);
        }

        public LogBucket GetLogBucket(string jobId)
        {
            // only for active job in migration worker
            if (_job.Id == jobId)
                return _log.GetCurentLogBucket(jobId);
            else
                return null;
        }

        public List<LogObject> GetVerboseMessages(string jobId)
        {
            // only for active job in migration worker
            if (_job.Id == jobId)
                return _log.GetVerboseMessages();
            else
                return null;
        }

        public string GetRunningJobId()
        {
            if (_job != null)
            {
                if (_migrationProcessor != null && _migrationProcessor.ProcessRunning)
                {
                    return _job.Id;
                }
                else
                    return string.Empty;
            }
            else
            {
                return string.Empty;
            }
        }

        public bool IsProcessRunning(string id)
        {
            if (id != null && _job!=null && id == _job.Id)
            {
                if (_migrationProcessor != null)
                    return _migrationProcessor.ProcessRunning;
                else
                    return ProcessRunning;
            }
            else
            {
                return false;
            }
        }

        public void StopMigration()
        {
            try
            {
                //if(_job!=null)
                //    _job.CurrentlyActive = false;

                _jobList?.Save();
                _migrationCancelled = true;
                _migrationProcessor?.StopProcessing();
                ProcessRunning = false;
                _migrationProcessor = null;
            }
            catch { }
        }

        public async Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, bool doBulkCopy, bool trackChangeStreams)
        {
            _job = job;
            StopMigration(); //stop any existing
            ProcessRunning = true;

            int maxRetries = 10;
            int attempts = 0;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            //encoding speacial characters
            sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(sourceConnectionString);
            targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(targetConnectionString);

            targetConnectionString = Helper.UpdateAppName(targetConnectionString, "MSFTMongoWebMigration-" + job.Id);

            LoadConfig();                      
            
            _job = job;
            _migrationCancelled = false;


            string logfile=_log.Init(_job.Id);
            if (logfile != _job.Id)
            {
                _log.WriteLine($"Error in reading _log. Orginal log backed up as {logfile}");
            }
            _log.WriteLine($"{_job.Id} Started on {_job.StartedOn} (UTC)");
            

            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(item => item.Trim())
                .ToArray();



            if (_job.UseMongoDump)
            {
                _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_log,_toolsDestinationFolder, _config);
                if (string.IsNullOrEmpty(_toolsLaunchFolder))
                {
                    //_job.CurrentlyActive = false;
                    _job.IsCompleted = false;
                    _jobList?.Save();

                    StopMigration();
                    return;
                }
            }

            bool continueProcessing = true;

            if (_job.MigrationUnits == null)
            {
                _job.MigrationUnits = new List<MigrationUnit>();
            }

            if (_job.MigrationUnits.Count == 0)
            {
                foreach (var fullName in collectionsInput)
                {
                    if (_migrationCancelled) return;

                    int firstDotIndex = fullName.IndexOf('.');
                    if (firstDotIndex <= 0 || firstDotIndex == fullName.Length - 1) continue;

                    string dbName = fullName.Substring(0, firstDotIndex).Trim();
                    string colName = fullName.Substring(firstDotIndex + 1).Trim();

                    var migrationUnit = new MigrationUnit(dbName, colName, null);
                    _job.MigrationUnits.Add(migrationUnit);
                    _jobList?.Save();
                }
            }




            while (attempts < maxRetries && !_migrationCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    _sourceClient = MongoClientFactory.Create(_log,sourceConnectionString,false, _config.CACertContentsForSourceServer);
                    _log.WriteLine("Source Client Created");
                    if (job.IsSimulatedRun)
                    {
                        _log.WriteLine("Simulated Run. No changes will be made to the target.");
                    }
                    else
                    {
                        if (job.AppendMode)
                        {
                            _log.WriteLine("Existing target collections will remain unchanged, and no indexes will be created.");
                        }
                        else
                        {
                            if (job.SkipIndexes)
                            {
                                _log.WriteLine("No indexes will be created.");
                            }
                        }
                    }
                    

                    if (_job.IsOnline)
                    {
                        _log.WriteLine("Checking if change stream is enabled on source");
                        


                        var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log,_config.CACertContentsForSourceServer,_job.SourceConnectionString, _job.MigrationUnits[0]);
                        _job.SourceServerVersion = retValue.Version;
                        _jobList?.Save();

                        if (!retValue.IsCSEnabled)
                        {
                            _job.IsCompleted = true;
                            continueProcessing = false;
                            StopMigration();
                            return;
                        }

                    }

                    _migrationProcessor?.StopProcessing(false);

                    _migrationProcessor = null;
                    if (!_job.UseMongoDump)
                    {
                        _migrationProcessor = new CopyProcessor(_log,_jobList, _job, _sourceClient, _config);
                    }
                    else
                    {
                        _migrationProcessor = new DumpRestoreProcessor(_log,_jobList, _job, _sourceClient, _config, _toolsLaunchFolder);
                    }
                    _migrationProcessor.ProcessRunning = true;


                    bool checkedCS = false;
                    foreach (var unit in _job.MigrationUnits)
                    {
                        if (_migrationCancelled) return;

                        if (await MongoHelper.CheckCollectionExists(_sourceClient, unit.DatabaseName, unit.CollectionName))
                        {
                            unit.SourceStatus = CollectionStatus.OK;

                            // set  start pointer for change stream
                            if (unit.ChangeStreamStartedOn==null || unit.ChangeStreamStartedOn==DateTime.MinValue)
                                 unit.ChangeStreamStartedOn = DateTime.UtcNow;

                            if (_job.IsOnline)
                            {
                                //if  reset CS needto get the latest CS resume token synchronously
                                if (unit.ResetChangeStream)
                                {
                                    _log.WriteLine($"Resetting Change Stream for {unit.DatabaseName}.{unit.CollectionName}. This can take upto 5 minutes");
                                    await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit);
                                }
                                else
                                {
                                    //run this job async to detect change stream resume token, if no chnage stream is detected, it will not be set and cancel in 5 minutes
                                    Task.Run(async () =>
                                    {
                                        await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit);
                                    });
                                }

                            }
                            if (unit.MigrationChunks == null || unit.MigrationChunks.Count == 0)
                            {                                

                                var chunks = await PartitionCollection(unit.DatabaseName, unit.CollectionName);

                                if(chunks.Count==0)
                                {  
                                    _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has no records to migrate", LogType.Error);
                                    unit.SourceStatus = CollectionStatus.NotFound;                                    
                                    continue;
                                }


                                _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks.Count} chunk(s)");
                                

                                unit.MigrationChunks= chunks;                                  

                                
                                if (!job.IsSimulatedRun && !job.AppendMode)
                                {
                                    var database = _sourceClient.GetDatabase(unit.DatabaseName);
                                    var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                                    await MongoHelper.DeleteAndCopyIndexesAsync(_log,targetConnectionString, collection, job.SkipIndexes);

                                    if (_job.SyncBackEnabled && !job.IsSimulatedRun && _job.IsOnline && !checkedCS)
                                    {
                                        _log.WriteLine("Sync Back: Checking if change stream is enabled on the target");
                                        

                                        //Thread.Sleep(30*1000); // Wait for 30 seconds to ensure the target is ready
                                        var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log,string.Empty,_job.TargetConnectionString, unit,true);
                                        checkedCS = true;
                                        if (!retValue.IsCSEnabled)
                                        {
                                            _job.IsCompleted = true;
                                            continueProcessing = false;
                                            StopMigration();
                                            return;
                                        }
                                    }
                                }
                            }

                        }
                        else
                        {
                            unit.SourceStatus = CollectionStatus.NotFound;
                            _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} does not exist on source or has zero records", LogType.Error);
                            
                        }
                    }

                    _jobList?.Save();
                    


                    foreach (var migrationUnit in _job.MigrationUnits)
                    {
                        if (_migrationCancelled) break;

                        if (migrationUnit.SourceStatus == CollectionStatus.OK)
                        {
                            if (await MongoHelper.CheckCollectionExists(_sourceClient, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                            {
                                MongoClient targetClient = null;
                                if (!_job.IsSimulatedRun)
                                { 
                                    targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                                    if (await MongoHelper.CheckCollectionExists(targetClient, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                                    {
                                        if (!_job.CSPostProcessingStarted)
                                        {
                                            _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} already exists on the target.");
                                        }
                                    }
                                }
                                if (_migrationProcessor != null)
                                {
                                    _migrationProcessor.StartProcess(migrationUnit, sourceConnectionString, targetConnectionString);

                                    // since CS processsing has started, we can break the loop. No need to process all collections
                                    if (_job.IsOnline && _job.SyncBackEnabled && _job.CSPostProcessingStarted && Helper.IsOfflineJobCompleted(_job))
                                    {
                                        continueProcessing = false;
                                        break;
                                    }
                                }

							}
                            else
                            {
                                migrationUnit.SourceStatus = CollectionStatus.NotFound;
                                _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source or has zero records", LogType.Error);
                                
                            }
                        }
                    }

                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    _log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Attempt {attempts} failed: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);

                }
            }
            if (attempts == maxRetries)
            {
                _log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                
                continueProcessing = false;
                StopMigration();

            }
        }

        public void LoadConfig()
        {
            if (_config == null)
                _config = new MigrationSettings();
             _config.Load();
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            _job = job;
            //StopMigration(); //stop any existing
            ProcessRunning = true;

            LoadConfig();

            TimeSpan backoff = TimeSpan.FromSeconds(2);
            bool continueProcessing = true;

            _migrationCancelled = false;

            if(_log==null)
                _log = new Log();
            string logfile = _log.Init(_job.Id);

            _log.WriteLine($"Sync Back: {_job.Id} started on {_job.StartedOn} (UTC)");
            

            job.ProcessingSyncBack = true;
            _jobList.Save();

            if (_migrationProcessor != null)
                _migrationProcessor.StopProcessing();

            _migrationProcessor = null;
            _migrationProcessor = new SyncBackProcessor(_log,_jobList, _job, null, _config, string.Empty);
            _migrationProcessor.ProcessRunning = true;
            _migrationProcessor.StartProcess(null, sourceConnectionString, targetConnectionString);
            
        }

        private async Task<List<MigrationChunk>> PartitionCollection(string databaseName, string collectionName, string idField = "_id")
        {

            var stas = await MongoHelper.GetCollectionStatsAsync(_sourceClient, databaseName, collectionName);

            long documentCount = stas.DocumentCount;
            long totalCollectionSizeBytes = stas.CollectionSizeBytes;

            var database = _sourceClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            int totalChunks = 0;
            long minDocsInChunk = 0;

            long targetChunkSizeBytes = _config.ChunkSizeInMb * 1024 * 1024;
            var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);


            if (_job.UseMongoDump)
            {
                totalChunks = totalChunksBySize;
                minDocsInChunk = documentCount / totalChunks;
                _log.WriteLine($"{databaseName}.{collectionName} Storage Size: {totalCollectionSizeBytes}");
            }
            else
            {
                _log.WriteLine($"{databaseName}.{collectionName} Estimated Document Count: {documentCount}");
                totalChunks = (int)Math.Min(SamplePartitioner.MaxSamples / SamplePartitioner.MaxSegments, documentCount / SamplePartitioner.MaxSamples);
                totalChunks = Math.Max(1, totalChunks); // At least one chunk
                totalChunks = Math.Max(totalChunks, totalChunksBySize);
                minDocsInChunk = documentCount / totalChunks;
            }

            List<MigrationChunk> migrationChunks = new List<MigrationChunk>();

            if (totalChunks > 1 || !_job.UseMongoDump)
            {
                _log.WriteLine($"Chunking {databaseName}.{collectionName}");
                


                List<DataType> dataTypes = new List<DataType> { DataType.Int, DataType.Int64, DataType.String, DataType.Object, DataType.Decimal128, DataType.Date, DataType.ObjectId };

                if (_config.ReadBinary)
                {
                    dataTypes.Add(DataType.Binary);
                }

                foreach (var dataType in dataTypes)
                {
                    long docCountByType;
                    ChunkBoundaries chunkBoundaries = SamplePartitioner.CreatePartitions(_log,_job.UseMongoDump, collection, idField, totalChunks, dataType, minDocsInChunk, out docCountByType);

                    if (docCountByType == 0)
                    {
                        continue;
                    }

                    if (chunkBoundaries == null)
                    {
                        if (_job.UseMongoDump)
                        {
                            continue;
                        }
                        else
                        {
                            var min = BsonNull.Value;
                            var max = BsonNull.Value;

                            var chunkBoundary = new Boundary
                            {
                                StartId = min,
                                EndId = max,
                                SegmentBoundaries = new List<Boundary>()
                            };

                            chunkBoundaries.Boundaries ??= new List<Boundary>();
                            chunkBoundaries.Boundaries.Add(chunkBoundary);
                            var segmentBoundary = new Boundary
                            {
                                StartId = min,
                                EndId = max
                            };
                            chunkBoundary.SegmentBoundaries.Add(segmentBoundary);
                        }
                    }

                    for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
                    {
                        var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i);
                        var chunk = new MigrationChunk(startId, endId, dataType, false, false);
                        migrationChunks.Add(chunk);

                        if (!_job.UseMongoDump && (chunkBoundaries.Boundaries[i].SegmentBoundaries == null || chunkBoundaries.Boundaries[i].SegmentBoundaries.Count == 0))
                        {
                            chunk.Segments ??= new List<Segment>();
                            chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false, Id = "1" });
                        }

                        if (!_job.UseMongoDump && chunkBoundaries.Boundaries[i].SegmentBoundaries.Count > 0)
                        {
                            for (int j = 0; j < chunkBoundaries.Boundaries[i].SegmentBoundaries.Count; j++)
                            {
                                var segment = chunkBoundaries.Boundaries[i].SegmentBoundaries[j];
                                var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, chunkBoundaries.Boundaries[i].SegmentBoundaries.Count, j, chunk.Lt, chunk.Gte);

                                chunk.Segments ??= new List<Segment>();
                                chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false, Id = (j + 1).ToString() });
                            }
                        }
                    }
                }
            }
            else
            {
                var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.String, false, false);
                migrationChunks.Add(chunk);
            }

            return migrationChunks;
        }

        private Tuple<string, string> GetStartEnd(bool isChunk, Boundary boundary, int totalBoundaries, int currentIndex, string chunkLt = "", string chunkGte = "")
        {
            string startId;
            string endId;

            if (currentIndex == 0)
            {
                startId = isChunk ? "" : chunkGte;
                endId = boundary.EndId?.ToString() ?? "";
            }
            else if (currentIndex == totalBoundaries - 1)
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = isChunk ? "" : chunkLt;
            }
            else
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = boundary.EndId?.ToString() ?? "";
            }

            return Tuple.Create(startId, endId);
        }

    }
}
