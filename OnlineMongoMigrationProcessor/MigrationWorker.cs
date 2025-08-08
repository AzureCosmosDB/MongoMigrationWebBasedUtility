using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Partitioner;
using OnlineMongoMigrationProcessor.Processors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace OnlineMongoMigrationProcessor
{


    public class MigrationWorker
    {
        public bool ProcessRunning { get; set; }

        private string _toolsDestinationFolder = $"{Helper.GetWorkingFolder()}mongo-tools";
        private string _toolsLaunchFolder = string.Empty;
        private bool _migrationCancelled = false;
    private JobList _jobList;
        private MigrationJob? _job;
        private Log _log;
        private MongoClient? _sourceClient;
    private IMigrationProcessor? _migrationProcessor;
    public MigrationSettings? _config;
        
    private CancellationTokenSource? _compare_cts;

        public MigrationWorker(JobList jobList)
        {            
            _log = new Log();
            _jobList = jobList;
            jobList.SetLog(_log);
        }

        public LogBucket? GetLogBucket(string jobId)
        {
            // only for active job in migration worker
            if (_job != null && _job.Id == jobId)
                return _log.GetCurentLogBucket(jobId);
            else
                return null;
        }

        public List<LogObject>? GetVerboseMessages(string jobId)
        {
            // only for active job in migration worker
            if (_job != null && _job.Id == jobId)
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
                    return _job?.Id ?? string.Empty;
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
                _compare_cts?.Cancel();
                _jobList.Save();
                _migrationCancelled = true;
                _migrationProcessor?.StopProcessing();
                ProcessRunning = false;
                _migrationProcessor = null;
            }
            catch { }
        }

        private async Task<TaskResult> PrepareForMigration()
        {
            if (_job == null)
                return TaskResult.FailedAfterRetries;
            if (_config == null)
                _config = new MigrationSettings();

            if (string.IsNullOrWhiteSpace(_job.SourceConnectionString))
                return TaskResult.FailedAfterRetries;
            _sourceClient = MongoClientFactory.Create(_log, _job.SourceConnectionString!, false, _config.CACertContentsForSourceServer ?? string.Empty);
            _log.WriteLine("Source Client Created");
            if (_job.IsSimulatedRun)
            {
                _log.WriteLine("Simulated Run. No changes will be made to the target.");
            }
            else
            {
                if (_job.AppendMode)
                {
                    _log.WriteLine("Existing target collections will remain unchanged, and no indexes will be created.");
                }
                else
                {
                    if (_job.SkipIndexes)
                    {
                        _log.WriteLine("No indexes will be created.");
                    }
                }
            }


            if (_job.IsOnline)
            {
                _log.WriteLine("Checking if change stream is enabled on source");

                if (_job.MigrationUnits == null || _job.MigrationUnits.Count == 0)
                    return TaskResult.FailedAfterRetries;
                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, _config.CACertContentsForSourceServer ?? string.Empty, _job.SourceConnectionString!, _job.MigrationUnits[0]);
                _job.SourceServerVersion = retValue.Version;
                _jobList.Save();

                if (!retValue.IsCSEnabled)
                {
                    _job.IsCompleted = true;
                    StopMigration();
                    return TaskResult.Abort;
                }

            }

        _migrationProcessor?.StopProcessing(false);

        _migrationProcessor = null;
            switch (_job.JobType)
            {
                case JobType.MongoDriver:
            _migrationProcessor = new CopyProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    break;
                case JobType.DumpAndRestore:
            _migrationProcessor = new DumpRestoreProcessor(_log, _jobList, _job, _sourceClient!, _config, _toolsLaunchFolder);
                    break;
                case JobType.RUOptimizedCopy:
            _migrationProcessor = new RUCopyProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    break;
                default:
                    _log.WriteLine($"Unknown JobType: {_job.JobType}. Defaulting to MongoDriver.", LogType.Error);
            _migrationProcessor = new CopyProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    break;
            }
            _migrationProcessor.ProcessRunning = true;

            return TaskResult.Success;
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> Default_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            _log.WriteLine($"{processName} attempt failed. Retrying in {currentBackoff} seconds...");
            return Task.FromResult(TaskResult.Retry);
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> MigrateCollections_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            if(ex is OperationCanceledException)
            {
                _log.WriteLine($"{processName} operation was cancelled.");
                return Task.FromResult(TaskResult.Canceled);
			}
			if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed due to timeout: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);
            }

            _log.WriteLine($"Retrying in {currentBackoff} seconds...", LogType.Error);            
            return Task.FromResult(TaskResult.Retry);
        }

        private async Task<TaskResult> PreparePartitions()
        {
            bool checkedCS = false;
            if (_job == null || _sourceClient == null)
                return TaskResult.FailedAfterRetries;

            var unitsForPrep = _job.MigrationUnits ?? new List<MigrationUnit>();
            foreach (var unit in unitsForPrep)
            {
                if (_migrationCancelled) return TaskResult.Abort;

                if (await MongoHelper.CheckCollectionExists(_sourceClient!, unit.DatabaseName, unit.CollectionName))
                {
                    unit.SourceStatus = CollectionStatus.OK;
                   

                    DateTime currrentTime= DateTime.UtcNow;

                    if (_job.IsOnline && unit.ResetChangeStream)
                    {
                        //if  reset CS needto get the latest CS resume token synchronously
                        _log.WriteLine($"Resetting Change Stream for {unit.DatabaseName}.{unit.CollectionName}. This can take upto 5 minutes");
                        await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit);
                    }

                    if (unit.MigrationChunks == null || unit.MigrationChunks.Count == 0)
                    {
                        List<MigrationChunk>? chunks=null;

                        if (_job.JobType == JobType.RUOptimizedCopy)
                        {
                            chunks= new RUPartitioner().CreatePartitions(_log, _sourceClient! , unit.DatabaseName, unit.CollectionName);
                        }
                        else
                        { 
                            chunks = await PartitionCollection(unit.DatabaseName, unit.CollectionName);
                        
                            if (chunks.Count == 0)
                            {
                                _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has no records to migrate", LogType.Error);
                                unit.SourceStatus = CollectionStatus.NotFound;
                                continue;
                            }
                        }

                        if (!_job.IsSimulatedRun && !_job.AppendMode)
                        {
                            var database = _sourceClient!.GetDatabase(unit.DatabaseName);
                            var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                            if (string.IsNullOrWhiteSpace(_job.TargetConnectionString))
                                return TaskResult.FailedAfterRetries;
                            var result=await MongoHelper.DeleteAndCopyIndexesAsync(_log,unit, _job.TargetConnectionString!, collection, _job.SkipIndexes);

                            if (!result)
                            {
                                return TaskResult.Retry;
                            }
                            _jobList.Save();
                            if (_job.SyncBackEnabled && !_job.IsSimulatedRun && _job.IsOnline && !checkedCS)
                            {
                                _log.WriteLine("Sync Back: Checking if change stream is enabled on target");

                                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, string.Empty, _job.TargetConnectionString, unit, true);
                                checkedCS = true;
                                if (!retValue.IsCSEnabled)
                                {
                                    return TaskResult.Abort;
                                }
                            }                            

                        }

                        if (_job.IsOnline)
                        {
                            //run this job async to detect change stream resume token, if no chnage stream is detected, it will not be set and cancel in 5 minutes
                            _ = Task.Run(async () =>
                            {
                                await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient!, _jobList, _job, unit);
                            });

                        }
                        
                        _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks!.Count} chunk(s)");                        
                        unit.MigrationChunks = chunks!;
                       
                        unit.ChangeStreamStartedOn = currrentTime;

                    }

                }
                else
                {
                    unit.SourceStatus = CollectionStatus.NotFound;
                    _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} does not exist on source or has zero records", LogType.Error);
                }
            }

            _jobList.Save();
            return TaskResult.Success;
        }

        private async Task<TaskResult> MigrateJobCollections()
        {
            if (_job == null)
                return TaskResult.FailedAfterRetries;

            var unitsForMigrate = _job.MigrationUnits ?? new List<MigrationUnit>();
            foreach (var migrationUnit in unitsForMigrate)
            {
                if (_migrationCancelled) break;

                if (migrationUnit.SourceStatus == CollectionStatus.OK)
                {
                    if (await MongoHelper.CheckCollectionExists(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                    {
                        MongoClient? targetClient = null;
                        if (!_job.IsSimulatedRun)
                        {
                            if (string.IsNullOrWhiteSpace(_job.TargetConnectionString))
                                return TaskResult.FailedAfterRetries;
                            targetClient = MongoClientFactory.Create(_log, _job.TargetConnectionString!);

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
                            if (string.IsNullOrWhiteSpace(_job.SourceConnectionString) || string.IsNullOrWhiteSpace(_job.TargetConnectionString))
                                return TaskResult.FailedAfterRetries;
                            await _migrationProcessor.StartProcessAsync(migrationUnit, _job.SourceConnectionString!, _job.TargetConnectionString!);

                            // since CS processsing has started, we can break the loop. No need to process all collections
                            if (_job.IsOnline && _job.SyncBackEnabled && _job.CSPostProcessingStarted && Helper.IsOfflineJobCompleted(_job))
                            {
                                return TaskResult.Success;
                            }
                        }
                        else
                        {
                            migrationUnit.SourceStatus = CollectionStatus.NotFound;
                            _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source or has zero records", LogType.Error);

                        }
                    }
                }                
            }
            return TaskResult.Success;
        }

        private void PopulateJobCollections(string namespacesToMigrate)
        {
            var job = _job;
            if (job == null)
            {
                return;
            }
            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(mu => mu.Trim())
                .ToArray();

            // Ensure the list is initialized before accessing it
            if (job.MigrationUnits == null)
            {
                job.MigrationUnits = new List<MigrationUnit>();
            }

            if (job.MigrationUnits.Count == 0)
            {
                foreach (var fullName in collectionsInput)
                {
                    if (_migrationCancelled) return;

                    int firstDotIndex = fullName.IndexOf('.');
                    if (firstDotIndex <= 0 || firstDotIndex == fullName.Length - 1) continue;

                    string dbName = fullName.Substring(0, firstDotIndex).Trim();
                    string colName = fullName.Substring(firstDotIndex + 1).Trim();

                    var migrationUnit = new MigrationUnit(dbName, colName, new List<MigrationChunk>());
                    job.MigrationUnits.Add(migrationUnit);
                    _jobList.Save();
                }
            }
        }

        public async Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, OnlineMongoMigrationProcessor.Models.JobType jobtype, bool trackChangeStreams)
        {
            _job = job;
            StopMigration(); //stop any existing
            ProcessRunning = true;


            //encoding speacial characters
            sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(sourceConnectionString);
            targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(targetConnectionString);

            targetConnectionString = Helper.UpdateAppName(targetConnectionString, $"MSFTMongoWebMigration{_job.IsOnline}-" + job.Id);

            _job.TargetConnectionString = targetConnectionString;
            _job.SourceConnectionString = sourceConnectionString;

            LoadConfig();

            _migrationCancelled = false;


            if (string.IsNullOrWhiteSpace(_job.Id)) _job.Id = Guid.NewGuid().ToString("N");
            string logfile = _log.Init(_job.Id);
            if (logfile != _job.Id)
            {
                _log.WriteLine($"Error in reading _log. Orginal log backed up as {logfile}");
            }
            _log.WriteLine($"{_job.Id} Started on {_job.StartedOn} (UTC)");


            if (_job.MigrationUnits == null)
            {
                _job.MigrationUnits = new List<MigrationUnit>();
            }
            PopulateJobCollections(namespacesToMigrate);

			if (_job.JobType==JobType.DumpAndRestore)
			{
                _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_log, _toolsDestinationFolder, _config!);
				if (string.IsNullOrEmpty(_toolsLaunchFolder))
				{
					StopMigration();
					return;
				}
			}


			TaskResult result = await new RetryHelper().ExecuteTask(
                () => PrepareForMigration(),
                (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                    ex, attemptCount,
                    "Preperation step", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                StopMigration();
                return;
            }



            result = await new RetryHelper().ExecuteTask(
                () => PreparePartitions(),
                (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                    ex, attemptCount,
                    "Partition step", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                StopMigration();
                return;
            }
          

            //if run comparison is set by customer.
            if (_job.RunComparison)
            { 
                var compareHelper = new ComparisonHelper();
                _compare_cts = new CancellationTokenSource();
                await compareHelper.CompareRandomDocumentsAsync(_log, _jobList, _job, _config!, _compare_cts.Token);
                compareHelper = null;
                _job.RunComparison = false;
                _jobList.Save();

                _log.WriteLine("Resuming migration.");
            }
            
            result = await new RetryHelper().ExecuteTask(
                () => MigrateJobCollections(),
                (ex, attemptCount, currentBackoff) => MigrateCollections_ExceptionHandler(
                    ex, attemptCount,
                    "Migrate collections", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                StopMigration();
                return;
            }

        }

        private void LoadConfig()
        {
            if (_config == null)
                _config = new MigrationSettings();
             _config.Load();
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            _job = job;

            ProcessRunning = true;

            LoadConfig();

            if(_log==null)
                _log = new Log();
            if (string.IsNullOrWhiteSpace(_job.Id)) _job.Id = Guid.NewGuid().ToString("N");
            string logfile = _log.Init(_job.Id);

            _log.WriteLine($"Sync Back: {_job.Id} started on {_job.StartedOn} (UTC)");
            
            job.ProcessingSyncBack = true;
            _jobList.Save();

            if (_migrationProcessor != null)
                _migrationProcessor.StopProcessing();

            _migrationProcessor = null;
            var dummySourceClient = MongoClientFactory.Create(_log, sourceConnectionString);
            _migrationProcessor = new SyncBackProcessor(_log,_jobList, _job, dummySourceClient, _config!, string.Empty);
            _migrationProcessor.ProcessRunning = true;
            var dummyUnit = new MigrationUnit("","", new List<MigrationChunk>());
            _migrationProcessor.StartProcessAsync(dummyUnit, sourceConnectionString, targetConnectionString).GetAwaiter().GetResult();
            
        }

        private async Task<List<MigrationChunk>> PartitionCollection(string databaseName, string collectionName, string idField = "_id")
        {

            if (_sourceClient == null || _config == null || _job == null)
                throw new InvalidOperationException("Worker not initialized");

            var stats = await MongoHelper.GetCollectionStatsAsync(_sourceClient!, databaseName, collectionName);

            long documentCount = stats.DocumentCount;
            long totalCollectionSizeBytes = stats.CollectionSizeBytes;

            var database = _sourceClient!.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            int totalChunks = 0;
            long minDocsInChunk = 0;

            long targetChunkSizeBytes = _config.ChunkSizeInMb * 1024 * 1024;
            var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);


            if (_job.JobType == JobType.DumpAndRestore)
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

            if (totalChunks > 1 || _job.JobType != JobType.DumpAndRestore)
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
                    ChunkBoundaries chunkBoundaries = SamplePartitioner.CreatePartitions(_log, _job.JobType == JobType.DumpAndRestore, collection, idField, totalChunks, dataType, minDocsInChunk, out docCountByType);

                    if (docCountByType == 0 || _job.JobType == JobType.DumpAndRestore) continue;

                    if (chunkBoundaries == null)
                    {
                        continue;
                    }
                    ProcessSegmentBoundaries(chunkBoundaries);
                    CreateSegments(chunkBoundaries, migrationChunks, dataType);
                }
            }
            else
            {
                var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.String, false, false);
                migrationChunks.Add(chunk);
            }

            return migrationChunks;
        }

        private void ProcessSegmentBoundaries(ChunkBoundaries chunkBoundaries)
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

        private void CreateSegments(ChunkBoundaries chunkBoundaries, List<MigrationChunk> migrationChunks, DataType dataType)
        {
            for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
            {
                var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i);
                var chunk = new MigrationChunk(startId, endId, dataType, false, false);
                migrationChunks.Add(chunk);

                var boundary = chunkBoundaries.Boundaries[i];
                if (_job != null && _job.JobType == JobType.MongoDriver && (boundary.SegmentBoundaries == null || boundary.SegmentBoundaries.Count == 0))
                {
                    chunk.Segments ??= new List<Segment>();
                    chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false, Id = "1" });
                }

        if (_job!.JobType == JobType.MongoDriver && boundary.SegmentBoundaries != null && boundary.SegmentBoundaries.Count > 0)
                {
                    for (int j = 0; j < boundary.SegmentBoundaries.Count; j++)
                    {
                        var segment = boundary.SegmentBoundaries[j];
            var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, boundary.SegmentBoundaries.Count, j, chunk.Lt ?? string.Empty, chunk.Gte ?? string.Empty);

                        chunk.Segments ??= new List<Segment>();
                        chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false, Id = (j + 1).ToString() });
                    }
                }
            }
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
