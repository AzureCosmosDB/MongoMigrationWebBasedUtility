using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8629
#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8625

    public class MigrationWorker
    {
        private string _toolsDestinationFolder = $"{Helper.GetWorkingFolder()}mongo-tools";
        private string _toolsLaunchFolder = string.Empty;
        private bool _migrationCancelled = false;
        private JobList? _jobs;
        private MigrationJob? _job;
        private MongoClient? _sourceClient;
        private IMigrationProcessor _migrationProcessor;

        private MongoChangeStreamProcessor _syncBackToSource;

        public MigrationSettings? Config { get; set; }
        public string? CurrentJobId { get; set; }

        public MigrationWorker(JobList jobs)
        {
            _jobs = jobs;
        }

        public bool IsProcessRunning()
        {
            if (Config == null)
            {
                Config = new MigrationSettings();
                Config.Load();
            }

            return _migrationProcessor?.ProcessRunning ?? false;
        }

        public void StopMigration()
        {           
            _migrationCancelled = true;
            _migrationProcessor?.StopProcessing();
            _migrationProcessor.ProcessRunning = false;
            _migrationProcessor = null;

            if(_syncBackToSource!= null)
            {
                _syncBackToSource.ExecutionCancelled = true;
                _syncBackToSource = null;
            }

        }

        public async Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, bool doBulkCopy, bool trackChangeStreams)
        {
            int maxRetries = 10;
            int attempts = 0;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            //encoding speacial characters
            sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(sourceConnectionString);
            targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(targetConnectionString);

            targetConnectionString = Helper.UpdateAppName(targetConnectionString, "MSFTMongoWebMigration-" + Guid.NewGuid().ToString());

            if (Config == null)
            {
                Config = new MigrationSettings();
                Config.Load();
            }


            try
            {
                _migrationProcessor?.StopProcessing();
                _migrationProcessor = null;
            }
            catch { }

            _job = job;
            _migrationCancelled = false;
            CurrentJobId = _job.Id;

            Log.Init(_job.Id);
            Log.WriteLine($"{_job.Id} Started on {_job.StartedOn} (UTC)");
            Log.Save();

            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(item => item.Trim())
                .ToArray();



            if (_job.UseMongoDump)
            {
                _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_toolsDestinationFolder, Config);
                if (string.IsNullOrEmpty(_toolsLaunchFolder))
                {
                    _job.CurrentlyActive = false;
                    _job.IsCompleted = false;
                    _jobs?.Save();

                    _migrationProcessor?.StopProcessing();
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

                    string[] parts = fullName.Split('.');
                    if (parts.Length != 2) continue;

                    string dbName = parts[0].Trim();
                    string colName = parts[1].Trim();

                    var migrationUnit = new MigrationUnit(dbName, colName, null);
                    _job.MigrationUnits.Add(migrationUnit);
                    _jobs?.Save();
                }
            }




            while (attempts < maxRetries && !_migrationCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    _sourceClient = new MongoClient(sourceConnectionString);
                    Log.WriteLine("Source Client Created");
                    if (job.IsSimulatedRun)
                    {
                        Log.WriteLine("Simulated Run. No changes will be made to the target.");
                    }
                    else
                    {                        
                        if (job.AppendMode)
                        {
                            Log.WriteLine("Existing target collections will remain unchanged, and no indexes will be created.");
                        }
                        else
                        {
                            if (job.SkipIndexes)
                            {
                                Log.WriteLine("No indexes will be created.");
                            }
                        }
                    }
                    Log.Save();

                    if (_job.IsOnline)
                    {
                        Log.WriteLine("Checking if Change Stream is enabled on Source");
                        Log.Save();


                        var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_job.SourceConnectionString, _job.MigrationUnits[0]);
                        _job.SourceServerVersion = retValue.Version;
                        _jobs?.Save();

                        if (!retValue.IsCSEnabled)
                        {
                            _job.CurrentlyActive = false;
                            _job.IsCompleted = true;
                            _jobs?.Save();
                            continueProcessing = false;

                            _migrationProcessor?.StopProcessing();
                            return;
                        }                        

                    }

                    _migrationProcessor?.StopProcessing();
                    _migrationProcessor = null;
                    if (!_job.UseMongoDump)
                    {
                        _migrationProcessor = new CopyProcessor(_jobs, _job, _sourceClient, Config);
                    }
                    else
                    {
                        _migrationProcessor = new DumpRestoreProcessor(_jobs, _job, _sourceClient, Config, _toolsLaunchFolder);
                    }
                    _migrationProcessor.ProcessRunning = true;

                    bool checkedCS = false;
                    foreach (var unit in _job.MigrationUnits)
                    {
                        if (_migrationCancelled) return;
                        
                        if (await MongoHelper.CheckCollectionExists(_sourceClient, unit.DatabaseName, unit.CollectionName))
                        {
                            unit.SourceStatus = CollectionStatus.OK;

                            if (_job.IsOnline)
                            {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                                Task.Run(async () =>
                                {
                                    await MongoHelper.SetChangeStreamResumeTokenAsync(_sourceClient, unit);
                                });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                            }
                            if (unit.MigrationChunks == null || unit.MigrationChunks.Count == 0)
                            {

                                var chunks = await PartitionCollection(unit.DatabaseName, unit.CollectionName);

                                Log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks.Count} Chunks");
                                Log.Save();

                                unit.MigrationChunks = chunks;
                                unit.ChangeStreamStartedOn = DateTime.Now;



                                if (!job.IsSimulatedRun && !job.AppendMode)
                                {
                                    var database = _sourceClient.GetDatabase(unit.DatabaseName);
                                    var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                                    await MongoHelper.DeleteAndCopyIndexesAsync(targetConnectionString, collection, job.SkipIndexes);

                                    if (_job.SyncBackAfterMigration && !job.IsSimulatedRun && _job.IsOnline && !checkedCS)
                                    {
                                        Log.WriteLine("Checking if Change Stream is enabled on Target for Reverse Sync");
                                        Log.Save();

                                        var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_job.TargetConnectionString, unit);
                                        checkedCS = true;
                                        if (!retValue.IsCSEnabled)
                                        {
                                            _job.CurrentlyActive = false;
                                            _job.IsCompleted = true;
                                            _jobs?.Save();
                                            continueProcessing = false;

                                            _migrationProcessor?.StopProcessing();
                                            return;
                                        }
                                    }
                                }                                
                            }
                           
                        }
                        else
                        {
                            unit.SourceStatus = CollectionStatus.NotFound;
                            Log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} does not exist on source or has zero records", LogType.Error);
                            Log.Save();
                        }
                    }

                    _jobs?.Save();
                    Log.Save();

                    
                    foreach (var migrationUnit in _job.MigrationUnits)
                    {
                        if (_migrationCancelled) break;

                        if (migrationUnit.SourceStatus == CollectionStatus.OK)
                        {
                            if (await MongoHelper.CheckCollectionExists(_sourceClient, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                            {
                                var targetClient = new MongoClient(targetConnectionString);

                                if (await MongoHelper.CheckCollectionExists(targetClient, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                                {
                                    Log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} already exists on target");
                                    Log.Save();
                                }
                                _migrationProcessor.Migrate(migrationUnit, sourceConnectionString, targetConnectionString);
    
                            }
                            else
                            {
                                migrationUnit.SourceStatus = CollectionStatus.NotFound;
                                Log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source or has zero records", LogType.Error);
                                Log.Save();
                            }
                        }
                    }

                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                   
                }
            }
            if (attempts == maxRetries)
            {
                Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                Log.Save();

                _job.CurrentlyActive = false;
                _jobs?.Save();
                continueProcessing = false;

                _migrationProcessor?.StopProcessing();

            }
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            int maxRetries = 10;
            int attempts = 0;

            TimeSpan backoff = TimeSpan.FromSeconds(2);
            bool continueProcessing = true;

            var sourceClient = new MongoClient(sourceConnectionString);
            var targetClient = new MongoClient(targetConnectionString);

            _syncBackToSource = null;
            _syncBackToSource = new MongoChangeStreamProcessor(sourceClient, targetClient, _jobs, Config);

            job.ReverseSyncStarted = true;
            _jobs.Save();

            while (attempts < maxRetries && !_migrationCancelled && continueProcessing)
            {
                attempts++;
                try
                {               
                    foreach (var migrationUnit in job.MigrationUnits)
                    {
                        if (_migrationCancelled) break;

                        if (migrationUnit.SourceStatus == CollectionStatus.OK)
                        {                                  
                           Task.Run(() => _syncBackToSource.CollectionSyncBackAsync(job, migrationUnit));                           
                        }
                    }
                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);

                }
            }
            if (attempts == maxRetries)
            {
                Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                Log.Save();

                job.CurrentlyActive = false;
                _jobs?.Save();
                continueProcessing = false;

                _migrationProcessor?.StopProcessing();

                _syncBackToSource.ExecutionCancelled = true;
                _syncBackToSource = null;
            }
        }

        private async Task<List<MigrationChunk>> PartitionCollection(string databaseName, string collectionName, string idField = "_id")
        {

            var stas=await MongoHelper.GetCollectionStatsAsync(_sourceClient, databaseName, collectionName);

            long documentCount = stas.DocumentCount;
            long totalCollectionSizeBytes = stas.CollectionSizeBytes;

            var database = _sourceClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            int totalChunks = 0;
            long minDocsInChunk = 0;

            long targetChunkSizeBytes = Config.ChunkSizeInMb * 1024 * 1024;
            var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);


            if (_job.UseMongoDump)
            {
                totalChunks = totalChunksBySize;
                minDocsInChunk = documentCount / totalChunks;
                Log.WriteLine($"{databaseName}.{collectionName} Storage Size: {totalCollectionSizeBytes}");
            }
            else
            {
                Log.WriteLine($"{databaseName}.{collectionName} Estimated Document Count: {documentCount}");
                totalChunks = (int)Math.Min(SamplePartitioner.MaxSamples / SamplePartitioner.MaxSegments, documentCount / SamplePartitioner.MaxSamples);
                totalChunks = Math.Max(1, totalChunks); // At least one chunk
                totalChunks = Math.Max(totalChunks, totalChunksBySize);
                minDocsInChunk = documentCount / totalChunks;
            }

            List<MigrationChunk> migrationChunks = new List<MigrationChunk>();

            if (totalChunks > 1 || !_job.UseMongoDump)
            {
                Log.WriteLine($"Chunking {databaseName}.{collectionName}");
                Log.Save();


                List<DataType> dataTypes = new List<DataType> { DataType.Int, DataType.Int64, DataType.String, DataType.Object, DataType.Decimal128, DataType.Date, DataType.ObjectId };

                if (Config.HasUuid)
                    dataTypes.Add(DataType.UUID);

                foreach (var dataType in dataTypes)
                {
                    long docCountByType;
                    ChunkBoundaries chunkBoundaries = SamplePartitioner.CreatePartitions(_job.UseMongoDump, collection, idField, totalChunks, dataType, minDocsInChunk, out docCountByType);

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
                            chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false,Id="1" });
                        }

                        if (!_job.UseMongoDump && chunkBoundaries.Boundaries[i].SegmentBoundaries.Count > 0)
                        {
                            for (int j = 0; j < chunkBoundaries.Boundaries[i].SegmentBoundaries.Count; j++)
                            {
                                var segment = chunkBoundaries.Boundaries[i].SegmentBoundaries[j];
                                var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, chunkBoundaries.Boundaries[i].SegmentBoundaries.Count, j, chunk.Lt, chunk.Gte);

                                chunk.Segments ??= new List<Segment>();
                                chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false , Id = (j+1).ToString() });
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
