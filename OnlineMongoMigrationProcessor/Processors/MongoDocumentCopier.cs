using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
namespace OnlineMongoMigrationProcessor
{
    public class MongoDocumentCopier
    {
        private Log _log;
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private int _pageSize = 500;
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;

        public void Initialize(
            Log log,
            MongoClient targetClient,
            IMongoCollection<BsonDocument> sourceCollection,
            string targetDatabase,
            string targetCollectionName,
            int pageSize)
        {
            _log = log;
			_targetClient = targetClient;
            _sourceCollection = sourceCollection;
            if(_targetClient!=null) 
                _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);

            _pageSize =pageSize;
        }

        private void UpdateProgress(
            string segmentId,
            JobList jobList,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            long successCount,
            long failureCount,
            long skippedCount)
        {
            MigrationChunk migrationChunk = item.MigrationChunks[migrationChunkIndex];
            var percent = Math.Round((double)(successCount + skippedCount) / targetCount * 100, 3);
            if (percent > 100)
            {
                Debug.WriteLine("Percent is greater than 100");
            }

            if (percent > 0)
            {
               _log.AddVerboseMessage($"Document copy for segment [{migrationChunkIndex}.{segmentId}] Progress: {successCount} documents copied, {skippedCount} documents skipped(duplicate), {failureCount} documents failed. Chunk completion percentage: {percent}");

                item.DumpPercent = basePercent + (percent * contribFactor);
                item.RestorePercent = item.DumpPercent;
                item.DumpComplete = item.DumpPercent == 100;
                item.RestoreComplete = item.DumpComplete;
            }

            migrationChunk.SkippedAsDuplicateCount = skippedCount;
            migrationChunk.DumpResultDocCount = successCount + skippedCount;
            migrationChunk.RestoredSuccessDocCount = successCount + skippedCount;
            migrationChunk.RestoredFailedDocCount = failureCount;

            jobList.Save();
        }

        public async Task<bool> CopyDocumentsAsync(
            JobList jobList,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            FilterDefinition<BsonDocument> filter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            ConcurrentBag<Exception> errors = new ConcurrentBag<Exception>();
            try
            {
               _log.WriteLine($"Document copy for Chunk [{migrationChunkIndex}] with {item.MigrationChunks[migrationChunkIndex].Segments.Count}. Segments started");
                

                List<Task> tasks = new List<Task>();
                int segmentIndex = 0;
                errors = new ConcurrentBag<Exception>();

                //int maxWorkerThreads, maxCompletionPortThreads;
                //ThreadPool.GetMaxThreads(out maxWorkerThreads, out maxCompletionPortThreads);
                SemaphoreSlim semaphore = new SemaphoreSlim(20);

                foreach (var segment in item.MigrationChunks[migrationChunkIndex].Segments)
                {

                    //back ward compatibility, Id was introduced later
                    segmentIndex++;
                    if (string.IsNullOrEmpty(segment.Id))
                        segment.Id = segmentIndex.ToString();

                    FilterDefinition<BsonDocument> combinedFilter = Builders<BsonDocument>.Filter.Empty;
                    if (item.MigrationChunks[migrationChunkIndex].Segments.Count == 1 || segment.IsProcessed == true)
                    {
                        combinedFilter = filter;
                    }
                    else
                    {
#pragma warning disable CS8604 // Possible null reference argument.
                        var bounds = SamplePartitioner.GetChunkBounds(segment.Gte, segment.Lt, item.MigrationChunks[migrationChunkIndex].DataType);
#pragma warning restore CS8604 // Possible null reference argument.
                        var gte = bounds.gte;
                        var lt = bounds.lt;

                        // Filter by id bounds
                        FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);

                        // Filter by datatype
                        BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(item.MigrationChunks[migrationChunkIndex].DataType, "_id");

                        // Combine the filters using $and
                        combinedFilter = Builders<BsonDocument>.Filter.And(idFilter, matchCondition);
                    }

                    await semaphore.WaitAsync();
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            
                            await ProcessSegmentAsync(segment, combinedFilter, jobList, item, migrationChunkIndex, basePercent, contribFactor, targetCount, errors, cancellationToken, isWriteSimulated);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }));

                   
                }

                await Task.WhenAll(tasks);
               
            }
            catch (OperationCanceledException)
            {
               _log.WriteLine("Document copy process was canceled");
                
                return false;
            }

            if (!errors.IsEmpty)
            {
               _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] encountered {errors.Count} errors, skipped {_skippedCount} during the process");
                
            }

            if (item.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount > 0)
            {
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[migrationChunkIndex].Gte, item.MigrationChunks[migrationChunkIndex].Lt, item.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

               _log.WriteLine($"Document copy for chunk[{ migrationChunkIndex}], counting documents on target");
                

                try
                {
                    item.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);
                    item.MigrationChunks[migrationChunkIndex].DumpQueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);
                }
                catch (Exception ex)
                {
                   _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] encountered error while counting documents on target. Chunk will be reprocessed. Details: {ex.ToString()}", LogType.Error);
                    
                    ResetSegmentsInChunk(item.MigrationChunks[migrationChunkIndex]);
                    return false;
                }

                if (item.MigrationChunks[migrationChunkIndex].DocCountInTarget == item.MigrationChunks[migrationChunkIndex].DumpQueryDocCount)
                {
                   _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}], verified no documents missing, count in target: {item.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
                    
                }
                else
                {
                   _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] count mismatch. Chunk will be reprocessed.", LogType.Error);
                    
                    return false;
                }
                jobList?.Save(); //persists state
            }
            return true;
        }

        private void ResetSegmentsInChunk(MigrationChunk migrationChunk)
        {
            foreach (var segment in migrationChunk.Segments)
            {
                segment.IsProcessed = false;
            }
        }

        private async Task ProcessSegmentAsync(
            Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            JobList jobList,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken,
            bool IsWriteSimulated)
        {
              
            string segmentId = segment.Id;
            TimeSpan backoff = TimeSpan.FromSeconds(2);

           _log.WriteLine($"Document copy started for segment [{migrationChunkIndex}.{segmentId}]");
            

            if (segment.IsProcessed == true)
            {
               _log.WriteLine($"Skipping processed segment [{migrationChunkIndex}.{segmentId}]");
                

                Interlocked.Add(ref _successCount, segment.QueryDocCount);
                return;
            }
           
            segment.QueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, combinedFilter);
            jobList.Save();


            try
            {
                bool failed = false;
                int pageIndex = 0; // Current page index
                List<BsonDocument> set = new List<BsonDocument>();
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // 1. Fetch documents from source
                        set = await _sourceCollection.Aggregate()
                            .Match(combinedFilter)
                            .Skip(pageIndex * _pageSize)
                            .Limit(_pageSize)
                            .ToListAsync(cancellationToken);

                        if (set.Count == 0)
                            break;

                        // 2. Write to target (unless simulated)
                        if (!IsWriteSimulated)
                        {
                            var insertModels = set.Select(doc =>
                            {
                                // _id handling only if really needed
                                if (doc.Contains("_id"))
                                {
                                    var id = doc["_id"];
                                    if (id.IsObjectId)
                                        doc["_id"] = id.AsObjectId;
                                }
                                return new InsertOneModel<BsonDocument>(doc);
                            }).ToList();

                            var result = await _targetCollection.BulkWriteAsync(
                                insertModels,
                                new BulkWriteOptions { IsOrdered = false },
                                cancellationToken);

                            long insertCount = result.InsertedCount;
                            Interlocked.Add(ref _successCount, insertCount);
                        }
                        else
                        {
                            Interlocked.Add(ref _successCount, set.Count);
                        }
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        // Partial success
                        long bulkInserted = ex.Result?.InsertedCount ?? 0;
                        Interlocked.Add(ref _successCount, bulkInserted);

                        // Analyze errors
                        int dupeCount = ex.WriteErrors.Count(e => e.Code == 11000);
                        if (dupeCount > 0)
                            Interlocked.Add(ref _skippedCount, dupeCount);

                        int otherErrors = ex.WriteErrors.Count(e => e.Code != 11000);
                        if (otherErrors > 0)
                        {
                            Interlocked.Add(ref _failureCount, otherErrors);
                            _log.WriteLine(
                                $"Document copy MongoBulkWriteException with non-dupe errors for segment [{migrationChunkIndex}.{segmentId}]",
                                LogType.Error);
                            LogErrors(
                                ex.WriteErrors.Where(e => e.Code != 11000).ToList(),
                                $"segment [{migrationChunkIndex}.{segmentId}]"
                            );
                        }
                    }
                    catch (OutOfMemoryException ex)
                    {
                        _log.WriteLine(
                            $"Document copy encountered OutOfMemoryException for segment [{migrationChunkIndex}.{segmentId}]. " +
                            $"Try reducing _pageSize. Details: {ex}", LogType.Error);
                        throw;
                    }
                    catch (Exception ex) when (ex.ToString().Contains("canceled."))
                    {
                        _log.WriteLine($"Document copy operation canceled for segment [{migrationChunkIndex}.{segmentId}]");
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                        _log.WriteLine(
                            $"Batch processing error during document copy for segment [{migrationChunkIndex}.{segmentId}]. Details: {ex}",
                            LogType.Error);
                        failed = true;
                    }
                    finally
                    {
                        UpdateProgress(segmentId, jobList, item, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                        pageIndex++;
                    }

                    if (failed)
                        break;                    
                }

                if(!cancellationToken.IsCancellationRequested)
                {
                   _log.WriteLine($"Document copy Operation completed for chunk [{migrationChunkIndex}.{segmentId}] with status {!failed}");
                    

                    segment.IsProcessed = !failed;
                    jobList.Save();
                }      
                
            }
            catch (Exception ex)
            {
                errors.Add(ex);
               _log.WriteLine($"Document copy encountered error while processing segment [{migrationChunkIndex}.{segmentId}], Details: {ex.ToString()}", LogType.Error);
                
            }
        }

        //private async Task<long> DeleteInBatchesAsync(IMongoCollection<BsonDocument> collection, FilterDefinition<BsonDocument> filter, int batchSize, string chunkindex)
        //{
        //    long deletedCount = 0;
        //    int ctr = 1;
        //    while (true)
        //    {
        //       _log.AddVerboseMessage($"Getting page {ctr} to delete from target for segment {chunkindex}");

        //        // Get a batch of document _ids to delete
        //        var batchIds = await collection.Find(filter)
        //                                       .Limit(batchSize)
        //                                       .Project(doc => doc["_id"])
        //                                       .ToListAsync();

        //        if (batchIds.Count == 0)
        //            break;  // No more documents to delete

        //        // Delete documents in this batch
        //        var deleteFilter = Builders<BsonDocument>.Filter.In("_id", batchIds);

        //       _log.AddVerboseMessage($"Deleting page {ctr} from target for segment {chunkindex}");
        //        var result = await collection.DeleteManyAsync(deleteFilter);
        //        deletedCount = deletedCount + result.DeletedCount;
        //        ctr++;
        //    }
        //    return deletedCount;
        //}

        private void LogErrors(List<BulkWriteError> exceptions, string location)
        {
            foreach (var error in exceptions)
            {
               _log.WriteLine($"Document copy encountered write errors for {location}. Details: {error.ToString()}");

            }
        }
    }
}

