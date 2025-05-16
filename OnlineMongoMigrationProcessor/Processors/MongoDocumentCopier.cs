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
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private int _pageSize = 500;
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;

        public void Initialize(
            MongoClient targetClient,
            IMongoCollection<BsonDocument> sourceCollection,
            string targetDatabase,
            string targetCollectionName,
            int pageSize)
        {
            _targetClient = targetClient;
            _sourceCollection = sourceCollection;
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
            long failureCount)
        {
            MigrationChunk migrationChunk = item.MigrationChunks[migrationChunkIndex];
            var percent = Math.Round((double)(successCount + _skippedCount) / targetCount * 100, 3);
            if (percent > 100)
            {
                Debug.WriteLine("Percent is greater than 100");
            }

            if (percent > 0)
            {
                Log.AddVerboseMessage($"Document copy for segment [{migrationChunkIndex}.{segmentId}] Progress: {successCount} documents copied, {_skippedCount} documents skipped(duplicate), {failureCount} documents failed. Chunk completion percentage: {percent}");
                Log.Save();
                item.DumpPercent = basePercent + (percent * contribFactor);
                item.RestorePercent = item.DumpPercent;
                item.DumpComplete = item.DumpPercent == 100;
                item.RestoreComplete = item.DumpComplete;
            }

            migrationChunk.SkippedAsDuplicateCount = _skippedCount;
            migrationChunk.DumpResultDocCount = successCount + _skippedCount;
            migrationChunk.RestoredSuccessDocCount = successCount + _skippedCount;
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
                Log.WriteLine($"Document copy for Chunk [{migrationChunkIndex}] with {item.MigrationChunks[migrationChunkIndex].Segments.Count}. Segments started");
                Log.Save();

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
                Log.WriteLine("Document copy process was canceled");
                Log.Save();
                return false;
            }

            if (!errors.IsEmpty)
            {
                Log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] encountered {errors.Count} errors, skipped {_skippedCount} during the process");
                Log.Save();
            }

            if (item.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount > 0)
            {
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[migrationChunkIndex].Gte, item.MigrationChunks[migrationChunkIndex].Lt, item.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                Log.WriteLine($"Document copy for chunk[{ migrationChunkIndex}], counting documents on target");
                Log.Save();

                try
                {
                    item.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] encountered error while counting documents on target. Chunk will be reprocessed. Details: {ex.ToString()}", LogType.Error);
                    Log.Save();
                    ResetSegmentsInChunk(item.MigrationChunks[migrationChunkIndex]);
                    return false;
                }

                if (item.MigrationChunks[migrationChunkIndex].DocCountInTarget == item.MigrationChunks[migrationChunkIndex].DumpQueryDocCount)
                {
                    Log.WriteLine($"Document copy for chunk [{migrationChunkIndex}], verified no documents missing, count in target: {item.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
                    Log.Save();
                }
                else
                {
                    Log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] count mismatch. Chunk will be reprocessed.", LogType.Error);
                    Log.Save();
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

            Log.WriteLine($"Document copy started for segment [{migrationChunkIndex}.{segmentId}]");
            Log.Save();

            if (segment.IsProcessed == true)
            {
                Log.WriteLine($"Skipping processed segment [{migrationChunkIndex}.{segmentId}]");
                Log.Save();

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
                        // Get the next page of results
                        set = await _sourceCollection.Aggregate()
                            .Match(combinedFilter)  // Apply the filter
                            .Skip(pageIndex * _pageSize)  // Skip documents based on the current page
                            .Limit(_pageSize)  // Limit to the page size
                            .ToListAsync(cancellationToken);

                        if (set.Count == 0)
                        {
                            // No more results, break the loop
                            break;
                        }

                        if (!IsWriteSimulated)
                        {
                            var options = new InsertManyOptions { IsOrdered = false };
                            // Insert the current batch into the target collection
                            await _targetCollection.InsertManyAsync(set, options, cancellationToken: cancellationToken);                           
                        }
                        Interlocked.Add(ref _successCount, set.Count);

                    }
                    catch (OutOfMemoryException ex)
                    {
                        Log.WriteLine($"Document copy encountered out of memory error for segment [{migrationChunkIndex}.{segmentId}]. Try reducing _pageSize in settings. Details: {ex.ToString()}", LogType.Error);
                        Log.Save();
                        throw;
                    }
                    catch (MongoException mex) when (mex.Message.Contains("DuplicateKey"))
                    {
                        Interlocked.Add(ref _skippedCount, ((MongoBulkWriteException)mex).WriteErrors.Count);

                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        int successfulInserts = set.Count - ex.WriteErrors.Count;
                        Interlocked.Add(ref _successCount, successfulInserts);
                        Interlocked.Add(ref _failureCount, ex.WriteErrors.Count);
                        LogErrors(ex);
                    }
                    catch (Exception ex) when (ex.ToString().Contains("canceled."))
                    {
                        Log.WriteLine($"Document copy operation canceled for segment [{migrationChunkIndex}.{segmentId}]");
                        Log.Save();
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                        Interlocked.Add(ref _failureCount, set.Count);
                        Log.WriteLine($"Batch processing error during document copy for segment [{migrationChunkIndex}.{segmentId}]. Details : {ex.ToString()}", LogType.Error);
                        Log.Save();

                        failed=true;

                    }
                    finally
                    {
                        UpdateProgress(segmentId, jobList, item, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount);

                        // Increment the page index to get the next batch
                        pageIndex++;
                    }

                    if (failed)
                        break;
                }

                if(!cancellationToken.IsCancellationRequested)
                {
                    Log.WriteLine($"Document copy Operation completed for chunk [{migrationChunkIndex}.{segmentId}] with satus {!failed}");
                    Log.Save();

                    segment.IsProcessed = !failed;
                    jobList.Save();
                }      
                
            }
            catch (Exception ex)
            {
                errors.Add(ex);
                Log.WriteLine($"Document copy encountered error while processing segment [{migrationChunkIndex}.{segmentId}], Details: {ex.ToString()}", LogType.Error);
                Log.Save();
            }
        }

        //private async Task<long> DeleteInBatchesAsync(IMongoCollection<BsonDocument> collection, FilterDefinition<BsonDocument> filter, int batchSize, string chunkindex)
        //{
        //    long deletedCount = 0;
        //    int ctr = 1;
        //    while (true)
        //    {
        //        Log.AddVerboseMessage($"Getting page {ctr} to delete from target for segment {chunkindex}");

        //        // Get a batch of document _ids to delete
        //        var batchIds = await collection.Find(filter)
        //                                       .Limit(batchSize)
        //                                       .Project(doc => doc["_id"])
        //                                       .ToListAsync();

        //        if (batchIds.Count == 0)
        //            break;  // No more documents to delete

        //        // Delete documents in this batch
        //        var deleteFilter = Builders<BsonDocument>.Filter.In("_id", batchIds);

        //        Log.AddVerboseMessage($"Deleting page {ctr} from target for segment {chunkindex}");
        //        var result = await collection.DeleteManyAsync(deleteFilter);
        //        deletedCount = deletedCount + result.DeletedCount;
        //        ctr++;
        //    }
        //    return deletedCount;
        //}

        private void LogErrors(MongoBulkWriteException<BsonDocument> ex)
        {
            foreach (var error in ex.WriteErrors)
            {
                Log.WriteLine($"Document copy encountered WriteErrors, Details: {ex.ToString()}");
                Log.Save();
            }
        }
    }
}

