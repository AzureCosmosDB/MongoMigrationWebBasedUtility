using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    public class MongoDocumentCopier
    {
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private const int PageSize = 500;
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;

        public void Initialize(
            MongoClient targetClient,
            IMongoCollection<BsonDocument> sourceCollection,
            string targetDatabase,
            string targetCollectionName)
        {
            _targetClient = targetClient;
            _sourceCollection = sourceCollection;
            _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);
        }

        private void UpdateProgress(
            int setIndex,
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
                Log.AddVerboseMessage($"Document copy for chunk segment [{migrationChunkIndex}.{setIndex}] Progress: {successCount} documents copied, {_skippedCount} documents skipped(duplicate), {failureCount} documents failed. Chunk completion percentage: {percent}");
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
            CancellationToken cancellationToken)
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
                SemaphoreSlim semaphore = new SemaphoreSlim(10);

                foreach (var segment in item.MigrationChunks[migrationChunkIndex].Segments)
                {
                    //if (segment.IsProcessed == true)
                    //{
                    //    segmentIndex++;
                    //    continue;
                    //}

                    FilterDefinition<BsonDocument> combinedFilter = Builders<BsonDocument>.Filter.Empty;
                    if (item.MigrationChunks[migrationChunkIndex].Segments.Count == 1 || segment.IsProcessed == true)
                    {
                        combinedFilter = filter;
                    }
                    else
                    {
                        var bounds = SamplePartitioner.GetChunkBounds(segment.Gte, segment.Lt, item.MigrationChunks[migrationChunkIndex].DataType);
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
                            segmentIndex++;
                            await ProcessSegmentAsync(segment, combinedFilter, segmentIndex, jobList, item, migrationChunkIndex, basePercent, contribFactor, targetCount, errors, cancellationToken);
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
                var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[migrationChunkIndex].Gte, item.MigrationChunks[migrationChunkIndex].Lt, item.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                item.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);

                if (item.MigrationChunks[migrationChunkIndex].DocCountInTarget == item.MigrationChunks[migrationChunkIndex].DumpResultDocCount)
                {
                    Log.WriteLine($"Document copy for chunk [{migrationChunkIndex}], verified no documents missing, count in target: {item.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
                    Log.Save();
                }

                jobList?.Save(); //persists state
            }
            return true;
        }

        private async Task ProcessSegmentAsync(
            Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            int segmentIndex,
            JobList jobList,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken)
        {
            Log.WriteLine($"Document copy started for chunk segment [{migrationChunkIndex}.{segmentIndex}]");
            Log.Save();

            if (segment.IsProcessed == true)
            {
                Log.WriteLine($"Skipping processed chunk segment [{migrationChunkIndex}.{segmentIndex}]");
                Log.Save();

                Interlocked.Add(ref _successCount, segment.QueryDocCount);
                return;
            }
            else if(segment.QueryDocCount > 0)
            {
                // Delete documents matching the filter
                var result = await _targetCollection.DeleteManyAsync(combinedFilter);
                if(result.DeletedCount > 0)
                {
                    // Output the number of deleted documents
                    Log.WriteLine($"Deleted {result.DeletedCount} documents from target to avoid duplicates in chunk segment [{migrationChunkIndex}.{segmentIndex}]");
                    Log.Save();
                }
                
            }
            segment.QueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, combinedFilter);
            jobList.Save();

            try
            {
                int pageIndex = 0; // Current page index
                List<BsonDocument> set = new List<BsonDocument>();
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Get the next page of results
                        set = await _sourceCollection.Aggregate()
                            .Match(combinedFilter)  // Apply the filter
                            .Skip(pageIndex * PageSize)  // Skip documents based on the current page
                            .Limit(PageSize)  // Limit to the page size
                            .ToListAsync(cancellationToken);

                        if (set.Count == 0)
                        {
                            // No more results, break the loop
                            break;
                        }

                        // Insert the current batch into the target collection
                        await _targetCollection.InsertManyAsync(set, cancellationToken: cancellationToken);

                        Interlocked.Add(ref _successCount, set.Count);

                        // Increment the page index to get the next batch
                        pageIndex++;
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
                    catch (Exception ex) when (ex.Message.Contains("canceled."))
                    {
                        Log.WriteLine($"Document copy operation canceled for chunk segment [{migrationChunkIndex}.{segmentIndex}]");
                        Log.Save();
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                        Interlocked.Add(ref _failureCount, set.Count);
                        Log.WriteLine($"Batch processing error during document copy for chunk segment [{migrationChunkIndex}.{segmentIndex}]. Details : {ex.Message}", LogType.Error);
                        Log.Save();
                    }
                    finally
                    {
                        UpdateProgress(segmentIndex, jobList, item, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount);
                    }
                }

                if(!cancellationToken.IsCancellationRequested)
                {
                    Log.WriteLine($"Document copy Operation completed for chunk [{migrationChunkIndex}.{segmentIndex}]");
                    Log.Save();

                    segment.IsProcessed = true;
                    jobList.Save();
                }            

                
            }
            catch (Exception ex)
            {
                errors.Add(ex);
                Log.WriteLine($"Document copy encountered error while processing chunk segment [{migrationChunkIndex}.{segmentIndex}], Details: {ex.Message}", LogType.Error);
                Log.Save();
            }
        }

        private void LogErrors(MongoBulkWriteException<BsonDocument> ex)
        {
            foreach (var error in ex.WriteErrors)
            {
                Log.WriteLine($"Document copy encountered WriteErrors, Details: {ex.Message}");
                Log.Save();
            }
        }
    }
}

