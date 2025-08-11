using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
namespace OnlineMongoMigrationProcessor.Workers
{
    public class DocumentCopyWorker
    {
        private Log _log;
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private int _pageSize = 500;
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;
              

        private void UpdateProgress(
            string segmentId,
            JobList jobList,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            long successCount,
            long failureCount,
            long skippedCount)
        {
            MigrationChunk migrationChunk = mu.MigrationChunks[migrationChunkIndex];
            var percent = Math.Round((double)(successCount + skippedCount) / targetCount * 100, 3);
            if (percent > 100)
            {
                Debug.WriteLine("Percent is greater than 100");
            }

            if (percent > 0)
            {
               _log.AddVerboseMessage($"Document copy for segment [{migrationChunkIndex}.{segmentId}] Progress: {successCount} documents copied, {skippedCount} documents skipped(duplicate), {failureCount} documents failed. Chunk completion percentage: {percent}");

                mu.DumpPercent = basePercent + percent * contribFactor;
                mu.RestorePercent = mu.DumpPercent;
                mu.DumpComplete = mu.DumpPercent == 100;
                mu.RestoreComplete = mu.DumpComplete;

                if(mu.DumpComplete && mu.RestoreComplete)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                }
            }

            migrationChunk.SkippedAsDuplicateCount = skippedCount;
            migrationChunk.DumpResultDocCount = successCount + skippedCount;
            migrationChunk.RestoredSuccessDocCount = successCount + skippedCount;
            migrationChunk.RestoredFailedDocCount = failureCount;

            jobList.Save();
        }

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
            if (_targetClient != null)
                _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);
            _pageSize=pageSize;
        }

        public async Task<TaskResult> CopyDocumentsAsync(
            JobList jobList,
            MigrationUnit mu,
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
               _log.WriteLine($"Document copy for Chunk [{migrationChunkIndex}] with {mu.MigrationChunks[migrationChunkIndex].Segments.Count}. Segments started");
                
                if(!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                    mu.BulkCopyStartedOn = DateTime.UtcNow;

				List<Task<TaskResult>> tasks = new List<Task<TaskResult>>();
				int segmentIndex = 0;
                errors = new ConcurrentBag<Exception>();

                SemaphoreSlim semaphore = new SemaphoreSlim(20);

                foreach (var segment in mu.MigrationChunks[migrationChunkIndex].Segments)
                {

                    //back ward compatibility, Id was introduced later
                    segmentIndex++;
                    if (string.IsNullOrEmpty(segment.Id))
                        segment.Id = segmentIndex.ToString();

                    FilterDefinition<BsonDocument> combinedFilter = Builders<BsonDocument>.Filter.Empty;
                    if (mu.MigrationChunks[migrationChunkIndex].Segments.Count == 1 || segment.IsProcessed == true)
                    {
                        combinedFilter = filter;
                    }
                    else
                    {
#pragma warning disable CS8604 // Possible null reference argument.
                        var bounds = SamplePartitioner.GetChunkBounds(segment.Gte, segment.Lt, mu.MigrationChunks[migrationChunkIndex].DataType);
#pragma warning restore CS8604 // Possible null reference argument.
                        var gte = bounds.gte;
                        var lt = bounds.lt;

                        // Filter by id bounds
                        FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType);

                        // Filter by datatype
                        BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(mu.MigrationChunks[migrationChunkIndex].DataType, "_id");

                        // Combine the filters using $and
                        combinedFilter = Builders<BsonDocument>.Filter.And(idFilter, matchCondition);
                    }

                    await semaphore.WaitAsync();
					tasks.Add(Task.Run(async () =>
					{
						try
						{
							// Directly return the awaited enum value
							return await ProcessSegmentAsync(
								segment, combinedFilter, jobList, mu,
								migrationChunkIndex, basePercent, contribFactor,
								targetCount, errors, cancellationToken, isWriteSimulated
							);
						}
						finally
						{
							semaphore.Release();
						}
					}));

				}

				TaskResult[] results=await Task.WhenAll(tasks);

				if (results.Any(r => r == TaskResult.Canceled))
                {
					return TaskResult.Canceled;
				}
            }
            catch (OperationCanceledException)
            {
				return TaskResult.Canceled;
            }

            if (!errors.IsEmpty)
            {
               _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] encountered {errors.Count} errors, skipped {_skippedCount} during the process");
                
            }

            if (mu.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount > 0 || errors.Count>0)
            {
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[migrationChunkIndex].Gte, mu.MigrationChunks[migrationChunkIndex].Lt, mu.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

               _log.WriteLine($"Document copy for chunk[{ migrationChunkIndex}], counting documents on target");
                

                try
                {
                    mu.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType);
                    mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType);
                }
                catch (Exception ex)
                {
                   _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] encountered error while counting documents on target. Chunk will be reprocessed. Details: {ex.ToString()}", LogType.Error);
                    
                    ResetSegmentsInChunk(mu.MigrationChunks[migrationChunkIndex]);
                    return TaskResult.Retry;
                }

                if (mu.MigrationChunks[migrationChunkIndex].DocCountInTarget == mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount)
                {
                   _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}], verified no documents missing, count in target: {mu.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
                }
                else
                {
                   _log.WriteLine($"Document copy for chunk [{migrationChunkIndex}] count mismatch. Chunk will be reprocessed.", LogType.Error);
					return TaskResult.Retry;
				}
                jobList?.Save(); //persists state
            }
            return TaskResult.Success;
        }

        private void ResetSegmentsInChunk(MigrationChunk migrationChunk)
        {
            foreach (var segment in migrationChunk.Segments)
            {
                segment.IsProcessed = false;
            }
        }

        private async Task<TaskResult> ProcessSegmentAsync(
            Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            JobList jobList,
            MigrationUnit mu,
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
                return TaskResult.Success;
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
						return TaskResult.Retry;
					}
                    catch (Exception ex) when (ex.ToString().Contains("canceled."))
                    {
                        _log.WriteLine($"Document copy operation for segment [{migrationChunkIndex}.{segmentId}] was canceled.");
						//throw new OperationCanceledException("Document copy operation was cancelled.");
						return TaskResult.Canceled;
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
                        UpdateProgress(segmentId, jobList, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                        pageIndex++;
                    }

                    if (failed)
                        break;                    
                }

                if(!cancellationToken.IsCancellationRequested)
                {
                    if(failed)
                    {
                        _log.WriteLine($"Document copy failed for segment [{migrationChunkIndex}.{segmentId}].");
                    }
                    else if(_failureCount > 0 || _skippedCount > 0)
					{
                        _log.WriteLine($"Document copy completed for segment [{migrationChunkIndex}.{segmentId}] with {_successCount} documents copied, {_skippedCount} documents skipped(duplicate), {_failureCount} documents failed.");
					}
                    else
                    {
						_log.WriteLine($"Document copy completed for segment [{migrationChunkIndex}.{segmentId}] with {_successCount} documents copied.");
					}
                    segment.IsProcessed = !failed;
                    jobList.Save();
					return TaskResult.Success;
				}
                else
					return TaskResult.Canceled;
			}
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                errors.Add(ex);
                _log.WriteLine($"Document copy encountered error while processing segment [{migrationChunkIndex}.{segmentId}], Details: {ex.ToString()}", LogType.Error);
				return TaskResult.Retry;
			}
        }


        private void LogErrors(List<BulkWriteError> exceptions, string location)
        {
            foreach (var error in exceptions)
            {
               _log.WriteLine($"Document copy encountered write errors for {location}. Details: {error.ToString()}");

            }
        }
    }
}

