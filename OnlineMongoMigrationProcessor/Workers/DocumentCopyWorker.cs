using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
namespace OnlineMongoMigrationProcessor.Workers
{
    public class DocumentCopyWorker
    {
        private Log _log;
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private int _pageSize = 5000; // Increased from 500 for better throughput
        private int _saveProgressEveryNPages = 20; // Reduce disk I/O by batching saves
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;
              

        private void UpdateProgress(
            string segmentId,
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
               _log.ShowInMonitor($"Document copy for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] Progress: {successCount} documents copied, {skippedCount} documents skipped(duplicate), {failureCount} documents failed. Chunk completion percentage: {percent}");

                mu.DumpPercent = basePercent + (percent * contribFactor);
                mu.RestorePercent = mu.DumpPercent;

                if (mu.MigrationChunks.All(chunk => chunk.IsDownloaded == true))
                {
                    mu.DumpComplete = mu.DumpPercent == 100;
                    mu.RestoreComplete = mu.DumpComplete;
                }

                if(mu.DumpComplete && mu.RestoreComplete)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                }
            }

            migrationChunk.SkippedAsDuplicateCount = skippedCount;
            migrationChunk.DumpResultDocCount = successCount + skippedCount;
            migrationChunk.RestoredSuccessDocCount = successCount + skippedCount;
            migrationChunk.RestoredFailedDocCount = failureCount;

            MigrationJobContext.SaveMigrationUnit(mu,true);

        }

        public void Initialize(
          Log log,
          MongoClient targetClient,
          IMongoCollection<BsonDocument> sourceCollection,
          string targetDatabase,
          string targetCollectionName,
          int pageSize)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.Initialize: targetDatabase={targetDatabase}, targetCollectionName={targetCollectionName}, pageSize={pageSize}");
            _log = log;
            _targetClient = targetClient;
            _sourceCollection = sourceCollection;
            if (_targetClient != null)
                _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);
            _pageSize=pageSize;
        }

        public async Task<TaskResult> CopyDocumentsAsync(            
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            FilterDefinition<BsonDocument> filter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.CopyDocumentsAsync: mu={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}, targetCount={targetCount}");

            ConcurrentBag<Exception> errors = new ConcurrentBag<Exception>();
            try
            {
               _log.WriteLine($"Document copy for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}] with {mu.MigrationChunks[migrationChunkIndex].Segments.Count} segments started");
                
                if(!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                    mu.BulkCopyStartedOn = DateTime.UtcNow;

				List<Task<TaskResult>> tasks = new List<Task<TaskResult>>();
				int segmentIndex = 0;
                errors = new ConcurrentBag<Exception>();
                mu.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount = 0;
                int parallelThreads = MigrationJobContext.CurrentlyActiveJob?.ParallelThreads ?? Environment.ProcessorCount * 5;
                SemaphoreSlim semaphore = new SemaphoreSlim(parallelThreads);

                foreach (var segment in mu.MigrationChunks[migrationChunkIndex].Segments)
                {
                    //back ward compatibility, Id was introduced later
                    segmentIndex++;
                    if (string.IsNullOrEmpty(segment.Id))
                        segment.Id = segmentIndex.ToString();

                    FilterDefinition<BsonDocument> combinedFilter = BuildSegmentFilter(
                        segment, 
                        filter, 
                        mu, 
                        migrationChunkIndex);

                    await semaphore.WaitAsync();
					tasks.Add(Task.Run(async () =>
					{
						try
						{
							// Directly return the awaited enum value
							return await ProcessSegmentAsync(
								segment, combinedFilter, mu,
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

                // Check for segments with missing documents after all segments are processed
                //typically caused because of online changes, we saw in some cases some documents are missed.
                await CheckForMissingDocumentsAsync(mu, migrationChunkIndex, filter, cancellationToken, isWriteSimulated);
            }
            catch (OperationCanceledException)
            {
				return TaskResult.Canceled;
            }

            if (!errors.IsEmpty)
            {
               _log.WriteLine($"Document copy for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}] encountered {errors.Count} errors, skipped {_skippedCount} during the process", LogType.Warning);                
            }

            if (mu.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount > 0 || errors.Count>0)
            {
                var validationResult = ValidateChunkDocumentCounts(mu, migrationChunkIndex);
                if (validationResult != TaskResult.Success)
                {
                    return validationResult;
                }

                MigrationJobContext.SaveMigrationUnit(mu,false);
            }
            return TaskResult.Success;
        }

        private void ResetSegmentsInChunk(MigrationChunk migrationChunk)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.ResetSegmentsInChunk: segments.Count={migrationChunk.Segments.Count}");

            foreach (var segment in migrationChunk.Segments)
            {
                segment.IsProcessed = false;
            }
        }

        private async Task<TaskResult> ProcessSegmentAsync(
            Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken,
            bool isWriteSimulated) 
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.ProcessSegmentAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}, segmentId={segment.Id}");

            string segmentId = segment.Id;
            TimeSpan backoff = TimeSpan.FromSeconds(2);

            if (segment.IsProcessed == true)
            {
                _log.WriteLine($"Skipping processed segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]", LogType.Debug);
                Interlocked.Add(ref _successCount, segment.QueryDocCount);
                return TaskResult.Success;
            }

            segment.QueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, combinedFilter,null);
            // Don't save immediately - reduces I/O overhead

            try
            {
                bool failed = false;
                segment.ResultDocCount = 0;

                // Use cursor-based processing instead of skip/limit pagination
                TaskResult result = await new RetryHelper().ExecuteTask(
                    () => ProcessSegmentWithCursorAsync(segment, combinedFilter, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, errors, cancellationToken, isWriteSimulated, segmentId),
                    (ex, attemptCount, currentBackoff) => ProcessSegmentExceptionHandler(
                        ex, attemptCount,
                        $"Processing segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]", currentBackoff
                    ),
                    _log
                );

                if (result == TaskResult.Canceled)
                {
                    return TaskResult.Canceled;
                }
                
                failed = (result != TaskResult.Success);                

                if (failed)
                {
                    _log.WriteLine($"Document copy failed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}].", LogType.Warning);
                }
                else if (_failureCount > 0 || _skippedCount > 0)
                {
                    _log.WriteLine($"Document copy completed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] with {_successCount} documents copied, {_skippedCount} documents skipped(duplicate), {_failureCount} documents failed.");
                }
                else
                {
                    _log.WriteLine($"Document copy completed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] with {_successCount} documents copied.", LogType.Debug);
                }
                segment.IsProcessed = !failed;
                MigrationJobContext.SaveMigrationUnit(mu, false);
                return failed ? TaskResult.Retry : TaskResult.Success;
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                errors.Add(ex);
                _log.WriteLine($"Document copy encountered error while processing segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}], Details: {ex}", LogType.Warning);
                return TaskResult.Retry;
            }
        }

        private Task<TaskResult> ProcessSegmentExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            _log.WriteLine($"{processName} attempt {attemptCount} failed. Error details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
            return Task.FromResult(TaskResult.Retry);
        }


        /// <summary>
        /// Process entire segment using cursor-based iteration (no skip/limit).
        /// This is much faster than skip/limit pagination for large datasets.
        /// </summary>
        private async Task<TaskResult> ProcessSegmentWithCursorAsync(
            Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken,
            bool isWriteSimulated,
            string segmentId)
        {
            var querySuccess = false;
            int batchCount = 0;
            List<BsonDocument> batch = new List<BsonDocument>(_pageSize);

            try
            {
                // Use Find with cursor - much faster than Aggregate + Skip/Limit
                var findOptions = new FindOptions<BsonDocument>
                {
                    BatchSize = _pageSize,
                    NoCursorTimeout = true
                };

                using var cursor = await _sourceCollection.FindAsync(combinedFilter, findOptions, cancellationToken);
                querySuccess = true;

                while (await cursor.MoveNextAsync(cancellationToken))
                {
                    foreach (var doc in cursor.Current)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return TaskResult.Canceled;

                        batch.Add(doc);

                        if (batch.Count >= _pageSize)
                        {
                            var writeResult = await WriteBatchAsync(batch, segment, mu, migrationChunkIndex, segmentId, isWriteSimulated, errors, cancellationToken);
                            if (writeResult == TaskResult.Canceled)
                                return TaskResult.Canceled;

                            batch.Clear();
                            batchCount++;

                            // Update progress less frequently to reduce I/O
                            if (batchCount % _saveProgressEveryNPages == 0)
                            {
                                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                            }
                        }
                    }
                }

                // Process remaining documents
                if (batch.Count > 0)
                {
                    var writeResult = await WriteBatchAsync(batch, segment, mu, migrationChunkIndex, segmentId, isWriteSimulated, errors, cancellationToken);
                    if (writeResult == TaskResult.Canceled)
                        return TaskResult.Canceled;
                }

                // Final progress update
                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                return TaskResult.Success;
            }
            catch (MongoBulkWriteException<BsonDocument> ex)
            {
                HandleBulkWriteException(ex, segment, mu, migrationChunkIndex, segmentId);
                return TaskResult.HasMore;
            }
            catch (OutOfMemoryException ex)
            {
                _log.WriteLine(
                    $"Encountered Out Of Memory exception for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Try reducing _pageSize. Details: {ex}",
                    LogType.Error);
                return TaskResult.Retry;
            }
            catch (Exception ex) when (ex.ToString().Contains("canceled."))
            {
                _log.WriteLine($"Document copy operation for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] was canceled.");
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                if (!querySuccess)
                {
                    _log.WriteLine($"Error in fetching documents for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] during command execution.", LogType.Warning);
                    return TaskResult.Retry;
                }
                errors.Add(ex);
                _log.WriteLine(
                    $"Batch processing error encountered during document copy of segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Details: {ex}.",
                    LogType.Error);
                return TaskResult.Retry;
            }
        }

        private async Task<TaskResult> WriteBatchAsync(
            List<BsonDocument> batch,
            Segment segment,
            MigrationUnit mu,
            int migrationChunkIndex,
            string segmentId,
            bool isWriteSimulated,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken)
        {
            try
            {
                if (!isWriteSimulated)
                {
                    var writeResult = await WriteDocumentsToTarget(batch, segment, cancellationToken);
                    if (writeResult == TaskResult.Canceled)
                        return TaskResult.Canceled;
                }
                else
                {
                    Interlocked.Add(ref _successCount, batch.Count);
                }
                return TaskResult.Success;
            }
            catch (MongoBulkWriteException<BsonDocument> ex)
            {
                HandleBulkWriteException(ex, segment, mu, migrationChunkIndex, segmentId);
                return TaskResult.Success; // Continue processing
            }
        }

        // Keep legacy method for backward compatibility but mark as obsolete
        [Obsolete("Use ProcessSegmentWithCursorAsync instead for better performance")]
        private async Task<TaskResult> ProcessSegmentPageAsync(Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken,
            bool isWriteSimulated,
            string segmentId,
            int pageIndex)
        {
            var querySuccess = false;
            try
            {
                querySuccess = false;

                // 1. Fetch documents from source
                var set = await _sourceCollection.Aggregate()
                    .Match(combinedFilter)
                    .Skip(pageIndex * _pageSize)
                    .Limit(_pageSize)
                    .ToListAsync(cancellationToken);

                querySuccess = true;

                if (set.Count == 0)
                    return TaskResult.Success;

                // 2. Write to target (unless simulated)
                if (!isWriteSimulated)
                {
                    var writeResult = await WriteDocumentsToTarget(set, segment, cancellationToken);
                    if (writeResult == TaskResult.Canceled)
                    {
                        return TaskResult.Canceled;
                    }
                }
                else
                {
                    Interlocked.Add(ref _successCount, set.Count);
                }
            }
            catch (MongoBulkWriteException<BsonDocument> ex)
            {
                HandleBulkWriteException(ex, segment, mu, migrationChunkIndex, segmentId);
            }
            catch (OutOfMemoryException ex)
            {
                _log.WriteLine(
                    $"Encountered Out Of Memory exception for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Try reducing _pageSize. Details: {ex}",
                    LogType.Error);
                return TaskResult.Retry;
            }
            catch (Exception ex) when (ex.ToString().Contains("canceled."))
            {
                _log.WriteLine($"Document copy operation for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] was canceled.");
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                if (!querySuccess)
                {
                    _log.WriteLine($"Error in fetching documents for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] during command execution.", LogType.Warning);
                    return TaskResult.Retry;
                }
                errors.Add(ex);
                _log.WriteLine(
                    $"Batch processing error encountered during document copy of segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Details: {ex}.",
                    LogType.Error);
                return TaskResult.Retry;
            }
            finally
            {
                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
            }

            return TaskResult.HasMore;
        }

        private void LogErrors(List<BulkWriteError> exceptions, string location)
        {
            foreach (var error in exceptions)
            {
               _log.WriteLine($"Document copy encountered write errors for {location}. Details: {error.ToString()}");

            }
        }

        private async Task CheckForMissingDocumentsAsync(
            MigrationUnit mu,
            int migrationChunkIndex,
            FilterDefinition<BsonDocument> baseFilter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.CheckForMissingDocumentsAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}");

            var migrationChunk = mu.MigrationChunks[migrationChunkIndex];
            
            // Check if any segment has QueryDocCount > ResultDocCount
            var segmentsWithMissingDocs = migrationChunk.Segments
                .Where(s => s.QueryDocCount > s.ResultDocCount)
                .ToList();

            if (!segmentsWithMissingDocs.Any())
            {
                _log.WriteLine($"Document count verified for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");
                return;
            }

            long missingDocCount = segmentsWithMissingDocs.Sum(s => s.QueryDocCount - s.ResultDocCount);
            _log.WriteLine($"Starting chunk-level comparison for {missingDocCount} missing documents in {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}].");

            // Build chunk-level filter instead of segment-specific filters
            FilterDefinition<BsonDocument> chunkFilter = baseFilter;
            
            if (migrationChunk.Segments.Count > 1)
            {
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(migrationChunk.Gte, migrationChunk.Lt, migrationChunk.DataType);
#pragma warning restore CS8604 // Possible null reference argument.
                var gte = bounds.gte;
                var lt = bounds.lt;

                FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, migrationChunk.DataType, MongoHelper.GetFilterDoc(mu.UserFilter),mu.DataTypeFor_Id.HasValue);
                
                BsonDocument? userFilter = MongoHelper.GetFilterDoc(mu.UserFilter);
                BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(migrationChunk.DataType, userFilter, mu.DataTypeFor_Id.HasValue);

                chunkFilter = Builders<BsonDocument>.Filter.And(idFilter, matchCondition);
            }

            try
            {
                MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.CompareChunkDocumentsAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}");

                await CompareChunkDocumentsAsync(migrationChunk, mu, migrationChunkIndex, chunkFilter, cancellationToken, isWriteSimulated);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error while chunk-level comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]: {ex}", LogType.Warning);
            }

            MigrationJobContext.SaveMigrationUnit(mu,false);
        }

        private async Task CompareChunkDocumentsAsync(
            MigrationChunk migrationChunk,
            MigrationUnit mu,
            int migrationChunkIndex,
            FilterDefinition<BsonDocument> chunkFilter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.CompareChunkDocumentsAsync: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}");

            var missingDocuments = new List<BsonDocument>();
            int pageIndex = 0;
            const int comparisonPageSize = 100; // Smaller page size for comparison
            long processedCount = 0;
            long foundMissingCount = 0;
            
            _log.WriteLine($"Starting chunk-level document comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");

            while (!cancellationToken.IsCancellationRequested)
            {
                // Get documents from source using chunk filter
                var sourceDocuments = await _sourceCollection.Aggregate()
                    .Match(chunkFilter)
                    .Skip(pageIndex * comparisonPageSize)
                    .Limit(comparisonPageSize)
                    .ToListAsync(cancellationToken);

                if (sourceDocuments.Count == 0)
                    break;

                processedCount += sourceDocuments.Count;

                // Batch check documents in target using $in operator for better performance
                var documentIds = sourceDocuments
                    .Where(doc => doc.Contains("_id"))
                    .Select(doc => doc["_id"])
                    .ToList();

                if (documentIds.Count > 0)
                {
                    foundMissingCount += await ProcessMissingDocuments(
                        sourceDocuments, 
                        documentIds, 
                        migrationChunk, 
                        isWriteSimulated, 
                        cancellationToken, 
                        missingDocuments);
                }

                pageIndex++;

                // Log progress every 10 pages
                if (pageIndex % 10 == 0)
                {
                    _log.ShowInMonitor($"Processed {processedCount} documents, found {foundMissingCount} missing documents in chunk {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");
                }

                // Limit the number of pages to prevent infinite loops
                if (pageIndex > 10000)
                {
                    _log.WriteLine($"Reached maximum page limit while comparing chunk {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]", LogType.Warning);
                    break;
                }
            }

            if (foundMissingCount > 0)
            {
                _log.WriteLine($"Found and processed {foundMissingCount} missing documents in chunk {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}] out of {processedCount} total documents checked");
                migrationChunk.SkippedAsDuplicateCount = _skippedCount;
                migrationChunk.DumpResultDocCount = _successCount + _skippedCount;
                migrationChunk.RestoredSuccessDocCount = _successCount + _skippedCount;
                migrationChunk.RestoredFailedDocCount = _failureCount;
            }
            else
            {
                _log.WriteLine($"No missing documents found during chunk-level comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");
            }
        }

        private void UpdateSegmentResultCount(MigrationChunk migrationChunk)
        {
            // Simple approach: increment the first segment that might contain this document
            // This is an approximation since we're not using segment-specific logic
            var segmentToUpdate = migrationChunk.Segments.FirstOrDefault(s => s.QueryDocCount > s.ResultDocCount);
            if (segmentToUpdate != null)
            {
                segmentToUpdate.ResultDocCount++;
            }
        }

        private FilterDefinition<BsonDocument> BuildSegmentFilter(
            Segment segment,
            FilterDefinition<BsonDocument> filter,
            MigrationUnit mu,
            int migrationChunkIndex)
        {
            FilterDefinition<BsonDocument> combinedFilter;

            // If only 1 segment, it doesn't have its own bounds - it covers the entire chunk
            // Use the chunk-level filter that was passed in
            if (mu.MigrationChunks[migrationChunkIndex].Segments.Count == 1 )
            {
                // Single segment covering entire chunk - use the chunk-level filter
                var userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
                if (userFilterDoc != null && userFilterDoc.ElementCount > 0)
                {
                    var userFilter = new BsonDocumentFilterDefinition<BsonDocument>(userFilterDoc);
                    combinedFilter = Builders<BsonDocument>.Filter.And(filter, userFilter);
                }
                else
                {
                    combinedFilter = filter;
                }
            }
            else
            {
                // Multiple segments - each segment has its own bounds
                // Generate a segment-specific filter
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(segment.Gte, segment.Lt, mu.MigrationChunks[migrationChunkIndex].DataType);
#pragma warning restore CS8604 // Possible null reference argument.
                var gte = bounds.gte;
                var lt = bounds.lt;

                combinedFilter = MongoHelper.GenerateQueryFilter(gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
            }

            return combinedFilter;
        }

        private TaskResult ValidateChunkDocumentCounts(MigrationUnit mu, int migrationChunkIndex)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.ValidateChunkDocumentCounts: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}");

#pragma warning disable CS8604 // Possible null reference argument.
            var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[migrationChunkIndex].Gte, mu.MigrationChunks[migrationChunkIndex].Lt, mu.MigrationChunks[migrationChunkIndex].DataType);
            var gte = bounds.gte;
            var lt = bounds.lt;

            _log.WriteLine($"Counting documents on target {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}].");

            try
            {
                mu.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
                mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Encountered error while counting documents on target for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]. Chunk will be reprocessed. Details: {ex}", LogType.Warning);
                ResetSegmentsInChunk(mu.MigrationChunks[migrationChunkIndex]);
                return TaskResult.Retry;
            }

            if (mu.MigrationChunks[migrationChunkIndex].DocCountInTarget == mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount)
            {
                mu.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount = 0;
                _log.WriteLine($"No documents missing in {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]. Target count: {mu.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
            }
            else
            {
                _log.WriteLine($"Count mismatch in {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]. Chunk will be reprocessed.", LogType.Warning);
                return TaskResult.Retry;
            }

            return TaskResult.Success;
        }

        private async Task<TaskResult> WriteDocumentsToTarget(List<BsonDocument> documents, Segment segment, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }

            if (!MongoHelper.IsCosmosRUEndpoint(_targetCollection))
            {
                return await WriteBulkDocuments(documents, segment, cancellationToken);
            }
            else
            {
                return await WriteDocumentsWithInsertMany(documents, segment, cancellationToken);
            }
        }

        private async Task<TaskResult> WriteBulkDocuments(List<BsonDocument> documents, Segment segment, CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.WriteBulkDocuments: documents.Count={documents.Count}, segmentId={segment.Id}");

            if (cancellationToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }

            try
            {
                var insertModels = documents.Select(doc =>
                {
                    if (doc.Contains("_id") && doc["_id"].IsObjectId)
                        doc["_id"] = doc["_id"].AsObjectId;
                    return new InsertOneModel<BsonDocument>(doc);
                }).ToList();

                var result = await _targetCollection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false }, cancellationToken);
                Interlocked.Add(ref _successCount, result.InsertedCount);
                segment.ResultDocCount += result.InsertedCount;
                
                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                return TaskResult.Canceled;
            }
        }

        private async Task<TaskResult> WriteDocumentsWithInsertMany(List<BsonDocument> documents, Segment segment, CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.WriteDocumentsWithInsertMany: documents.Count={documents.Count}, segmentId={segment.Id}");

            if (cancellationToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }

            try
            {
                var docsToInsert = documents.Select(doc =>
                {
                    if (doc.Contains("_id") && doc["_id"].IsObjectId)
                        doc["_id"] = doc["_id"].AsObjectId;
                    return doc;
                }).ToList();

                await _targetCollection.InsertManyAsync(docsToInsert, new InsertManyOptions { IsOrdered = false }, cancellationToken);
                Interlocked.Add(ref _successCount, docsToInsert.Count);
                segment.ResultDocCount += docsToInsert.Count;
                
                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                return TaskResult.Canceled;
            }
        }

        private void HandleBulkWriteException(
            MongoBulkWriteException<BsonDocument> ex,
            Segment segment,
            MigrationUnit mu,
            int migrationChunkIndex,
            string segmentId)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.HandleBulkWriteException: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}, segmentId={segmentId}");

            long bulkInserted = ex.Result?.InsertedCount ?? 0;
            Interlocked.Add(ref _successCount, bulkInserted);
            segment.ResultDocCount += bulkInserted;

            int dupeCount = ex.WriteErrors.Count(e => e.Code == 11000);
            if (dupeCount > 0)
            {
                Interlocked.Add(ref _skippedCount, dupeCount);
                segment.ResultDocCount += dupeCount;
            }

            int otherErrors = ex.WriteErrors.Count(e => e.Code != 11000);
            if (otherErrors > 0)
            {
                Interlocked.Add(ref _failureCount, otherErrors);
                _log.WriteLine(
                    $"Document copy encountered errors for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]",
                    LogType.Error);
                LogErrors(
                    ex.WriteErrors.Where(e => e.Code != 11000).ToList(),
                    $"segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]"
                );
            }
        }

        private async Task<long> ProcessMissingDocuments(
            List<BsonDocument> sourceDocuments,
            List<BsonValue> documentIds,
            MigrationChunk migrationChunk,
            bool isWriteSimulated,
            CancellationToken cancellationToken,
            List<BsonDocument> missingDocuments)
        {

            long foundMissingCount = 0;
            
            _log.WriteLine($"Processing {documentIds.Count} individual documents as bulk insert failed.");

            var targetFilter = Builders<BsonDocument>.Filter.In("_id", documentIds);
            var existingTargetDocs = await _targetCollection.Find(targetFilter)
                .Project(Builders<BsonDocument>.Projection.Include("_id"))
                .ToListAsync(cancellationToken);

            var existingIds = new HashSet<BsonValue>(existingTargetDocs.Select(doc => doc["_id"]));

            
            foreach (var sourceDoc in sourceDocuments)
            {
                _log.ShowInMonitor("Processing document with _id : " + sourceDoc["_id"].ToString());
                if (cancellationToken.IsCancellationRequested)
                    break;

                if (!sourceDoc.Contains("_id"))
                    continue;

                var idValue = sourceDoc["_id"];

                if (!existingIds.Contains(idValue))
                {
                    missingDocuments.Add(sourceDoc);
                    foundMissingCount++;
                    
                    if (!isWriteSimulated)
                    {
                        await InsertMissingDocument(sourceDoc, idValue, migrationChunk);
                    }
                }
            }

            MigrationJobContext.AddVerboseLog($"Count after loop, Failure: {_failureCount}, Sucess: {_successCount}");
            
            return foundMissingCount;
        }

        private async Task InsertMissingDocument(BsonDocument sourceDoc, BsonValue idValue, MigrationChunk migrationChunk)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.InsertMissingDocument: _id={idValue}");
            try
            {
                await _targetCollection.InsertOneAsync(sourceDoc, cancellationToken: CancellationToken.None);
                Interlocked.Increment(ref _successCount);
                Interlocked.Decrement(ref _failureCount);
                UpdateSegmentResultCount(migrationChunk);
            }
            catch (MongoDuplicateKeyException)
            {
                // Document was inserted by another process, skip
                Interlocked.Increment(ref _skippedCount);
                Interlocked.Decrement(ref _failureCount);
                UpdateSegmentResultCount(migrationChunk);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Failed to insert missing document with _id: {idValue}. Details: {ex}", LogType.Error);
                Interlocked.Increment(ref _failureCount);
            }
        }
    }
}

