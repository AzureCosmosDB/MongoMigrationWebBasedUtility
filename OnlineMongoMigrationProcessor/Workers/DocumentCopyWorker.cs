using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
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
        private IMongoCollection<RawBsonDocument> _sourceRawCollection;
        private IMongoCollection<RawBsonDocument> _targetRawCollection;
        private int _pageSize = 5000; // Increased from 500 for better throughput
        private int _rawFetchByIdChunkSize = 500;
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
            if (_sourceCollection != null)
                _sourceRawCollection = _sourceCollection.Database.GetCollection<RawBsonDocument>(_sourceCollection.CollectionNamespace.CollectionName);
            if (_targetClient != null)
            {
                _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);
                _targetRawCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<RawBsonDocument>(targetCollectionName);
            }
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
            try
            {
                _log.WriteLine(
                    $"[DEBUG] Segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] starting fast-path raw cursor processing.",
                    LogType.Debug);

                var fastPathResult = await TryProcessSegmentWithRawCursorAsync(
                    segment,
                    combinedFilter,
                    mu,
                    migrationChunkIndex,
                    basePercent,
                    contribFactor,
                    targetCount,
                    errors,
                    cancellationToken,
                    isWriteSimulated,
                    segmentId);

                if (fastPathResult.HasValue)
                {
                    _log.WriteLine(
                        $"[DEBUG] Segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] completed via fast-path raw cursor with result {fastPathResult.Value}.",
                        LogType.Debug);
                    return fastPathResult.Value;
                }

                _log.WriteLine(
                    $"[DEBUG] Segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] switching to fallback _id-based fetch processing.",
                    LogType.Debug);

                return await ProcessSegmentWithIdFallbackAsync(
                    segment,
                    combinedFilter,
                    mu,
                    migrationChunkIndex,
                    basePercent,
                    contribFactor,
                    targetCount,
                    errors,
                    cancellationToken,
                    isWriteSimulated,
                    segmentId);
            }
            catch (MongoBulkWriteException<BsonDocument> ex)
            {
                HandleBulkWriteException(ex, segment, mu, migrationChunkIndex, segmentId);
                return TaskResult.HasMore;
            }
            catch (MongoBulkWriteException<RawBsonDocument> ex)
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
                errors.Add(ex);
                _log.WriteLine(
                    $"Batch processing error encountered during document copy of segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Details: {ex}.",
                    LogType.Error);
                return TaskResult.Retry;
            }
        }

        private async Task<TaskResult?> TryProcessSegmentWithRawCursorAsync(
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
            int batchCount = 0;
            var rawFindOptions = new FindOptions<RawBsonDocument>
            {
                BatchSize = _pageSize,
                NoCursorTimeout = false
            };

            try
            {
                var renderedFilter = RenderFilterForRawCollection(combinedFilter);
                var rawFilter = new BsonDocumentFilterDefinition<RawBsonDocument>(renderedFilter);
                List<RawBsonDocument> rawBatch = new List<RawBsonDocument>(_pageSize);

                using var rawCursor = await _sourceRawCollection.FindAsync(rawFilter, rawFindOptions, cancellationToken);

                while (await rawCursor.MoveNextAsync(cancellationToken))
                {
                    foreach (var rawDoc in rawCursor.Current)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return TaskResult.Canceled;

                        rawBatch.Add(rawDoc);

                        if (rawBatch.Count >= _pageSize)
                        {
                            var writeResult = await WriteRawBatchAsync(rawBatch, segment, mu, migrationChunkIndex, segmentId, isWriteSimulated, errors, cancellationToken);
                            if (writeResult == TaskResult.Canceled)
                                return TaskResult.Canceled;

                            rawBatch.Clear();
                            batchCount++;

                            if (batchCount % _saveProgressEveryNPages == 0)
                            {
                                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                            }
                        }
                    }
                }

                if (rawBatch.Count > 0)
                {
                    var writeResult = await WriteRawBatchAsync(rawBatch, segment, mu, migrationChunkIndex, segmentId, isWriteSimulated, errors, cancellationToken);
                    if (writeResult == TaskResult.Canceled)
                        return TaskResult.Canceled;
                }

                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                return TaskResult.Success;
            }
            catch (Exception rawReadEx) when (
                rawReadEx is not OperationCanceledException &&
                rawReadEx is not OutOfMemoryException &&
                rawReadEx is not MongoBulkWriteException<BsonDocument> &&
                rawReadEx is not MongoBulkWriteException<RawBsonDocument>)
            {
                if (!ShouldFallbackToIdBasedFetch(rawReadEx))
                {
                    throw;
                }

                _log.WriteLine(
                    $"Raw cursor read failed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Falling back to _id-based fetch. Details: {rawReadEx.Message}",
                    LogType.Warning);
                _log.WriteLine(
                    $"[DEBUG] Segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] fast-path failure matched fallback policy ({rawReadEx.GetType().Name}).",
                    LogType.Debug);
                return null;
            }
        }

        private static bool ShouldFallbackToIdBasedFetch(Exception ex)
        {
            if (ex is MongoCommandException || ex is MongoQueryException || ex is FormatException)
            {
                return true;
            }

            if (ex is InvalidOperationException invalidOperationException)
            {
                var message = invalidOperationException.Message ?? string.Empty;
                return message.Contains("Duplicate element name", StringComparison.OrdinalIgnoreCase)
                    || message.Contains("cannot be deserialized", StringComparison.OrdinalIgnoreCase)
                    || message.Contains("deserializ", StringComparison.OrdinalIgnoreCase);
            }

            return false;
        }

        private static BsonDocument RenderFilterForRawCollection(FilterDefinition<BsonDocument> filter)
        {
            var serializerRegistry = BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<BsonDocument>();
#if LEGACY_MONGODB_DRIVER
            return filter.Render(documentSerializer, serializerRegistry);
#else
            var renderArgs = new RenderArgs<BsonDocument>(documentSerializer, serializerRegistry);
            return filter.Render(renderArgs);
#endif
        }

        private async Task<TaskResult> ProcessSegmentWithIdFallbackAsync(
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
            int batchCount = 0;
            List<BsonValue> idBatch = new List<BsonValue>(_pageSize);

            try
            {
                _log.WriteLine(
                    $"[DEBUG] Segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] entered fallback _id-based fetch path.",
                    LogType.Debug);

                var findOptions = new FindOptions<BsonDocument>
                {
                    BatchSize = _pageSize,
                    NoCursorTimeout = false,
                    Projection = Builders<BsonDocument>.Projection.Include("_id")
                };

                using var cursor = await _sourceCollection.FindAsync(combinedFilter, findOptions, cancellationToken);

                while (await cursor.MoveNextAsync(cancellationToken))
                {
                    foreach (var doc in cursor.Current)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return TaskResult.Canceled;

                        if (!doc.TryGetValue("_id", out var idValue))
                            continue;

                        idBatch.Add(idValue);

                        if (idBatch.Count >= _pageSize)
                        {
                            var rawDocs = await FetchRawDocumentsByIdsAsync(idBatch, cancellationToken);
                            var writeResult = await WriteRawBatchAsync(rawDocs, segment, mu, migrationChunkIndex, segmentId, isWriteSimulated, errors, cancellationToken);
                            if (writeResult == TaskResult.Canceled)
                                return TaskResult.Canceled;

                            idBatch.Clear();
                            batchCount++;

                            if (batchCount % _saveProgressEveryNPages == 0)
                            {
                                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                            }
                        }
                    }
                }

                if (idBatch.Count > 0)
                {
                    var rawDocs = await FetchRawDocumentsByIdsAsync(idBatch, cancellationToken);
                    var writeResult = await WriteRawBatchAsync(rawDocs, segment, mu, migrationChunkIndex, segmentId, isWriteSimulated, errors, cancellationToken);
                    if (writeResult == TaskResult.Canceled)
                        return TaskResult.Canceled;
                }

                UpdateProgress(segmentId, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                _log.WriteLine(
                    $"[DEBUG] Segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] completed fallback _id-based fetch path.",
                    LogType.Debug);
                return TaskResult.Success;
            }
            catch (Exception ex) when (
                ex is not OperationCanceledException &&
                ex is not OutOfMemoryException &&
                ex is not MongoBulkWriteException<BsonDocument> &&
                ex is not MongoBulkWriteException<RawBsonDocument>)
            {
                _log.WriteLine($"Fallback segment processing failed for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] during command execution. Details: {ex}", LogType.Warning);
                return TaskResult.Retry;
            }
        }

        private async Task<List<RawBsonDocument>> FetchRawDocumentsByIdsAsync(List<BsonValue> documentIds, CancellationToken cancellationToken)
        {
            if (documentIds == null || documentIds.Count == 0)
                return new List<RawBsonDocument>();

            var documents = new List<RawBsonDocument>(documentIds.Count);
            for (int index = 0; index < documentIds.Count; index += _rawFetchByIdChunkSize)
            {
                var idChunk = documentIds.Skip(index).Take(_rawFetchByIdChunkSize).ToList();
                var filter = Builders<RawBsonDocument>.Filter.In("_id", idChunk);
                var chunkDocs = await _sourceRawCollection.Find(filter).ToListAsync(cancellationToken);
                documents.AddRange(chunkDocs);
            }

            return documents;
        }

        private async Task<TaskResult> WriteRawBatchAsync(
            List<RawBsonDocument> batch,
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
                    var writeResult = await WriteRawDocumentsToTarget(batch, segment, cancellationToken);
                    if (writeResult == TaskResult.Canceled)
                        return TaskResult.Canceled;
                }
                else
                {
                    Interlocked.Add(ref _successCount, batch.Count);
                }
                return TaskResult.Success;
            }
            catch (MongoBulkWriteException<RawBsonDocument> ex)
            {
                HandleBulkWriteException(ex, segment, mu, migrationChunkIndex, segmentId);
                return TaskResult.Success; // Continue processing
            }
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
                // Parse Lt and Lte separately to maintain correct query semantics
                var bounds = SamplePartitioner.GetChunkBounds(migrationChunk.Gte ?? "", migrationChunk.Lt ?? "", migrationChunk.Lte ?? "", migrationChunk.DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;
                var lte = bounds.lte;

                FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, lte, migrationChunk.DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
#pragma warning restore CS8604 // Possible null reference argument.
                
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

            int pageIndex = 0;
            const int comparisonPageSize = 100; // Smaller page size for comparison
            long processedCount = 0;
            long foundMissingCount = 0;
            
            _log.WriteLine($"Starting chunk-level document comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");

            while (!cancellationToken.IsCancellationRequested)
            {
                // Compare by _id only to avoid deserializing full documents that may contain duplicate field names.
                var sourceDocuments = await _sourceCollection.Find(chunkFilter)
                    .Project(Builders<BsonDocument>.Projection.Include("_id"))
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
                        documentIds, 
                        migrationChunk, 
                        isWriteSimulated, 
                        cancellationToken);
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
                // Parse Lt and Lte separately to maintain correct query semantics
                var bounds = SamplePartitioner.GetChunkBounds(segment.Gte ?? "", segment.Lt ?? "", segment.Lte ?? "", mu.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;
                var lte = bounds.lte;

                combinedFilter = MongoHelper.GenerateQueryFilter(gte, lt, lte, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
#pragma warning restore CS8604 // Possible null reference argument.
            }

            return combinedFilter;
        }

        private TaskResult ValidateChunkDocumentCounts(MigrationUnit mu, int migrationChunkIndex)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.ValidateChunkDocumentCounts: collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}");

#pragma warning disable CS8604 // Possible null reference argument.
            // Parse Lt and Lte separately to maintain correct query semantics
            var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[migrationChunkIndex].Gte ?? "", mu.MigrationChunks[migrationChunkIndex].Lt ?? "", mu.MigrationChunks[migrationChunkIndex].Lte ?? "", mu.MigrationChunks[migrationChunkIndex].DataType);
            var gte = bounds.gte;
            var lt = bounds.lt;
            var lte = bounds.lte;

            _log.WriteLine($"Counting documents on target {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}].");

            try
            {
                mu.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, lte, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
                mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, gte, lt, lte, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);
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


        private async Task<TaskResult> WriteRawDocumentsToTarget(List<RawBsonDocument> documents, Segment segment, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }

            if (!MongoHelper.IsCosmosRUEndpoint(_targetCollection))
            {
                return await WriteBulkRawDocuments(documents, segment, cancellationToken);
            }
            else
            {
                return await WriteRawDocumentsWithInsertMany(documents, segment, cancellationToken);
            }
        }



        private async Task<TaskResult> WriteBulkRawDocuments(List<RawBsonDocument> documents, Segment segment, CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.WriteBulkRawDocuments: documents.Count={documents.Count}, segmentId={segment.Id}");

            if (cancellationToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }

            try
            {
                var insertModels = documents.Select(doc => new InsertOneModel<RawBsonDocument>(doc)).ToList();

                var result = await _targetRawCollection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false }, cancellationToken);
                Interlocked.Add(ref _successCount, result.InsertedCount);
                segment.ResultDocCount += result.InsertedCount;

                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                return TaskResult.Canceled;
            }
        }

        private async Task<TaskResult> WriteRawDocumentsWithInsertMany(List<RawBsonDocument> documents, Segment segment, CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.WriteRawDocumentsWithInsertMany: documents.Count={documents.Count}, segmentId={segment.Id}");

            if (cancellationToken.IsCancellationRequested)
            {
                return TaskResult.Canceled;
            }

            try
            {
                await _targetRawCollection.InsertManyAsync(documents, new InsertManyOptions { IsOrdered = false }, cancellationToken);
                Interlocked.Add(ref _successCount, documents.Count);
                segment.ResultDocCount += documents.Count;

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

        private void HandleBulkWriteException(
            MongoBulkWriteException<RawBsonDocument> ex,
            Segment segment,
            MigrationUnit mu,
            int migrationChunkIndex,
            string segmentId)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.HandleBulkWriteException(Raw): collection={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={migrationChunkIndex}, segmentId={segmentId}");

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
            List<BsonValue> documentIds,
            MigrationChunk migrationChunk,
            bool isWriteSimulated,
            CancellationToken cancellationToken)
        {

            long foundMissingCount = 0;
            
            _log.WriteLine($"Processing {documentIds.Count} candidate missing documents individually during chunk reconciliation.");

            var targetFilter = Builders<BsonDocument>.Filter.In("_id", documentIds);
            var existingTargetDocs = await _targetCollection.Find(targetFilter)
                .Project(Builders<BsonDocument>.Projection.Include("_id"))
                .ToListAsync(cancellationToken);

            var existingIds = new HashSet<BsonValue>(existingTargetDocs.Select(doc => doc["_id"]));

            foreach (var idValue in documentIds)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                if (!existingIds.Contains(idValue))
                {
                    foundMissingCount++;
                    _log.ShowInMonitor("Processing missing document with _id : " + idValue.ToString());
                    
                    if (!isWriteSimulated)
                    {
                        await InsertMissingDocument(idValue, migrationChunk);
                    }
                }
            }

            MigrationJobContext.AddVerboseLog($"Count after loop, Failure: {_failureCount}, Sucess: {_successCount}");
            
            return foundMissingCount;
        }

        private async Task InsertMissingDocument(BsonValue idValue, MigrationChunk migrationChunk)
        {
            MigrationJobContext.AddVerboseLog($"DocumentCopyWorker.InsertMissingDocument: _id={idValue}");
            try
            {
                var sourceFilter = Builders<RawBsonDocument>.Filter.Eq("_id", idValue);
                var sourceDoc = await _sourceRawCollection.Find(sourceFilter).FirstOrDefaultAsync(CancellationToken.None);

                if (sourceDoc == null)
                {
                    _log.WriteLine($"Missing source document could not be fetched for _id: {idValue}", LogType.Warning);
                    return;
                }

                await _targetRawCollection.InsertOneAsync(sourceDoc, cancellationToken: CancellationToken.None);
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

