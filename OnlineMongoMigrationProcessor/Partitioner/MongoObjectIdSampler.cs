using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Partitioner
{
    public class ObjectIdRange
    {
        public ObjectId MinId { get; set; }
        public ObjectId MaxId { get; set; }
        public DateTime MinTimestamp => MinId.CreationTime;
        public DateTime MaxTimestamp => MaxId.CreationTime;
    }

    public class MongoObjectIdSampler
    {
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly int _timeoutSeconds;

        public MongoObjectIdSampler(IMongoCollection<BsonDocument> collection, int timeoutSeconds = 60000)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler: Constructor called, timeoutSeconds={timeoutSeconds}");
            _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            _timeoutSeconds = timeoutSeconds;
        }

        /// <summary>
        /// Retrieves the smallest and largest ObjectId in the collection with extended timeout.
        /// </summary>
        private async Task<ObjectIdRange> GetObjectIdRangeAsync(BsonDocument? filter)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler.GetObjectIdRangeAsync");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));

            // Smallest ObjectId
            var minDoc = await _collection
                .Find(filter)
                .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                .Limit(1)
                .FirstOrDefaultAsync(cts.Token);

            // Largest ObjectId
            var maxDoc = await _collection
                .Find(FilterDefinition<BsonDocument>.Empty)
                .Sort(Builders<BsonDocument>.Sort.Descending("_id"))
                .Limit(1)
                .FirstOrDefaultAsync(cts.Token);

            if (minDoc == null || maxDoc == null)
                throw new InvalidOperationException("Collection is empty or inaccessible.");

            return new ObjectIdRange
            {
                MinId = minDoc["_id"].AsObjectId,
                MaxId = maxDoc["_id"].AsObjectId
            };
        }

        /// <summary>
        /// Tries to get the document count with retry logic for timeout handling.
        /// </summary>
        /// <param name="filter">The filter to apply (BsonDocument)</param>
        /// <param name="maxRetries">Maximum number of retry attempts (default: 3)</param>
        /// <returns>Tuple of (count, timedOut) - if timedOut is true, count is -1</returns>
        private async Task<(long count, bool timedOut)> TryGetCountWithRetryAsync(BsonDocument filter, int maxRetries = 3)
        {
            return await TryGetCountWithRetryAsync((FilterDefinition<BsonDocument>)filter, maxRetries);
        }

        /// <summary>
        /// Tries to get the document count with retry logic for timeout handling.
        /// </summary>
        /// <param name="filter">The filter to apply (FilterDefinition)</param>
        /// <param name="maxRetries">Maximum number of retry attempts (default: 3)</param>
        /// <returns>Tuple of (count, timedOut) - if timedOut is true, count is -1</returns>
        private async Task<(long count, bool timedOut)> TryGetCountWithRetryAsync(FilterDefinition<BsonDocument> filter, int maxRetries = 3)
        {
            MigrationJobContext.AddVerboseLog($"TryGetCountWithRetryAsync: maxRetries={maxRetries}");

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));
                    var count = await _collection.CountAsync(filter, cancellationToken: cts.Token);
                    return (count, false);
                }
                catch (OperationCanceledException)
                {
                    MigrationJobContext.AddVerboseLog($"CountAsync attempt {attempt}/{maxRetries} timed out");
                }
                catch (MongoExecutionTimeoutException)
                {
                    MigrationJobContext.AddVerboseLog($"CountAsync attempt {attempt}/{maxRetries} timed out (MongoExecutionTimeoutException)");
                }
            }

            MigrationJobContext.AddVerboseLog($"CountAsync timed out after {maxRetries} retries");
            return (-1, true);
        }

        /// <summary>
        /// Generates boundaries by splitting the ObjectId range when count cannot be obtained.
        /// </summary>
        private async Task<List<BsonValue>> GenerateBoundariesFromObjectIdRangeAsync(int count, BsonDocument filter, int maxRecordsPerRange)
        {
            MigrationJobContext.AddVerboseLog($"GenerateBoundariesFromObjectIdRangeAsync: count={count}, maxRecordsPerRange={maxRecordsPerRange}");

            var range = await GetObjectIdRangeAsync(filter);

            // Use SplitLargeRange with estimated large count to force splitting into 'count' chunks
            long estimatedCount = (long)count * maxRecordsPerRange;

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));
            var splitBoundaries = await SplitLargeRange(
                range.MinId,
                range.MaxId,
                estimatedCount,
                filter,
                maxRecordsPerRange,
                cts.Token);

            // Add the end boundary if not already included
            if (splitBoundaries.Count > 0 && !splitBoundaries.Last().Equals(range.MaxId))
            {
                splitBoundaries.Add(range.MaxId);
            }

            return splitBoundaries;
        }

        /// <summary>
        /// Generates time-based equidistant ObjectIds, then validates and adjusts ranges to ensure 
        /// each range has 1K-1M records. Returns empty list if total records < 1K.
        /// </summary>
        public async Task<List<BsonValue>> GenerateEquidistantObjectIdsAsync(int count, BsonDocument filter, MigrationSettings settings)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler.GenerateEquidistantObjectIdsAsync: count={count}, ObjectIdPartitioner={settings.ObjectIdPartitioner}");
            const int MIN_RECORDS_PER_RANGE = 1000;
            const int MAX_RECORDS_PER_RANGE = 25000000;

            // Try to get total count with retry logic
            var (totalCount, countTimedOut) = await TryGetCountWithRetryAsync(filter);

            // If count timed out, fall back to splitting based on ObjectId range
            if (countTimedOut)
            {
                return await GenerateBoundariesFromObjectIdRangeAsync(count, filter, MAX_RECORDS_PER_RANGE);
            }

            // Create a cancellation token for the remaining operations
            using var operationCts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));
                        
            // Return empty list if total records < 1K
            if (totalCount < MIN_RECORDS_PER_RANGE)
            {
                return new List<BsonValue>();
            }

            // Use pagination to directly sample ObjectIds at regular intervals
            if (settings.ObjectIdPartitioner == PartitionerType.UsePagination)
            {
                long pageSize = totalCount / count;
                return await GeneratePaginationBasedBoundaries(filter, totalCount, pageSize, operationCts.Token);
            }            

            // Generate initial time-based equidistant boundaries
            var timeBased = await GenerateTimeBasedBoundaries(count, filter, operationCts.Token);

            if (timeBased.Count < 2)
            {
                return timeBased;
            }
            
            if (settings.ObjectIdPartitioner == PartitionerType.UseAdjustedTimeBoundaries)
            {
                // Validate and adjust ranges based on actual record counts
                var adjusted = await ValidateAndAdjustRanges(timeBased, filter, totalCount, MIN_RECORDS_PER_RANGE, MAX_RECORDS_PER_RANGE, operationCts.Token);
                return adjusted;
            }
            else
            {
                return timeBased;
            }
        }

        /// <summary>
        /// Generates time-based equidistant ObjectIds (original algorithm).
        /// </summary>
        private async Task<List<BsonValue>> GenerateTimeBasedBoundaries(int count, BsonDocument filter, CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler.GenerateTimeBasedBoundaries: count={count}");
            var result = new List<BsonValue>();
            if (count < 2)
            {
                return result;
            }

            var range = await GetObjectIdRangeAsync(filter);

            var minBytes = range.MinId.ToByteArray();
            var maxBytes = range.MaxId.ToByteArray();

            var minInt = new BigInteger(minBytes, isUnsigned: true, isBigEndian: true);
            var maxInt = new BigInteger(maxBytes, isUnsigned: true, isBigEndian: true);
            var step = (maxInt - minInt) / (count - 1);

            for (int i = 0; i < count; i++)
            {
                var nextInt = minInt + (step * i);
                var nextBytes = nextInt.ToByteArray(isUnsigned: true, isBigEndian: true);

                // Ensure exactly 12 bytes
                if (nextBytes.Length < 12)
                {
                    var padded = new byte[12];
                    Array.Copy(nextBytes, 0, padded, 12 - nextBytes.Length, nextBytes.Length);
                    nextBytes = padded;
                }
                else if (nextBytes.Length > 12)
                {
                    nextBytes = nextBytes[^12..];
                }

                result.Add(new ObjectId(nextBytes));
            }

            return result;
        }

        /// <summary>
        /// Generates boundaries by paginating through the collection and sampling ObjectIds 
        /// at regular record intervals (e.g., every 100K records).
        /// Uses progressive $gt filters to avoid loading all documents into memory.
        /// </summary>
        private async Task<List<BsonValue>> GeneratePaginationBasedBoundaries(
            BsonDocument filter,
            long totalCount,
            long recordsPerRange,
            CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler.GeneratePaginationBasedBoundaries: totalCount={totalCount}, recordsPerRange={recordsPerRange}");
            var result = new List<BsonValue>();
            
            // Calculate number of ranges needed
            int numRanges = (int)Math.Ceiling((double)totalCount / recordsPerRange);
            
            if (numRanges < 2)
            {
                return result; // Not enough data to create ranges
            }

            BsonValue? lastId = null;

            // Sample ObjectIds at regular intervals
            for (int i = 0; i < numRanges; i++)
            {
                FilterDefinition<BsonDocument> rangeFilter;
                
                if (lastId == null)
                {
                    // First iteration: use original filter
                    rangeFilter = filter;
                }
                else
                {
                    // Subsequent iterations: add $gt filter to continue from last ID
                    rangeFilter = Builders<BsonDocument>.Filter.And(
                        filter,
                        Builders<BsonDocument>.Filter.Gt("_id", lastId)
                    );
                }
                
                // Skip to the next boundary position and get the document
                var doc = await _collection
                    .Find(rangeFilter)
                    .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                    .Skip((int)recordsPerRange - 1) // Skip to get the boundary document
                    .Limit(1)
                    .FirstOrDefaultAsync(cancellationToken);
                
                if (doc != null && doc.Contains("_id"))
                {
                    lastId = doc["_id"];
                    result.Add(lastId);
                }
                else
                {
                    break; // No more documents
                }
            }

            return result;
        }

        /// <summary>
        /// Validates ranges and merges/splits them to ensure each range has 10K-200K records.
        /// Uses efficient counting without sorting the entire collection.
        /// </summary>
        private async Task<List<BsonValue>> ValidateAndAdjustRanges(
            List<BsonValue> boundaries,
            BsonDocument filter,
            long totalCount,
            int minRecords,
            int maxRecords,
            CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler.ValidateAndAdjustRanges: boundaries.Count={boundaries.Count}, totalCount={totalCount}, minRecords={minRecords}, maxRecords={maxRecords}");
            if (boundaries.Count < 2)
                return boundaries;

            var adjustedBoundaries = new List<BsonValue>();
            var rangeStats = new List<(BsonValue startId, BsonValue endId, long count)>();

            // Step 1: Count records in each initial range
            for (int i = 0; i < boundaries.Count - 1; i++)
            {
                var startId = boundaries[i];
                var endId = boundaries[i + 1];
                
                var rangeFilter = Builders<BsonDocument>.Filter.And(
                    filter,
                    Builders<BsonDocument>.Filter.Gte("_id", startId),
                    Builders<BsonDocument>.Filter.Lt("_id", endId)
                );
                
                var (count, timedOut) = await TryGetCountWithRetryAsync(rangeFilter);
                if (timedOut)
                {
                    // If count timed out, split the range as it's likely too large
                    MigrationJobContext.AddVerboseLog($"Range {i} count timed out, splitting range into smaller chunks");
                    
                    // Estimate a large count to trigger splitting into multiple sub-ranges
                    long estimatedLargeCount = maxRecords * 10;
                    var splitBoundaries = await SplitLargeRange(
                        startId,
                        endId,
                        estimatedLargeCount,
                        filter,
                        maxRecords,
                        cancellationToken);
                    
                    // Add each split as a separate range stat with estimated counts
                    long estimatedCountPerSplit = estimatedLargeCount / splitBoundaries.Count;
                    for (int j = 0; j < splitBoundaries.Count - 1; j++)
                    {
                        rangeStats.Add((splitBoundaries[j], splitBoundaries[j + 1], estimatedCountPerSplit));
                    }
                    // Add the last segment to endId
                    if (splitBoundaries.Count > 0)
                    {
                        rangeStats.Add((splitBoundaries[splitBoundaries.Count - 1], endId, estimatedCountPerSplit));
                    }
                }
                else
                {
                    rangeStats.Add((startId, endId, count));
                }
            }

            // Step 2: Merge small ranges and split large ranges
            int idx = 0;
            while (idx < rangeStats.Count)
            {
                var currentRange = rangeStats[idx];
                
                // If range is too small, try to merge with next ranges
                if (currentRange.count < minRecords)
                {
                    long mergedCount = currentRange.count;
                    int mergeEndIdx = idx;
                    
                    // Keep merging until we have enough records or run out of ranges
                    while (mergeEndIdx < rangeStats.Count - 1 && mergedCount < minRecords)
                    {
                        mergeEndIdx++;
                        mergedCount += rangeStats[mergeEndIdx].count;
                    }
                    
                    // Add merged range
                    if (adjustedBoundaries.Count == 0 || !adjustedBoundaries[adjustedBoundaries.Count - 1].Equals(currentRange.startId))
                    {
                        adjustedBoundaries.Add(currentRange.startId);
                    }
                    
                    // Move to next unmerged range
                    idx = mergeEndIdx + 1;
                }
                // If range is too large, split it
                else if (currentRange.count > maxRecords)
                {
                    var splitBoundaries = await SplitLargeRange(
                        currentRange.startId,
                        currentRange.endId,
                        currentRange.count,
                        filter,
                        maxRecords,
                        cancellationToken);
                    
                    adjustedBoundaries.AddRange(splitBoundaries);
                    idx++;
                }
                // Range is just right
                else
                {
                    if (adjustedBoundaries.Count == 0 || !adjustedBoundaries[adjustedBoundaries.Count - 1].Equals(currentRange.startId))
                    {
                        adjustedBoundaries.Add(currentRange.startId);
                    }
                    idx++;
                }
            }

            return adjustedBoundaries;
        }

        /// <summary>
        /// Splits a large range into smaller ranges by generating intermediate time-based boundaries.
        /// </summary>
        private async Task<List<BsonValue>> SplitLargeRange(
            BsonValue startId,
            BsonValue endId,
            long recordCount,
            BsonDocument filter,
            int maxRecords,
            CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"MongoObjectIdSampler.SplitLargeRange: recordCount={recordCount}, maxRecords={maxRecords}");
            var result = new List<BsonValue> { startId };
            
            // Calculate how many splits we need
            int splits = (int)Math.Ceiling((double)recordCount / maxRecords);
            
            if (splits <= 1)
            {
                return result;
            }

            // Generate intermediate boundaries using time-based calculation
            var startOid = startId.AsObjectId;
            var endOid = endId.AsObjectId;
            
            var minBytes = startOid.ToByteArray();
            var maxBytes = endOid.ToByteArray();
            
            var minInt = new BigInteger(minBytes, isUnsigned: true, isBigEndian: true);
            var maxInt = new BigInteger(maxBytes, isUnsigned: true, isBigEndian: true);
            var step = (maxInt - minInt) / splits;

            for (int i = 1; i < splits; i++)
            {
                var nextInt = minInt + (step * i);
                var nextBytes = nextInt.ToByteArray(isUnsigned: true, isBigEndian: true);

                // Ensure exactly 12 bytes
                if (nextBytes.Length < 12)
                {
                    var padded = new byte[12];
                    Array.Copy(nextBytes, 0, padded, 12 - nextBytes.Length, nextBytes.Length);
                    nextBytes = padded;
                }
                else if (nextBytes.Length > 12)
                {
                    nextBytes = nextBytes[^12..];
                }

                result.Add(new ObjectId(nextBytes));
            }

            return result;
        }

        /// <summary>
        /// Splits an ObjectId chunk into smaller sub-chunks by generating intermediate boundaries.
        /// Uses BigInteger arithmetic to evenly divide the ObjectId range.
        /// </summary>
        /// <param name="originalChunk">The original chunk to split</param>
        /// <param name="splitCount">Number of sub-chunks to create (default: 10)</param>
        /// <returns>List of new MigrationChunk objects representing the sub-chunks</returns>
        public static List<MigrationChunk> SplitObjectIdChunkIntoSubChunks(MigrationChunk originalChunk, int splitCount = 10)
        {
            MigrationJobContext.AddVerboseLog($"SplitObjectIdChunkIntoSubChunks: Gte={originalChunk.Gte}, Lt={originalChunk.Lt}, splitCount={splitCount}");

            if (originalChunk.DataType != DataType.ObjectId)
            {
                return new List<MigrationChunk> { originalChunk };
            }

            var subChunks = new List<MigrationChunk>();

            try
            {
                // Parse the ObjectId bounds
                var startOid = ObjectId.Parse(originalChunk.Gte!);
                var endOid = ObjectId.Parse(originalChunk.Lt!);

                // Generate intermediate boundaries using BigInteger arithmetic
                var boundaries = GenerateObjectIdBoundaries(startOid, endOid, splitCount);

                // Create sub-chunks from boundaries
                for (int i = 0; i < boundaries.Count - 1; i++)
                {
                    var subChunk = new MigrationChunk(
                        boundaries[i].ToString(),
                        boundaries[i + 1].ToString(),
                        DataType.ObjectId,
                        false,
                        false
                    );
                    subChunks.Add(subChunk);
                }
            }
            catch (Exception)
            {
                return new List<MigrationChunk> { originalChunk };
            }

            return subChunks;
        }

        /// <summary>
        /// Generates evenly distributed ObjectId boundaries between start and end.
        /// </summary>
        /// <param name="startOid">Starting ObjectId</param>
        /// <param name="endOid">Ending ObjectId</param>
        /// <param name="count">Number of segments to create</param>
        /// <returns>List of ObjectId boundaries (count + 1 elements)</returns>
        public static List<ObjectId> GenerateObjectIdBoundaries(ObjectId startOid, ObjectId endOid, int count)
        {
            var result = new List<ObjectId> { startOid };

            if (count <= 1)
            {
                result.Add(endOid);
                return result;
            }

            // Convert ObjectIds to BigInteger for arithmetic
            var minBytes = startOid.ToByteArray();
            var maxBytes = endOid.ToByteArray();

            var minInt = new BigInteger(minBytes, isUnsigned: true, isBigEndian: true);
            var maxInt = new BigInteger(maxBytes, isUnsigned: true, isBigEndian: true);
            var step = (maxInt - minInt) / count;

            // Generate intermediate boundaries
            for (int i = 1; i < count; i++)
            {
                var nextInt = minInt + (step * i);
                var nextBytes = nextInt.ToByteArray(isUnsigned: true, isBigEndian: true);

                // Ensure exactly 12 bytes for ObjectId
                if (nextBytes.Length < 12)
                {
                    var padded = new byte[12];
                    Array.Copy(nextBytes, 0, padded, 12 - nextBytes.Length, nextBytes.Length);
                    nextBytes = padded;
                }
                else if (nextBytes.Length > 12)
                {
                    nextBytes = nextBytes[^12..];
                }

                result.Add(new ObjectId(nextBytes));
            }

            result.Add(endOid);
            return result;
        }

    }
}
