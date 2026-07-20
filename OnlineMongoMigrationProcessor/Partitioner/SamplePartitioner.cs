using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Partitioner;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    public static class SamplePartitioner
    {
        /// <summary>
        /// $sample is oversampled by this factor when picking partition boundaries, so
        /// each chunk receives equally-sized partitions after quantile selection. Also
        /// used by <see cref="Workers.MigrationWorker"/> to derive the effective sample
        /// size when capping at 5% of the document count.
        /// </summary>
        public const int SampleOversampleFactor = 10;

        /// <summary>
        /// Minimum number of documents per segment for small-to-medium collections
        /// (<see cref="LargeCollectionThreshold"/> or fewer docs) when the MongoDriver
        /// path subdivides a chunk for parallel writes. Chunks smaller than this
        /// collapse to 1 segment.
        /// </summary>
        public const int MinDocsPerSegment = 10_000;

        /// <summary>
        /// Minimum number of documents per segment for large collections (more than
        /// <see cref="LargeCollectionThreshold"/> docs). Coarser segments keep the
        /// per-segment work meaningful on multi-billion-doc collections.
        /// </summary>
        public const int MinDocsPerSegmentLarge = 100_000;

        /// <summary>
        /// Returns the per-collection segment floor: <see cref="MinDocsPerSegmentLarge"/>
        /// for collections over <see cref="LargeCollectionThreshold"/>, otherwise
        /// <see cref="MinDocsPerSegment"/>.
        /// </summary>
        public static int GetMinDocsPerSegment(long documentCount)
        {
            return documentCount > LargeCollectionThreshold ? MinDocsPerSegmentLarge : MinDocsPerSegment;
        }

        /// <summary>
        /// Minimum number of documents per chunk for small-to-medium collections
        /// (<see cref="LargeCollectionThreshold"/> or fewer docs). Chunks running as a
        /// single unit of work (DumpAndRestore, or MongoDriver when segments collapse
        /// to 1) must hold at least this many documents.
        /// </summary>
        public const int MinDocsPerChunk = 100_000;

        /// <summary>
        /// Minimum number of documents per chunk for large collections (more than
        /// <see cref="LargeCollectionThreshold"/> docs). Coarser chunks keep total
        /// chunk count manageable on multi-billion-doc collections.
        /// </summary>
        public const int MinDocsPerChunkLarge = 1_000_000;

        /// <summary>
        /// Collections larger than this use <see cref="MinDocsPerChunkLarge"/> as the
        /// chunk floor; smaller collections use <see cref="MinDocsPerChunk"/>.
        /// </summary>
        public const long LargeCollectionThreshold = 100_000_000L;

        /// <summary>
        /// Returns the per-collection chunk floor: <see cref="MinDocsPerChunkLarge"/>
        /// for collections over <see cref="LargeCollectionThreshold"/>, otherwise
        /// <see cref="MinDocsPerChunk"/>.
        /// </summary>
        public static int GetMinDocsPerChunk(long documentCount)
        {
            return documentCount > LargeCollectionThreshold ? MinDocsPerChunkLarge : MinDocsPerChunk;
        }

        public static int GetMaxSegments()
        {
            try
            {
                int maxConcurrentPartitions = MigrationJobContext.CurrentlyActiveJob?.ParallelThreads ?? Environment.ProcessorCount * 5;
                int MaxSegments = Math.Max(20, maxConcurrentPartitions);
                return MaxSegments;
            }
            catch
            {
                return 20;
            }
        }

        public static int GetMaxSamples(bool hasUserFilter = false)
        {
            // $sample is cheap as long as size < 5% of the collection (random-cursor path).
            // With a user filter $sample falls back to a full-scan + top-k sort, so keep the
            // cap small. Without a filter we can request many more boundaries.
            return hasUserFilter ? 3000 : 300_000;
        }

        /// <summary>
        /// Creates partitions based on sampled data from the collection.
        /// </summary>
        /// <param name="idField">The field used as the partition key.</param>
        /// <param name="partitionCount">The number of desired partitions.</param>
        /// <returns>A list of partition boundaries.</returns>
        public static ChunkBoundaries? CreatePartitions(Log log, bool optimizeForMongoDump, IMongoCollection<BsonDocument> collection, int chunkCount, DataType dataType, long minDocsPerChunk, CancellationToken cts, MigrationUnit migrationUnit, bool optimizeForObjectId, MigrationSettings config, bool forceSkipDataTypeFilter, out long docCountByType)
        {
            MigrationJobContext.AddVerboseLog($"SamplePartitioner.CreatePartitions: collection={collection.CollectionNamespace}, chunkCount={chunkCount}, dataType={dataType}, optimizeForObjectId={optimizeForObjectId}, forceSkipDataTypeFilter={forceSkipDataTypeFilter}");

            int segmentCount = 1;
            int sampleCount = 0;


            BsonDocument? userFilter = null;
            userFilter = MongoHelper.GetFilterDoc(migrationUnit.UserFilter);
            bool hasUserFilter = userFilter != null && userFilter.ElementCount > 0;

            // When only one data type exists in the collection, the caller asks us to skip the $type filter.
            bool skipDataTypeFilter = forceSkipDataTypeFilter;
            ChunkBoundaries? resultBoundaries = null;

            if (skipDataTypeFilter)
            {
                log.WriteLine($"Skipping DataType filtering for {collection.CollectionNamespace} as only one _id type is present: {dataType}", LogType.Info);
            }
            else
            {
                log.ShowInMonitor($"Counting documents in {collection.CollectionNamespace}. Sampling data where _id is {dataType}");
            }
            try
            {
                docCountByType = 0;
                cts.ThrowIfCancellationRequested();

                try
                {
                    if (optimizeForObjectId && userFilter != null && userFilter.ElementCount == 0)
                        docCountByType = GetDocumentCountByDataType(collection, DataType.ObjectId, true, new BsonDocument(), true);//use esimated count, don't need exact count for objectId 
                    else
                    {
                        docCountByType = GetDocumentCountByDataType(collection, dataType, false, userFilter, skipDataTypeFilter);
                        log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} for {dataType} with user filter {userFilter}", LogType.Debug);
                    }
                    MigrationJobContext.AddVerboseLog($"SamplePartitioner.GetDocumentCountByDataType: collection={collection.CollectionNamespace}, docCountByType={docCountByType}, dataType={dataType}, optimizeForObjectId={optimizeForObjectId}, userFilter={userFilter}");
                }
                catch (Exception ex)
                {
                    log.WriteLine($"Exception occurred while counting documents in {collection.CollectionNamespace}. Details: {ex}", LogType.Warning);//don't show call stack
                    if (userFilter == null || userFilter.ElementCount == 0)
                    {
                        // A filtered count is a full scan that can reset the connection on some sources.
                        // Fall back to an estimated count so partitioning still proceeds with multiple chunks instead of
                        // collapsing to a single partition. Prefer the collection-level count already computed during setup
                        // (collStats/estimatedDocumentCount) to avoid another server round-trip; otherwise get a metadata-only
                        // estimate (skipDataTypeFilter forces the scan-free estimate path). Over-estimating the per-type count
                        // is harmless because the value only decides how many chunks to create.
                        long seededCount = migrationUnit.EstimatedDocCount > 0 ? migrationUnit.EstimatedDocCount : migrationUnit.ActualDocCount;
                        if (seededCount > 0)
                        {
                            docCountByType = seededCount;
                            log.WriteLine($"Using previously computed collection document count ({docCountByType}) for {collection.CollectionNamespace} due to error in counting documents.");
                        }
                        else
                        {
                            log.WriteLine($"Using Estimated document count for {collection.CollectionNamespace} due to error in counting documents.");
                            docCountByType = GetDocumentCountByDataType(collection, dataType, true, null, true);
                        }
                        MigrationJobContext.AddVerboseLog($"SamplePartitioner.GetDocumentCountByDataType in Ex: collection={collection.CollectionNamespace}, docCountByType={docCountByType}, dataType={dataType}, optimizeForObjectId={optimizeForObjectId}, userFilter={userFilter}");
                    }
                    else
                        return null;
                }

                if (docCountByType == 0)
                {
                    if (skipDataTypeFilter)
                    {
                        log.WriteLine($"{collection.CollectionNamespace} has no documents (DataType filtering bypassed)");
                    }
                    else
                    {
                        log.WriteLine($"{collection.CollectionNamespace} has no documents where _id is {dataType}");
                    }
                    return null;
                }
                else if (docCountByType < minDocsPerChunk)
                {
                    if (skipDataTypeFilter)
                    {
                        log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} document(s) (DataType filtering bypassed). Count is less than minimum chunk size.");
                    }
                    else
                    {
                        log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} document(s) where _id is {dataType}. Count is less than minimum chunk size.");
                    }
                    sampleCount = 1;
                    chunkCount = 1;
                }
                else
                {
                    if (skipDataTypeFilter)
                    {
                        log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} document(s) (DataType filtering bypassed)");
                    }
                    else
                    {
                        log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} document(s) where _id is {dataType}");
                    }
                }

                bool usePaginationPartitioner = dataType != DataType.ObjectId && config.NonObjectIdPartitioner == PartitionerType.UsePagination;

                // Will the boundaries actually come from a MongoDB $sample call? The
                // analytical ObjectId sampler and the pagination probe path do NOT use
                // $sample, so the 10x oversample factor only multiplies sampleCount on
                // the genuine $sample path. (sampleCount is ignored downstream by the
                // analytical sampler, but pagination uses it as a probe count -- so we
                // must NOT oversample there.)
                bool willUseDollarSample =
                    !(optimizeForObjectId && dataType == DataType.ObjectId
                        && config.ObjectIdPartitioner != PartitionerType.UseSampleCommand)
                    && !usePaginationPartitioner;
                int oversampleFactor = willUseDollarSample ? SampleOversampleFactor : 1;

                // chunkCount and segmentCount are derived purely from the per-job math that
                // MigrationWorker.CalculatePartitioningStrategy already encoded into
                // minDocsPerChunk. The 5% $sample cap lives in CalculatePartitioningStrategy;
                // here we only enforce MinDocsPerSegment (drive segments down when a chunk
                // is too small to split MaxSegments ways).
                if (optimizeForMongoDump)
                {
                    // DumpAndRestore: 1 segment per chunk. $sample oversamples 10x when
                    // the $sample path is in use; the quantile-select step in
                    // GetChunkBoundariesGeneral collapses it back down to chunkCount.
                    // Segments=1 means the chunk IS the unit of work, so MinDocsPerSegment
                    // applies to the chunk directly.
                    chunkCount = Math.Max(1, (int)Math.Ceiling((double)docCountByType / minDocsPerChunk));
                    int dumpChunkFloor = (int)Math.Max(1L, docCountByType / GetMinDocsPerChunk(docCountByType));
                    if (chunkCount > dumpChunkFloor)
                    {
                        chunkCount = dumpChunkFloor;
                    }
                    segmentCount = 1;
                    sampleCount = (int)Math.Min((long)chunkCount * oversampleFactor, int.MaxValue);
                    if (willUseDollarSample && hasUserFilter)
                    {
                        // $sample size hard-capped to keep top-k sort bounded; effective
                        // oversample factor drops (down to 1x) as chunks*segs approaches the cap.
                        sampleCount = Math.Min(sampleCount, GetMaxSamples(true));
                    }

                    MigrationJobContext.AddVerboseLog($"SamplePartitioner DumpAndRestore: collection={collection.CollectionNamespace}, dataType={dataType}, chunkCount={chunkCount}, sampleCount={sampleCount}, oversample={oversampleFactor}");
                }
                else
                {
                    // MongoDriver: chunkCount comes from the pre-divided dump count
                    // (minDocsPerChunk already reflects the /MaxSegments grouping done in
                    // CalculatePartitioningStrategy). Two floors apply, both reducing
                    // chunkCount rather than dropping segmentCount:
                    //   1. Every chunk must be big enough to saturate MaxSegments parallel
                    //      segments at the tier's MinDocsPerSegment minimum, so segs stays
                    //      pinned at MaxSegments whenever the collection itself is large
                    //      enough to support it.
                    //   2. Universal MinDocsPerChunk floor (tiered) as a backstop.
                    chunkCount = Math.Max(1, (int)Math.Ceiling((double)docCountByType / minDocsPerChunk));
                    int maxSegmentsForColl = GetMaxSegments();
                    int minDocsPerSegmentForColl = GetMinDocsPerSegment(docCountByType);
                    long perChunkFloorForFullSegments = (long)maxSegmentsForColl * minDocsPerSegmentForColl;
                    long perChunkFloor = Math.Max(perChunkFloorForFullSegments, GetMinDocsPerChunk(docCountByType));
                    int chunkFloor = (int)Math.Max(1L, docCountByType / perChunkFloor);
                    if (chunkCount > chunkFloor)
                    {
                        chunkCount = chunkFloor;
                    }
                    long docsPerChunk = docCountByType / chunkCount;
                    int segmentByDocsFloor = (int)Math.Max(1L, docsPerChunk / minDocsPerSegmentForColl);
                    segmentCount = Math.Min(maxSegmentsForColl, segmentByDocsFloor);

                    sampleCount = (int)Math.Min((long)chunkCount * segmentCount * oversampleFactor, int.MaxValue);
                    if (willUseDollarSample && hasUserFilter)
                    {
                        sampleCount = Math.Min(sampleCount, GetMaxSamples(true));
                    }

                    MigrationJobContext.AddVerboseLog($"SamplePartitioner MongoDriver: collection={collection.CollectionNamespace}, dataType={dataType}, chunkCount={chunkCount}, segmentCount={segmentCount}, sampleCount={sampleCount}, oversample={oversampleFactor}, docsPerChunk={docsPerChunk}");
                }

                MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating chunkCount: collection={collection.CollectionNamespace}, dataType={dataType}, chunkCount={chunkCount}");

                if (chunkCount < 1)
                    throw new ArgumentException("Chunk count must be greater than 0.");


                if (optimizeForObjectId && dataType == DataType.ObjectId && config.ObjectIdPartitioner != PartitionerType.UseSampleCommand)
                {
                    try
                    {
                        // ObjectId sampler generates exactly N equidistant boundaries (not oversampled).
                        // Use chunkCount * segmentCount as the desired boundary count, not the
                        // oversampled sampleCount which is only for $sample + quantile selection.
                        int objectIdBoundaryCount = chunkCount * segmentCount;
                        resultBoundaries = GetChunkBoundariesForObjectId(log, collection, optimizeForMongoDump, objectIdBoundaryCount, segmentCount, userFilter, config, docCountByType);
                    }
                    catch (Exception ex)
                    {
                        log.WriteLine($"Falling back to general sampler for {collection.CollectionNamespace} as ObjectId sampler failed. Details:  {ex}", LogType.Warning);
                        MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating chunkCount: collection={collection.CollectionNamespace}, dataType={dataType}, userFilter={userFilter}, skipDataTypeFilter={skipDataTypeFilter}, sampleCount={sampleCount}, chunkCount={chunkCount}, segmentCount={segmentCount}");
                        resultBoundaries = GetChunkBoundariesGeneral(log, collection, optimizeForMongoDump, dataType, userFilter, skipDataTypeFilter, sampleCount, chunkCount, segmentCount);
                    }
                }
                else
                {
                    MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating chunkCount: collection={collection.CollectionNamespace}, dataType={dataType}, userFilter={userFilter}, skipDataTypeFilter={skipDataTypeFilter}, sampleCount={sampleCount}, chunkCount={chunkCount}, segmentCount={segmentCount}");

                    if (optimizeForObjectId && dataType == DataType.ObjectId && config.ObjectIdPartitioner == PartitionerType.UseSampleCommand)
                        skipDataTypeFilter = true;

                    if (usePaginationPartitioner)
                        resultBoundaries = GetChunkBoundariesGeneralWithPagination(log, collection, dataType, userFilter, skipDataTypeFilter, sampleCount, segmentCount, chunkCount, docCountByType);
                    else
                        resultBoundaries = GetChunkBoundariesGeneral(log, collection, optimizeForMongoDump, dataType, userFilter, skipDataTypeFilter, sampleCount, chunkCount, segmentCount);

                }

                return resultBoundaries;

            }
            catch (OperationCanceledException)
            {
                ///do nothing
                docCountByType = 0;
                return null;
            }
            catch (Exception ex)
            {
                if (skipDataTypeFilter)
                {
                    log.WriteLine($"Error during sampling data (DataType filtering bypassed): {ex}", LogType.Error);
                }
                else
                {
                    log.WriteLine($"Error during sampling data where _id is {dataType}: {ex}", LogType.Error);
                }
                docCountByType = 0;
                return null;
            }

        }

        private static ChunkBoundaries? GetChunkBoundariesGeneral(Log log, IMongoCollection<BsonDocument> collection, bool optimizeForMongoDump, DataType dataType, BsonDocument userFilter, bool skipDataTypeFilter, long sampleCount, long chunkCount, int segmentCount)
        {
            ChunkBoundaries chunkBoundaries = new ChunkBoundaries();


            Boundary? segmentBoundary = null;
            Boundary? chunkBoundary = null;

            if (chunkCount == 1 || dataType == DataType.Other)
            {
                // If the data type is Other, we treat it as a single chunk, no lte and gte
                chunkBoundary = new Boundary
                {
                    StartId = BsonNull.Value,
                    EndId = BsonNull.Value,
                    SegmentBoundaries = new List<Boundary>() // Initialize SegmentBoundaries here
                };
                chunkBoundaries.Boundaries ??= new List<Boundary>(); // Use null-coalescing assignment
                chunkBoundaries.Boundaries.Add(chunkBoundary);

                log.WriteLine($"Chunk Count: {chunkBoundaries.Boundaries.Count} where _id is {dataType}");

                return chunkBoundaries;
            }

            BsonDocument matchCondition = BuildDataTypeCondition(dataType, userFilter, skipDataTypeFilter);

            // Step 2: Sample the data

            if (skipDataTypeFilter)
            {
                log.WriteLine($"Sampling {collection.CollectionNamespace} (DataType filtering bypassed) with {sampleCount} samples, Chunk Count: {chunkCount}");
            }
            else
            {
                log.WriteLine($"Sampling {collection.CollectionNamespace} for data where _id is {dataType} with {sampleCount} samples, Chunk Count: {chunkCount}");
            }


            var pipelineStages = new List<BsonDocument>();

            // Only add $match stage if matchCondition is not empty
            if (matchCondition != null && matchCondition.ElementCount > 0)
            {
                pipelineStages.Add(new BsonDocument("$match", matchCondition));
            }

            pipelineStages.Add(new BsonDocument("$sample", new BsonDocument("size", sampleCount)));
            pipelineStages.Add(new BsonDocument("$project", new BsonDocument("_id", 1)));

            var pipeline = pipelineStages.ToArray();

            List<BsonValue> partitionValues = new List<BsonValue>();
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    AggregateOptions options = new AggregateOptions
                    {
                        MaxTime = TimeSpan.FromSeconds(3600 * 10)
                    };
                    log.ShowInMonitor($"Running $sample on {collection.CollectionNamespace} (size={sampleCount}); this can take several minutes for large collections...");
                    var sampleStartedAt = DateTime.UtcNow;
                    var sampledData = collection.Aggregate<BsonDocument>(pipeline, options).ToList();
                    var sampleElapsed = DateTime.UtcNow - sampleStartedAt;
                    log.ShowInMonitor($"$sample on {collection.CollectionNamespace} returned {sampledData.Count} document(s) in {sampleElapsed.TotalSeconds:F1}s; computing chunk boundaries...");
                    partitionValues = sampledData
                        .Select(doc => doc.GetValue("_id", BsonNull.Value))
                        .Where(value => value != BsonNull.Value)
                        .Distinct()
                        .OrderBy(value => value, BsonValueOrdinalComparer.Instance)
                        .ToList();

                    break;
                }
                catch (Exception ex)
                {
                    if (skipDataTypeFilter)
                    {
                        log.WriteLine($"{collection.CollectionNamespace} encountered error in attempt {i} while sampling data (DataType filtering bypassed): {ex}");
                        log.ShowInMonitor($"$sample attempt {i + 1}/10 failed for {collection.CollectionNamespace}: {ex.Message}", LogType.Warning);
                    }
                    else
                    {
                        log.WriteLine($"{collection.CollectionNamespace} encountered error in attempt {i} while sampling data where _id is {dataType}: {ex}");
                        log.ShowInMonitor($"$sample attempt {i + 1}/10 failed for {collection.CollectionNamespace} (_id={dataType}): {ex.Message}", LogType.Warning);
                    }
                }
            }

            //long docCountByType;
            if (partitionValues == null || partitionValues.Count == 0)
            {
                //docCountByType = 0;
                if (skipDataTypeFilter)
                {
                    log.WriteLine($"{collection.CollectionNamespace} No data found (DataType filtering bypassed)");
                }
                else
                {
                    log.WriteLine($"{collection.CollectionNamespace} No data found where _id is {dataType}");
                }
                return null;
            }
            // Step 3: Calculate partition boundaries
            // Both paths now oversample $sample by SampleOversampleFactor (see
            // CreatePartitions) to get equally-sized partitions; collapse the oversampled
            // list to the desired boundary count via equidistant quantile selection.
            //  - DumpAndRestore: target = chunkCount      (1 segment per chunk)
            //  - MongoDriver:    target = chunkCount * segmentCount
            long targetBoundaryCount = optimizeForMongoDump
                ? chunkCount
                : (long)chunkCount * Math.Max(1, segmentCount);
            if (partitionValues.Count > targetBoundaryCount)
            {
                var quantileBoundaries = new List<BsonValue>((int)targetBoundaryCount);
                for (long k = 0; k < targetBoundaryCount; k++)
                {
                    int idx = (int)(k * partitionValues.Count / targetBoundaryCount);
                    quantileBoundaries.Add(partitionValues[idx]);
                }
                partitionValues = quantileBoundaries;
            }

            chunkBoundaries = ConvertToBoundaries(partitionValues, segmentCount);


            if (skipDataTypeFilter)
            {
                log.WriteLine($"{collection.CollectionNamespace} total chunks: {chunkBoundaries.Boundaries.Count} (DataType filtering bypassed)");
                log.ShowInMonitor($"Computed {chunkBoundaries.Boundaries.Count} chunk boundaries for {collection.CollectionNamespace}.");
            }
            else
            {
                log.WriteLine($"{collection.CollectionNamespace} total chunks: {chunkBoundaries.Boundaries.Count} where _id is {dataType}");
                log.ShowInMonitor($"Computed {chunkBoundaries.Boundaries.Count} chunk boundaries for {collection.CollectionNamespace} (_id={dataType}).");
            }
            return chunkBoundaries;
        }

        private static ChunkBoundaries? GetChunkBoundariesGeneralWithPagination(Log log, IMongoCollection<BsonDocument> collection, DataType dataType, BsonDocument userFilter, bool skipDataTypeFilter, long targetPartitionCount, int segmentCount, int targetChunkCount, long docCountByType)
        {
            var rawCollection = collection.Database.GetCollection<RawBsonDocument>(collection.CollectionNamespace.CollectionName);

            Boundary? chunkBoundary = null;
            targetChunkCount = (int)Math.Max(1, Math.Min((long)targetChunkCount, docCountByType));
            targetPartitionCount = Math.Max(1, Math.Min(targetPartitionCount, docCountByType));
            segmentCount = Math.Max(1, segmentCount);

            if (targetChunkCount <= 1 || dataType == DataType.Other)
            {
                ChunkBoundaries singleChunkBoundaries = new ChunkBoundaries();
                chunkBoundary = new Boundary
                {
                    StartId = BsonNull.Value,
                    EndId = BsonNull.Value,
                    SegmentBoundaries = new List<Boundary>()
                };
                singleChunkBoundaries.Boundaries ??= new List<Boundary>();
                singleChunkBoundaries.Boundaries.Add(chunkBoundary);
                log.WriteLine($"Chunk Count: {singleChunkBoundaries.Boundaries.Count} where _id is {dataType}");
                return singleChunkBoundaries;
            }

            if (!MongoHelper.UsesIdFieldInFilter(userFilter!))
            {
                userFilter = MongoHelper.GetFilterDoc("");
            }

            BsonDocument matchCondition = BuildDataTypeCondition(dataType, userFilter, skipDataTypeFilter);
            FilterDefinition<RawBsonDocument> baseFilter = (matchCondition != null && matchCondition.ElementCount > 0)
                ? new BsonDocumentFilterDefinition<RawBsonDocument>(matchCondition)
                : FilterDefinition<RawBsonDocument>.Empty;

            long recordsPerRange = Math.Max(1, docCountByType / targetPartitionCount);
            int skipCount = (int)Math.Min(recordsPerRange - 1, int.MaxValue - 1);

            if (skipDataTypeFilter)
            {
                log.WriteLine($"Using pagination for {collection.CollectionNamespace} (DataType filtering bypassed), records per range: {recordsPerRange}, target chunk count: {targetChunkCount}, target partition count: {targetPartitionCount}, segment count: {segmentCount}");
            }
            else
            {
                log.WriteLine($"Using pagination for {collection.CollectionNamespace} where _id is {dataType}, records per range: {recordsPerRange}, target chunk count: {targetChunkCount}, target partition count: {targetPartitionCount}, segment count: {segmentCount}");
            }

            List<BsonValue> partitionValues = new List<BsonValue>();
            BsonValue? lastId = null;

            for (long partitionIndex = 0; partitionIndex < targetPartitionCount; partitionIndex++)
            {
                try
                {
                    FilterDefinition<RawBsonDocument> rangeFilter = baseFilter;
                    if (lastId != null)
                    {
                        rangeFilter = Builders<RawBsonDocument>.Filter.And(
                            baseFilter,
                            Builders<RawBsonDocument>.Filter.Gt("_id", lastId));
                    }

                    var doc = rawCollection
                        .Find(rangeFilter)
                        .Sort(Builders<RawBsonDocument>.Sort.Ascending("_id"))
                        // Keep projection typed as RawBsonDocument to avoid BsonDocument deserialization
                        // for malformed legacy UUID-like BinData payloads.
                        .Project<RawBsonDocument>(Builders<RawBsonDocument>.Projection.Include("_id"))
                        .Skip(skipCount)
                        .Limit(1)
                        .FirstOrDefault();

                    if (doc == null || !doc.Contains("_id"))
                    {
                        break;
                    }

                    var currentId = doc["_id"];
                    if (currentId == BsonNull.Value)
                    {
                        break;
                    }

                    lastId = currentId;
                    partitionValues.Add(currentId);
                }
                catch (Exception ex)
                {
                    // Some sources contain legacy UUID subtype payloads with non-standard byte lengths.
                    // When this appears while reading the next pagination probe document, stop probing
                    // and continue with the boundaries already collected instead of spamming retries.
                    if (dataType == DataType.BinData && IsMalformedLegacyUuidLengthError(ex))
                    {
                        log.WriteLine($"Stopping BinData pagination probe at attempt {partitionIndex} due to malformed legacy UUID binary length. Continuing with collected boundaries.", LogType.Warning);
                        break;
                    }

                    log.WriteLine($"Encountered error in attempt {partitionIndex} while paginating data where _id is {dataType}: {ex}");
                }
            }

            partitionValues = partitionValues
                .Distinct()
                .OrderBy(value => value, BsonValueOrdinalComparer.Instance)
                .ToList();

            if (partitionValues.Count == 0)
            {
                if (skipDataTypeFilter)
                {
                    log.WriteLine("No data found while generating pagination boundaries (DataType filtering bypassed)");
                }
                else
                {
                    log.WriteLine($"No data found while generating pagination boundaries where _id is {dataType}");
                }
                return null;
            }

            ChunkBoundaries chunkBoundaries = ConvertToBoundaries(partitionValues, segmentCount);

            if (skipDataTypeFilter)
            {
                log.WriteLine($"Total Chunks: {chunkBoundaries.Boundaries.Count} (DataType filtering bypassed) using pagination with segment grouping. Requested chunks: {targetChunkCount}, requested partitions: {targetPartitionCount}, segment count: {segmentCount}");
            }
            else
            {
                log.WriteLine($"Total Chunks: {chunkBoundaries.Boundaries.Count} where _id is {dataType} using pagination with segment grouping. Requested chunks: {targetChunkCount}, requested partitions: {targetPartitionCount}, segment count: {segmentCount}");
            }

            if (chunkBoundaries.Boundaries.Count != targetChunkCount)
            {
                log.WriteLine($"Pagination created {chunkBoundaries.Boundaries.Count} chunks instead of requested {targetChunkCount}. This can happen when _id boundaries are not unique enough for requested partition count.", LogType.Warning);
            }

            return chunkBoundaries;
        }

        private static bool IsMalformedLegacyUuidLengthError(Exception ex)
        {
            var message = ex.ToString();
            return message.IndexOf("UuidLegacy", StringComparison.OrdinalIgnoreCase) >= 0
                && message.IndexOf("Length must be 16", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static ChunkBoundaries? GetChunkBoundariesForObjectId(Log log, IMongoCollection<BsonDocument> collection, bool optimizeForMongoDump, int sampleCount, int segmentCount, BsonDocument userFilter, MigrationSettings config, long collectionTotalDocCount)
        {

            log.WriteLine($"Using objectId sampler {config.ObjectIdPartitioner} for sampling {collection.CollectionNamespace} with {sampleCount} samples, Segment Count: {segmentCount}");
            MongoObjectIdSampler objectIdSampler = new MongoObjectIdSampler(collection);
            //var objectIdRange = objectIdSampler.GetObjectIdRangeAsync(userFilter).GetAwaiter().GetResult();
            var ids = objectIdSampler.GenerateEquidistantObjectIdsAsync(sampleCount, userFilter, config, collectionTotalDocCount).GetAwaiter().GetResult();

            // Adjust segmentCount if we got fewer boundaries than expected (due to data skew or recursive splitting)
            // Only set segmentCount to 1 when optimizeForMongoDump is true (need many chunks for parallel dump/restore)
            // When optimizeForMongoDump is false, keep original segmentCount to have multiple segments per chunk for parallel writes
            int effectiveSegmentCount = segmentCount;
            if (ids.Count > 0 && ids.Count <= segmentCount && optimizeForMongoDump)
            {
                effectiveSegmentCount = 1;
                log.WriteLine($"Adjusted segmentCount from {segmentCount} to {effectiveSegmentCount} (each boundary becomes a chunk for parallel dump/restore) based on actual boundary count ({ids.Count})", LogType.Debug);
            }

            return ConvertToBoundaries(ids, effectiveSegmentCount);
        }

        private static ChunkBoundaries ConvertToBoundaries(List<BsonValue> ids, int segmentCount)
        {
            MigrationJobContext.AddVerboseLog($"SamplePartitioner.ConvertToBoundaries: ids.Count={ids.Count}, segmentCount={segmentCount}");

            ChunkBoundaries chunkBoundaries = new ChunkBoundaries();
            Boundary? segmentBoundary = null;
            Boundary? chunkBoundary = null;

            for (int i = 0; i < ids.Count; i++)
            {
                var min = ids[i];
                var max = i == ids.Count - 1 ? MongoHelper.GetIdRangeMax(new BsonDocument()) : ids[i + 1];

                if (i % segmentCount == 0) // Parent boundary

                {
                    chunkBoundary = new Boundary
                    {
                        StartId = min,
                        EndId = max,
                        SegmentBoundaries = new List<Boundary>() // Initialize SegmentBoundaries here
                    };

                    chunkBoundaries.Boundaries ??= new List<Boundary>(); // Use null-coalescing assignment
                    chunkBoundaries.Boundaries.Add(chunkBoundary);
                }
                else // Child boundary
                {
                    if (chunkBoundary == null)
                    {
                        throw new Exception("Parent boundary not found");
                    }

                    segmentBoundary = new Boundary
                    {
                        StartId = min,
                        EndId = max
                    };

                    chunkBoundary.SegmentBoundaries.Add(segmentBoundary);
                    chunkBoundary.EndId = max; // Update the EndId of the parent boundary to match the last segment.
                }
            }

            return chunkBoundaries;
        }


        public static long GetDocumentCountByDataType(IMongoCollection<BsonDocument> collection, DataType dataType, bool useEstimate = false, BsonDocument? userFilter = null, bool skipDataTypeFilter = false)
        {
            MigrationJobContext.AddVerboseLog($"SamplePartitioner.GetDocumentCountByDataType: dataType={dataType}, useEstimate={useEstimate}, skipDataTypeFilter={skipDataTypeFilter}");

            var filterBuilder = Builders<BsonDocument>.Filter;

            BsonDocument matchCondition = BuildDataTypeCondition(dataType, userFilter, skipDataTypeFilter);

            // Get the count of documents matching the filter
            if (useEstimate && (matchCondition == null || matchCondition.ElementCount == 0))
            {
                var options = new EstimatedDocumentCountOptions { MaxTime = TimeSpan.FromSeconds(300) };
                var count = collection.EstimatedDocumentCount(options);
                return count;
            }
            else
            {
                var options = new CountOptions { MaxTime = TimeSpan.FromSeconds(60000) }; //keep it very high for large collections
#pragma warning disable CS0618 // Type or member is obsolete
                var count = collection.Count(matchCondition, options); //using count as its faster, we don't need accurate numbers
#pragma warning restore CS0618 // Type or member is obsolete
                return count;
            }
        }

        public static BsonDocument BuildDataTypeCondition(DataType dataType, BsonDocument? userFilter = null, bool skipDataTypeFilter = false)
        {
            MigrationJobContext.AddVerboseLog($"SamplePartitioner.BuildDataTypeCondition: dataType={dataType}, skipDataTypeFilter={skipDataTypeFilter}");

            BsonDocument matchCondition;

            if (skipDataTypeFilter)
            {
                // When skipping DataType filter, use empty condition (no _id type restriction)
                matchCondition = new BsonDocument();
            }
            else
            {
                // Original DataType filtering logic
                switch (dataType)
                {
                    case DataType.ObjectId:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 7)); // 7 is BSON type for ObjectId
                        break;
                    case DataType.Int:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 16)); // 16 is BSON type for Int32
                        break;
                    case DataType.Int64:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 18)); // 18 is BSON type for Int64
                        break;
                    case DataType.Double:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 1)); // 1 is BSON type for Double
                        break;
                    case DataType.String:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 2)); // 2 is BSON type for String
                        break;
                    case DataType.Decimal128:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 19)); // 19 is BSON type for Decimal128
                        break;
                    case DataType.Date:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 9)); // 9 is BSON type for Date
                        break;
                    case DataType.Timestamp:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 17)); // 17 is BSON type for Timestamp
                        break;
                    case DataType.BinData:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 5)); // 5 is BSON type for Binary
                        break;
                    case DataType.Object:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 3)); // 3 is BSON type for embedded document (Object)
                        break;
                    case DataType.Other:
                        // Exclude all known types to catch "others"
                        var excludedTypes = new BsonArray { 1, 2, 7, 9, 16, 17, 18, 19 };
                        matchCondition = new BsonDocument("_id", new BsonDocument("$nin", new BsonDocument("$type", excludedTypes)));
                        break;
                    default:
                        throw new ArgumentException($"Unsupported DataType: {dataType}");
                }
            }

            if (userFilter == null || userFilter.ElementCount == 0)
            {
                return matchCondition;
            }
            else
            {
                if (skipDataTypeFilter && matchCondition.ElementCount == 0)
                {
                    // If no DataType filter and we have user filter, just return user filter
                    return userFilter;
                }
                else
                {
                    // Combine using $and
                    return new BsonDocument("$and", new BsonArray { matchCondition, userFilter });
                }
            }
        }

        public static (BsonValue gte, BsonValue lt, BsonValue lte) GetChunkBounds(
            string gteString,
            string ltString,
            string lteString,
            DataType dataType)
        {
            BsonValue? gte = null;
            BsonValue? lt = null;
            BsonValue? lte = null;

            // Initialize `gte` based on special cases
            if (gteString.Equals("BsonMaxKey"))
                gte = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(gteString) || gteString == "BsonNull")
                gte = BsonNull.Value;

            // Initialize lt based on special cases
            if (ltString.Equals("BsonMaxKey"))
                lt = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(ltString) || ltString == "BsonNull")
                lt = BsonNull.Value;

            // Initialize lte based on special cases
            if (lteString.Equals("BsonMaxKey"))
                lte = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(lteString) || lteString == "BsonNull")
                lte = BsonNull.Value;

            // Handle by DataType
            switch (dataType)
            {
                case DataType.ObjectId:
                    gte ??= new BsonObjectId(ObjectId.Parse(gteString));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonObjectId(ObjectId.Parse(ltString));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonObjectId(ObjectId.Parse(lteString));
                    break;

                case DataType.Int:
                    gte ??= new BsonInt32(int.Parse(gteString));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonInt32(int.Parse(ltString));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonInt32(int.Parse(lteString));
                    break;

                case DataType.Int64:
                    gte ??= new BsonInt64(long.Parse(gteString));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonInt64(long.Parse(ltString));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonInt64(long.Parse(lteString));
                    break;

                case DataType.Double:
                    gte ??= new BsonDouble(double.Parse(gteString, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonDouble(double.Parse(ltString, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonDouble(double.Parse(lteString, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture));
                    break;

                case DataType.String:
                    gte ??= new BsonString(gteString);
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonString(ltString);
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonString(lteString);
                    break;

                case DataType.Object:
                    gte ??= string.IsNullOrEmpty(gteString) ? BsonNull.Value : BsonDocument.Parse(gteString);
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : BsonDocument.Parse(ltString);
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : BsonDocument.Parse(lteString);
                    break;

                case DataType.Decimal128:
                    gte ??= new BsonDecimal128(Decimal128.Parse(gteString));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonDecimal128(Decimal128.Parse(ltString));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonDecimal128(Decimal128.Parse(lteString));
                    break;

                case DataType.Date:
                    gte ??= new BsonDateTime(DateTime.Parse(gteString));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonDateTime(DateTime.Parse(ltString));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonDateTime(DateTime.Parse(lteString));
                    break;

                case DataType.Timestamp:
                    gte ??= new BsonTimestamp(long.Parse(gteString, System.Globalization.CultureInfo.InvariantCulture));
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : new BsonTimestamp(long.Parse(ltString, System.Globalization.CultureInfo.InvariantCulture));
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : new BsonTimestamp(long.Parse(lteString, System.Globalization.CultureInfo.InvariantCulture));
                    break;

                case DataType.BinData:
                    gte ??= string.IsNullOrEmpty(gteString) ? BsonNull.Value : ParseBinaryBound(gteString);
                    lt ??= string.IsNullOrEmpty(ltString) ? BsonNull.Value : ParseBinaryBound(ltString);
                    lte ??= string.IsNullOrEmpty(lteString) ? BsonNull.Value : ParseBinaryBound(lteString);
                    break;

                case DataType.Other:
                    // For these, we treat it as a special case with no specific bounds
                    gte ??= BsonNull.Value;
                    lt ??= BsonMaxKey.Value;
                    lte ??= BsonMaxKey.Value;
                    break;
                default:
                    throw new ArgumentException($"Unsupported data type: {dataType}");
            }

            return (gte, lt, lte);
        }

        private static BsonBinaryData ParseBinaryBound(string boundValue)
        {
            if (string.IsNullOrWhiteSpace(boundValue))
                throw new ArgumentException("Binary bound value is empty.");

            // Extended JSON path, e.g. {"$binary":{"base64":"...","subType":"03"}}
            try
            {
                var parsed = BsonDocument.Parse($"{{\"v\":{boundValue}}}")["v"];
                if (parsed.IsBsonBinaryData)
                    return parsed.AsBsonBinaryData;
            }
            catch
            {
                // Fallback to shell BinData syntax parser below.
            }

            // Mongo shell style fallback, e.g. BinData(3, "base64...")
            var match = Regex.Match(boundValue, @"^BinData\((\d+),\s*""?(.*?)""?\)$", RegexOptions.CultureInvariant);
            if (!match.Success)
                throw new ArgumentException($"Unsupported BinData boundary format: {boundValue}");

            byte subType = byte.Parse(match.Groups[1].Value);
            string base64 = match.Groups[2].Value;
            return new BsonBinaryData(Convert.FromBase64String(base64), (BsonBinarySubType)subType);
        }

        /// <summary>
        /// Samples the collection to find boundary values for splitting chunks.
        /// Uses MongoDB $sample aggregation stage for efficient sampling.
        /// </summary>
        public static async Task<List<BsonDocument>> SampleCollectionForSplitPointAsync(
                    IMongoCollection<BsonDocument> collection,
                    MigrationChunk chunk,
                    BsonDocument? userFilterDoc,
                    int sampleSize)
        {
            MigrationJobContext.AddVerboseLog($"SampleCollectionForSplitPointAsync: sampleSize={sampleSize}, idField=_id");

            try
            {
                // Build match stage for chunk bounds and user filter
                var matchStages = new List<BsonDocument>();

                // Get properly parsed bounds using SamplePartitioner (handles type-specific parsing)
                // Use Lt if available, otherwise use Lte for the upper bound
                string upperBound = !string.IsNullOrEmpty(chunk.Lt) ? chunk.Lt : (chunk.Lte ?? "");
                var bounds = GetChunkBounds(chunk.Gte ?? "", upperBound, chunk.Lte ?? "", chunk.DataType);
                BsonValue? gteBsonValue = bounds.gte != null && !(bounds.gte is BsonMaxKey) && !(bounds.gte is BsonNull) ? bounds.gte : null;
                BsonValue? ltBsonValue = bounds.lt != null && !(bounds.lt is BsonMaxKey) && !(bounds.lt is BsonNull) ? bounds.lt : null;
                BsonValue? lteBsonValue = bounds.lte != null && !(bounds.lte is BsonMaxKey) && !(bounds.lte is BsonNull) ? bounds.lte : null;

                // Build range filter for _id
                BsonDocument rangeFilter = new BsonDocument();
                if (gteBsonValue != null)
                {
                    rangeFilter["$gte"] = gteBsonValue;
                }
                if (ltBsonValue != null)
                {
                    rangeFilter["$lt"] = ltBsonValue;
                }
                else if (lteBsonValue != null)
                {
                    rangeFilter["$lte"] = lteBsonValue;
                }

                // Build the combined condition: data type + range + user filter
                BsonDocument filterCondition = BuildDataTypeCondition(chunk.DataType, userFilterDoc, true);

                // Add range filter to the condition
                if (rangeFilter.ElementCount > 0)
                {
                    if (filterCondition.Contains("_id") && filterCondition["_id"].IsBsonDocument)
                    {
                        // Merge with existing _id filter
                        var existingIdFilter = filterCondition["_id"].AsBsonDocument;
                        foreach (var element in rangeFilter)
                        {
                            existingIdFilter[element.Name] = element.Value;
                        }
                    }
                    else
                    {
                        filterCondition["_id"] = rangeFilter;
                    }
                }

                // Add combined filter to match stages
                if (filterCondition != null && filterCondition.ElementCount > 0)
                {
                    matchStages.Add(new BsonDocument("$match", filterCondition));
                }

                // Add sample stage
                var sampleStage = new BsonDocument("$sample", new BsonDocument("size", sampleSize));

                // Only project _id to avoid full-document deserialization issues.
                var projectStage = new BsonDocument("$project", new BsonDocument("_id", 1));

                // Add sort by _id to ensure consistent ordering
                var sortStage = new BsonDocument("$sort", new BsonDocument("_id", 1));

                // Execute aggregation
                var pipeline = matchStages.Concat(new[] { sampleStage, projectStage, sortStage }).ToList();

                var samples = await collection.Aggregate<BsonDocument>(pipeline)
                    .ToListAsync()
                    .ConfigureAwait(false);

                MigrationJobContext.AddVerboseLog($"Sampled {samples.Count} documents from {collection.CollectionNamespace.CollectionName} for split boundaries");
                return samples;
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"Error sampling collection for split points: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Creates sub-chunks based on sampled document boundaries.
        /// Each sub-chunk spans from one sample value (inclusive) to the next (exclusive).
        /// </summary>
        public static async Task<List<MigrationChunk>> CreateSubChunksFromSampleAsync(
            MigrationChunk originalChunk,
            List<BsonDocument> samples,
            string databaseName,
            string collectionName)
        {
            MigrationJobContext.AddVerboseLog($"CreateSubChunksFromSampleAsync: creating {samples.Count - 1} sub-chunks from samples");

            var subChunks = new List<MigrationChunk>();

            try
            {
                for (int i = 0; i < samples.Count - 1; i++)
                {
                    var currentSample = samples[i];
                    var nextSample = samples[i + 1];

                    string gte = SerializeBoundaryForSplit(currentSample["_id"], originalChunk.DataType);
                    string lt = SerializeBoundaryForSplit(nextSample["_id"], originalChunk.DataType);

                    // Create sub-chunk (inherit properties from original)
                    var subChunk = new MigrationChunk(
                        gte,
                        lt,
                        originalChunk.DataType,
                        false,
                        false)
                    {
                        Id = i.ToString() // Will be reassigned in ReplaceChunkWithSubChunks
                    };

                    subChunks.Add(subChunk);
                }

                // Last sub-chunk goes up to original upper bound (use Lt if available, otherwise Lte)
                if (samples.Count > 0)
                {
                    var lastSample = samples[samples.Count - 1];
                    string lastGte = SerializeBoundaryForSplit(lastSample["_id"], originalChunk.DataType);
                    string lastLt = originalChunk.Lt ?? ""; // Prefer Lt
                    string lastLte = ""; // Default empty
                    
                    // If Lt is empty, use Lte for the upper bound
                    if (string.IsNullOrEmpty(lastLt) && !string.IsNullOrEmpty(originalChunk.Lte))
                    {
                        lastLte = originalChunk.Lte;
                    }
                    else if (string.IsNullOrEmpty(lastLt))
                    {
                        lastLt = originalChunk.Lt ?? ""; // Use whatever was in original
                    }

                    var lastSubChunk = new MigrationChunk(
                        lastGte,
                        lastLt,
                        originalChunk.DataType,
                        false,
                        false)
                    {
                        Id = (samples.Count - 1).ToString(),
                        Lte = lastLte
                    };

                    subChunks.Add(lastSubChunk);
                }

                MigrationJobContext.AddVerboseLog($"Created {subChunks.Count} sub-chunks for {databaseName}.{collectionName} via sample-based splitting");
                return subChunks;
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"Error creating sub-chunks from samples: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Serializes a BsonValue boundary for use in sub-chunk Gte/Lt strings.
        /// Must be consistent with SerializeBoundaryValue in MigrationWorker:
        /// BinData and Object use ToJson(), all other types use ToString().
        /// Using ToJson() for String types wraps values in quotes, which corrupts
        /// the boundary when later parsed back via GetChunkBounds.
        /// </summary>
        private static string SerializeBoundaryForSplit(BsonValue value, DataType dataType)
        {
            if (value == null || value.IsBsonNull)
                return string.Empty;

            return dataType switch
            {
                DataType.BinData => value.ToJson(),
                DataType.Object => value.ToJson(),
                DataType.Double => value.AsDouble.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
                DataType.Timestamp => value.AsBsonTimestamp.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                _ => value.ToString()!
            };
        }

        /// <summary>
        /// Compares BsonValues using ordinal (binary) comparison for strings,
        /// matching MongoDB's default string comparison order.
        /// BsonValue.CompareTo uses culture-sensitive string.CompareTo() which
        /// disagrees with MongoDB for characters like _ (underscore), lowercase
        /// letters, and other symbols (ASCII 91-96, 123-126).
        /// </summary>
        private sealed class BsonValueOrdinalComparer : IComparer<BsonValue>
        {
            public static readonly BsonValueOrdinalComparer Instance = new();

            public int Compare(BsonValue? x, BsonValue? y)
            {
                if (x == null && y == null) return 0;
                if (x == null) return -1;
                if (y == null) return 1;

                if (x.BsonType == BsonType.String && y.BsonType == BsonType.String)
                    return string.Compare(x.AsString, y.AsString, StringComparison.Ordinal);

                return x.CompareTo(y);
            }
        }


    }
}
