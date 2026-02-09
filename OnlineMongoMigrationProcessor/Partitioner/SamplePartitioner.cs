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
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using static MongoDB.Driver.WriteConcern;

namespace OnlineMongoMigrationProcessor
{
    public static class SamplePartitioner
    {

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

        public static int GetMaxSamples()
        {
            return 3000;
        }

        /// <summary>
        /// Creates partitions based on sampled data from the collection.
        /// </summary>
        /// <param name="idField">The field used as the partition key.</param>
        /// <param name="partitionCount">The number of desired partitions.</param>
        /// <returns>A list of partition boundaries.</returns>
        public static ChunkBoundaries? CreatePartitions(Log log, bool optimizeForMongoDump, IMongoCollection<BsonDocument> collection, int chunkCount, DataType dataType, long minDocsPerChunk, CancellationToken cts, MigrationUnit migrationUnit, bool optimizeForObjectId, MigrationSettings config, out long docCountByType)
        {
            MigrationJobContext.AddVerboseLog($"SamplePartitioner.CreatePartitions: collection={collection.CollectionNamespace}, chunkCount={chunkCount}, dataType={dataType}, optimizeForObjectId={optimizeForObjectId}");

            int segmentCount = 1;
            int minDocsPerSegment = 10000;
            long docsInChunk = 0;
            int sampleCount = 0;


            BsonDocument? userFilter = null;
            userFilter = MongoHelper.GetFilterDoc(migrationUnit.UserFilter);

            int adjustedMaxSamples = GetMaxSamples();
            if (optimizeForObjectId && dataType == DataType.ObjectId && config.ObjectIdPartitioner != PartitionerType.UseSampleCommand)
            {
                adjustedMaxSamples = GetMaxSamples() * 1000;
            }

            // Determine if we should skip DataType filtering
            bool skipDataTypeFilter = migrationUnit?.DataTypeFor_Id.HasValue == true;

            if (skipDataTypeFilter)
            {
                log.ShowInMonitor($"Skipping DataType filtering for {collection.CollectionNamespace} as DataTypeFor_Id is specified: {migrationUnit!.DataTypeFor_Id!.Value}");
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
                        log.WriteLine($"Using Estimated document count for {collection.CollectionNamespace} due to error in counting documents.");
                        docCountByType = GetDocumentCountByDataType(collection, dataType, true, userFilter, skipDataTypeFilter);
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

                if (chunkCount > adjustedMaxSamples)
                {
                    int count = 2;
                    long newCount = docCountByType / ((long)minDocsPerSegment * count);
                    while (newCount > adjustedMaxSamples)
                    {
                        count++;

                        // Check for potential overflow before multiplication
                        long multiplier = (long)minDocsPerSegment * count;
                        if (multiplier <= 0 || multiplier > docCountByType)
                        {                           
                            break;
                        }
                        newCount = docCountByType / multiplier;
                        MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating adjustedMaxSamples: collection={collection.CollectionNamespace}, newCount={newCount}, dataType={dataType}, docCountByType={docCountByType}, multiplier={multiplier}");
                    }
                    

                    log.WriteLine($"Requested chunk count {chunkCount} exceeds maximum samples {adjustedMaxSamples} for {collection.CollectionNamespace}. Adjusting to {newCount}", LogType.Warning);
                    chunkCount = (int)newCount;
                }


                // Ensure minimum documents per chunk
                chunkCount = Math.Max(1, (int)Math.Ceiling((double)docCountByType / minDocsPerChunk));
                docsInChunk = docCountByType / chunkCount;

                // Calculate segments based on documents per chunk
                segmentCount = Math.Min(
                    Math.Max(1, (int)Math.Ceiling((double)docsInChunk / minDocsPerSegment)),
                    GetMaxSegments()
                );


                // Calculate the total sample count
                sampleCount = Math.Min(chunkCount * segmentCount, adjustedMaxSamples);


                MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating sampleCount: collection={collection.CollectionNamespace}, dataType={dataType}, segmentCount={segmentCount}, sampleCount{sampleCount}");

                // Adjust segments per chunk based on the new sample count
                segmentCount = Math.Max(1, sampleCount / chunkCount);

                // Optimize for non-dump scenarios
                if (!optimizeForMongoDump)
                {
                    while (chunkCount > segmentCount && segmentCount < GetMaxSegments())
                    {
                        chunkCount--;
                        segmentCount++;
                    }

                    chunkCount = sampleCount / segmentCount;
                }

                MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating chunkCount: collection={collection.CollectionNamespace}, dataType={dataType}, chunkCount={chunkCount}");

                if (chunkCount < 1)
                    throw new ArgumentException("Chunk count must be greater than 0.");


                if (optimizeForObjectId && dataType == DataType.ObjectId && config.ObjectIdPartitioner != PartitionerType.UseSampleCommand)
                {
                    try
                    {
                        return GetChunkBoundariesForObjectId(log, collection, optimizeForMongoDump, sampleCount, segmentCount, userFilter, config, docCountByType);
                    }
                    catch (Exception ex)
                    {
                        log.WriteLine($"Falling back to general sampler for {collection.CollectionNamespace} as ObjectId sampler failed. Details:  {ex}", LogType.Warning);
                        MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating chunkCount: collection={collection.CollectionNamespace}, dataType={dataType}, userFilter={userFilter}, skipDataTypeFilter={skipDataTypeFilter}, sampleCount={sampleCount}, chunkCount={chunkCount}, segmentCount={segmentCount}");
                        return GetChunkBoundariesGeneral(log, collection, optimizeForMongoDump, dataType, userFilter, skipDataTypeFilter, sampleCount, chunkCount, segmentCount);
                    }
                }
                else
                {
                    MigrationJobContext.AddVerboseLog($"SamplePartitioner.Calculating chunkCount: collection={collection.CollectionNamespace}, dataType={dataType}, userFilter={userFilter}, skipDataTypeFilter={skipDataTypeFilter}, sampleCount={sampleCount}, chunkCount={chunkCount}, segmentCount={segmentCount}");

                    if (optimizeForObjectId && dataType == DataType.ObjectId && config.ObjectIdPartitioner == PartitionerType.UseSampleCommand)
                        skipDataTypeFilter = true;

                    return GetChunkBoundariesGeneral(log, collection, optimizeForMongoDump, dataType, userFilter, skipDataTypeFilter, sampleCount, chunkCount, segmentCount);
                }

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

            // Step 1: Build the filter pipeline based on the data type
            //no need to process user filter in partitioning if it doesn't use _id
            if (!MongoHelper.UsesIdFieldInFilter(userFilter!))
            {
                userFilter = MongoHelper.GetFilterDoc("");
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
                    var sampledData = collection.Aggregate<BsonDocument>(pipeline, options).ToList();
                    partitionValues = sampledData
                        .Select(doc => doc.GetValue("_id", BsonNull.Value))
                        .Where(value => value != BsonNull.Value)
                        .Distinct()
                        .OrderBy(value => value)
                        .ToList();

                    break;
                }
                catch (Exception ex)
                {
                    if (skipDataTypeFilter)
                    {
                        log.WriteLine($"Encountered error in attempt {i} while sampling data (DataType filtering bypassed): {ex}");
                    }
                    else
                    {
                        log.WriteLine($"Encountered error in attempt {i} while sampling data where _id is {dataType}: {ex}");
                    }
                }
            }

            //long docCountByType;
            if (partitionValues == null || partitionValues.Count == 0)
            {
                //docCountByType = 0;
                if (skipDataTypeFilter)
                {
                    log.WriteLine($"No data found (DataType filtering bypassed)");
                }
                else
                {
                    log.WriteLine($"No data found where _id is {dataType}");
                }
                return null;
            }
            // Step 3: Calculate partition boundaries

            chunkBoundaries = ConvertToBoundaries(partitionValues, segmentCount);


            if (skipDataTypeFilter)
            {
                log.WriteLine($"Total Chunks: {chunkBoundaries.Boundaries.Count} (DataType filtering bypassed)");
            }
            else
            {
                log.WriteLine($"Total Chunks: {chunkBoundaries.Boundaries.Count} where _id is {dataType}");
            }
            return chunkBoundaries;
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
                    case DataType.String:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 2)); // 2 is BSON type for String
                        break;
                    case DataType.Decimal128:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 19)); // 19 is BSON type for Decimal128
                        break;
                    case DataType.Date:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 9)); // 9 is BSON type for Date
                        break;
                    case DataType.BinData:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 5)); // 5 is BSON type for Binary
                        break;
                    case DataType.Object:
                        matchCondition = new BsonDocument("_id", new BsonDocument("$type", 3)); // 3 is BSON type for embedded document (Object)
                        break;
                    case DataType.Other:
                        // Exclude all known types to catch "others"
                        var excludedTypes = new BsonArray { 2, 7, 9, 16, 18, 19 };
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

        public static (BsonValue gte, BsonValue lt) GetChunkBounds(string gteString, string ltString, DataType dataType)
        {
            BsonValue? gte = null;
            BsonValue? lt = null;

            // Initialize `gte` and `lt` based on special cases
            if (gteString.Equals("BsonMaxKey"))
                gte = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(gteString) || gteString == "BsonNull")
                gte = BsonNull.Value;

            if (ltString.Equals("BsonMaxKey"))
                lt = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(ltString) || ltString== "BsonNull")
                lt = BsonNull.Value;

            // Handle by DataType
            switch (dataType)
            {
                case DataType.ObjectId:
                    gte ??= new BsonObjectId(ObjectId.Parse(gteString));
                    lt ??= new BsonObjectId(ObjectId.Parse(ltString));
                    break;

                case DataType.Int:
                    gte ??= new BsonInt32(int.Parse(gteString));
                    lt ??= new BsonInt32(int.Parse(ltString));
                    break;

                case DataType.Int64:
                    gte ??= new BsonInt64(long.Parse(gteString));
                    lt ??= new BsonInt64(long.Parse(ltString));
                    break;

                case DataType.String:
                    gte ??= new BsonString(gteString);
                    lt ??= new BsonString(ltString);
                    break;

                case DataType.Object:
                    gte ??= BsonDocument.Parse(gteString);
                    lt ??= BsonDocument.Parse(ltString);
                    break;

                case DataType.Decimal128:
                    gte ??= new BsonDecimal128(Decimal128.Parse(gteString));
                    lt ??= new BsonDecimal128(Decimal128.Parse(ltString));
                    break;

                case DataType.Date:
                    gte ??= new BsonDateTime(DateTime.Parse(gteString));
                    lt ??= new BsonDateTime(DateTime.Parse(ltString));
                    break;

                case DataType.BinData:
                case DataType.Other:
                    // For these, we treat it as a special case with no specific bounds
                    gte ??= BsonNull.Value;
                    lt ??= BsonMaxKey.Value;
                    break;
                default:
                    throw new ArgumentException($"Unsupported data type: {dataType}");
            }

            return (gte, lt);
        }
    }


}
