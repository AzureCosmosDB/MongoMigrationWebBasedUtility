using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using MongoDB.Bson.Serialization.Serializers;
using System.Xml.Linq;
using MongoDB.Bson.IO;

namespace OnlineMongoMigrationProcessor
{
    public static class SamplePartitioner
    {
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.

        public static int MaxSegments = 20;
        public static int MaxSamples = 2000;

        
		/// <summary>
		/// Creates partitions based on sampled data from the collection.
		/// </summary>
		/// <param name="idField">The field used as the partition key.</param>
		/// <param name="partitionCount">The number of desired partitions.</param>
		/// <returns>A list of partition boundaries.</returns>
		public static ChunkBoundaries CreatePartitions(Log log,bool optimizeForMongoDump,IMongoCollection<BsonDocument> collection, string userFilterCondition, int chunkCount, DataType dataType, long minDocsPerChunk, CancellationToken cts,out long docCountByType)
        {
            int segmentCount = 1;
            int minDocsPerSegment = 10000;
            long docsInChunk=0;           
            int sampleCount=0;
           
            BsonDocument userFilter = null;
            if (userFilterCondition != null && !string.IsNullOrEmpty(userFilterCondition))
            {
                userFilter= BsonDocument.Parse(userFilterCondition);
            }

            log.AddVerboseMessage($"Counting documents in {collection.CollectionNamespace}. Sampling data where _id is {dataType}");

            try
            {

                cts.ThrowIfCancellationRequested();

                try
                {
                    docCountByType = GetDocumentCountByDataType(collection, dataType,false,userFilter);
                }
                catch (Exception ex)
                {
                    log.WriteLine($"Exception occurred while counting documents in {collection.CollectionNamespace}. Details: {ex.ToString()}", LogType.Error);
                    log.WriteLine($"Using Estimated document count for {collection.CollectionNamespace}");

                    docCountByType = GetDocumentCountByDataType(collection, dataType, true, userFilter);
                }


                if (docCountByType == 0)
                {
                    log.WriteLine($"{collection.CollectionNamespace} has no documents where _id is {dataType}");
                    return null;
                }
                else if (docCountByType < minDocsPerChunk)
                {
                    log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} document(s) where _id is {dataType}. Count is less than minimum chunk size.");
                    sampleCount = 1;
                    chunkCount = 1;
                }
                else
                {
                    log.WriteLine($"{collection.CollectionNamespace} has {docCountByType} document(s) where _id is {dataType}");

                }

                if (chunkCount > MaxSamples)
                    throw new ArgumentException("Chunk count too large. Retry with larger 'Chunk Size'.");


                // Ensure minimum documents per chunk
                chunkCount = Math.Max(1, (int)Math.Ceiling((double)docCountByType / minDocsPerChunk));
                docsInChunk = docCountByType / chunkCount;

                // Calculate segments based on documents per chunk
                segmentCount = Math.Min(
                    Math.Max(1, (int)Math.Ceiling((double)docsInChunk / minDocsPerSegment)),
                    MaxSegments
                );

                // Calculate the total sample count
                sampleCount = Math.Min(chunkCount * segmentCount, MaxSamples);

                // Adjust segments per chunk based on the new sample count
                segmentCount = Math.Max(1, sampleCount / chunkCount);

                // Optimize for non-dump scenarios
                if (!optimizeForMongoDump)
                {
                    while (chunkCount > segmentCount && segmentCount < 20)
                    {
                        chunkCount--;
                        segmentCount++;
                    }

                    chunkCount = sampleCount / segmentCount;
                }


                if (chunkCount < 1)
                    throw new ArgumentException("Chunk count must be greater than 0.");


                // Step 1: Build the filter pipeline based on the data type

                BsonDocument matchCondition = BuildDataTypeCondition(dataType, userFilter);

                // Step 2: Sample the data

                log.WriteLine($"Sampling data where _id is {dataType} with {sampleCount} samples, Chunk Count: {chunkCount}");


                var pipeline = new[]
                {
                    new BsonDocument("$match", matchCondition),  // Add the match condition for the data type
                    new BsonDocument("$sample", new BsonDocument("size", sampleCount)),
                    new BsonDocument("$project", new BsonDocument("_id", 1)) // Keep only the _id key
                };

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
                        log.WriteLine($"Attempt {i} encountered error while sampling data where _id is {dataType}: {ex.ToString()}");

                    }
                }

                if (partitionValues == null || partitionValues.Count == 0)
                {
                    docCountByType = 0;
                    log.WriteLine($"No data found where _id is {dataType}");

                    return null;
                }
                // Step 3: Calculate partition boundaries

                ChunkBoundaries chunkBoundaries = new ChunkBoundaries();


                Boundary segmentBoundary = null;
                Boundary chunkBoundary = null;

                if (dataType == DataType.Other)
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
                }
                else
                {
                    for (int i = 0; i < partitionValues.Count; i++)
                    {
                        var min = partitionValues[i];
                        var max = i == partitionValues.Count - 1 ? BsonMaxKey.Value : partitionValues[i + 1];

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
                }

                log.WriteLine($"Total Chunks: {chunkBoundaries.Boundaries.Count} where _id is {dataType}");
                return chunkBoundaries;
            }
            catch(OperationCanceledException)
            {
                ///do nothing
                docCountByType = 0;
                return null;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error during sampling data where _id is {dataType}: {ex.ToString()}", LogType.Error);
                docCountByType = 0;
                return null;
            }

        }


        public static long GetDocumentCountByDataType(IMongoCollection<BsonDocument> collection, DataType dataType, bool useEstimate = false, BsonDocument? userFilter = null)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            BsonDocument matchCondition = BuildDataTypeCondition(dataType, userFilter);

            // Get the count of documents matching the filter
            if (useEstimate)
            {
                var options = new EstimatedDocumentCountOptions { MaxTime = TimeSpan.FromSeconds(300) };
                var count = collection.EstimatedDocumentCount(options);
                return count;
            }
            else
            {
                var options = new CountOptions { MaxTime = TimeSpan.FromSeconds(600) };
                var count = collection.CountDocuments(matchCondition, options);
                return count;
            }
        }

        public static BsonDocument BuildDataTypeCondition(DataType dataType, BsonDocument? userFilter = null)
        {
            BsonDocument matchCondition;
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
            if (userFilter == null || userFilter.ElementCount == 0)
            {
                return matchCondition;
            }
            else
            {
                // Combine using $and
                return new BsonDocument("$and", new BsonArray { matchCondition, userFilter });
            }
        }

        public static (BsonValue gte, BsonValue lt) GetChunkBounds(string gteStrring,string ltString, DataType dataType)
        {
            BsonValue gte = null;
            BsonValue lt = null;

            // Initialize `gte` and `lt` based on special cases
            if (gteStrring.Equals("BsonMaxKey"))
                gte = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(gteStrring))
                gte = BsonNull.Value;

            if (ltString.Equals("BsonMaxKey"))
                lt = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(ltString))
                lt = BsonNull.Value;

            // Handle by DataType
            switch (dataType)
            {
                case DataType.ObjectId:
                    gte ??= new BsonObjectId(ObjectId.Parse(gteStrring));
                    lt ??= new BsonObjectId(ObjectId.Parse(ltString));
                    break;

                case DataType.Int:
                    gte ??= new BsonInt32(int.Parse(gteStrring));
                    lt ??= new BsonInt32(int.Parse(ltString));
                    break;

                case DataType.Int64:
                    gte ??= new BsonInt64(long.Parse(gteStrring));
                    lt ??= new BsonInt64(long.Parse(ltString));
                    break;

                case DataType.String:
                    gte ??= new BsonString(gteStrring);
                    lt ??= new BsonString(ltString);
                    break;

                case DataType.Object:
                    gte ??= BsonDocument.Parse(gteStrring);
                    lt ??= BsonDocument.Parse(ltString);
                    break;

                case DataType.Decimal128:
                    gte ??= new BsonDecimal128(Decimal128.Parse(gteStrring));
                    lt ??= new BsonDecimal128(Decimal128.Parse(ltString));
                    break;

                case DataType.Date:
                    gte ??= new BsonDateTime(DateTime.Parse(gteStrring));
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
