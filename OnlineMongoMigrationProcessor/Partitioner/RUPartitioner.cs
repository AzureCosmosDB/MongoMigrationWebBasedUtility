using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using ZstdSharp.Unsafe;

namespace OnlineMongoMigrationProcessor.Partitioner
{
    public class RUPartitioner
    {

        Log _log = new Log();
        IMongoCollection<BsonDocument> _sourceCollection = null!;
        /// <summary>
        /// Process partitions using RU-optimized approach
        /// </summary>
        /// 
        public List<MigrationChunk> CreatePartitions(Log log, MongoClient sourceClient, string databaseName, string collectionName, CancellationToken _cts, bool isVerificationMode=false)
        {
            _log = log;

            var database = sourceClient.GetDatabase(databaseName);
            _sourceCollection = database.GetCollection<BsonDocument>(collectionName);

            try
            {
                // Get partition tokens
                var startTokens = GetRUPartitionTokens(new BsonTimestamp(0, 0));

                if (!startTokens.Any())
                {
                    _log.WriteLine($"No partition found for {_sourceCollection.CollectionNamespace}", LogType.Error);
                    return new List<MigrationChunk>();
                }

                List<MigrationChunk> chunks = new List<MigrationChunk>();

                int counter = 0;
                foreach (var token in startTokens)
                {
                    if(isVerificationMode)
                        _log.AddVerboseMessage($"Verifying partition #{_sourceCollection.CollectionNamespace}[{counter + 1}]");
                    else
                        _log.AddVerboseMessage($"Processing partition #{_sourceCollection.CollectionNamespace}[{counter + 1}]");

                    //for FFCF create a new resume token with the current timestamp
                    var currentToken = UpdateStartAtOperationTime(token, MongoHelper.ConvertToBsonTimestamp(DateTime.UtcNow)); // Set initial timestamp to 0

                    var chunk = new MigrationChunk(counter.ToString(), token.ToJson(), currentToken.ToJson());
                    var boundary= GetChunksStopLSN_Async(currentToken, _sourceCollection, _cts).GetAwaiter().GetResult();
                    if (boundary != null)
                    {
                        chunk.RUStopLSN = boundary.LSN;
                        chunk.Gte = boundary?.StartId?.ToString();
                        chunk.Lt = boundary?.EndId?.ToString();
                    }

                    chunks.Add(chunk);
                    counter++;
                }
                if (!isVerificationMode)
                    _log.WriteLine($"Partitioning complete for {_sourceCollection.CollectionNamespace}.");

                return chunks;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing partitions for {_sourceCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                return new List<MigrationChunk>();
            }
        }

        // <summary>
        /// Fetch change stream tokens for all partitions using the custom Cosmos DB command
        /// </summary>
        private List<BsonDocument> GetRUPartitionTokens(BsonTimestamp timestamp)
        {
            try
            {
                var database = _sourceCollection.Database;
                var command = new BsonDocument
                {
                    ["customAction"] = "GetChangeStreamTokens",
                    ["collection"] = _sourceCollection.CollectionNamespace.CollectionName,
                    ["startAtOperationTime"] = timestamp
                };

                _log.WriteLine($"Getting RU partition tokens for {_sourceCollection.CollectionNamespace}");
                var result = database.RunCommand<BsonDocument>(command);

                if (result.Contains("resumeAfterTokens"))
                {
                    var tokens = result["resumeAfterTokens"].AsBsonArray.Select(t => t.AsBsonDocument).ToList();
                    _log.WriteLine($"Found {tokens.Count} RU partition tokens for {_sourceCollection.CollectionNamespace}");
                    return tokens;
                }
                else
                {
                    throw new InvalidOperationException("No RU partition tokens found in command response");
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error getting RU partition tokens: {ex}", LogType.Error);
                throw;
            }
        }

        private async Task<Boundary?> GetChunksStopLSN_Async(BsonDocument resumeAfterToken, IMongoCollection<BsonDocument> sourceCollection,
            CancellationToken token)
        {
            try
            {
                var options = new ChangeStreamOptions
                {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    ResumeAfter = resumeAfterToken
                };

                var pipeline = new BsonDocument[]
                {
                    new BsonDocument("$match", new BsonDocument("operationType",
                        new BsonDocument("$in", new BsonArray { "insert", "update", "replace" }))
                    ),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "_id", 1 },
                        { "fullDocument", 1 },
                        { "ns", 1 },
                        { "documentKey", 1 }
                    })
                };

                // Create the change stream cursor
                using var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options);

                await Task.Run(() => cursor.MoveNext(token), token);

                var resumetoken = cursor.GetResumeToken();
                if (resumetoken == null)
                {
                    return null;
                }

                // Extract the LSN from the resume token using ExtractValuesFromResumeToken helper method
                var(lsn, rid, min, max) = MongoHelper.ExtractValuesFromResumeToken(resumetoken);
                var boundary = new Boundary();
                boundary.StartId=min;
                boundary.EndId=max;
                boundary.LSN = lsn;
                boundary.Rid = rid;
                return boundary;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error getting stop LSN for partition: {ex}", LogType.Error);
            }
            return null;
        }

       

        public static BsonDocument UpdateStartAtOperationTime(BsonDocument originalDoc, BsonTimestamp newTimestamp)
        {
            if (originalDoc == null) throw new ArgumentNullException(nameof(originalDoc));

            // deep clone so original is not mutated
            var doc = originalDoc.DeepClone().AsBsonDocument;
           
            var field = doc["_startAtOperationTime"];
                        
            if (field.IsBsonTimestamp)
            {
                // Replace the whole field (it was a BsonTimestamp) with the new one
                doc["_startAtOperationTime"] = newTimestamp;
            }           

            return doc;
        }
    }
}
