using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public List<MigrationChunk> CreatePartitions(Log log, MongoClient sourceClient, string databaseName, string collectionName)
        {
            _log = log;

            var database = sourceClient.GetDatabase(databaseName);
            _sourceCollection = database.GetCollection<BsonDocument>(collectionName);

            try
            {
                // Get partition tokens
                var startTokens = GetRUPartitionTokens(new BsonTimestamp(0, 0));
                var stopTokens = GetRUPartitionTokens(MongoHelper.ConvertToBsonTimestamp(DateTime.UtcNow));
                //

                if (!startTokens.Any() || !stopTokens.Any())
                {
                    _log.WriteLine($"No RU partition tokens found for {_sourceCollection.CollectionNamespace}", LogType.Error);
                    return new List<MigrationChunk>();
                }

                List<MigrationChunk> chunks = new List<MigrationChunk>();

                int counter = 0;
                foreach (var token in startTokens)
                {
                    var stopToken = GetCurrentResumeTokenAsync(stopTokens[counter], _sourceCollection);
                    //var partitionId = token.ToJson();

                    var chunk = new MigrationChunk(counter.ToString(), token.ToJson(), stopToken.ToJson());
                    chunks.Add(chunk);
                    counter++;
                }
                return chunks;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing RU partitions: {ex}", LogType.Error);
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

        /// <summary>
        /// Get the current resume token for a partition (StopFeedItem)
        /// </summary>
        private BsonDocument GetCurrentResumeTokenAsync(
            BsonDocument partitionToken,
            IMongoCollection<BsonDocument> collection)
        {
            var pipeline = new BsonDocument[]
            {
                new BsonDocument("$match", new BsonDocument("operationType",
                    new BsonDocument("$in", new BsonArray { "insert", "update", "replace" })
                )),
                new BsonDocument("$project", new BsonDocument
                {
                    { "_id", 1 },
                    { "fullDocument", 1 },
                    { "ns", 1 },
                    { "documentKey", 1 }
                })
            };

            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                ResumeAfter = partitionToken
            };

            //using var cursor = collection.Watch<BsonDocument>(pipeline, options);
            using var cursor = collection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options);

            cursor.MoveNext();

            return cursor.GetResumeToken();

            /*
            while (cursor.MoveNext())
            {

                foreach (var change in cursor.Current)
                {
                    var resumeToken = change.ResumeToken;
                    var document = change.FullDocument;
                    var namespaceInfo = change.CollectionNamespace; // optional

                    // TODO: Process the document
                    Console.WriteLine(change.ResumeToken.ToJson());
                    Console.WriteLine(change.DocumentKey.ToJson());

                    return change.DocumentKey;
                    //cursor.MoveNext();

                    //return cursor.GetResumeToken();
                }
            }
            return null;
            */
        }
    }
}
