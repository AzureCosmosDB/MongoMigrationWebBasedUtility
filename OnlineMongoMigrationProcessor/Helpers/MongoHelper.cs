using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel.Serializers;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


// Nullability handled explicitly; removed pragmas.

namespace OnlineMongoMigrationProcessor
{
    internal static class MongoHelper
    {
        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit mu)
        {
            return collection.CountDocuments(Builders<BsonDocument>.Filter.Empty);
        }

        public static FilterDefinition<BsonDocument> GenerateQueryFilter(BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            // Initialize an empty filter
            FilterDefinition<BsonDocument> filter = FilterDefinition<BsonDocument>.Empty;

            // Create the $type filter
            var typeFilter = filterBuilder.Eq("_id", new BsonDocument("$type", DataTypeToBsonType(dataType)));

            // Add conditions based on gte and lt values
            if (!(gte == null || gte.IsBsonNull) && !(lte == null || lte.IsBsonNull) && (gte is not BsonMaxKey && lte is not BsonMaxKey))
            {
                filter = filterBuilder.And(
                    typeFilter,
                    BuildFilterGte("_id", gte, dataType),
                    BuildFilterLt("_id", lte, dataType)
                );
            }
            else if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
            {
                filter = filterBuilder.And(typeFilter, BuildFilterGte("_id", gte, dataType));
            }
            else if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
            {
                filter = filterBuilder.And(typeFilter, BuildFilterLt("_id", lte, dataType));
            }
            else
            {
                filter = typeFilter;
            }
            return filter;
        }

        public static long GetDocumentCount(IMongoCollection<BsonDocument> collection, BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            FilterDefinition<BsonDocument> filter = GenerateQueryFilter(gte, lte, dataType);

            // Execute the query and return the count
            return collection.CountDocuments(filter);
        }

        public static long GetDocumentCount(IMongoCollection<BsonDocument> collection, FilterDefinition<BsonDocument> filter)
        {
            var countOptions = new CountOptions
            {
                MaxTime = TimeSpan.FromMinutes(120) // Set the timeout
            };

            // Execute the query and return the count with the specified timeout
            return collection.CountDocuments(filter, countOptions);
        }

        public static bool GetPendingOplogCountAsync(Log log, MongoClient client, long secondsSinceEpoch, string collectionNameNamespace)
        {
            try
            {
                var localDb = client.GetDatabase("local");
                var oplog = localDb.GetCollection<BsonDocument>("oplog.rs");

                // Convert secondsSinceEpoch (UNIX timestamp) to DateTime (UTC)
                var wallTime = DateTimeOffset.FromUnixTimeSeconds(secondsSinceEpoch).UtcDateTime;

                var filter = Builders<BsonDocument>.Filter.And(
                    Builders<BsonDocument>.Filter.Gte("wall", wallTime),
                    Builders<BsonDocument>.Filter.Eq("ns", collectionNameNamespace)
                );

                var count = oplog.CountDocuments(filter);
                log.WriteLine($"Approximate pending oplog entries for  {collectionNameNamespace} is {count}");
                return true;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Could not calculate pending oplog entries. Reason: {ex.Message}");
                //do nothing
                return false;
            }
        }


        public static async Task<(bool IsCSEnabled, string Version)> IsChangeStreamEnabledAsync(Log log,string PEMFileContents,string connectionString, MigrationUnit unit, bool createCollection=false)
        {
            string version = string.Empty;
            string collectionName = string.Empty;
            string databaseName = string.Empty;
            MongoClient? client = null;
            try
            {
                //// Connect to the MongoDB server
                client = MongoClientFactory.Create(log,connectionString,true, PEMFileContents);

                
                if (createCollection)
                {
                    databaseName = Guid.NewGuid().ToString();
                    collectionName = "test";

                    var database = client.GetDatabase(databaseName);
                    var collection = database.GetCollection<BsonDocument>(collectionName);

                    // Insert a dummy document
                    var dummyDoc = new BsonDocument
                    {
                        { "name", "dummy" },
                        { "timestamp", DateTime.UtcNow }
                    };

                    await collection.InsertOneAsync(dummyDoc);
                }
                else
                {
                    databaseName = unit.DatabaseName;
                    collectionName = unit.CollectionName;
                }

                if (connectionString.Contains("mongocluster.cosmos.azure.com")) //for vcore
                {
                    var database = client.GetDatabase(databaseName);
                    var collection = database.GetCollection<BsonDocument>(collectionName);

                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    };
                    using var cursor = await collection.WatchAsync(options);

                    return (IsCSEnabled: true, Version: "");
                }
                else
                {


                    // Check the server status to verify replica set or sharded cluster
                    var adminDatabase = client.GetDatabase("admin");
                    var masterCommand = new BsonDocument("isMaster", 1);
                    var isMasterResult = await adminDatabase.RunCommandAsync<BsonDocument>(masterCommand);

                    // Get Mongo Version
                    var verCommand = new BsonDocument("buildInfo", 1);
                    var result = await adminDatabase.RunCommandAsync<BsonDocument>(verCommand);

                    version = result["version"].AsString;

                    // Check if the server is part of a replica set or a sharded cluster
                    if (isMasterResult.Contains("setName") || isMasterResult.GetValue("msg", "").AsString == "isdbgrid")
                    {
						log.WriteLine("Change streams are enabled on source (replica set or sharded cluster).");
                        
                        return (IsCSEnabled: true, Version: version);
                    }
                    else
                    {
						log.WriteLine("Change streams are not enabled on source (standalone server).", LogType.Error);
                        
                        return (IsCSEnabled: false, Version: version);
                    }
                }
            }
            catch (MongoCommandException ex) when (ex.Message.Contains("$changeStream is not supported"))
            {
				log.WriteLine("Change streams are not enabled on vCore.", LogType.Error);
                
                return (IsCSEnabled: false, Version: "");

            }
            catch (Exception ex)
            {
				log.WriteLine($"Error checking for change streams: {ex.ToString()}", LogType.Error);
                
                //return (IsCSEnabled: false, Version: version);
                throw;
            }
            finally
            {
                if (createCollection && client != null)
                {
                    await client.DropDatabaseAsync(databaseName); //drop the dummy database created to test CS
                }
            }
        }

    public static Task SetChangeStreamResumeTokenAsync(Log log,MongoClient client, JobList jobList, MigrationJob job, MigrationUnit unit)
        {
            int retryCount = 0;
            bool isSucessful = false;

            while (!isSucessful && retryCount<10)
            {
                try
                {

                    BsonDocument resumeToken = new BsonDocument();
                    bool resetCS = unit.ResetChangeStream;
                    var database = client.GetDatabase(unit.DatabaseName);
                    var collection = database.GetCollection<BsonDocument>(unit.CollectionName);

                    // Initialize with safe defaults; will be overridden below
                    var options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
  

                    if (resetCS)
                    {
                        if (!string.IsNullOrEmpty(unit.OriginalResumeToken))
                        {
                            var startedOnUtc = unit.ChangeStreamStartedOn.HasValue ? unit.ChangeStreamStartedOn.Value.ToUniversalTime() : DateTime.UtcNow;
                            log.WriteLine($"Resetting change stream resume token for {unit.DatabaseName}.{unit.CollectionName} to {startedOnUtc} (UTC)");
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(unit.OriginalResumeToken) };

                        }
                        else
                        {
                            //try to go 15 min back in time, temporary fix for backward compatibility
                            var start = (unit.ChangeStreamStartedOn ?? DateTime.UtcNow).AddMinutes(-15).ToUniversalTime();
                            log.WriteLine($"Resetting change stream start time token for {unit.DatabaseName}.{unit.CollectionName} to {start} (UTC)");
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(start);
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }

                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(unit.ResumeToken))
                        {
                            log.WriteLine($"Change stream resume token for {unit.DatabaseName}.{unit.CollectionName} already set");

                            return Task.CompletedTask;
                        }

                        options = new ChangeStreamOptions
                        {
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                        };
                    }


                    //new way to get resume token
                    //On MongoDB 4.0+, the WatchChangeStreamAsync method opens a change stream and waits for changes.
                    //On MongoDB 3.6, TailOplogAsync opens a tailable cursor on the oplog, filtering on namespace and timestamp to detect new operations.
                    if(!string.IsNullOrEmpty(job.SourceServerVersion) && job.SourceServerVersion.StartsWith("3") )
                        TailOplogAsync(client, unit.DatabaseName, unit.CollectionName, unit, CancellationToken.None).Wait();
                    else                       
                        WatchChangeStreamUntilChangeAsync(log, client, jobList, job, unit, collection, options, resetCS).Wait();
                    //end of new way to get resume token

                    isSucessful = true;

                }
                catch (OperationCanceledException)
                {
                    // Cancellation requested - exit quietly
                }
                catch (Exception ex)
                {
                    retryCount++;

                    log.WriteLine($"Attempt {retryCount}. Error setting change stream resume token for {unit.DatabaseName}.{unit.CollectionName}: {ex.ToString()}", LogType.Error);
                }
                finally
                {
                    jobList.Save();

                }
            }
            return Task.CompletedTask;
        }

        private static async Task WatchChangeStreamUntilChangeAsync(Log log, MongoClient client, JobList jobList, MigrationJob job ,MigrationUnit unit, IMongoCollection<BsonDocument> collection, ChangeStreamOptions options, bool resetCS)
        {
            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            using (var cursor = await collection.WatchAsync(options, cts.Token))
            {
                try
                {
                    // Iterate until cancellation or first change detected
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(cts.Token);
                        if (!hasNext)
                        {
                            break; // Stream closed or no more data
                        }

                        foreach (var change in cursor.Current)
                        {
                            

                            //if  bulk load is complete, no point in continuing to watch
                            if ((unit.RestoreComplete || job.IsSimulatedRun) && unit.DumpComplete && !unit.ResetChangeStream )
                                return;


                            // Persist values
                            unit.ResumeToken = change.ResumeToken.ToJson();

                            if(!resetCS && string.IsNullOrEmpty(unit.OriginalResumeToken))
                                unit.OriginalResumeToken = change.ResumeToken.ToJson();

                            if (change.ClusterTime != null)
                            {
                                unit.CursorUtcTimestamp = BsonTimestampToUtcDateTime(change.ClusterTime);
                            }
                            else if (change.WallTime.HasValue)
                            {
                                unit.CursorUtcTimestamp = change.WallTime.Value.ToUniversalTime();
                            }

                            unit.ResumeTokenOperation = (ChangeStreamOperationType)change.OperationType;

                            string json = change.DocumentKey["_id"].ToJson(); // save as string
                            // Deserialize the BsonValue to ensure it is stored correctly
                            unit.ResumeDocumentId = BsonSerializer.Deserialize<BsonValue>(json); ;
                            
                            // Exit immediately after first change detected
                            return; ;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                   Console.WriteLine($"Change stream watching was cancelled for {unit.DatabaseName}.{unit.CollectionName}.");
                    // Cancellation requested - exit quietly
                }
            }
        }

        /// Manually tails the oplog.rs capped collection for MongoDB 3.6 support.
        /// </summary>
    private static async Task TailOplogAsync(MongoClient client, string dbName, string collectionName,MigrationUnit unit, CancellationToken cancellationToken)
        {
            var localDb = client.GetDatabase("local");
            var oplog = localDb.GetCollection<BsonDocument>("oplog.rs");

            // The namespace string for filtering is "db.collection"
            string ns = $"{dbName}.{collectionName}";

            // Construct filter: ts > last timestamp or start from now
            var tsFilter = unit.ChangeStreamStartedOn.HasValue
                ? ConvertToBsonTimestamp(unit.ChangeStreamStartedOn.Value)
                : ConvertToBsonTimestamp(DateTime.UtcNow.AddSeconds(-1));

            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Gt("ts", tsFilter) & filterBuilder.Eq("ns", ns);

            // Options for tailable cursor
            var options = new FindOptions<BsonDocument>
            {
                CursorType = CursorType.TailableAwait,
                NoCursorTimeout = true,
                BatchSize = 100
            };

            using (var cursor = await oplog.FindAsync(filter, options, cancellationToken))
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    while (await cursor.MoveNextAsync(cancellationToken))
                    {
                        foreach (var doc in cursor.Current)
                        {
                            // Parse oplog document for resume info
                            var ts = doc["ts"].AsBsonTimestamp;
                            var nsValue = doc["ns"].AsString;

                            if (!string.Equals(ns, nsValue, StringComparison.Ordinal))
                                continue; // Filter different ns, just in case

                            // Update unit fields
                            unit.ChangeStreamStartedOn = BsonTimestampToUtcDateTime(ts);
                            // Oplog does not have a resume token; use ts as marker.

                            // If you have document key or op, you can extract here

                            return; // Exit on first oplog mu detected
                        }
                    }

                    // If no data yet, wait a bit before retrying
                    await Task.Delay(1000, cancellationToken);
                }
            }
        }


        public static async Task<bool> CheckCollectionExists(MongoClient client, string databaseName, string collectionName)
        {

            var database = client.GetDatabase(databaseName);

            var collection = database.GetCollection<BsonDocument>(collectionName);

            // Try to find one document (limit query to 1 for efficiency)
            var document = await collection.Find(FilterDefinition<BsonDocument>.Empty)
                                           .Limit(1)
                                           .FirstOrDefaultAsync();

            return document != null; // If a document is found, collection exists
        }

        public static async Task<(long CollectionSizeBytes, long DocumentCount)> GetCollectionStatsAsync(MongoClient client, string databaseName, string collectionName)
        {
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            var statsCommand = new BsonDocument { { "collStats", collectionName } };
            var stats = await database.RunCommandAsync<BsonDocument>(statsCommand);
            long totalCollectionSizeBytes = stats.Contains("storageSize") ? stats["storageSize"].ToInt64() : stats["size"].ToInt64();

            long documentCount;
            if (stats["count"].IsInt32)
            {
                documentCount = stats["count"].ToInt32();
            }
            else if (stats["count"].IsInt64)
            {
                documentCount = stats["count"].ToInt64();
            }
            else
            {
                throw new InvalidOperationException("Unexpected data type for document count.");
            }

            return new (totalCollectionSizeBytes, documentCount);
        }


        public static async Task<bool> DeleteAndCopyIndexesAsync(Log log,MigrationUnit mu, string targetConnectionString, IMongoCollection<BsonDocument> sourceCollection, bool skipIndexes)
        {
            try
            {
                // Extract database and collection details from the source collection
                var sourceDatabase = sourceCollection.Database;
                var sourceCollectionName = sourceCollection.CollectionNamespace.CollectionName;

                // Connect to the target database
                var targetClient = MongoClientFactory.Create(log,targetConnectionString);
                var targetDatabaseName = sourceDatabase.DatabaseNamespace.DatabaseName;
                var targetDatabase = targetClient.GetDatabase(targetDatabaseName);
                var targetCollectionName = sourceCollectionName;

				log.WriteLine($"Creating collection: {targetDatabaseName}.{targetCollectionName}");
                

                // Check if the target collection exists
                var collectionNamesCursor = await targetDatabase.ListCollectionNamesAsync();
                var collectionNames = await collectionNamesCursor.ToListAsync();
                bool targetCollectionExists = collectionNames.Contains(targetCollectionName);

                // Delete the target collection if it exists
                if (targetCollectionExists)
                {
                    await targetDatabase.DropCollectionAsync(targetCollectionName);
					log.WriteLine($"Deleted existing target collection: {targetDatabaseName}.{targetCollectionName}");
                    
                }

                if (skipIndexes)
                    return true;

				log.WriteLine($"Creating indexes for: {targetDatabaseName}.{targetCollectionName}");
                

                // Create the target collection
                await targetDatabase.CreateCollectionAsync(targetCollectionName);
                var targetCollection = targetDatabase.GetCollection<BsonDocument>(targetCollectionName);

                IndexCopier indexCopier = new IndexCopier();
                int count=await indexCopier.CopyIndexesAsync(sourceCollection, targetClient, targetDatabaseName, targetCollectionName, log);
                mu.IndexesMigrated = count;
                log.WriteLine($"{count} Indexes copied successfully to {targetDatabaseName}.{targetCollectionName}");
                
                return true;
            }
            catch (Exception ex)
            {
				log.WriteLine($"Error copying indexes: {ex.ToString()}", LogType.Error);
                
                return false;
            }
        }

        private static FilterDefinition<BsonDocument> BuildFilterLt(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            return dataType switch
            {
                DataType.ObjectId => filterBuilder.Lt(fieldName, value.AsObjectId),
                DataType.Int => filterBuilder.Lt(fieldName, value.AsInt32),
                DataType.Int64 => filterBuilder.Lt(fieldName, value.AsInt64),
                DataType.String => filterBuilder.Lt(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Lt(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Lt(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Object => filterBuilder.Lt(fieldName, value.AsBsonDocument),
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        private static FilterDefinition<BsonDocument> BuildFilterGte(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            return dataType switch
            {
                DataType.ObjectId => filterBuilder.Gte(fieldName, value.AsObjectId),
                DataType.Int => filterBuilder.Gte(fieldName, value.AsInt32),
                DataType.Int64 => filterBuilder.Gte(fieldName, value.AsInt64),
                DataType.String => filterBuilder.Gte(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Gte(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Gte(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Object => filterBuilder.Gte(fieldName, value.AsBsonDocument),
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        private static string DataTypeToBsonType(DataType dataType)
        {
            return dataType switch
            {
                DataType.ObjectId => "objectId",
                DataType.Int => "int",
                DataType.Int64 => "long",
                DataType.String => "string",
                DataType.Decimal128 => "decimal",
                DataType.Date => "date",
                DataType.Object => "object",
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        public static string GenerateQueryString(BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            // Initialize the query string
            string queryString = "{ \\\"_id\\\": { ";

            // Track the conditions added to ensure correct formatting
            var conditions = new List<string>();

            conditions.Add($"\\\"$type\\\": \\\"{DataTypeToBsonType(dataType)}\\\"");

            // Add $gte condition if present
            if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
            {
                conditions.Add($"\\\"$gte\\\": {BsonValueToString(gte, dataType)}");
            }

            // Add $lte condition if present
            if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
            {
                conditions.Add($"\\\"$lt\\\": {BsonValueToString(lte, dataType)}");
            }

            // Combine the conditions with a comma
            queryString += string.Join(", ", conditions);

            // Close the query string
            queryString += " } }";

            return queryString;
        }

        private static string BsonValueToString(BsonValue? value, DataType dataType)
        {
            if (value == null || value.IsBsonNull) return string.Empty;

            if (value is BsonMaxKey)
                return "{ \\\"$maxKey\\\": 1 }"; // Return a $maxKey representation

            return dataType switch
            {
                DataType.ObjectId => $"{{\\\"$oid\\\":\\\"{value.AsObjectId}\\\"}}",
                DataType.Int => value.AsInt32.ToString(),
                DataType.Int64 => value.AsInt64.ToString(),
                DataType.String => $"\\\"{value.AsString}\\\"",
                DataType.Decimal128 => $"{{\\\"$numberDecimal\\\":\\\"{value.AsDecimal128}\\\"}}",
                DataType.Date => $"{{\\\"$date\\\":\\\"{((BsonDateTime)value).ToUniversalTime():yyyy-MM-ddTHH:mm:ssZ}\\\"}}",
                DataType.Object => value.AsBsonDocument.ToString(),
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        public static DateTime BsonTimestampToUtcDateTime(BsonTimestamp bsonTimestamp)
        {
            // Extract seconds from the timestamp's value
            long secondsSinceEpoch = bsonTimestamp.Timestamp;

            // Convert seconds since Unix epoch to DateTime in UTC
            return DateTimeOffset.FromUnixTimeSeconds(secondsSinceEpoch).UtcDateTime;
        }

        public static BsonTimestamp ConvertToBsonTimestamp(DateTime dateTime)
        {
            // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
            long secondsSinceEpoch = new DateTimeOffset(dateTime).ToUnixTimeSeconds();

            // BsonTimestamp requires seconds and increment (logical clock)
            // Here we're using a default increment of 0. You can adjust this if needed.
            return new BsonTimestamp((int)secondsSinceEpoch, 0);
        }
    }
}


