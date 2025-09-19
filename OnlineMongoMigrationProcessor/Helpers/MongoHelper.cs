﻿using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.GeoJsonObjectModel.Serializers;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;


// Nullability handled explicitly; removed pragmas.

namespace OnlineMongoMigrationProcessor
{
    internal static class MongoHelper
    {
        // Define the new delegate type
        public delegate void CounterDelegate<TMigration>(
             TMigration migration,
             CounterType type,
             ChangeStreamOperationType? operationType = null,
             int count = 1);


        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit mu )
        {
            FilterDefinition<BsonDocument>? userFilter = BsonDocument.Parse(mu.UserFilter ?? "{}");
            var filter = userFilter ?? Builders<BsonDocument>.Filter.Empty;
            return collection.CountDocuments(filter);
        }

		/// <summary>
		/// Helper method to list all databases from MongoDB connection
		/// </summary>
		/// <param name="connectionString">MongoDB connection string</param>
		/// <returns>List of database names, excluding system databases</returns>
		public static async Task<List<string>> ListDatabasesAsync(string connectionString)
		{
			var databases = new List<string>();
			try
			{
				var client = new MongoClient(connectionString);
				var databasesCursor = await client.ListDatabasesAsync();
				var databasesDocument = await databasesCursor.ToListAsync();

				foreach (var db in databasesDocument)
				{
					var dbName = db["name"].AsString;
					// Skip system databases
					if (!IsSystemDatabase(dbName))
					{
						databases.Add(dbName);
					}
				}
			}
			catch (Exception)
			{
				// Return empty list if connection fails
			}
			return databases;
		}

		/// <summary>
		/// Helper method to list all collections from a specific database
		/// </summary>
		/// <param name="connectionString">MongoDB connection string</param>
		/// <param name="databaseName">Database name</param>
		/// <returns>List of collection names, excluding system collections</returns>
		public static async Task<List<string>> ListCollectionsAsync(string connectionString, string databaseName)
		{
			var collections = new List<string>();
			try
			{
				var client = new MongoClient(connectionString);
				var database = client.GetDatabase(databaseName);
				var collectionsCursor = await database.ListCollectionNamesAsync();
				var allCollections = await collectionsCursor.ToListAsync();

				foreach (var collectionName in allCollections)
				{
					// Skip system collections
					if (!IsSystemCollection(collectionName))
					{
						collections.Add(collectionName);
					}
				}
			}
			catch (Exception)
			{
				// Return empty list if connection fails
			}
			return collections;
		}

		/// <summary>
		/// Check if database name is a system database
		/// </summary>
		private static bool IsSystemDatabase(string databaseName)
		{
			var systemDatabases = new[] { "admin", "local", "config" };
			return systemDatabases.Contains(databaseName, StringComparer.OrdinalIgnoreCase);
		}

		/// <summary>
		/// Check if collection name is a system collection
		/// </summary>
		private static bool IsSystemCollection(string collectionName)
		{
			return collectionName.StartsWith("system.", StringComparison.OrdinalIgnoreCase);
		}
		public static (long Lsn, string Rid, string Min, string Max) ExtractValuesFromResumeToken(BsonDocument bsonDoc)
        {
            if (bsonDoc == null || !bsonDoc.Contains("_data"))
                throw new ArgumentException("Invalid BSON document or missing _data field", nameof(bsonDoc));

            // Step 1: Get the Base64 string from the BSON binary field
            string base64Str = Convert.ToBase64String(bsonDoc["_data"].AsBsonBinaryData.Bytes);

            // Step 2: Base64 decode into ASCII JSON string
            byte[] bytes = Convert.FromBase64String(base64Str);
            string asciiJson = Encoding.ASCII.GetString(bytes);

            // Step 3: Parse the decoded JSON
            using JsonDocument jsonDoc = JsonDocument.Parse(asciiJson);
            var root = jsonDoc.RootElement;

            // Step 4: Extract LSN
            string? rawValue = root
                .GetProperty("Continuation")[0]
                .GetProperty("State")
                .GetProperty("value")
                .GetString();

            if (rawValue == null)
                throw new InvalidOperationException("Resume token LSN value is missing or null.");

            rawValue = rawValue.Trim('"');
            long lsn = long.Parse(rawValue);

            // Step 5: Extract Rid, Min, and Max
            string rid = root.GetProperty("Rid").GetString() ?? string.Empty;

            string min = root
                .GetProperty("Continuation")[0]
                .GetProperty("FeedRange")
                .GetProperty("value")
                .GetProperty("min")
                .GetString() ?? string.Empty;

            string max = root
                .GetProperty("Continuation")[0]
                .GetProperty("FeedRange")
                .GetProperty("value")
                .GetProperty("max")
                .GetString() ?? string.Empty;

            return (lsn, rid, min, max);
        }
        //public static FilterDefinition<BsonDocument> GenerateQueryFilter(BsonValue? gte, BsonValue? lte, DataType dataType)
        //{
        //    var filterBuilder = Builders<BsonDocument>.Filter;

        //    // Initialize an empty filter
        //    FilterDefinition<BsonDocument> filter = FilterDefinition<BsonDocument>.Empty;

        //    // Create the $type filter
        //    var typeFilter = filterBuilder.Eq("_id", new BsonDocument("$type", DataTypeToBsonType(dataType)));

        //    // Add conditions based on gte and lt values
        //    if (!(gte == null || gte.IsBsonNull) && !(lte == null || lte.IsBsonNull) && (gte is not BsonMaxKey && lte is not BsonMaxKey))
        //    {
        //        filter = filterBuilder.And(
        //            typeFilter,
        //            BuildFilterGte("_id", gte, dataType),
        //            BuildFilterLt("_id", lte, dataType)
        //        );
        //    }
        //    else if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
        //    {
        //        filter = filterBuilder.And(typeFilter, BuildFilterGte("_id", gte, dataType));
        //    }
        //    else if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
        //    {
        //        filter = filterBuilder.And(typeFilter, BuildFilterLt("_id", lte, dataType));
        //    }
        //    else
        //    {
        //        filter = typeFilter;
        //    }
        //    return filter;
        //}

        public static FilterDefinition<BsonDocument> GenerateQueryFilter(
            BsonValue? gte,
            BsonValue? lte,
            DataType dataType,
            BsonDocument userFilterDoc,
            bool skipDataTypeFilter = false) 
        {

            var userFilter = new BsonDocumentFilterDefinition<BsonDocument>(userFilterDoc);

            var filterBuilder = Builders<BsonDocument>.Filter;

            FilterDefinition<BsonDocument> typeFilter;
            
            if(dataType== DataType.Other)
                skipDataTypeFilter=true;

            if (skipDataTypeFilter)
            {
                // Skip DataType filtering - use empty filter for type
                typeFilter = FilterDefinition<BsonDocument>.Empty;
            }
            else
            {
                // Original DataType filtering logic
                typeFilter = filterBuilder.Eq("_id", new BsonDocument("$type", DataTypeToBsonType(dataType)));
            }

            bool hasGte = gte != null && !gte.IsBsonNull && !(gte is BsonMaxKey);
            bool hasLte = lte != null && !lte.IsBsonNull && !(lte is BsonMaxKey);

            FilterDefinition<BsonDocument> idFilter;

            if (hasGte && hasLte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = filterBuilder.And(
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLt("_id", lte!, dataType)
                    );
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLt("_id", lte!, dataType)
                    );
                }
            }
            else if (hasGte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = BuildFilterGte("_id", gte!, dataType);
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterGte("_id", gte!, dataType)
                    );
                }
            }
            else if (hasLte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = BuildFilterLt("_id", lte!, dataType);
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterLt("_id", lte!, dataType)
                    );
                }
            }
            else
            {
                idFilter = typeFilter;
            }

            if (userFilter != null)
            {
                // Combine userFilter with idFilter using AND
                return filterBuilder.And(userFilter, idFilter);
            }

            return idFilter;
        }

        public static long GetDocumentCount(IMongoCollection<BsonDocument> collection, BsonValue? gte, BsonValue? lte, DataType dataType, BsonDocument userFilterDoc, bool skipDataTypeFilter = false)
        {
            FilterDefinition<BsonDocument> filter = GenerateQueryFilter(gte, lte, dataType, userFilterDoc, skipDataTypeFilter);

            // Execute the query and return the count
            return collection.CountDocuments(filter);
        }

        public static long GetDocumentCount(
            IMongoCollection<BsonDocument> collection,
            FilterDefinition<BsonDocument> filter,
            BsonDocument userFilterDoc)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            FilterDefinition<BsonDocument> combinedFilter;

            if (userFilterDoc != null && userFilterDoc.ElementCount > 0)
            {
                // Convert userFilterDoc (BsonDocument) to FilterDefinition
                var userFilter = new BsonDocumentFilterDefinition<BsonDocument>(userFilterDoc);

                // Combine filters with AND
                combinedFilter = filterBuilder.And(filter, userFilter);
            }
            else
            {
                combinedFilter = filter;
            }

            var countOptions = new CountOptions
            {
                MaxTime = TimeSpan.FromMinutes(120) // Set the timeout
            };

            return collection.CountDocuments(combinedFilter, countOptions);
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


                if(connectionString.Contains("mongo.cosmos.azure.com")) //for MongoDB API
                {
                    var database = client.GetDatabase(databaseName);
                    var collection = database.GetCollection<BsonDocument>(collectionName);
                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    };
                    var pipeline = new BsonDocument[]
                    {
                        new BsonDocument("$match", new BsonDocument("operationType",
                            new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" }))),
                        new BsonDocument("$project", new BsonDocument
                        {
                            { "operationType", 1 },  // ✅ include this
                            { "_id", 1 },
                            { "fullDocument", 1 },
                            { "ns", 1 },
                            { "documentKey", 1 }
                        })
                    };

                    using var cursor = collection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options);
                    return (IsCSEnabled: true, Version: "");
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
            catch (MongoCommandException ex) when (ex.Message.Contains("Match stage must include constraints on"))
            {
                log.WriteLine("Online migration capability is not enabled on source RU account. Please contact cdbmigrationsupport@microsoft.com", LogType.Error);

                return (IsCSEnabled: false, Version: "");

            }
            catch (Exception ex)
            {
				log.WriteLine($"Error checking for change streams: {ex}", LogType.Error);
                
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

    public async static Task SetChangeStreamResumeTokenAsync(Log log,MongoClient client, JobList jobList, MigrationJob job, MigrationUnit unit, int seconds, CancellationToken cts)
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

                            return;
                        }
                       
                        options = new ChangeStreamOptions
                        {                            
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                        };
                    }


                    //new way to get resume token
                    //On MongoDB 4.0+, the WatchChangeStreamAsync method opens a change stream and waits for changes.
                    //On MongoDB 3.6, TailOplogAsync opens a tailable cursor on the oplog, filtering on namespace and timestamp to detect new operations.
                    if (!string.IsNullOrEmpty(job.SourceServerVersion) && job.SourceServerVersion.StartsWith("3"))
                    {
                        if (!await TailOplogAsync(client, unit.DatabaseName, unit.CollectionName, unit, cts) && !resetCS)
                        {
                            //if failed to tail oplog, fallback to watching change stream infinelty. Should be called async only
                            await WatchChangeStreamUntilChangeAsync(log, client, jobList, job, unit, collection, options, resetCS, -1, cts);                           
                        }
                    }
                    else
                        await WatchChangeStreamUntilChangeAsync(log, client, jobList, job, unit, collection, options, resetCS, seconds, cts);
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

                    log.WriteLine($"Attempt {retryCount}. Error setting change stream resume token for {unit.DatabaseName}.{unit.CollectionName}: {ex}", LogType.Error);
                }
                finally
                {
                    jobList.Save();
                }
            }
            return;
        }

        private static async Task WatchChangeStreamUntilChangeAsync(Log log, MongoClient client, JobList jobList, MigrationJob job, MigrationUnit unit, IMongoCollection<BsonDocument> collection, ChangeStreamOptions options, bool resetCS, int seconds, CancellationToken manualCts)
        {
            var pipeline = new BsonDocument[] { };
            if (job.JobType == JobType.RUOptimizedCopy)
            {
                pipeline = new BsonDocument[]
                    {
                    new BsonDocument("$match", new BsonDocument("operationType",
                        new BsonDocument("$in", new BsonArray { "insert", "update", "replace","delete" }))
                    ),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "_id", 1 },
                        { "fullDocument", 1 },
                        { "ns", 1 },
                        { "documentKey", 1 }
                    })
                    };
            }

            CancellationTokenSource cts;
            if (seconds > 0)
                cts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
            else
                cts = new CancellationTokenSource();

            CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, manualCts);

            using var cursor = await collection.WatchAsync<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);
            {
                try
                {
                    if (job.JobType == JobType.RUOptimizedCopy)                    {

                        if (await cursor.MoveNextAsync(cts.Token))
                        {
                            if (!resetCS && string.IsNullOrEmpty(unit.OriginalResumeToken))
                            {
                                unit.ResumeToken = cursor.GetResumeToken().ToJson();
                                unit.OriginalResumeToken = unit.ResumeToken;
                            }
                            return;
                        }
                    }

                    // Iterate until cancellation or first change detected
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(linkedCts.Token);
                        if (!hasNext)
                        {
                            break; // Stream closed or no more data
                        }

                        foreach (var change in cursor.Current)
                        {
                            //if  bulk load is complete, no point in continuing to watch
                            if ((unit.RestoreComplete || job.IsSimulatedRun) && unit.DumpComplete && !unit.ResetChangeStream)
                                return;

                            // Persist values
                            unit.ResumeToken = change.ResumeToken.ToJson();

                            if (!resetCS && string.IsNullOrEmpty(unit.OriginalResumeToken))
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
                            string json = change.DocumentKey.ToJson();                            
                            unit.ResumeDocumentId = json;

                            // Exit immediately after first change detected
                            return; ;
                        }
                    }
                }
                catch (OperationCanceledException)
                {                   
                    // Cancellation requested - exit quietly
                }
            }
        }

        /// Manually tails the oplog.rs capped collection for MongoDB 3.6 support.
        /// </summary>
    private static async Task<bool> TailOplogAsync(MongoClient client, string dbName, string collectionName,MigrationUnit unit, CancellationToken cancellationToken)
        {
            try
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

                                return true; // Exit on first oplog mu detected
                            }
                        }

                        // If no data yet, wait a bit before retrying
                        await Task.Delay(1000, cancellationToken);
                    }
                }
                return true; // Successfully tailing oplog
            }
            catch {
            return false; // Return false if any error occurs
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
				log.WriteLine($"Error copying indexes: {ex}", LogType.Error);
                
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
                DataType.BinData => filterBuilder.Lt(fieldName, value.AsBsonDocument),
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
                DataType.BinData => filterBuilder.Gte(fieldName, value.AsBsonDocument),
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
                DataType.BinData => "binData",
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        public static string GenerateQueryString(BsonValue? gte, BsonValue? lte, DataType dataType, BsonDocument? userFilterDoc, MigrationUnit? migrationUnit = null)
        {
            // Check if we should skip DataType filter when DataTypeFor_Id is specified
            bool skipDataTypeFilter = migrationUnit?.DataTypeFor_Id.HasValue == true;

            // Build the _id sub-object
            var idConditions = new List<string>();

            // Only add $type condition if we're not skipping DataType filter
            if (!skipDataTypeFilter || dataType== DataType.Other)
            {
                idConditions.Add($"\\\"$type\\\": \\\"{DataTypeToBsonType(dataType)}\\\"");
            }

            if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
            {
                idConditions.Add($"\\\"$gte\\\": {BsonValueToString(gte, dataType)}");
            }

            if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
            {
                idConditions.Add($"\\\"$lte\\\": {BsonValueToString(lte, dataType)}");
            }

            var rootConditions = new List<string>();

            // Only add _id filter if we have conditions
            if (idConditions.Count > 0)
            {
                rootConditions.Add($"\\\"_id\\\": {{ {string.Join(", ", idConditions)} }}");
            }

            // Add user filter at the root level if provided
            if (userFilterDoc != null && userFilterDoc.ElementCount > 0)
            {
                // Escape quotes for command-line use
                var userFilterJsonEscaped = userFilterDoc.ToJson().Replace("\"", "\\\"");
                rootConditions.Add(userFilterJsonEscaped.TrimStart('{').TrimEnd('}'));
            }

            // If no conditions exist, return empty filter
            if (rootConditions.Count == 0)
            {
                return "{}";
            }

            // Combine into a valid JSON object
            var queryString = "{ " + string.Join(", ", rootConditions) + " }";
            return queryString;
        }

        public static bool CheckForUserFilterMatch(BsonDocument doc, BsonDocument filter)
        {
            foreach (var element in filter.Elements)
            {
                if (element.Name == "$and")
                {
                    foreach (var cond in element.Value.AsBsonArray)
                    {
                        if (!CheckForUserFilterMatch(doc, cond.AsBsonDocument)) return false;
                    }
                }
                else if (element.Name == "$or")
                {
                    bool any = false;
                    foreach (var cond in element.Value.AsBsonArray)
                    {
                        if (CheckForUserFilterMatch(doc, cond.AsBsonDocument))
                        {
                            any = true;
                            break;
                        }
                    }
                    if (!any) return false;
                }
                else if (element.Value.IsBsonDocument)
                {
                    var opDoc = element.Value.AsBsonDocument;
                    foreach (var op in opDoc.Elements)
                    {
                        switch (op.Name)
                        {
                            case "$eq":
                                if (!doc.Contains(element.Name) || doc[element.Name] != op.Value) return false;
                                break;
                            case "$gte":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) < 0) return false;
                                break;
                            case "$gt":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) <= 0) return false;
                                break;
                            case "$lte":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) > 0) return false;
                                break;
                            case "$lt":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) >= 0) return false;
                                break;
                            case "$in":
                                if (!doc.Contains(element.Name) || !op.Value.AsBsonArray.Contains(doc[element.Name])) return false;
                                break;
                            default:
                                throw new NotSupportedException($"Operator {op.Name} is not supported yet.");
                        }
                    }
                }
                else
                {
                    if (!doc.Contains(element.Name) || doc[element.Name] != element.Value) return false;
                }
            }

            return true;
        }


        public static string GenerateQueryString(BsonDocument? userFilterDoc)
        {
            if (userFilterDoc == null || userFilterDoc.ElementCount == 0)
            {
                return "{}"; // Empty filter
            }

            // Convert to JSON and escape quotes for shell usage
            var userFilterJsonEscaped = userFilterDoc.ToJson().Replace("\"", "\\\"");

            return userFilterJsonEscaped;
        }
                

        public static BsonDocument ConvertUserFilterToBSONDocument(string userFilter)
        {
            // Start with user filter or an empty JSON object
            return string.IsNullOrWhiteSpace(userFilter)
                ? new BsonDocument()
                : MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(userFilter);
             
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
                DataType.BinData => $"{{\\\"$binary\\\":{{\\\"base64\\\":\\\"{Convert.ToBase64String(value.AsBsonBinaryData.Bytes)}\\\",\\\"subType\\\":\\\"{value.AsBsonBinaryData.SubType:x2}\\\"}}}}",
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

        //public static FilterDefinition<BsonDocument> BuildFilterFromDocumentKey(BsonDocument documentKey)
        //{
        //    if (documentKey == null || !documentKey.Elements.Any())
        //        throw new ArgumentException("documentKey cannot be null or empty", nameof(documentKey));

        //    var builder = Builders<BsonDocument>.Filter;
        //    FilterDefinition<BsonDocument> filter = builder.Empty;

        //    foreach (var element in documentKey.Elements)
        //    {
        //        filter &= builder.Eq(element.Name, element.Value);
        //    }

        //    return filter;
        //}

        public static FilterDefinition<BsonDocument> BuildFilterFromDocumentKey(BsonDocument documentKey)
        {
            var filters = new List<FilterDefinition<BsonDocument>>();

            foreach (var element in documentKey.Elements)
            {
                filters.Add(Builders<BsonDocument>.Filter.Eq(element.Name, element.Value));
            }

            return filters.Count == 1
                ? filters[0]
                : Builders<BsonDocument>.Filter.And(filters);
        }



        public static BsonTimestamp ConvertToBsonTimestamp(DateTime dateTime)
        {
            // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
            long secondsSinceEpoch = new DateTimeOffset(dateTime).ToUnixTimeSeconds();

            // BsonTimestamp requires seconds and increment (logical clock)
            // Here we're using a default increment of 0. You can adjust this if needed.
            return new BsonTimestamp((int)secondsSinceEpoch, 0);
        }

        public static async Task<int> ProcessInsertsAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            AggressiveChangeStreamHelper? aggressiveHelper = null;
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                aggressiveHelper = new AggressiveChangeStreamHelper(targetClient, log, jobId);
            }

            // Insert operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate inserts by _id to avoid duplicate key errors within the same batch
                var deduplicatedInserts = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson()) //use document key instead of _id
                    .Select(g => g.First()) // Take the first occurrence of each document
                    .ToList();

                //log all ids inbatch as CSV
                //var idsInBatch = deduplicatedInserts
                //    .Select(e => e.FullDocument["_id"].ToString())
                //    .ToList();
                //log.WriteLine($"{logPrefix} Processing {deduplicatedInserts.Count} inserts (deduplicated from {batch.Length}) with _ids: {string.Join(", ", idsInBatch)} in {collection.CollectionNamespace.FullName}");

                // Remove from temp collection if aggressive mode is enabled
                if (isAggressive && !isAggressiveComplete && aggressiveHelper != null)
                {
                    var documentKeysToRemove = deduplicatedInserts
                        .Where(insertEvent => insertEvent.DocumentKey != null)
                        .Select(insertEvent => insertEvent.DocumentKey)
                        .ToList();

                    if (documentKeysToRemove.Count > 0)
                    {
                        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeysToRemove);
                    }
                }

                var insertModels = deduplicatedInserts
                    .Select(e =>
                    {
                        var doc = e.FullDocument;
                        var id = doc["_id"];

                        if (id.IsObjectId)
                            doc["_id"] = id.AsObjectId;

                        return new InsertOneModel<BsonDocument>(doc);
                    })
                    .ToList();

                long insertCount = 0;
                try
                {
                    if (insertModels.Any())
                    {
                        var result = await collection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
                        insertCount += result.InsertedCount;
                    }
                }
                catch (MongoBulkWriteException<BsonDocument> ex)
                {
                    insertCount += ex.Result?.InsertedCount ?? 0;

                    var duplicateKeyErrors = ex.WriteErrors
                        .Where(err => err.Code == 11000)
                        .ToList();

                    incrementCounter(mu, CounterType.Skipped, ChangeStreamOperationType.Insert, (int)duplicateKeyErrors.Count);


                    // Log non-duplicate key errors for inserts
                    var otherErrors = ex.WriteErrors
                        .Where(err => err.Code != 11000)
                        .ToList();

                    if (otherErrors.Any())
                    {
                        failures += otherErrors.Count;
                        log.WriteLine($"{logPrefix} Insert BulkWriteException (non-duplicate errors) in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}");
                    }
                    else if (duplicateKeyErrors.Count > 0)
                    {
                        log.AddVerboseMessage($"{logPrefix} Skipped {duplicateKeyErrors.Count} duplicate key inserts in {collection.CollectionNamespace.FullName}");
                    }
                }
                finally
                {
                    incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);
                }
            }
            return failures; // Return the count of failures encountered during processing
        }

        

        public static async Task<int> ProcessUpdatesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            AggressiveChangeStreamHelper? aggressiveHelper = null;
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                aggressiveHelper = new AggressiveChangeStreamHelper(targetClient, log, jobId);
            }

            // Update operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Group by _id to handle multiple updates to the same document in the batch
                var groupedUpdates = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson()) //use document key instead of _id
                    .Select(g => g.OrderByDescending(e => e.ClusterTime ?? new MongoDB.Bson.BsonTimestamp(0, 0)).First()) // Take the latest update for each document
                    .ToList();

                // Remove from temp collection if aggressive mode is enabled
                if (isAggressive  && isAggressiveComplete && aggressiveHelper != null)
                {
                    var documentKeysToRemove = groupedUpdates
                        .Where(updateEvent => updateEvent.DocumentKey != null)
                        .Select(updateEvent => updateEvent.DocumentKey)
                        .ToList();

                    if (documentKeysToRemove.Count > 0)
                    {
                        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeysToRemove);
                    }
                }

                var updateModels = groupedUpdates
                    .Select(e =>
                    {
                        var doc = e.FullDocument;
                        var id = doc["_id"];

                        if (id.IsObjectId)
                            id = id.AsObjectId;

                        var filter = MongoHelper.BuildFilterFromDocumentKey(e.DocumentKey);
                        //var filter = Builders<BsonDocument>.Filter.Eq("_id", id);

                        // Use ReplaceOneModel instead of UpdateOneModel to avoid conflicts with unique indexes
                        return new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                    })
                    .ToList();

                long updateCount = 0;
                try
                {
                    if (updateModels.Any())
                    {

                        var result = await collection.BulkWriteAsync(updateModels, new BulkWriteOptions { IsOrdered = false });

                        updateCount += result.ModifiedCount + result.Upserts.Count;
                    }
                }
                catch (MongoBulkWriteException<BsonDocument> ex)
                {
                    updateCount += (ex.Result?.ModifiedCount ?? 0) + (ex.Result?.Upserts?.Count ?? 0);

                    var duplicateKeyErrors = ex.WriteErrors
                        .Where(err => err.Code == 11000)
                        .ToList();

                    incrementCounter(mu, CounterType.Skipped, ChangeStreamOperationType.Update, duplicateKeyErrors.Count);

                    // Log non-duplicate key errors
                    var otherErrors = ex.WriteErrors
                        .Where(err => err.Code != 11000)
                        .ToList();

                    failures += otherErrors.Count;

                    if (otherErrors.Any())
                    {
                        log.WriteLine($"{logPrefix} Update BulkWriteException (non-duplicate errors) in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}");
                    }

                    // Handle duplicate key errors by attempting individual operations
                    if (duplicateKeyErrors.Any())
                    {
                        log.AddVerboseMessage($"{logPrefix} Handling {duplicateKeyErrors.Count} duplicate key errors for updates in {collection.CollectionNamespace.FullName}");

                        // Process failed operations individually
                        foreach (var error in duplicateKeyErrors)
                        {
                            try
                            {
                                // Try to find the corresponding update model and retry as update without upsert
                                var failedModel = updateModels[error.Index];
                                if (failedModel is ReplaceOneModel<BsonDocument> replaceModel)
                                {
                                    // Try update without upsert first
                                    var updateResult = await collection.ReplaceOneAsync(replaceModel.Filter, replaceModel.Replacement, new ReplaceOptions { IsUpsert = false });
                                    if (updateResult.ModifiedCount > 0)
                                    {
                                        updateCount++;
                                    }
                                    else
                                    {
                                        // Document might not exist, but we can't upsert due to unique constraint
                                        // This is expected in some scenarios - count as skipped

                                        incrementCounter(mu, CounterType.Skipped, ChangeStreamOperationType.Update, 1);
                                    }
                                }
                            }
                            catch (Exception retryEx)
                            {
                                failures++;
                                log.AddVerboseMessage($"{logPrefix} Individual retry failed for update in {collection.CollectionNamespace.FullName}: {retryEx.Message}");
                            }
                        }
                    }
                }
                finally
                {

                    incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Update, (int)updateCount);
                }
            }
            return failures; // Return the count of failures encountered during processing
        }
       

        public static async Task<int> ProcessDeletesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            AggressiveChangeStreamHelper? aggressiveHelper = null;
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                aggressiveHelper = new AggressiveChangeStreamHelper(targetClient, log, jobId);
            }

            // Delete operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate deletes by _id within the same batch
                var deduplicatedDeletes = batch
                    .GroupBy(e => e.DocumentKey != null ? e.DocumentKey.ToJson() : string.Empty) // safe null handling
                    .Where(g => !string.IsNullOrEmpty(g.Key))
                    .Select(g => g.First())
                    .ToList();


                //log all ids inbatch as CSV
                //var idsInBatch = deduplicatedDeletes
                //    .Select(e => e.DocumentKey.ToJson())
                //    .ToList();
                //log.WriteLine($"{logPrefix} Processing {deduplicatedDeletes.Count} deletes (deduplicated from {batch.Length}) with _ids: {string.Join(", ", idsInBatch)} in {collection.CollectionNamespace.FullName}");

                // Handle aggressive change stream scenario
                if (isAggressive && !isAggressiveComplete && aggressiveHelper != null)
                {
                    // Store document keys in temporary collection instead of deleting (batch operation)
                    var documentKeysToStore = deduplicatedDeletes
                        .Where(deleteEvent => deleteEvent.DocumentKey != null)
                        .Select(deleteEvent => deleteEvent.DocumentKey)
                        .ToList();

                    if (documentKeysToStore.Count > 0)
                    {
                        try
                        {
                            int storedCount = await aggressiveHelper.StoreDocumentKeysAsync(databaseName, collectionName, documentKeysToStore);
                            // Count as processed for tracking purposes
                            incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, storedCount);
                        }
                        catch (Exception ex)
                        {
                            failures++;
                            log.WriteLine($"{logPrefix} Error storing delete document keys for aggressive change stream: {ex.Message}", LogType.Error);
                        }
                    }
                }
                else
                {
                    // Normal delete processing
                    var deleteModels = deduplicatedDeletes
                        .Select(e =>
                        {
                            try
                            {
                                if (!e.DocumentKey.Contains("_id"))
                                {
                                    log.WriteLine($"{logPrefix} Delete event missing _id in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                    return null;
                                }

                                var id = e.DocumentKey.GetValue("_id", null);
                                if (id == null)
                                {
                                    log.WriteLine($"{logPrefix} _id is null in DocumentKey in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                    return null;
                                }

                                if (id.IsObjectId)
                                    id = id.AsObjectId;

                                var filter = MongoHelper.BuildFilterFromDocumentKey(e.DocumentKey);
                                return new DeleteOneModel<BsonDocument>(filter);
                            }
                            catch (Exception dex)
                            {
                                log.WriteLine($"{logPrefix} Error building delete model in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}, Error: {dex.Message}");
                                return null;
                            }
                        })
                        .Where(model => model != null)
                        .ToList();

                    if (deleteModels.Any())
                    {
                        try
                        {
                            var result = await collection.BulkWriteAsync(deleteModels, new BulkWriteOptions { IsOrdered = false });

                            incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, (int)result.DeletedCount);
                        }
                        catch (MongoBulkWriteException<BsonDocument> ex)
                        {
                            // Count successful deletes even when some fail
                            long deletedCount = ex.Result?.DeletedCount ?? 0;

                            incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, (int)deletedCount);

                            // Log errors that are not "document not found" (which is expected)
                            var significantErrors = ex.WriteErrors
                                .Where(err => !err.Message.Contains("not found") && err.Code != 11000)
                                .ToList();

                            if (significantErrors.Any())
                            {
                                failures += significantErrors.Count;
                                log.WriteLine($"{logPrefix} Bulk delete error in {collection.CollectionNamespace.FullName}: {string.Join(", ", significantErrors.Select(e => e.Message))}");
                            }
                        }
                    }
                }
            }
            return failures; // Return the count of failures encountered during processing
        }

    }
}

































