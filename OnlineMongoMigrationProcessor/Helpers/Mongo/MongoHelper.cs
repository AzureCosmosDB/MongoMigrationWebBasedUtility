using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.GeoJsonObjectModel.Serializers;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;


// Nullability handled explicitly; removed pragmas.

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    internal static class MongoHelper
    {
        // Define the new delegate type - made public for use in ParallelWriteHelper
        public delegate void CounterDelegate<TMigration>(
             TMigration migration,
             CounterType type,
             ChangeStreamOperationType? operationType = null,
             int count = 1);

   

        public static BsonValue GetIdRangeMin(BsonDocument? filter)
        {
            if (filter == null)
                return BsonMinKey.Value;

            // Direct _id clause
            if (filter.Contains("_id") && filter["_id"].IsBsonDocument)
            {
                var idDoc = filter["_id"].AsBsonDocument;
                if (idDoc.TryGetValue("$gte", out var gte))
                    return gte;
                if (idDoc.TryGetValue("$gt", out var gt))
                    return gt;
            }

            // Handle logical operators recursively
            foreach (var key in new[] { "$and", "$or" })
            {
                if (filter.Contains(key) && filter[key].IsBsonArray)
                {
                    var clauses = filter[key].AsBsonArray;

                    var mins = clauses
                        .Where(c => c.IsBsonDocument)
                        .Select(c => GetIdRangeMin(c.AsBsonDocument))
                        .Where(v => v != BsonMinKey.Value)
                        .ToList();

                    if (mins.Count > 0)
                        return mins.OrderBy(v => v, new BsonValueComparerSimple()).First();
                }
            }

            return BsonMinKey.Value;
        }

        public static BsonValue GetIdRangeMax(BsonDocument? filter)
        {
            if (filter == null)
                return BsonMaxKey.Value;

            // Direct _id clause
            if (filter.Contains("_id") && filter["_id"].IsBsonDocument)
            {
                var idDoc = filter["_id"].AsBsonDocument;
                if (idDoc.TryGetValue("$lte", out var lte))
                    return lte;
                if (idDoc.TryGetValue("$lt", out var lt))
                    return lt;
            }

            // Handle logical operators recursively
            foreach (var key in new[] { "$and", "$or" })
            {
                if (filter.Contains(key) && filter[key].IsBsonArray)
                {
                    var clauses = filter[key].AsBsonArray;

                    var maxes = clauses
                        .Where(c => c.IsBsonDocument)
                        .Select(c => GetIdRangeMax(c.AsBsonDocument))
                        .Where(v => v != BsonMaxKey.Value)
                        .ToList();

                    if (maxes.Count > 0)
                        return maxes.OrderByDescending(v => v, new BsonValueComparerSimple()).First();
                }
            }

            return BsonMaxKey.Value;
        }
        private class BsonValueComparerSimple : IComparer<BsonValue>
        {
            public int Compare(BsonValue? x, BsonValue? y)
            {
                if (x == null && y == null) return 0;
                if (x == null) return -1;
                if (y == null) return 1;
                return x.CompareTo(y);
            }
        }
        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit mu )
        {
            FilterDefinition<BsonDocument>? userFilter = GetFilterDoc(mu.UserFilter);
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
		/// <returns>List of collection names, excluding system collections, views, and other non-collection types</returns>
		public static async Task<List<string>> ListCollectionsAsync(string connectionString, string databaseName)
		{
			var collections = new List<string>();
			try
			{
				var client = new MongoClient(connectionString);
				var database = client.GetDatabase(databaseName);
				
				// Get all collections (system collections will be filtered below)
				var collectionsCursor = await database.ListCollectionsAsync();
				var allCollections = await collectionsCursor.ToListAsync();

				foreach (var collectionInfo in allCollections)
				{
					var collectionName = collectionInfo["name"].AsString;
					
					// Skip system collections (additional check)
					if (IsSystemCollection(collectionName))
						continue;
					
					// Check if it's a collection (not a view or other type)
					var type = collectionInfo.GetValue("type", "collection").AsString;
					if (type == "collection")
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

            if (dataType == DataType.Other)
                skipDataTypeFilter = true;

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

            if (userFilter != null && userFilter.Document.ElementCount>0)
            {
                // Combine userFilter with idFilter using AND
                //if (!MongoHelper.UsesIdFieldInFilter(userFilterDoc)) // if user filter does not use _id, we can combine at root                {
                //{
                    // Combine userFilter with idFilter using AND
                    return filterBuilder.And(userFilter, idFilter);
                //}
            }

            return idFilter;
        }


        public static long GetDocumentCount(IMongoCollection<BsonDocument> collection, BsonValue? gte, BsonValue? lte, DataType dataType, BsonDocument userFilterDoc, bool skipDataTypeFilter = false)
        {
            FilterDefinition<BsonDocument> filter = GenerateQueryFilter(gte, lte, dataType,userFilterDoc, skipDataTypeFilter);
                        
            // Execute the query and return the count with 10 minute timeout
            return collection.CountDocuments(filter, new CountOptions { MaxTime = TimeSpan.FromMinutes(10) });           
        }

        public static long GetDocumentCount(
           IMongoCollection<BsonDocument> collection,
           FilterDefinition<BsonDocument>? filter,
           BsonDocument? userFilterDoc)
        {

            // Use empty filter if null
            filter ??= Builders<BsonDocument>.Filter.Empty;

            FilterDefinition<BsonDocument> combinedFilter = filter;

            // Only combine if userFilterDoc is non-null and has elements
            if (userFilterDoc != null && userFilterDoc.ElementCount > 0)
            {
                try
                {
                    var userFilter = new BsonDocumentFilterDefinition<BsonDocument>(userFilterDoc);

                    // Safely combine filters
                    combinedFilter = Builders<BsonDocument>.Filter.And(filter, userFilter);
                }
                catch (Exception ex)
                {
                    combinedFilter = filter; // fallback to base filter
                }
            }

            return collection.CountDocuments(
                combinedFilter,
                new CountOptions { MaxTime = TimeSpan.FromMinutes(120) }
            );
            
        }

        public static async Task<(bool IsCSEnabled, string Version)> IsChangeStreamEnabledAsync(Log log,string PEMFileContents,string connectionString, MigrationUnit mu, bool createCollection=false)
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
                    databaseName = mu.DatabaseName;
                    collectionName = mu.CollectionName;
                }


                if(Helper.IsRU(connectionString)) //for MongoDB API
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

                    version = GetServerVersion(client);

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


        public static string GetServerVersion(MongoClient client)
        {
            // Check the server status to verify replica set or sharded cluster
            var adminDatabase = client.GetDatabase("admin");

            // Get Mongo Version
            var verCommand = new BsonDocument("buildInfo", 1);
            var result = adminDatabase.RunCommandAsync<BsonDocument>(verCommand).GetAwaiter().GetResult();
                        
            string version = result["version"].AsString;
            return version;
        }

        public async static Task SetChangeStreamResumeTokenAsync(Log log, MongoClient client, MigrationJob job, MigrationUnit mu, int seconds, bool syncBack, CancellationToken cts)
        {
            int retryCount = 0;
            bool isSucessful = false;
           bool resetCS = false;

           if(mu!=null)
                    resetCS = mu.ResetChangeStream;
        
            // Determine if we should use server-level or collection-level processing
            bool useServerLevel = job.ChangeStreamLevel == ChangeStreamLevel.Server;
            bool skipLoops = false;
            string resumeToken=string.Empty;    
            string syncBackPrefix = syncBack ? "SyncBack: " : string.Empty;

            string originalResumeToken = string.Empty;
            DateTime startedOnUtc= DateTime.MinValue;
            DateTime start= DateTime.MinValue;
        
            IMongoCollection<BsonDocument>? collection = null;
            var options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };

            while (!isSucessful && retryCount < 10)
            {
                try
                {
                    if (!useServerLevel)
                    {
                        //BsonDocument resumeToken = new BsonDocument();
                        var database = client.GetDatabase(mu.DatabaseName);
                        collection = database.GetCollection<BsonDocument>(mu.CollectionName);

                        // Initialize with safe defaults; will be overridden below
                    
                        if (job.JobType == JobType.RUOptimizedCopy)
                        {
                            if (string.IsNullOrEmpty(mu.OriginalResumeToken))
                            {
                                await WatchChangeStreamUntilChangeAsync(log, client, job, mu, collection, options, resetCS, seconds, syncBack, cts, useServerLevel);
                            }
                            isSucessful = true;
                            return;
                        }

                        resumeToken = mu.ResumeToken ?? string.Empty;
                        originalResumeToken = mu.OriginalResumeToken ?? string.Empty;
                        startedOnUtc = mu.ChangeStreamStartedOn.HasValue ? mu.ChangeStreamStartedOn.Value.ToUniversalTime() : DateTime.UtcNow;
                        start = (mu.ChangeStreamStartedOn ?? DateTime.UtcNow).AddMinutes(-15).ToUniversalTime();
                    }
                    if (syncBack)
                    {
                        if (useServerLevel)
                        {
                            resumeToken = job.SyncBackResumeToken ?? string.Empty;
                            originalResumeToken = job.OriginalResumeToken ?? string.Empty;
                            startedOnUtc = job.SyncBackChangeStreamStartedOn.HasValue ? job.SyncBackChangeStreamStartedOn.Value.ToUniversalTime() : DateTime.UtcNow;
                            start = startedOnUtc;
                        }
                        else
                        {
                            resumeToken = mu.SyncBackResumeToken ?? string.Empty;
                            originalResumeToken = mu.SyncBackResumeToken ?? string.Empty;
                            startedOnUtc = mu.SyncBackChangeStreamStartedOn.HasValue ? mu.SyncBackChangeStreamStartedOn.Value.ToUniversalTime() : DateTime.UtcNow;
                            start = startedOnUtc;
                        }
                    }
                    else
                    {
                        if (useServerLevel)
                        {
                            resumeToken = job.ResumeToken ?? string.Empty;
                            originalResumeToken = job.OriginalResumeToken ?? string.Empty;
                            startedOnUtc= job.ChangeStreamStartedOn.HasValue ? job.ChangeStreamStartedOn.Value.ToUniversalTime() : DateTime.UtcNow;
                            start = startedOnUtc;
                        }
                    }

                    if (resetCS)
                    {
                        if (!string.IsNullOrEmpty(resumeToken))
                        {
                            if(useServerLevel)
                               log.WriteLine($"{syncBackPrefix}Resetting server-level change stream resume token  to {startedOnUtc} (UTC)");
                            else
                               log.WriteLine($"{syncBackPrefix}Resetting change stream resume token for {mu.DatabaseName}.{mu.CollectionName} to {startedOnUtc} (UTC)");

                            options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(mu.OriginalResumeToken) };
                        }
                        else
                        {
                            //try to go 15 min back in time, temporary fix for backward compatibility, not applicable for syncBack
                            if(useServerLevel)
                               log.WriteLine($"{syncBackPrefix}Resetting server-level change stream start time  to {start} (UTC)");
                            else
                               log.WriteLine($"{syncBackPrefix}Resetting change stream start time for {mu.DatabaseName}.{mu.CollectionName} to {start} (UTC)");

                            var bsonTimestamp = ConvertToBsonTimestamp(start);
                            options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }
                    }
                    else
                    {
                        // Check if resume token already exists based on processing level
                        string existingResumeToken = string.Empty;

                        if (!string.IsNullOrEmpty(resumeToken))
                        {
                            if (useServerLevel)
                                log.WriteLine($"{syncBackPrefix}Server-level change stream resume token already set", LogType.Debug);
                            else
                                log.WriteLine($"{syncBackPrefix}Collection-level change stream resume token for {mu.DatabaseName}.{mu.CollectionName} already set", LogType.Debug);
                            return;
                        }
                        else
                        {

                            DateTime csLastChecked = useServerLevel ? (job.CSLastChecked ?? DateTime.MinValue) : (mu.CSLastChecked ?? DateTime.MinValue);

                            if (csLastChecked == DateTime.MinValue)
                            {
                                if(useServerLevel)
                                {
                                    job.CSLastChecked = startedOnUtc;
                                    log.WriteLine($"{syncBackPrefix}Server-level CSLastChecked set to {job.CSLastChecked}", LogType.Debug);

                                }
                                else
                                {
                                    mu.CSLastChecked = startedOnUtc;
                                    log.WriteLine($"{syncBackPrefix}Collection-level CSLastChecked for {mu.DatabaseName}.{mu.CollectionName} set to {mu.CSLastChecked}", LogType.Debug);
                                }
                                csLastChecked = startedOnUtc;

                            }

                            //effective start time is startedOnUtc if its less than 10 minutes from now,else use CSLastChecked
                            var effctiveStartTime = (DateTime.UtcNow - startedOnUtc).TotalMinutes <= 10 ? startedOnUtc : csLastChecked;
                                                                                           
                            var bsonTimestamp = ConvertToBsonTimestamp(effctiveStartTime.ToUniversalTime());
                            options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                            resetCS = true; //reusing existing varibale
                        }

                    }

                    await WatchChangeStreamUntilChangeAsync(log, client, job, mu, collection, options, resetCS, seconds, syncBack, cts, useServerLevel);


                    isSucessful = true;
                }
                catch (OperationCanceledException)
                {                
                    isSucessful = true;
                }
                catch (Exception ex) when (ex is MongoExecutionTimeoutException || ex is TimeoutException)
                {
                    log.WriteLine($"{syncBackPrefix}Timeout when setting change stream resume token for {mu.DatabaseName}.{mu.CollectionName}: {ex}",LogType.Debug);

                    if (resetCS && string.IsNullOrEmpty(resumeToken))
                        skipLoops = true;
                }
                catch (Exception ex)
                {
                    if (resetCS && !string.IsNullOrEmpty(resumeToken))
                    {
                        retryCount++;
                        log.WriteLine($"{syncBackPrefix}Attempt {retryCount}. Error setting change stream resume token for {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Error);

                        // If all retries exhausted, clear the flag to prevent infinite retry loop
                        if (retryCount >= 10 && mu.ResetChangeStream)
                        {
                            mu.ResetChangeStream = false;
                            log.WriteLine($"{syncBackPrefix}Failed to reset change stream for {mu.DatabaseName}.{mu.CollectionName} after {retryCount} attempts. Flag cleared to prevent infinite retries. Manual intervention may be required.", LogType.Error);
                        }
                    }
                    else
                        skipLoops = true;
                }
                finally
                {
                    if (useServerLevel)
                        log.WriteLine($"{syncBackPrefix}Exiting Server-level SetChangeStreamResumeTokenAsync ", LogType.Debug);
                    else
                        log.WriteLine($"{syncBackPrefix}Exiting Collection-level SetChangeStreamResumeToken for {mu.DatabaseName}.{mu.CollectionName} - ResumeToken: {(!string.IsNullOrEmpty(mu.ResumeToken) ? "SET" : "NOT SET")}, InitialDocReplayed: {mu.InitialDocumenReplayed}", LogType.Debug);

                    if(mu!=null)
                        MigrationJobContext.SaveMigrationUnit(mu, false);                    
                }

                if (skipLoops)
                    return;
            }
            return;
        }

        private static async Task WatchChangeStreamUntilChangeAsync(Log log, MongoClient client, MigrationJob job, MigrationUnit mu, IMongoCollection<BsonDocument> collection, ChangeStreamOptions options, bool forced, int seconds, bool syncBack, CancellationToken manualCts, bool useServerLevel = false)
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

            // Set MaxAwaitTime to control how long each MoveNextAsync waits for changes
            // This allows multiple polling attempts within the overall timeout
            if (options.MaxAwaitTime == null)
            {
                options.MaxAwaitTime = TimeSpan.FromMilliseconds(500); // Poll every 500ms
            }

            CancellationTokenSource cts;
            if (seconds > 0)
                cts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
            else
                cts = new CancellationTokenSource();

            CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, manualCts);

            // Choose between server-level or collection-level change stream
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
        
        
            if (useServerLevel)
            {
                // Server-level change stream    
                job.CSLastChecked = DateTime.UtcNow;           
                log.WriteLine($"Setting up server-level change stream resume token ");
                cursor = await client.WatchAsync<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);
            }
            else
            {
                // Collection-level change stream
                mu.CSLastChecked = DateTime.UtcNow;
                MigrationJobContext.SaveMigrationUnit(mu, true);
            
                MigrationJobContext.AddVerboseLog(($"Setting up collection-level change stream resume token for {mu.DatabaseName}.{mu.CollectionName}"));
                cursor = await collection.WatchAsync<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);
            }


            using (cursor)
            {
                try
                {                   

                    if (job.JobType == JobType.RUOptimizedCopy)
                    {
                        // Use linkedCts.Token instead of cts.Token to respect both timeout and manual cancellation
                        if (await cursor.MoveNextAsync(linkedCts.Token))
                        {
                            if (!forced || string.IsNullOrEmpty(mu.OriginalResumeToken))
                            {
                                var resumeTokenJson = cursor.GetResumeToken().ToJson();

                                mu.ResumeToken = resumeTokenJson;
                                mu.OriginalResumeToken = resumeTokenJson;

                            }
                            return;
                        }
                        return;
                    }

                    // Iterate until cancellation or first change detected
                    // Use linkedCts to respect both timeout and manual cancellation
                    while (!linkedCts.Token.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(linkedCts.Token);
                        if (!hasNext)
                        {
                            break; // Stream closed or no more data
                        }

                        foreach (var change in cursor.Current)
                        {
                            // Handle server-level vs collection-level resume token storage
                            if (useServerLevel)
                            {
                                var databaseName = change.CollectionNamespace.DatabaseNamespace.DatabaseName;
                                var collectionName = change.CollectionNamespace.CollectionName;
                                var collectionKey = $"{databaseName}.{collectionName}";

                                //checking if change is in collections to be migrated.
                                var key=Helper.GenerateMigrationUnitId(databaseName, collectionName);

                                var migrationUnit= MigrationJobContext.GetMigrationUnit(key);

                                // Use common function for server-level resume token setting
                                SetResumeTokenProperties(job, change, forced,syncBack,log, migrationUnit);

                                //resetting counters for all migration units
                                ResetCountersForAllMigrationUnits(job, syncBack);

                                log.WriteLine($"Server-level resume token set  with collection key {job.ResumeCollectionKey}");
                                // Exit immediately after first change detected
                                return;
                            
                            }
                            else
                            {
                                //if bulk load is complete, no point in continuing to watch
                                if ((mu.RestoreComplete || job.IsSimulatedRun) && mu.DumpComplete && !forced)
                                    return;

                                // Use common function for collection-level resume token setting
                                SetResumeTokenProperties(mu, change, forced, syncBack,log);

                                MigrationJobContext.AddVerboseLog($"Collection-level resume token set for {mu.DatabaseName}.{mu.CollectionName} - Operation: {mu.ResumeTokenOperation}, DocumentId: {mu.ResumeDocumentId}");

                                // Exit immediately after first change detected
                                return;
                            }

                        }
                    }

                }
                catch (Exception ex) when (ex is TimeoutException)
                {
                    MigrationJobContext.AddVerboseLog($"Timeout while watching change stream for {mu.DatabaseName}.{mu.CollectionName}: {ex}");
                }
                catch (Exception ex) when (ex is OperationCanceledException)
                {
                    // Cancellation requested - exit quietly
                }
            }
        }

        private static void ResetCountersForAllMigrationUnits(MigrationJob job, bool syncBack)
        {
            foreach (var mu in Helper.GetMigrationUnitsToMigrate(job))
            {
                ResetCounters(mu, syncBack);
            }
        }

    
        public static async Task<(bool Exits,bool IsCollection)> CheckIsCollectionAsync(MongoClient client, string databaseName, string collectionName)
        {

            var database = client.GetDatabase(databaseName);

            // Filter by collection name
            var filter = new BsonDocument("name", collectionName);

            using var cursor = await database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            var collectionInfo = await cursor.FirstOrDefaultAsync();

            if (collectionInfo == null)
            {
                return new(false, false);
            }

            // Check the "type" field returned in listCollections
            var type = collectionInfo.GetValue("type", "collection").AsString;
            return new(true, type == "collection");

        }

        public static async Task<bool> CheckRUCollectionExistsAsync(MongoClient client, string databaseName, string collectionName)
        {
            var db = client.GetDatabase(databaseName);
            var coll = db.GetCollection<RawBsonDocument>(collectionName);
            // Check if collection has at least one document or any indexes
            var hasData = await coll.Find(FilterDefinition<RawBsonDocument>.Empty)
                                    .Limit(1)
                                    .AnyAsync();
            if (hasData)
                return true;

            // If no data, check if any indexes exist (other than default)
            var indexList = await coll.Indexes.ListAsync();
            var indexes = await indexList.ToListAsync();

            // Collection exists if there are any indexes (including _id)
            return indexes.Count > 0;
        }


        public static async Task<bool> CheckCollectionExistsAsync(MongoClient client, string databaseName, string collectionName)
        {               
            MigrationJobContext.AddVerboseLog($"Checking if collection exists: {databaseName}.{collectionName}");

            var db = client.GetDatabase(databaseName);
            var coll = db.GetCollection<RawBsonDocument>(collectionName);
            try
            {
                var result = await coll.Aggregate()
                    .AppendStage<RawBsonDocument>(@"{ $collStats: { count: {} } }")
                    .FirstOrDefaultAsync();

                if (result == null)
                    return false;
                else
                    return true;
            }
            catch (MongoCommandException ex) when (ex.CodeName == "NamespaceNotFound")
            {
                return false;
            }
            catch
            {

                // Check if collection has at least one index
                var indexCursor = await coll.Indexes.ListAsync();
                var indexes = await indexCursor.ToListAsync();

                bool collectionExists = indexes.Count > 0;
                return collectionExists;
            }

        }

        public static async Task<bool> CheckCollectionValidAsync(MongoClient client, string databaseName, string collectionName)
        {
            if(await CheckCollectionExistsAsync(client, databaseName, collectionName))
            {
                (bool Exits, bool IsCollection) ret;
                try
                {
                    ret = await CheckIsCollectionAsync(client, databaseName, collectionName); //fails if connnected to secondary
                }
                catch
                {
                    return true;
                }                
                if(ret.Exits)
                {
                    return ret.IsCollection;
                }
                else
                    return false;
            }
            else
            {
                return false;
            }
        }

        public static async Task<(long CollectionSizeBytes, long DocumentCount)> GetCollectionStatsAsync(MongoClient client, string databaseName, string collectionName)
        {
            MigrationJobContext.AddVerboseLog($"Getting collection stats for {databaseName}.{collectionName}");

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
            MigrationJobContext.AddVerboseLog($"Starting index copy for {sourceCollection.CollectionNamespace.DatabaseNamespace.DatabaseName}.{sourceCollection.CollectionNamespace.CollectionName}");
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
                mu.TargetCreated = true;

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

        public static bool UsesIdFieldInFilter(BsonDocument filter)
        {
            foreach (var element in filter)
            {
                var name = element.Name;

                // Direct reference to _id
                if (name == "_id")
                    return true;

                var value = element.Value;

                // Logical operators like $and, $or, $nor contain arrays of filters
                if (name.StartsWith("$") && value.IsBsonArray)
                {
                    foreach (var sub in value.AsBsonArray)
                    {
                        if (sub.IsBsonDocument && UsesIdFieldInFilter(sub.AsBsonDocument))
                            return true;
                    }
                }

                // Nested document (e.g. { "customer": { "_id": ... } })
                if (value.IsBsonDocument && UsesIdFieldInFilter(value.AsBsonDocument))
                    return true;
            }

            return false;
        }

        public static BsonDocument GetFilterDoc(string? filter)
        {
            if (string.IsNullOrWhiteSpace(filter))
                return new BsonDocument(); // return empty document

            return BsonDocument.TryParse(filter, out var filterDoc)
                ? filterDoc
                : new BsonDocument(); // return empty if parsing fails
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
                //if (!MongoHelper.UsesIdFieldInFilter(userFilterDoc)) // if user filter does not use _id, we can combine at root
                //{
                    // Escape quotes for command-line use
                    var userFilterJsonEscaped = userFilterDoc.ToJson().Replace("\"", "\\\"");
                    rootConditions.Add(userFilterJsonEscaped.TrimStart('{').TrimEnd('}'));
                //}
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
                : BsonSerializer.Deserialize<BsonDocument>(userFilter);
             
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



        /// <summary>
        /// Common function to set resume token properties on either MigrationJob or MigrationUnit
        /// </summary>
        /// <param name="target">The target object (MigrationJob or MigrationUnit) to set properties on</param>
        /// <param name="change">The change stream document containing the resume token information</param>
        /// <param name="resetCS">Whether this is a change stream reset operation</param>
        /// <param name="databaseName">Database name for server-level operations</param>
        /// <param name="collectionName">Collection name for server-level operations</param>
        private static void SetResumeTokenProperties(object target, ChangeStreamDocument<BsonDocument> change, bool resetCS, bool syncBack, Log log, MigrationUnit muInServerMode = null)
        {
            string resumeTokenJson = change.ResumeToken.ToJson();
            string documentKeyJson = change.DocumentKey.ToJson();
            var operationType = change.OperationType;

            // Determine timestamp
            DateTime timestamp;
            if (change.ClusterTime != null)
            {
                timestamp = BsonTimestampToUtcDateTime(change.ClusterTime);
            }
            else if (change.WallTime.HasValue)
            {
                timestamp = change.WallTime.Value.ToUniversalTime();
            }
            else
            {
                timestamp = DateTime.UtcNow;
            }

            // Set properties based on target type
            if (target is MigrationJob job)
            {
                // Server-level resume token setting               

                string token=string.Empty;
                if (syncBack)
                {
                    job.SyncBackResumeToken = resumeTokenJson;
                    token =job.SyncBackOriginalResumeToken;
                    job.SyncBackCursorUtcTimestamp = timestamp;
                }
                else
                {
                    job.ResumeToken = resumeTokenJson;
                    token =job.OriginalResumeToken;
                    job.CursorUtcTimestamp = timestamp;
                }

                if (!resetCS && string.IsNullOrEmpty(token))
                {
                    if (syncBack)
                        job.SyncBackOriginalResumeToken = resumeTokenJson;                        
                    else
                        job.OriginalResumeToken = resumeTokenJson;
                }
                  
                job.ResumeTokenOperation = operationType;
                job.ResumeDocumentId = documentKeyJson;


                if (muInServerMode != null)
                {
                    if (syncBack)
                        muInServerMode.SyncBackCursorUtcTimestamp = timestamp;                        
                    else                    
                        muInServerMode.CursorUtcTimestamp = timestamp;

                    MigrationJobContext.SaveMigrationUnit(muInServerMode, true);                    
                }

                // Store collection key for server-level auto replay
                if (change.CollectionNamespace != null && muInServerMode != null)
                {
                    job.ResumeCollectionKey = $"{muInServerMode.DatabaseName}.{muInServerMode.CollectionName}";
                }

                MigrationJobContext.SaveMigrationJob(job);
            }
            else if (target is MigrationUnit mu)
            {
                // Collection-level resume token setting                
                if (string.IsNullOrEmpty(mu.OriginalResumeToken))
                {
                    if (!syncBack)
                    {
                        mu.OriginalResumeToken = resumeTokenJson;
                    }
                }

                SetResumeParameters(mu, timestamp, resumeTokenJson, syncBack);


                mu.ResumeTokenOperation = operationType;
                mu.ResumeDocumentId = documentKeyJson;

                // Clear the reset flag after successfully resetting the change stream
                // This prevents duplicate "Resetting change stream resume token" log messages
                if (resetCS)
                {
                    mu.ResetChangeStream = false;

                    ResetCounters(mu,syncBack);
                    log.WriteLine($"Counters reset for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                }
                MigrationJobContext.SaveMigrationUnit(mu,true);
            }
        }


        public static void SetResumeParameters(MigrationUnit mu, DateTime timestamp, string resumeToken, bool syncBack)
        {
            if (!syncBack)
            {
                mu.CursorUtcTimestamp = timestamp;
                mu.ResumeToken = resumeToken;
            }
            else
            {
                mu.SyncBackCursorUtcTimestamp = timestamp;
                mu.SyncBackResumeToken = resumeToken;
            }
        }

        private static void ResetCounters(MigrationUnit mu, bool syncBack)
        {
            MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.ResetCounters: muId={mu.Id}, syncBack={syncBack}");

            if (!syncBack)
            {
                mu.CSDocsUpdated = 0;
                mu.CSDocsInserted = 0;
                mu.CSDocsDeleted = 0;
                mu.CSDuplicateDocsSkipped = 0;

                mu.CSDInsertEvents = 0;
                mu.CSDeleteEvents = 0;
                mu.CSUpdateEvents = 0;
            }
            else
            {
                mu.SyncBackDocsUpdated = 0;
                mu.SyncBackDocsInserted = 0;
                mu.SyncBackDocsDeleted = 0;
                mu.SyncBackDuplicateDocsSkipped = 0;

                mu.SyncBackInsertEvents = 0;
                mu.SyncBackDeleteEvents = 0;
                mu.SyncBackUpdateEvents = 0;
            }
        }

        public static BsonTimestamp ConvertToBsonTimestamp(DateTime dateTime)
        {
            // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
            long secondsSinceEpoch = new DateTimeOffset(dateTime).ToUnixTimeSeconds();

            // BsonTimestamp requires seconds and increment (logical clock)
            // Here we're using a default increment of 0. You can adjust this if needed.
            return new BsonTimestamp((int)secondsSinceEpoch, 0);
        }

        public static bool IsCosmosRUEndpoint<T>(IMongoCollection<T> collection)
        {
            if (collection == null) return false;

            // Access the client settings via the database's client
            var settings = collection.Database.Client.Settings;

            // Check all servers (endpoints) in the settings
            return settings.Servers
                .Any(s => s.Host.Contains("mongo.cosmos.azure.com"));
        }


    }
}