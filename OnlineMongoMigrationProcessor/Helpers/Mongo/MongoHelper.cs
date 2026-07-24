using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.GeoJsonObjectModel.Serializers;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;


// Nullability handled explicitly; removed pragmas.

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    public static class MongoHelper
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

                // Use ordinal comparison for strings to match MongoDB's binary sort order.
                // BsonValue.CompareTo uses culture-sensitive string.CompareTo which
                // disagrees with MongoDB for _ (underscore), lowercase letters, etc.
                if (x.BsonType == BsonType.String && y.BsonType == BsonType.String)
                    return string.Compare(x.AsString, y.AsString, StringComparison.Ordinal);

                return x.CompareTo(y);
            }
        }
        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit mu )
        {
            FilterDefinition<BsonDocument>? userFilter = GetFilterDoc(mu.UserFilter);
            var filter = userFilter ?? Builders<BsonDocument>.Filter.Empty;
            return collection.CountDocuments(filter, new CountOptions { MaxTime = TimeSpan.FromMinutes(10) });
        }

		/// <summary>
		/// Helper method to list all databases from MongoDB connection
		/// </summary>
		/// <param name="connectionString">MongoDB connection string</param>
		/// <returns>List of database names, excluding system databases</returns>
		public static async Task<List<string>> ListDatabasesAsync(string connectionString, string? pemFileContents = null)
		{
			var databases = new List<string>();
			try
			{
				// Route through the factory so the source CA (.pem uploaded in Settings) is applied via the
				// custom certificate validation callback. The .NET driver ignores tlsCAFile in the connection
				// string, so a bare "new MongoClient(connectionString)" cannot trust an AWS DocumentDB CA.
				var client = MongoClientFactory.Create(MigrationJobContext.Logger, connectionString, false, pemFileContents);
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
				// Re-throw so the caller (Add Collections UI) can surface the real cause (e.g. an
				// invalid source connection string) instead of silently returning an empty list, which
				// previously masked the failure as "no collections matched".
				throw;
			}
			return databases;
		}

		/// <summary>
		/// Helper method to list all collections from a specific database
		/// </summary>
		/// <param name="connectionString">MongoDB connection string</param>
		/// <param name="databaseName">Database name</param>
		/// <returns>List of collection names, excluding system collections, views, and other non-collection types</returns>
		public static async Task<List<string>> ListCollectionsAsync(string connectionString, string databaseName, string? pemFileContents = null)
		{
			var collections = new List<string>();
			try
			{
				// Route through the factory so the source CA (.pem uploaded in Settings) is applied. See
				// ListDatabasesAsync for why the raw connection string alone cannot trust an AWS DocumentDB CA.
				var client = MongoClientFactory.Create(MigrationJobContext.Logger, connectionString, false, pemFileContents);
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
				// Re-throw so the caller (Add Collections UI) can surface the real cause (e.g. an
				// invalid source connection string) instead of silently returning an empty list, which
				// previously masked the failure as "no collections matched".
				throw;
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

        /// <summary>
        /// GenerateQueryFilter with separate handling for exclusive (Lt, $lt) and inclusive (Lte, $lte) upper bounds.
        /// </summary>
        public static FilterDefinition<BsonDocument> GenerateQueryFilter(
             BsonValue? gte,
             BsonValue? lt,
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
                typeFilter = FilterDefinition<BsonDocument>.Empty;
            }
            else
            {
                typeFilter = filterBuilder.Eq("_id", new BsonDocument("$type", DataTypeToBsonType(dataType)));
            }

            bool hasGte = gte != null && !gte.IsBsonNull && !(gte is BsonMaxKey);
            bool haslt = lt != null && !lt.IsBsonNull && !(lt is BsonMaxKey);
            bool haslte = lte != null && !lte.IsBsonNull && !(lte is BsonMaxKey);

            FilterDefinition<BsonDocument> idFilter;

            if (hasGte && haslt)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = filterBuilder.And(
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLt("_id", lt!, dataType)
                    );
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLt("_id", lt!, dataType)
                    );
                }
            }
            else if (hasGte && haslte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = filterBuilder.And(
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLte("_id", lte!, dataType)
                    );
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLte("_id", lte!, dataType)
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
            else if (haslt)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = BuildFilterLt("_id", lt!, dataType);
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterLt("_id", lt!, dataType)
                    );
                }
            }
            else if (haslte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = BuildFilterLte("_id", lte!, dataType);
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterLte("_id", lte!, dataType)
                    );
                }
            }
            else
            {
                idFilter = typeFilter;
            }

            if (userFilter != null && userFilter.Document.ElementCount > 0)
            {
                return filterBuilder.And(userFilter, idFilter);
            }

            return idFilter;
        }


        public static long GetDocumentCount(IMongoCollection<BsonDocument> collection, BsonValue? gte, BsonValue? lt, BsonValue? lte, DataType dataType, BsonDocument userFilterDoc, bool skipDataTypeFilter = false)
        {
            MigrationJobContext.AddVerboseLog($"Processing GetDocumentCount for {collection.CollectionNamespace} with gte={gte?.ToJson()}, lt={lt?.ToJson()}, lte={lte?.ToJson()}, datatype={dataType}, userFilterDoc={userFilterDoc.ToJson()} ");
            try
            {
                FilterDefinition<BsonDocument> filter = GenerateQueryFilter(gte, lt, lte, dataType, userFilterDoc, skipDataTypeFilter);

                //bool genError= false;
                //if (genError)
                //    throw new Exception("Testing timeout");

                // Execute the query and return the count with 10 minute timeout
                return collection.CountDocuments(filter, new CountOptions { MaxTime = TimeSpan.FromMinutes(10) });


                
            }
            catch(Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"Exception in GetDocumentCount. Details:{ex}");
#pragma warning disable CA2200 // Rethrow to preserve stack details
                throw (ex);
#pragma warning restore CA2200 // Rethrow to preserve stack details
            }
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

                if (IsDocumentDBEndpoint(client)) //for vcore
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

                    version = await GetServerVersionAsync(client).ConfigureAwait(false);

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


        public static async Task<string> GetServerVersionAsync(MongoClient client, int timeoutSeconds = 30)
        {
            // Check the server status to verify replica set or sharded cluster
            var adminDatabase = client.GetDatabase("admin");

            // Get Mongo Version
            var verCommand = new BsonDocument("buildInfo", 1);
            
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
            var result = await adminDatabase.RunCommandAsync<BsonDocument>(verCommand, cancellationToken: cts.Token).ConfigureAwait(false);
                        
            string version = result["version"].AsString;
            return version;
        }

#if !LEGACY_MONGODB_DRIVER
        public async static Task ResetCS(MigrationJob job, MigrationUnit mu, bool syncBack)
        {
            MigrationJobContext.AddVerboseLog($"{(syncBack ? "SyncBack: " : string.Empty)}Resetting change stream resume token for {mu.DatabaseName}.{mu.CollectionName}");
           
            mu.SetResumeToken(syncBack, null);
            if (!syncBack)
                mu.OriginalResumeToken = null;
            mu.SetCursorUtcTimestamp(syncBack, DateTime.MinValue);

            // Pin the change stream start so the bootstrap opens the cursor at a known
            // historical time, not "now". Without this, units that never had
            // ChangeStreamStartedOn populated (e.g. after a Server -> Collection
            // transition) fall back to DateTime.UtcNow in the bootstrap path, which stamps
            // CursorUtcTimestamp to "now" and then rejects real historical changes with
            // a "Timestamp mismatch: Old is newer than New" exception. Anchor on this
            // collection's BulkCopyStartedOn - 4h so the cursor never starts before the
            // collection's own bulk copy began.
            if (mu.BulkCopyStartedOn.HasValue && mu.BulkCopyStartedOn.Value != DateTime.MinValue)
            {
                mu.SetChangeStreamStartedOn(syncBack, mu.BulkCopyStartedOn.Value.ToUniversalTime().AddHours(-4));
            }
            mu.CSLastChecked = DateTime.MinValue;

            mu.SetCSLastChange(syncBack, null, null);
            ResetCounters(mu, syncBack);
        }

        public async static Task SetChangeStreamResumeTokenAsync(Log log, MongoClient client, MigrationJob job, MigrationUnit mu, int seconds, bool syncBack, CancellationToken cts)
        {
            int retryCount = 0;
            bool isSucessful = false;

            // Determine if we should use server-level or collection-level processing
            bool useServerLevel = job.ChangeStreamLevel == ChangeStreamLevel.Server;
            string syncBackPrefix = syncBack ? "SyncBack: " : string.Empty;
            bool forceBootstrapAfterTransition = useServerLevel && job.GetTransitionBootstrapPending(syncBack);

            bool skipLoops = false;
            string resumeToken=string.Empty;    
            

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
                        var collectionDatabaseName = syncBack ? mu.GetEffectiveTargetDatabaseName() : mu.DatabaseName;
                        var collectionName = syncBack ? mu.GetEffectiveTargetCollectionName() : mu.CollectionName;
                        var database = client.GetDatabase(collectionDatabaseName);
                        collection = database.GetCollection<BsonDocument>(collectionName);

                        // Initialize with safe defaults; will be overridden below
                        
                        if (job.JobType == JobType.RUOptimizedCopy)
                        {
                            if (string.IsNullOrEmpty(mu.OriginalResumeToken))
                            {
                                await WatchChangeStreamUntilChangeAsync(log, client, job, mu, collection, options, seconds, syncBack, cts, useServerLevel);
                            }
                            isSucessful = true;
                            return;
                        }

                        resumeToken = mu.GetResumeToken(false) ?? string.Empty;
                        originalResumeToken = mu.OriginalResumeToken ?? string.Empty;
                        // Prefer ChangeStreamStartedOn; if missing (older builds wiped it during
                        // a Server→Collection transition), fall back to BulkCopyStartedOn − 4h
                        // and heal the MU by persisting the recovered value so subsequent rounds,
                        // UI, and other readers see a real timestamp instead of relying on the
                        // fallback. Last resort: DateTime.UtcNow (skips history but won't crash).
                        if (mu.ChangeStreamStartedOn == null)
                        {
                            var recovered = mu.BulkCopyStartedOn?.AddHours(-4) ?? DateTime.UtcNow;
                            mu.ChangeStreamStartedOn = recovered;
                            MigrationJobContext.SaveMigrationUnit(mu, false);
                            log.WriteLine($"{syncBackPrefix}ChangeStreamStartedOn was missing for {mu.DatabaseName}.{mu.CollectionName}; recovered to {recovered:O} from BulkCopyStartedOn-4h.", LogType.Warning);
                        }
                        startedOnUtc = mu.ChangeStreamStartedOn!.Value.ToUniversalTime();
                        start = mu.ChangeStreamStartedOn!.Value.AddMinutes(-15).ToUniversalTime();
                    }
                    if (syncBack)
                    {
                        if (useServerLevel)
                        {
                            resumeToken = job.GetResumeToken(true);
                            originalResumeToken = job.OriginalResumeToken ?? string.Empty;
                            startedOnUtc = job.GetChangeStreamStartedOn(true)?.ToUniversalTime() ?? DateTime.UtcNow;
                            start = startedOnUtc;
                        }
                        else
                        {
                            resumeToken = mu.GetResumeToken(true) ?? string.Empty;
                            originalResumeToken = mu.GetOriginalResumeToken(true) ?? string.Empty;
                            startedOnUtc = mu.GetChangeStreamStartedOn(true)?.ToUniversalTime() ?? DateTime.UtcNow;
                            start = startedOnUtc;
                        }
                    }
                    else
                    {
                        if (useServerLevel)
                        {
                            resumeToken = job.GetResumeToken(false);
                            originalResumeToken = job.OriginalResumeToken ?? string.Empty;
                            startedOnUtc = job.GetChangeStreamStartedOn(false)?.ToUniversalTime() ?? DateTime.UtcNow;
                            start = startedOnUtc;
                        }
                    }

                    // Check if resume token already exists based on processing level
                    string existingResumeToken = string.Empty;

                    if (!string.IsNullOrEmpty(resumeToken) && !forceBootstrapAfterTransition)
                    {
                        if (useServerLevel)
                            log.WriteLine($"{syncBackPrefix}Server-level change stream resume token already set", LogType.Debug);
                        else
                            log.WriteLine($"{syncBackPrefix}Collection-level change stream resume token for {mu.DatabaseName}.{mu.CollectionName} already set", LogType.Debug);
                        return;
                    }

                    if (forceBootstrapAfterTransition)
                    {
                        if (job.GetServerLevelChangeStreamResetPending(syncBack))
                        {
                            log.WriteLine($"{syncBackPrefix} Forcing WatchChangeStreamUntilChangeAsync bootstrap from job StartedOn.", LogType.Warning);
                        }
                        else
                        {
                            log.WriteLine($"{syncBackPrefix} Forcing WatchChangeStreamUntilChangeAsync bootstrap.", LogType.Warning);
                        }
                    }
                       

                    // Bootstrap path: we only reach here when there is no existing resume token
                    // (or a forced bootstrap was requested). Always honor startedOnUtc as the
                    // start of replay — do NOT fall back to CSLastChecked. CSLastChecked may
                    // have been stamped to "near now" by previous (broken) bootstrap attempts
                    // or by WatchChangeStreamUntilChangeAsync itself, which would silently
                    // skip all history between startedOnUtc and now. Reset CSLastChecked to
                    // startedOnUtc so subsequent rounds don't reintroduce the same drift.
                    if (useServerLevel)
                    {
                        job.CSLastChecked = startedOnUtc;
                        log.WriteLine($"{syncBackPrefix}Server-level CSLastChecked reset to {job.CSLastChecked:O} for bootstrap", LogType.Debug);
                    }
                    else
                    {
                        mu.CSLastChecked = startedOnUtc;
                        log.WriteLine($"{syncBackPrefix}Collection-level CSLastChecked for {mu.DatabaseName}.{mu.CollectionName} reset to {mu.CSLastChecked:O} for bootstrap", LogType.Debug);
                    }

                    var effctiveStartTime = startedOnUtc;
                    var bsonTimestamp = ConvertToBsonTimestamp(effctiveStartTime.ToUniversalTime());
                    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        
                    await WatchChangeStreamUntilChangeAsync(log, client, job, mu, collection, options,  seconds, syncBack, cts, useServerLevel);

                    if (forceBootstrapAfterTransition)
                    {
                        job.SetTransitionBootstrapPending(syncBack, false);
                        MigrationJobContext.SaveMigrationJob(job);
                    }


                    isSucessful = true;
                }
                catch (OperationCanceledException)
                {                
                    isSucessful = true;
                }
                catch (Exception ex) when (ex is MongoExecutionTimeoutException || ex is TimeoutException)
                {
                    var scope = useServerLevel
                        ? "server-level"
                        : $"{mu?.DatabaseName}.{mu?.CollectionName}";

                    log.WriteLine($"{syncBackPrefix}Timeout when setting change stream resume token for {scope}: {ex}", LogType.Debug);
                    skipLoops = true;
                }
                catch (Exception ex)
                {
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

        private static async Task WatchChangeStreamUntilChangeAsync(Log log, MongoClient client, MigrationJob job, MigrationUnit mu, IMongoCollection<BsonDocument> collection, ChangeStreamOptions options, int seconds, bool syncBack, CancellationToken manualCts, bool useServerLevel = false)
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
                        //if (await cursor.MoveNextAsync(linkedCts.Token))
                        //{
                            if (string.IsNullOrEmpty(mu.OriginalResumeToken))
                            {
                                var resumeTokenJson = cursor.GetResumeToken().ToJson();

                                mu.ResumeToken = resumeTokenJson;
                                mu.OriginalResumeToken = resumeTokenJson;
                            }
                            return;
                        //}                       
                        //return;
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
                                SetResumeTokenProperties(job, change, syncBack,log, migrationUnit);

                                log.WriteLine($"Server-level resume token set  with collection key {job.ResumeCollectionKey}");
                                // Exit immediately after first change detected
                                return;
                            
                            }
                            else
                            {
                                //if bulk load is complete, no point in continuing to watch
                                //if ((mu.RestoreComplete || job.IsSimulatedRun) && mu.DumpComplete && !forced)
                                //    return;

                                // Use common function for collection-level resume token setting
                                SetResumeTokenProperties(mu, change, syncBack,log);

                                MigrationJobContext.AddVerboseLog($"Collection-level resume token set for {mu.DatabaseName}.{mu.CollectionName} - Operation: {mu.ResumeTokenOperation}, DocumentKey: {mu.ResumeDocumentKey}");

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

                // No changes detected — capture the cursor's postBatchResumeToken
                // so the collection gets a valid resume position even when idle.
                // Placed outside try/catch so it runs after timeout/cancellation too.
                try
                {
                    var postBatchToken = cursor.GetResumeToken();
                    if (postBatchToken != null)
                    {
                        var postBatchTokenJson = postBatchToken.ToJson();
                        if (useServerLevel)
                        {
                            if (string.IsNullOrEmpty(job.GetResumeToken(syncBack)))
                            {
                                job.SetResumeToken(syncBack, postBatchTokenJson);
                                MigrationJobContext.SaveMigrationJob(job);
                                log.WriteLine($"Server-level postBatchResumeToken captured (no changes detected)", LogType.Debug);
                            }
                        }
                        else
                        {
                            var currentToken = mu.GetResumeToken(syncBack);
                            if (string.IsNullOrEmpty(currentToken))
                            {
                                // postBatchResumeToken from an idle cursor reflects the cursor's
                                // StartAtOperationTime, not "now". Stamping CursorUtcTimestamp =
                                // DateTime.UtcNow here creates a desync where the token points to
                                // the past but the timestamp says "now", which later trips the
                                // "don't go backwards" guard in the collection-level processor.
                                var tokenTs = options?.StartAtOperationTime != null
                                    ? BsonTimestampToUtcDateTime(options.StartAtOperationTime)
                                    : DateTime.UtcNow;
                                SetResumeParameters(mu, tokenTs, postBatchTokenJson, syncBack);
                                mu.SetInitialDocumenReplayed(syncBack, true); // No change to replay
                                MigrationJobContext.SaveMigrationUnit(mu, true);
                                MigrationJobContext.AddVerboseLog($"Collection-level postBatchResumeToken captured for {mu.DatabaseName}.{mu.CollectionName} (no changes detected, syncBack={syncBack}, ts={tokenTs:O})");
                            }
                        }
                    }
                }
                catch
                {
                    // cursor may be in a bad state after cancellation — ignore
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
#endif

    
        public static async Task<(bool Exits, bool IsCollection, bool IsTimeSeries)> CheckIsCollectionAsync(MongoClient client, string databaseName, string collectionName)
        {
            var database = client.GetDatabase(databaseName);

            // The internal bucket namespace of a time-series collection cannot be restored
            // to Cosmos DocumentDB (system.* namespaces are reserved). Treat as TS up-front.
            if (!string.IsNullOrEmpty(collectionName) && collectionName.StartsWith("system.buckets.", StringComparison.Ordinal))
            {
                return (true, false, true);
            }

            // Filter by collection name
            var filter = new BsonDocument("name", collectionName);

            using var cursor = await database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            var collectionInfo = await cursor.FirstOrDefaultAsync();

            if (collectionInfo == null)
            {
                return (false, false, false);
            }

            // Check the "type" field returned in listCollections
            var type = collectionInfo.GetValue("type", "collection").AsString;
            bool isTimeSeries = string.Equals(type, "timeseries", StringComparison.OrdinalIgnoreCase);
            if (!isTimeSeries && collectionInfo.TryGetValue("options", out var optsVal) && optsVal.IsBsonDocument && optsVal.AsBsonDocument.Contains("timeseries"))
            {
                isTimeSeries = true;
            }

            bool isCollection = string.Equals(type, "collection", StringComparison.OrdinalIgnoreCase) && !isTimeSeries;
            return (true, isCollection, isTimeSeries);
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
                try
                {
                    // Check if collection has at least one index
                    var indexCursor = await coll.Indexes.ListAsync();
                    var indexes = await indexCursor.ToListAsync();

                    bool collectionExists = indexes.Count > 0;
                    return collectionExists;
                }
                catch (MongoCommandException listIxEx)
                    when (listIxEx.Code == 166 /* CommandNotSupportedOnView */
                          || listIxEx.CodeName == "CommandNotSupportedOnView"
                          || (listIxEx.Message?.IndexOf("is a view, not a collection", StringComparison.OrdinalIgnoreCase) ?? -1) >= 0)
                {
                    // Namespace exists as a view. Caller's IsCollection check will downgrade it to IsView.
                    return true;
                }
            }

        }

        public static async Task<bool> CheckCollectionValidAsync(MongoClient client, string databaseName, string collectionName)
        {
            if(await CheckCollectionExistsAsync(client, databaseName, collectionName))
            {
                (bool Exits, bool IsCollection, bool IsTimeSeries) ret;
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

        public static async Task<(long CollectionSizeBytes, long DocumentCount)> GetCollectionStatsAsync(MongoClient client, string databaseName, string collectionName, CancellationToken cancellationToken = default, int timeoutSeconds = 10, int maxAttempts = 5)
        {
            MigrationJobContext.AddVerboseLog($"Getting collection stats for {databaseName}.{collectionName}");

            var database = client.GetDatabase(databaseName);
            var statsCommand = new BsonDocument { { "collStats", collectionName } };
            
            long totalCollectionSizeBytes = 0;
            long documentCount = 0;
            var log = new Log();
            
            TaskResult result = await new RetryHelper().ExecuteTask(
                async () =>
                {
                    var stats = await ExecuteCollStatsCommandAsync(
                        database, statsCommand, databaseName, collectionName,
                        timeoutSeconds, cancellationToken
                    );
                    totalCollectionSizeBytes = stats.CollectionSizeBytes;
                    documentCount = stats.DocumentCount;
                    return TaskResult.Success;
                },
                (ex, attemptCount, currentBackoff) => CollectionStats_ExceptionHandler(
                    ex, attemptCount,
                    $"GetCollectionStats for {databaseName}.{collectionName}", currentBackoff,
                    cancellationToken
                ),
                log,
                maxTries: maxAttempts,
                initialDelayMs: 2000
            );
            
            if (result != TaskResult.Success)
            {
                throw new Exception($"Failed to get collection stats for {databaseName}.{collectionName} after {maxAttempts} attempts");
            }

            return (totalCollectionSizeBytes, documentCount);
        }

        private static async Task<(long CollectionSizeBytes, long DocumentCount)> ExecuteCollStatsCommandAsync(
            IMongoDatabase database, BsonDocument statsCommand,
            string databaseName, string collectionName,
            int timeoutSeconds, CancellationToken cancellationToken)
        {
            const int hardTimeoutSeconds = 300; // 15-minute absolute ceiling via Task.WhenAny
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
            
            MigrationJobContext.AddVerboseLog($"Executing collStats command for {databaseName}.{collectionName} with {timeoutSeconds}s timeout");
            
            BsonDocument stats;
            try
            {
                var commandTask = database.RunCommandAsync<BsonDocument>(statsCommand, cancellationToken: linkedCts.Token);
                var hardTimeoutTask = Task.Delay(TimeSpan.FromSeconds(hardTimeoutSeconds), cancellationToken);

                var completed = await Task.WhenAny(commandTask, hardTimeoutTask);

                if (completed == hardTimeoutTask)
                {
                    throw new TimeoutException($"GetCollectionStatsAsync hard timeout: did not complete within {hardTimeoutSeconds} seconds for {databaseName}.{collectionName}");
                }

                stats = await commandTask; // re-await to propagate any exception from the command
                MigrationJobContext.AddVerboseLog($"collStats command completed for {databaseName}.{collectionName}");
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"GetCollectionStatsAsync timed out after {timeoutSeconds} seconds for {databaseName}.{collectionName}");
            }
            
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
            
            return (totalCollectionSizeBytes, documentCount);
        }

        private static Task<TaskResult> CollectionStats_ExceptionHandler(
            Exception ex, int attemptCount, string processName, int currentBackoff,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                MigrationJobContext.AddVerboseLog($"{processName} cancelled");
                return Task.FromResult(TaskResult.Abort);
            }
            
            if (ex is TimeoutException)
            {
                MigrationJobContext.AddVerboseLog($"{processName} attempt {attemptCount} timed out. Retrying in {currentBackoff}s...");
            }
            else
            {
                MigrationJobContext.AddVerboseLog($"{processName} attempt {attemptCount} failed. Error: {ex.Message}. Retrying in {currentBackoff}s...");
            }
            
            return Task.FromResult(TaskResult.Retry);
        }


        public static async Task<bool> DeleteAndCopyIndexesAsync(Log log,MigrationUnit mu, string targetConnectionString, IMongoCollection<BsonDocument> sourceCollection, bool skipIndexes, bool uniqueOnly = false)
        {
            MigrationJobContext.AddVerboseLog($"Starting index copy for {sourceCollection.CollectionNamespace.DatabaseNamespace.DatabaseName}.{sourceCollection.CollectionNamespace.CollectionName}");
            try
            {
                // Extract database and collection details from the source collection
                var sourceDatabase = sourceCollection.Database;
                var sourceCollectionName = sourceCollection.CollectionNamespace.CollectionName;

                // Connect to the target database
                var targetClient = MongoClientFactory.Create(log,targetConnectionString);
                var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
                var targetDatabase = targetClient.GetDatabase(targetDatabaseName);
                var targetCollectionName = mu.GetEffectiveTargetCollectionName();
                var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

			    log.WriteLine($"Creating collection: {namespaceForLog}");
                

                // Check if the target collection exists
                var collectionNamesCursor = await targetDatabase.ListCollectionNamesAsync();
                var collectionNames = await collectionNamesCursor.ToListAsync();
                bool targetCollectionExists = collectionNames.Contains(targetCollectionName);

                // Delete the target collection if it exists
                if (targetCollectionExists)
                {
                    await targetDatabase.DropCollectionAsync(targetCollectionName);
				    log.WriteLine($"Deleted existing target collection: {namespaceForLog}");
                    
                }

                if (skipIndexes)
                {
                    // Ensure the collection is created even when indexes are skipped
                    await targetDatabase.CreateCollectionAsync(targetCollectionName);
                    mu.TargetCreated = true;
                    return true;
                }

                log.WriteLine($"Creating {(uniqueOnly ? "unique " : "")}indexes for: {namespaceForLog}");
                

                // Create the target collection
                await targetDatabase.CreateCollectionAsync(targetCollectionName);
                mu.TargetCreated = true;

                var targetCollection = targetDatabase.GetCollection<BsonDocument>(targetCollectionName);

                IndexCopier indexCopier = new IndexCopier();
                int count;
                if (uniqueOnly)
                    count = await indexCopier.CopyUniqueIndexesAsync(sourceCollection, targetClient, targetDatabaseName, targetCollectionName, log);
                else
                    count = await indexCopier.CopyIndexesAsync(sourceCollection, targetClient, targetDatabaseName, targetCollectionName, log);
                mu.IndexesMigrated = count;
                log.WriteLine($"{count} {(uniqueOnly ? "unique " : "")}Indexes copied successfully to {namespaceForLog}");
                
                return true;
            }
            catch (Exception ex)
            {
			    log.WriteLine($"Error copying indexes: {ex}", LogType.Error);
                
                return false;
            }
        }

        /// <summary>
        /// Retrieves the shard key definition for a collection from the source config.collections.
        /// Returns null if the collection is not sharded or the shard key cannot be determined.
        /// </summary>
        public static async Task<BsonDocument?> GetShardKeyFromSourceAsync(Log log, MongoClient sourceClient, string databaseName, string collectionName)
        {
            try
            {
                var configDb = sourceClient.GetDatabase("config");
                var collectionsCol = configDb.GetCollection<BsonDocument>("collections");
                var ns = $"{databaseName}.{collectionName}";

                var filter = Builders<BsonDocument>.Filter.Eq("_id", ns);
                var doc = await collectionsCol.Find(filter).FirstOrDefaultAsync();

                if (doc != null && doc.Contains("key"))
                {
                    var key = doc["key"].AsBsonDocument;
                    log.WriteLine($"Found shard key for {ns}: {key}", LogType.Debug);
                    return key;
                }

                log.WriteLine($"No shard key found for {ns}", LogType.Debug);
                return null;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error reading shard key for {databaseName}.{collectionName}: {ex.Message}", LogType.Warning);
                return null;
            }
        }

        /// <summary>
        /// Shards a target collection using a hashed version of the source shard key.
        /// Only single-field shard keys are supported on Cosmos DB; compound keys use the first field.
        /// </summary>
        public static async Task<bool> ShardCollectionAsync(Log log, MongoClient targetClient, string databaseName, string collectionName, BsonDocument sourceShardKey)
        {
            var ns = $"{databaseName}.{collectionName}";
            try
            {
                // Convert to hashed shard key (Cosmos DB only supports hashed)
                // For compound keys, use just the first field
                BsonDocument hashedKey;
                if (sourceShardKey.ElementCount > 1)
                {
                    var firstField = sourceShardKey.Elements.First().Name;
                    log.WriteLine($"Compound shard key {sourceShardKey} not fully supported. Using first field '{firstField}' as hashed.", LogType.Warning);
                    hashedKey = new BsonDocument(firstField, "hashed");
                }
                else
                {
                    hashedKey = new BsonDocument(
                        sourceShardKey.Elements.Select(e => new BsonElement(e.Name, "hashed"))
                    );
                }

                log.WriteLine($"Sharding collection {ns} with key: {hashedKey}");

                var adminDb = targetClient.GetDatabase("admin");
                var command = new BsonDocument
                {
                    { "shardCollection", ns },
                    { "key", hashedKey }
                };

                await adminDb.RunCommandAsync<BsonDocument>(command);
                log.WriteLine($"Successfully sharded collection {ns}");
                return true;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error sharding collection {ns}: {ex.Message}", LogType.Error);
                return false;
            }
        }

        /// <summary>
        /// Gets the storage size (in bytes) of a source collection using collStats.
        /// Returns 0 if the size cannot be determined.
        /// </summary>
        public static async Task<long> GetCollectionStorageSizeAsync(Log log, MongoClient sourceClient, string databaseName, string collectionName)
        {
            try
            {
                var db = sourceClient.GetDatabase(databaseName);
                var command = new BsonDocument
                {
                    { "collStats", collectionName }
                };

                var result = await db.RunCommandAsync<BsonDocument>(command);

                if (result.Contains("storageSize"))
                    return result["storageSize"].ToInt64();

                if (result.Contains("size"))
                    return result["size"].ToInt64();

                return 0;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error getting storage size for {databaseName}.{collectionName}: {ex.Message}", LogType.Warning);
                return 0;
            }
        }

        /// <summary>
        /// Moves an unsharded collection to a specific shard using the moveCollection admin command.
        /// Returns (true, null) on success (including when the collection is already on the target shard).
        /// On failure returns (false, errorMessage) so the caller can include the detail in its own log.
        /// </summary>
        public static async Task<(bool Ok, string? Error)> MoveCollectionAsync(Log log, MongoClient targetClient, string databaseName, string collectionName, string toShard)
        {
            var ns = $"{databaseName}.{collectionName}";
            try
            {
                log.WriteLine($"Moving collection {ns} to shard: {toShard}");

                var adminDb = targetClient.GetDatabase("admin");
                var command = new BsonDocument
                {
                    { "moveCollection", ns },
                    { "toShard", toShard }
                };

                await adminDb.RunCommandAsync<BsonDocument>(command);
                log.WriteLine($"Successfully moved collection {ns} to shard {toShard}");
                return (true, null);
            }
            catch (Exception ex)
            {
                // Ignore error if collection is already on the target shard
                if (ex.Message.Contains("cannot move shard to the same node", StringComparison.OrdinalIgnoreCase))
                {
                    log.WriteLine($"Collection {ns} is already on shard {toShard}, skipping move", LogType.Debug);
                    return (true, null);
                }

                return (false, ex.Message);
            }
        }

        /// <summary>
        /// Builds non-unique indexes on the target collection after offline data copy.
        /// Returns the number of non-unique indexes created.
        /// </summary>
        public static async Task<int> BuildNonUniqueIndexesAsync(Log log, MigrationUnit mu, string targetConnectionString, IMongoCollection<BsonDocument> sourceCollection, bool useBlockingBuilds = false)
        {
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, mu.GetEffectiveTargetDatabaseName(), mu.GetEffectiveTargetCollectionName());
            log.WriteLine($"Building non-unique indexes for: {namespaceForLog}");

            try
            {
                var targetClient = MongoClientFactory.Create(log, targetConnectionString);
                var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
                var targetCollectionName = mu.GetEffectiveTargetCollectionName();

                IndexCopier indexCopier = new IndexCopier();
                int count = await indexCopier.CopyNonUniqueIndexesAsync(sourceCollection, targetClient, targetDatabaseName, targetCollectionName, log, useBlockingBuilds);
                mu.IndexesMigrated += count;
                log.WriteLine($"{count} non-unique indexes created on {namespaceForLog}");
                return count;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error building non-unique indexes on {namespaceForLog}: {ex}", LogType.Error);
                return -1;
            }
        }

        /// <summary>
        /// Checks active index builds on a target collection using the currentOp command.
        /// Returns (activeBuilds, progressPercent) where progressPercent is based on how many
        /// non-unique indexes are already READY on the target vs the expected total.
        /// This avoids unreliable currentOp percentage parsing entirely.
        /// </summary>
        public static async Task<(int ActiveBuilds, double ProgressPercent)> CheckIndexBuildProgressAsync(Log log, string targetConnectionString, string databaseName, string collectionName, int expectedTotalBuilds = 0)
        {
            try
            {
                var targetClient = MongoClientFactory.Create(log, targetConnectionString);
                var adminDb = targetClient.GetDatabase("admin");

                // Some servers (e.g. Azure DocumentDB) emit currentOp
                // responses that contain duplicate field names (most commonly `createIndexes`
                // on parent worker ops). The driver's default BsonDocument serializer rejects
                // any such response with "Duplicate element name", so we use a permissive
                // serializer that walks the BSON tree manually and allows duplicates.
                var currentOpCommand = new BsonDocument { { "currentOp", 1 }, { "$all", true } };
                var command = new BsonDocumentCommand<BsonDocument>(currentOpCommand, DuplicateTolerantBsonDocumentSerializer.Instance);
                var result = await adminDb.RunCommandAsync(command);

                var targetNs = $"{databaseName}.{collectionName}";
                int activeBuilds = 0;
                double partialFromActive = 0;
                var seenBuildIds = new HashSet<string>(StringComparer.Ordinal);

                if (result.Contains("inprog") && result["inprog"].IsBsonArray)
                {
                    foreach (var op in result["inprog"].AsBsonArray)
                    {
                        if (!op.IsBsonDocument) continue;
                        var opDoc = op.AsBsonDocument;

                        var nsValue = opDoc.GetValue("ns", "");
                        var ns = nsValue.IsString ? nsValue.AsString : nsValue.ToString();
                        if (ns != targetNs) continue;

                        bool isCreateIndexes = false;
                        string idxName = "";
                        if (opDoc.TryGetValue("command", out var cmdVal) && cmdVal.IsBsonDocument)
                        {
                            var cmdDoc = cmdVal.AsBsonDocument;
                            if (cmdDoc.Contains("createIndexes"))
                            {
                                isCreateIndexes = true;
                                if (cmdDoc.TryGetValue("indexes", out var idxArr) && idxArr.IsBsonArray && idxArr.AsBsonArray.Count > 0 && idxArr.AsBsonArray[0].IsBsonDocument)
                                    idxName = idxArr.AsBsonArray[0].AsBsonDocument.GetValue("name", "").ToString();
                            }
                        }

                        if (!isCreateIndexes && opDoc.TryGetValue("msg", out var msgVal) && msgVal.IsString)
                        {
                            var m = msgVal.AsString;
                            if (m.Contains("index build", StringComparison.OrdinalIgnoreCase) || m.Contains("queued", StringComparison.OrdinalIgnoreCase))
                                isCreateIndexes = true;
                        }

                        if (!isCreateIndexes) continue;

                        var opid = opDoc.GetValue("opid", "").ToString();
                        var key = string.IsNullOrEmpty(opid) ? $"queued|{idxName}" : $"{idxName}|{opid}";
                        if (seenBuildIds.Add(key))
                        {
                            activeBuilds++;
                            partialFromActive += TryGetOpBuildFraction(opDoc);
                        }
                    }
                }

                if (activeBuilds == 0)
                {
                    return (0, 100); // No builds in-flight — treat as complete (caller may apply warm-up guard)
                }

                // completed (whole) = expected - pending(currentOp)
                // plus partial credit from active op's progress.builds[].terms_progress (each op contributes 0..1)
                double progress = 0;
                if (expectedTotalBuilds > 0)
                {
                    var effectivePending = Math.Min(activeBuilds, expectedTotalBuilds);
                    var completedCount = Math.Max(0, expectedTotalBuilds - effectivePending);
                    // Cap partial credit to the number of pending builds so we never exceed expected.
                    var partialCapped = Math.Min(effectivePending, partialFromActive);
                    var doneEquivalent = completedCount + partialCapped;
                    progress = Math.Min(99, (doneEquivalent * 100.0) / expectedTotalBuilds);
                }

                return (activeBuilds, progress);
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error checking index build progress for {databaseName}.{collectionName}: {ex.Message}", LogType.Warning);
                return (-1, 0); // Signal error: callers must not treat this as completion.
            }
        }

        /// <summary>
        /// Extracts a 0..1 progress fraction for a single index-build op from currentOp output.
        /// Reads progress.builds[0].terms_progress when present, otherwise falls back to
        /// terms_done/terms_total. Returns 0 for queued ops or when no usable field is found.
        /// </summary>
        private static double TryGetOpBuildFraction(BsonDocument opDoc)
        {
            try
            {
                if (!opDoc.TryGetValue("progress", out var progVal) || !progVal.IsBsonDocument)
                    return 0;
                var progDoc = progVal.AsBsonDocument;
                if (!progDoc.TryGetValue("builds", out var buildsVal) || !buildsVal.IsBsonArray)
                    return 0;
                var buildsArr = buildsVal.AsBsonArray;
                if (buildsArr.Count == 0 || !buildsArr[0].IsBsonDocument)
                    return 0;
                var b = buildsArr[0].AsBsonDocument;

                if (b.TryGetValue("terms_progress", out var tp))
                {
                    try
                    {
                        var v = tp.ToDouble();
                        if (v > 0)
                            return Math.Min(1.0, v / 100.0);
                    }
                    catch { }
                }

                if (b.TryGetValue("terms_done", out var td) && b.TryGetValue("terms_total", out var tt))
                {
                    try
                    {
                        var done = td.ToDouble();
                        var total = tt.ToDouble();
                        if (total > 0)
                            return Math.Min(1.0, done / total);
                    }
                    catch { }
                }
            }
            catch { }
            return 0;
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
                DataType.Double => filterBuilder.Lt(fieldName, value.AsDouble),
                DataType.String => filterBuilder.Lt(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Lt(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Lt(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Timestamp => filterBuilder.Lt(fieldName, value.AsBsonTimestamp),
                DataType.Object => filterBuilder.Lt(fieldName, value.AsBsonDocument),
                DataType.BinData => filterBuilder.Lt(fieldName, value.AsBsonBinaryData),
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        private static FilterDefinition<BsonDocument> BuildFilterLte(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            return dataType switch
            {
                DataType.ObjectId => filterBuilder.Lte(fieldName, value.AsObjectId),
                DataType.Int => filterBuilder.Lte(fieldName, value.AsInt32),
                DataType.Int64 => filterBuilder.Lte(fieldName, value.AsInt64),
                DataType.Double => filterBuilder.Lte(fieldName, value.AsDouble),
                DataType.String => filterBuilder.Lte(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Lte(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Lte(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Timestamp => filterBuilder.Lte(fieldName, value.AsBsonTimestamp),
                DataType.Object => filterBuilder.Lte(fieldName, value.AsBsonDocument),
                DataType.BinData => filterBuilder.Lte(fieldName, value.AsBsonBinaryData),
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
                DataType.Double => filterBuilder.Gte(fieldName, value.AsDouble),
                DataType.String => filterBuilder.Gte(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Gte(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Gte(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Timestamp => filterBuilder.Gte(fieldName, value.AsBsonTimestamp),
                DataType.Object => filterBuilder.Gte(fieldName, value.AsBsonDocument),
                DataType.BinData => filterBuilder.Gte(fieldName, value.AsBsonBinaryData),
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
                DataType.Double => "double",
                DataType.String => "string",
                DataType.Decimal128 => "decimal",
                DataType.Date => "date",
                DataType.Timestamp => "timestamp",
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

        /// <summary>
        /// Validates a user-supplied collection filter using the same MongoDB (relaxed/extended JSON)
        /// parser the migration pipeline uses at runtime. Unlike strict JSON validation, this accepts
        /// MongoDB query operators (e.g. <c>$exists</c>, <c>$gte</c>), unquoted field names, and
        /// extended-JSON types (e.g. <c>{ "$date": ... }</c>). Returns true for null/whitespace filters.
        /// </summary>
        public static bool IsValidFilter(string? filter, out string? error)
        {
            error = null;
            if (string.IsNullOrWhiteSpace(filter))
                return true;

            try
            {
                BsonDocument.Parse(filter);
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
            }
        }


        public static string GenerateQueryString(BsonValue? gte, BsonValue? lt, BsonValue? lte, DataType dataType, BsonDocument? userFilterDoc, MigrationUnit? migrationUnit = null)
        {
            // Skip the $type predicate when the migration unit's _id has only one BSON type.
            bool skipDataTypeFilter = migrationUnit?.SkipDataTypeFilterForId == true;

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

            if (!(lt == null || lt.IsBsonNull) && lt is not BsonMaxKey)
            {
                idConditions.Add($"\\\"$lt\\\": {BsonValueToString(lt, dataType)}");
            }
            else if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
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
                            case "$ne":
                                // MongoDB $ne matches when the field is absent or holds a different value.
                                if (doc.Contains(element.Name) && doc[element.Name] == op.Value) return false;
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
                            case "$exists":
                                // MongoDB $exists true => field must be present; false => field must be absent.
                                if (op.Value.ToBoolean() != doc.Contains(element.Name)) return false;
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
                DataType.Double => $"{{\\\"$numberDouble\\\":\\\"{value.AsDouble.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}\\\"}}",
                DataType.String => $"\\\"{EscapeStringForJsonQueryArg(value.AsString)}\\\"",
                DataType.Decimal128 => $"{{\\\"$numberDecimal\\\":\\\"{value.AsDecimal128}\\\"}}",
                DataType.Date => $"{{\\\"$date\\\":\\\"{((BsonDateTime)value).ToUniversalTime():yyyy-MM-ddTHH:mm:ssZ}\\\"}}",
                DataType.Timestamp => $"{{\\\"$timestamp\\\":{{\\\"t\\\":{value.AsBsonTimestamp.Timestamp},\\\"i\\\":{value.AsBsonTimestamp.Increment}}}}}",
                DataType.Object => value.AsBsonDocument.ToString(),
                DataType.BinData => $"{{\\\"$binary\\\":{{\\\"base64\\\":\\\"{Convert.ToBase64String(value.AsBsonBinaryData.Bytes)}\\\",\\\"subType\\\":\\\"{((byte)value.AsBsonBinaryData.SubType):x2}\\\"}}}}",
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        /// <summary>
        /// Escapes a string value for safe embedding inside a JSON string literal
        /// that will be passed as a command-line argument to mongodump/mongorestore.
        /// Handles control characters, backslashes, and double quotes that would
        /// otherwise break JSON parsing or argument parsing.
        /// </summary>
        private static string EscapeStringForJsonQueryArg(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            // Fast path: check if escaping is needed
            bool needsEscaping = false;
            foreach (char c in value)
            {
                if (c < ' ' || c == '\\' || c == '"')
                {
                    needsEscaping = true;
                    break;
                }
            }
            if (!needsEscaping)
                return value;

            // Emit backslash and double-quote as JSON \uXXXX escapes rather than \\ and \"
            // so the resulting string contains no literal backslashes or quotes. This avoids
            // ambiguity with Win32/Go argv parsing of the outer --query="..." shell quoting,
            // where a trailing run of backslashes before the closing quote can be mis-parsed
            // as escaping the quote (producing "end of input in JSON string" or
            // "provide only one MongoDB connection string" errors from mongodump).
            var sb = new StringBuilder(value.Length + 16);
            foreach (char c in value)
            {
                switch (c)
                {
                    case '\\': sb.Append("\\u005C"); break;
                    case '"': sb.Append("\\u0022"); break;
                    default:
                        if (c < ' ')
                            sb.Append($"\\u{(int)c:x4}");
                        else
                            sb.Append(c);
                        break;
                }
            }
            return sb.ToString();
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

            AddEqualityFiltersFromDocument(documentKey, string.Empty, filters);

            if (filters.Count == 0)
            {
                // Defensive fallback: match the document key elements as-is.
                foreach (var element in documentKey.Elements)
                {
                    filters.Add(Builders<BsonDocument>.Filter.Eq(element.Name, element.Value));
                }
            }

            return filters.Count == 1
                ? filters[0]
                : Builders<BsonDocument>.Filter.And(filters);
        }

        /// <summary>
        /// Builds an equality filter that targets a single document by its <c>_id</c>.
        /// When <paramref name="idValue"/> is an embedded document (a composite key), the
        /// filter is flattened into dotted-path equality predicates (e.g. "_id.aggregatedChallengeId")
        /// so that sharded collections whose shard key is a nested field can extract the shard key
        /// for single-shard targeting on upsert/replace/delete.
        /// </summary>
        public static FilterDefinition<TDocument> BuildIdEqualityFilter<TDocument>(BsonValue idValue)
        {
            if (idValue != null && idValue.IsBsonDocument)
            {
                var nestedFilters = new List<FilterDefinition<TDocument>>();
                AddEqualityFiltersFromDocument(idValue.AsBsonDocument, "_id", nestedFilters);

                if (nestedFilters.Count > 0)
                {
                    return nestedFilters.Count == 1
                        ? nestedFilters[0]
                        : Builders<TDocument>.Filter.And(nestedFilters);
                }
            }

            return Builders<TDocument>.Filter.Eq("_id", idValue);
        }

        // Flattens embedded documents (e.g. a composite _id) into dotted-path equality
        // predicates. This is required for sharded collections whose shard key is a nested
        // field such as { "_id.aggregatedChallengeId": "hashed" }: an equality match on the
        // whole embedded _id document does not expose the nested value to mongos, so upsert /
        // replace / delete fail with
        // "An {upsert:true} update on a sharded collection must target a single shard".
        // Matching every scalar leaf still uniquely identifies the document while surfacing
        // the nested shard key.
        private static void AddEqualityFiltersFromDocument<TDocument>(BsonDocument document, string prefix, List<FilterDefinition<TDocument>> filters)
        {
            foreach (var element in document.Elements)
            {
                var path = prefix.Length == 0 ? element.Name : $"{prefix}.{element.Name}";

                if (element.Value.IsBsonDocument)
                {
                    AddEqualityFiltersFromDocument(element.Value.AsBsonDocument, path, filters);
                }
                else
                {
                    filters.Add(Builders<TDocument>.Filter.Eq(path, element.Value));
                }
            }
        }



#if !LEGACY_MONGODB_DRIVER
        /// <summary>
        /// Common function to set resume token properties on either MigrationJob or MigrationUnit
        /// </summary>
        /// <param name="target">The target object (MigrationJob or MigrationUnit) to set properties on</param>
        /// <param name="change">The change stream document containing the resume token information</param>
        /// <param name="resetCS">Whether this is a change stream reset operation</param>
        /// <param name="databaseName">Database name for server-level operations</param>
        /// <param name="collectionName">Collection name for server-level operations</param>
        private static void SetResumeTokenProperties(object target, ChangeStreamDocument<BsonDocument> change, bool syncBack, Log log, MigrationUnit muInServerMode = null)
        {
            string token = string.Empty;
            string resumeTokenJson = change.ResumeToken.ToJson();
            string documentKeyJson = change.DocumentKey.ToJson();
            var operationType = change.OperationType;
            bool isNotSet = false;
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
              
                job.SetResumeToken(syncBack, resumeTokenJson);
                token = job.GetOriginalResumeToken(syncBack);
                job.SetCursorUtcTimestamp(syncBack, timestamp);

                if (string.IsNullOrEmpty(token))
                    isNotSet = true;


                if (isNotSet)
                {
                    job.SetOriginalResumeToken(syncBack, resumeTokenJson);


                    job.ResumeTokenOperation = operationType;
                    job.ResumeDocumentId = documentKeyJson; // Deprecated - kept for backward compatibility
                    job.ResumeDocumentKey = documentKeyJson;


                    if (muInServerMode != null)
                    {
                        muInServerMode.SetCursorUtcTimestamp(syncBack, timestamp);

                        MigrationJobContext.SaveMigrationUnit(muInServerMode, true);
                    }

                    // Store collection key for server-level auto replay
                    if (change.CollectionNamespace != null && muInServerMode != null)
                    {
                        job.ResumeCollectionKey = $"{muInServerMode.DatabaseName}.{muInServerMode.CollectionName}";
                    }

                    //resetting counters for all migration units
                    ResetCountersForAllMigrationUnits(job, syncBack);

                }
                
                MigrationJobContext.SaveMigrationJob(job);
            }
            else if (target is MigrationUnit mu)
            {
                isNotSet = string.IsNullOrEmpty(mu.GetOriginalResumeToken(syncBack));

                if (isNotSet)                
                {
                    mu.SetOriginalResumeToken(syncBack, resumeTokenJson);
                    SetResumeParameters(mu, timestamp, resumeTokenJson, syncBack);
                    mu.SetResumeDocumentInfo(syncBack, operationType, documentKeyJson);
                }               
                
                MigrationJobContext.SaveMigrationUnit(mu,true);
            }
        }


        public static void SetResumeParameters(MigrationUnit mu, DateTime timestamp, string resumeToken, bool syncBack)
        {
            mu.SetCursorUtcTimestamp(syncBack, timestamp);
            mu.SetResumeToken(syncBack, resumeToken);
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
            mu.ResetChangeStream = false;
            mu.ClearResumeDocumentInfo(syncBack);
            mu.CSLastChecked = DateTime.MinValue; 
            mu.CSLastBatchDurationSeconds = 0;
            mu.CSNormalizedUpdatesInLastBatch = 0;
            mu.CSUpdatesInLastBatch = 0;

        }
#endif

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

        // Cached per-client result of the hello probe; MongoClient instances are long-lived singletons here.
        private static readonly ConcurrentDictionary<MongoClient, bool> _isDocumentDbCache = new();

        /// <summary>
        /// Detects whether the given <see cref="MongoClient"/> targets Azure Cosmos DB for MongoDB vCore
        /// (DocumentDB) by issuing <c>db.runCommand({ hello: 1 })</c> against the admin database and
        /// inspecting <c>internal.kind == "azuredocumentdb"</c>. Result is cached per client instance.
        /// </summary>
        public static bool IsDocumentDBEndpoint(MongoClient client)
        {
            if (client == null) return false;
            return _isDocumentDbCache.GetOrAdd(client, static c =>
            {
                try
                {
                    var admin = c.GetDatabase("admin");
                    var response = admin.RunCommand<BsonDocument>(new BsonDocument("hello", 1));
                    if (response != null
                        && response.TryGetValue("internal", out var internalVal)
                        && internalVal.IsBsonDocument
                        && internalVal.AsBsonDocument.TryGetValue("kind", out var kindVal)
                        && kindVal.IsString)
                    {
                        return string.Equals(kindVal.AsString, "azuredocumentdb", StringComparison.OrdinalIgnoreCase);
                    }
                }
                catch
                {
                    // Probe failure — treat as non-DocumentDB; caller paths already handle either branch safely.
                }
                return false;
            });
        }

        /// <summary>
        /// Removes documents from the temp collection whose _id already exists in the target collection.
        /// Uses the chunk's _id range filter (Gte/Lt/Lte) to efficiently query the target via index scan,
        /// then deletes matching _ids from temp in batches.
        /// </summary>
        public static async Task<long> RemoveDuplicatesFromTempAsync(
            IMongoCollection<BsonDocument> tempCollection,
            IMongoCollection<BsonDocument> targetCollection,
            FilterDefinition<BsonDocument> chunkFilter,
            int batchSize,
            string namespaceAndChunk,
            Log? log,
            CancellationToken cancellationToken)
        {
            long totalRemoved = 0;

            // Query existing _ids in target using the chunk's range filter (efficient index scan)
            using var cursor = await targetCollection.FindAsync(
                chunkFilter,
                new FindOptions<BsonDocument>
                {
                    Projection = Builders<BsonDocument>.Projection.Include("_id"),
                    BatchSize = batchSize
                },
                cancellationToken);

            while (await cursor.MoveNextAsync(cancellationToken))
            {
                var batch = cursor.Current.ToList();
                if (batch.Count == 0) continue;

                // Delete these _ids from temp
                var ids = batch.Select(d => d["_id"]).ToList();
                var deleteFilter = Builders<BsonDocument>.Filter.In("_id", ids);
                var deleteResult = await tempCollection.DeleteManyAsync(deleteFilter, cancellationToken);
                totalRemoved += deleteResult.DeletedCount;
            }

            if (totalRemoved > 0)
            {
                log?.WriteLine(
                    $"[Merge] Removing duplicates for {namespaceAndChunk}: removed {totalRemoved} duplicate(s) from temp",
                    LogType.Info);
            }

            return totalRemoved;
        }

        /// <summary>
        /// Inserts all documents from the temp collection into the target collection using parallel InsertMany.
        /// Documents that already exist in the target (duplicate _id) are skipped, preserving the existing data.
        /// </summary>
        public static async Task<(long totalInserted, long totalSkipped)> InsertTempToTargetInParallelAsync(
            IMongoCollection<BsonDocument> tempCollection,
            IMongoCollection<BsonDocument> targetCollection,
            int parallelThreads,
            int batchSize,
            string namespaceAndChunk,
            Log? log,
            CancellationToken cancellationToken)
        {
            long totalInserted = 0;
            long totalSkipped = 0;
            int batchNumber = 0;

            int effectiveThreads = Math.Max(1, parallelThreads);
            using var semaphore = new SemaphoreSlim(effectiveThreads);
            var insertTasks = new ConcurrentBag<Task>();
            var fatalErrors = new ConcurrentBag<Exception>();

            long tempDocCount = await tempCollection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: cancellationToken);
            log?.WriteLine($"[Merge] Starting parallel insert for {namespaceAndChunk} with {effectiveThreads} threads, batchSize={batchSize}, Docs to Merge={tempDocCount}", LogType.Info);

            using var cursor = await tempCollection.FindAsync(
                FilterDefinition<BsonDocument>.Empty,
                new FindOptions<BsonDocument> { BatchSize = batchSize },
                cancellationToken);

            while (await cursor.MoveNextAsync(cancellationToken))
            {
                var batch = cursor.Current.ToList();
                if (batch.Count == 0) continue;

                var currentBatchNum = Interlocked.Increment(ref batchNumber);
                var batchDocs = batch;

                await semaphore.WaitAsync(cancellationToken);

                var task = Task.Run(async () =>
                {
                    try
                    {
                        await targetCollection.InsertManyAsync(
                            batchDocs,
                            new InsertManyOptions { IsOrdered = false },
                            cancellationToken);
                        Interlocked.Add(ref totalInserted, batchDocs.Count);
                    }
                    catch (MongoBulkWriteException<BsonDocument> bwe)
                    {
                        // Partial success: some docs inserted, duplicates skipped
                        long dupCount = bwe.WriteErrors.Count(e => e.Category == ServerErrorCategory.DuplicateKey);
                        long inserted = batchDocs.Count - bwe.WriteErrors.Count;
                        Interlocked.Add(ref totalInserted, inserted);
                        Interlocked.Add(ref totalSkipped, dupCount);

                        // Non-duplicate errors are fatal
                        if (bwe.WriteErrors.Any(e => e.Category != ServerErrorCategory.DuplicateKey))
                        {
                            fatalErrors.Add(bwe);
                            log?.WriteLine(
                                $"[Merge] {namespaceAndChunk}: batch {currentBatchNum} had non-duplicate errors: {Helper.RedactPii(bwe.Message)}",
                                LogType.Warning);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        fatalErrors.Add(ex);
                        log?.WriteLine(
                            $"[Merge] {namespaceAndChunk}: batch {currentBatchNum} failed: {Helper.RedactPii(ex.Message)}",
                            LogType.Error);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cancellationToken);

                insertTasks.Add(task);

                log?.ShowInMonitor(
                    $"[Merge] {namespaceAndChunk}: dispatched batch {currentBatchNum}, newDocsAdded={Interlocked.Read(ref totalInserted)}, duplicatesSkipped={Interlocked.Read(ref totalSkipped)}");
            }

            await Task.WhenAll(insertTasks);

            if (!fatalErrors.IsEmpty)
            {
                throw new AggregateException(
                    $"[Merge] {fatalErrors.Count} batch(es) failed for {namespaceAndChunk}",
                    fatalErrors);
            }

            log?.WriteLine(
                $"[Merge] Insert phase done for {namespaceAndChunk}: newDocsAdded={Interlocked.Read(ref totalInserted)}, duplicatesSkipped={Interlocked.Read(ref totalSkipped)}",
                LogType.Info);

            return (Interlocked.Read(ref totalInserted), Interlocked.Read(ref totalSkipped));
        }

        /// <summary>
        /// Probes the source collection with a $type findOne per candidate DataType (combined with the user filter)
        /// and returns only the types that actually have at least one document. Types whose probe throws are kept
        /// (fail-open) so a transient error doesn't shrink the partition plan.
        /// </summary>
        public static List<DataType> PruneAbsentIdDataTypes(
            Log log,
            IMongoCollection<BsonDocument> collection,
            List<DataType> candidateTypes,
            BsonDocument? userFilter,
            CancellationToken cancellationToken)
        {
            if (candidateTypes == null || candidateTypes.Count <= 1)
                return candidateTypes ?? new List<DataType>();

            var present = new List<DataType>(candidateTypes.Count);
            var rawCollection = collection.Database.GetCollection<RawBsonDocument>(collection.CollectionNamespace.CollectionName);

            foreach (var dataType in candidateTypes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(dataType, userFilter, skipDataTypeFilter: false);
                    FilterDefinition<RawBsonDocument> filter = (matchCondition != null && matchCondition.ElementCount > 0)
                        ? new BsonDocumentFilterDefinition<RawBsonDocument>(matchCondition)
                        : FilterDefinition<RawBsonDocument>.Empty;

                    var doc = rawCollection
                        .Find(filter)
                        .Project<RawBsonDocument>(Builders<RawBsonDocument>.Projection.Include("_id"))
                        .Limit(1)
                        .FirstOrDefault(cancellationToken);

                    if (doc != null)
                    {
                        present.Add(dataType);
                        log.WriteLine($"Probe for {collection.CollectionNamespace} _id type {dataType}: present", LogType.Info);
                    }
                    else
                    {
                        log.WriteLine($"Probe for {collection.CollectionNamespace} _id type {dataType}: absent (skipping)", LogType.Info);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Fail-open: keep the type so we don't accidentally drop data on transient errors.
                    present.Add(dataType);
                    log.WriteLine($"Probe for {collection.CollectionNamespace} _id type {dataType} failed; keeping it. Details: {ex.Message}", LogType.Warning);
                }
            }

            if (present.Count == 0)
            {
                // Nothing matched: caller will treat as empty collection. Return empty list.
                log.WriteLine($"No _id data types matched any document in {collection.CollectionNamespace}", LogType.Debug);
            }
            else
            {
                log.WriteLine($"Detected _id data types in {collection.CollectionNamespace}: [{string.Join(", ", present)}]", LogType.Info);
            }

            return present;
        }

        /// <summary>
        /// Post-partition: Populates Lte for the last chunk and its segments with the snapshot max _id.
        /// This ensures that when Lt is null (unbounded), Lte provides the upper bound for queries.
        /// </summary>
        public static void PopulateLteForLastChunk(
            Log log,
            IMongoCollection<BsonDocument> collection,
            List<MigrationChunk> migrationChunks,
            DataType dataType,
            BsonDocument userFilter,
            bool skipDataTypeFilter)
        {
            if (migrationChunks == null || migrationChunks.Count == 0)
                return;

            try
            {
                var snapshotMaxId = GetSnapshotMaxId(collection, dataType, userFilter, skipDataTypeFilter);
                if (snapshotMaxId == null || snapshotMaxId.IsBsonNull)
                    return;

                var lastChunk = migrationChunks[^1];
                var rawCollection = collection.Database.GetCollection<RawBsonDocument>(collection.CollectionNamespace.CollectionName);
                var serializedMaxId = SerializeBsonValue(snapshotMaxId, dataType);

                // Set Lte for the last chunk
                if (!string.IsNullOrEmpty(serializedMaxId))
                {
                    lastChunk.Lte = serializedMaxId;

                    // Set Lte for all segments in the last chunk
                    if (lastChunk.Segments != null && lastChunk.Segments.Count > 0)
                    {
                        foreach (var segment in lastChunk.Segments)
                        {
                            segment.Lte = serializedMaxId;
                        }
                    }

                    log.WriteLine($"Populated Lte={serializedMaxId} for last chunk and its segments", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                log.WriteLine($"Unable to populate Lte for last chunk in {collection.CollectionNamespace}. Continuing without Lte. Details: {ex.Message}", LogType.Warning);
            }
        }

        private static BsonValue? GetSnapshotMaxId(
            IMongoCollection<BsonDocument> collection,
            DataType dataType,
            BsonDocument userFilter,
            bool skipDataTypeFilter)
        {
            var rawCollection = collection.Database.GetCollection<RawBsonDocument>(collection.CollectionNamespace.CollectionName);
            BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(dataType, userFilter, skipDataTypeFilter);

            FilterDefinition<RawBsonDocument> filter = (matchCondition != null && matchCondition.ElementCount > 0)
                ? new BsonDocumentFilterDefinition<RawBsonDocument>(matchCondition)
                : FilterDefinition<RawBsonDocument>.Empty;

            var maxDoc = rawCollection
                .Find(filter)
                .Sort(Builders<RawBsonDocument>.Sort.Descending("_id"))
                .Project<RawBsonDocument>(Builders<RawBsonDocument>.Projection.Include("_id"))
                .Limit(1)
                .FirstOrDefault();

            if (maxDoc == null || !maxDoc.Contains("_id"))
                return null;

            return maxDoc["_id"];
        }

        private static string? SerializeBsonValue(BsonValue? value, DataType dataType)
        {
            if (value == null)
                return null;

            if (value.IsBsonNull)
                return "BsonNull";

            if (value.IsBsonMaxKey)
                return "BsonMaxKey";

            return dataType switch
            {
                DataType.BinData => value.ToJson(),
                DataType.Object => value.ToJson(),
                DataType.Double => value.AsDouble.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
                DataType.Timestamp => value.AsBsonTimestamp.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                _ => value.ToString()
            };
        }

        /// <summary>
        /// Discovers cluster nodes (shards) for the Cosmos vCore / MongoDB target.
        /// Returns shard IDs for vCore, node identifiers for replica sets, or empty for standalone/RU.
        /// Used to populate the "Move to" dropdown when ShardingStrategy = DontShard.
        /// </summary>
        public static async Task<List<string>> GetClusterNodesAsync(string connectionString)
        {
            var nodes = new List<string>();
            
            try
            {
                // Route through the factory for connection reuse (avoids TooManyLogicalSessions) and so
                // a source CA would be honored if this is ever called against a custom-CA source. Callers
                // currently pass the target connection string, which uses a publicly trusted CA.
                var client = MongoClientFactory.Create(MigrationJobContext.Logger, connectionString);
                
                // 1. vCore: db.adminCommand({ listShards: 1 }) -> { shards: [ { _id, host, ... } ], ok: 1 }
                if (IsDocumentDBEndpoint(client))
                {
                    try
                    {
                        var adminDb = client.GetDatabase("admin");
                        var command = new BsonDocument { { "listShards", 1 } };
                        var result = await adminDb.RunCommandAsync<BsonDocument>(command);

                        if (result.Contains("shards") && result["shards"].IsBsonArray)
                        {
                            foreach (var shard in result["shards"].AsBsonArray)
                            {
                                if (shard.IsBsonDocument && shard.AsBsonDocument.Contains("_id"))
                                {
                                    nodes.Add(shard.AsBsonDocument["_id"].AsString);
                                }
                            }
                        }

                        if (nodes.Count > 0)
                            return nodes;
                    }
                    catch
                    {
                        // Fall through to next probes
                    }
                }
                
                // 2. RU (MongoDB API for Cosmos DB): Return empty - no sharding control available
                if (Helper.IsRU(connectionString))
                {
                    return nodes; // Empty list
                }
                
                // 3. Native sharded cluster: Query config.shards
                try
                {
                    var configDb = client.GetDatabase("config");
                    var shardsCollection = configDb.GetCollection<BsonDocument>("shards");
                    
                    var shardsCursor = await shardsCollection.FindAsync(FilterDefinition<BsonDocument>.Empty);
                    var shardDocs = await shardsCursor.ToListAsync();
                    
                    foreach (var doc in shardDocs)
                    {
                        if (doc.Contains("_id"))
                        {
                            nodes.Add(doc["_id"].AsString);
                        }
                    }
                    
                    if (nodes.Count > 0)
                        return nodes;
                }
                catch
                {
                    // Not a sharded cluster or no access to config db
                }
                
                // 4. Native replica set: Use SDAM (Server Discovery and Monitoring)
                try
                {
                    var cluster = client.Cluster;
                    var description = cluster.Description;
                    if (description.Type == ClusterType.ReplicaSet)
                    {
                        foreach (var server in description.Servers)
                        {
                            // Add server endpoint as node identifier
                            nodes.Add(server.EndPoint.ToString());
                        }
                        
                        if (nodes.Count > 0)
                            return nodes;
                    }
                }
                catch
                {
                    // SDAM probe failed
                }
                
                // 5. Standalone or unable to determine: Return empty
                return nodes;
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"GetClusterNodesAsync failed: {ex.Message}");
                return nodes; // Empty list on error
            }
        }

    }
}
