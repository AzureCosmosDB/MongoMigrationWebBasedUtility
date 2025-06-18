using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;


#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8625
#pragma warning disable CS8600

namespace OnlineMongoMigrationProcessor
{
    internal static class MongoHelper
    {
        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit item)
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




        public static async Task<(bool IsCSEnabled, string Version)> IsChangeStreamEnabledAsync(string connectionString, MigrationUnit unit)
        {
            
            string version = string.Empty;
            try
            {
                var mongoUrl = new MongoUrl(connectionString);
                var settings = MongoClientSettings.FromUrl(mongoUrl);
                settings.ReadConcern = ReadConcern.Majority;
                var client = new MongoClient(settings);


                if (connectionString.Contains("mongocluster.cosmos.azure.com")) //for vcore
                {
                    var database = client.GetDatabase(unit.DatabaseName);
                    var collection = database.GetCollection<BsonDocument>(unit.CollectionName);

                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    };
                    var cursor = await collection.WatchAsync(options) ;
                    Log.WriteLine("Change streams are enabled on vCore.");
                    Log.Save();
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
                        Log.WriteLine("Change streams are enabled (replica set or sharded cluster).");
                        Log.Save();
                        return (IsCSEnabled: true, Version: version);
                    }
                    else
                    {
                        Log.WriteLine("Change streams are not enabled (standalone server).", LogType.Error);
                        Log.Save();
                        return (IsCSEnabled: false, Version: version);
                    }
                }
            }
            catch (MongoCommandException ex) when (ex.Message.Contains("$changeStream is not supported"))
            {
                Log.WriteLine("Change streams are not enabled on vCore.", LogType.Error);
                Log.Save();
                return (IsCSEnabled: false, Version: "");

            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error checking for change streams: {ex.ToString()}", LogType.Error);
                Log.Save();
                //return (IsCSEnabled: false, Version: version);
                throw ex;
            }
        }

        public async static Task SetChangeStreamResumeTokenAsync(MongoClient client, MigrationUnit unit)
        {
            int retryCount = 0;
            bool isSucessful = false;

            while (!isSucessful && retryCount<10)
            {
                ChangeStreamOperationType? changeType = null;
                BsonValue? documentId = null;
                try
                {
                    if (!string.IsNullOrEmpty(unit.ResumeToken))
                    {
                        Log.WriteLine($"Change stream resume token for {unit.DatabaseName}.{unit.CollectionName} already set");
                        Log.Save();
                        return;
                    }


                    BsonDocument resumeToken = new BsonDocument();

                    var database = client.GetDatabase(unit.DatabaseName);
                    var collection = database.GetCollection<BsonDocument>(unit.CollectionName);

                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    };


                    using (var cursor = await collection.WatchAsync(options))
                    {
                        // Try to get a resume token, even if no changes exist
                        resumeToken = cursor.GetResumeToken();

                        //3.6 mongo  doesn't return resume token if no changes exist
                        if (resumeToken == null || resumeToken.ElementCount == 0)
                        {
                            foreach (var change in cursor.ToEnumerable())
                            {
                                resumeToken = change.ResumeToken;
                                changeType = change.OperationType;
                                documentId = change.DocumentKey["_id"];
                                break;
                            }
                        }

                    }

                    if (resumeToken == null || resumeToken.ElementCount == 0)
                    {
                        Log.WriteLine($"Blank resume token for {unit.DatabaseName}.{unit.CollectionName}", LogType.Error);
                    }
                    else
                    {
                        Log.WriteLine($"Saved change stream resume token for {unit.DatabaseName}.{unit.CollectionName}");
                        Log.Save();

                        unit.ResumeToken = resumeToken.ToJson();

                        if (changeType != null)
                        {
                            unit.ResumeTokenOperation = (ChangeStreamOperationType)changeType;
                            unit.ResumeDocumentId = documentId;
                        }

                    }
                    isSucessful = true;

                }
                catch (Exception ex)
                {
                    retryCount++;

                    Log.WriteLine($"Attempt {retryCount}. Error setting change stream resume token for {unit.DatabaseName}.{unit.CollectionName}: {ex.ToString()}", LogType.Error);
                    Log.Save();
                    
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


        public static async Task<bool> DeleteAndCopyIndexesAsync(string targetConnectionString, IMongoCollection<BsonDocument> sourceCollection, bool skipIndexes)
        {
            try
            {
                // Extract database and collection details from the source collection
                var sourceDatabase = sourceCollection.Database;
                var sourceCollectionName = sourceCollection.CollectionNamespace.CollectionName;

                // Connect to the target database
                var targetClient = new MongoClient(targetConnectionString);
                var targetDatabaseName = sourceDatabase.DatabaseNamespace.DatabaseName;
                var targetDatabase = targetClient.GetDatabase(targetDatabaseName);
                var targetCollectionName = sourceCollectionName;

                Log.WriteLine($"Creating collection: {targetDatabaseName}.{targetCollectionName}");
                Log.Save();

                // Check if the target collection exists
                var collectionNamesCursor = await targetDatabase.ListCollectionNamesAsync();
                var collectionNames = await collectionNamesCursor.ToListAsync();
                bool targetCollectionExists = collectionNames.Contains(targetCollectionName);

                // Delete the target collection if it exists
                if (targetCollectionExists)
                {
                    await targetDatabase.DropCollectionAsync(targetCollectionName);
                    Log.WriteLine($"Deleted existing target collection: {targetDatabaseName}.{targetCollectionName}");
                    Log.Save();
                }

                if (skipIndexes)
                    return true;

                Log.WriteLine($"Creating indexes for: {targetDatabaseName}.{targetCollectionName}");
                Log.Save();

                // Get the indexes from the source collection
                var indexes = await sourceCollection.Indexes.ListAsync();
                var indexDocuments = await indexes.ToListAsync();

                // Create the target collection
                await targetDatabase.CreateCollectionAsync(targetCollectionName);
                var targetCollection = targetDatabase.GetCollection<BsonDocument>(targetCollectionName);

                // Copy the indexes to the target collection
                foreach (var indexDocument in indexDocuments)
                {
                    // Exclude the default "_id_" index as it is automatically created
                    if (indexDocument.GetValue("name", "") == "_id_")
                        continue;

                    // Extract the keys and options for the index
                    var keys = indexDocument["key"].AsBsonDocument;

                    CreateIndexOptions options = null;
                    try
                    {
                        options = new CreateIndexOptions
                        {
                            Name = indexDocument.GetValue("name", default(BsonValue)).AsString,
                            Unique = indexDocument.GetValue("unique", false).ToBoolean()
                        };

                        // Create the index on the target collection
                        var indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                        await targetCollection.Indexes.CreateOneAsync(indexModel);
                    }
                    catch (Exception ex)
                    {
                        Log.WriteLine($"Error copying index {options?.Name} for {targetDatabaseName}.{targetCollectionName}. Details: {ex.ToString()}", LogType.Error);
                        Log.Save();
                    }
                }

                Log.WriteLine($"{indexDocuments.Count} Indexes copied successfully to {targetDatabaseName}.{targetCollectionName}");
                Log.Save();
                return true;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error copying indexes: {ex.ToString()}", LogType.Error);
                Log.Save();
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
                DataType.UUID => filterBuilder.Lt(fieldName, value.AsGuid),
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
                DataType.UUID => filterBuilder.Gte(fieldName, value.AsGuid),
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
                DataType.UUID => "binData",
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
                DataType.UUID => $"{{\\\"$binary\\\":\\\"{value.AsGuid}\\\", \\\"$type\\\":\\\"04\\\"}}",
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


