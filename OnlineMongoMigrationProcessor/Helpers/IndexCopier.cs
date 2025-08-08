using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace OnlineMongoMigrationProcessor.Helpers
{
    class IndexCopier
    {

        public async Task<int> CopyIndexesAsync(IMongoCollection<BsonDocument> sourceCollection,
            MongoClient _targetClient,
            string databaseName,
            string collectionName,
            Log log)
        {
 
            var targetCollection = _targetClient
                .GetDatabase(databaseName)
                .GetCollection<BsonDocument>(collectionName);

            var indexDocuments = await sourceCollection.Indexes.List().ToListAsync();

            int counter = 0;
            foreach (var indexDocument in indexDocuments)
            {
                var indexName = indexDocument.GetValue("name", null)?.AsString;

                // Skip the default _id_ index since it is created automatically
                if (indexName == "_id_")
                {
                    Console.WriteLine("Skipping default _id index");
                    continue;
                }

                try
                {
                    var keys = indexDocument["key"].AsBsonDocument;

                    var options = new CreateIndexOptions<BsonDocument>();

                    // Name
                    if (indexDocument.TryGetValue("name", out var name))
                        options.Name = name.AsString;

                    // Unique
                    if (indexDocument.TryGetValue("unique", out var unique))
                        options.Unique = unique.ToBoolean();

                    // Sparse
                    if (indexDocument.TryGetValue("sparse", out var sparse))
                        options.Sparse = sparse.ToBoolean();

                    // Background (deprecated in newer Mongo, but may be present)
                    if (indexDocument.TryGetValue("background", out var background))
                        options.Background = background.ToBoolean();

                    // Hidden
                    if (indexDocument.TryGetValue("hidden", out var hidden))
                        options.Hidden = hidden.ToBoolean();

                    // Partial filter expression (using generic CreateIndexOptions to support it)
                    if (indexDocument.TryGetValue("partialFilterExpression", out var partialFilter))
                        options.PartialFilterExpression = partialFilter.AsBsonDocument;

                    // TTL index - expireAfterSeconds in seconds -> TimeSpan
                    if (indexDocument.TryGetValue("expireAfterSeconds", out var expireAfterSeconds))
                    {
                        options.ExpireAfter = TimeSpan.FromSeconds(expireAfterSeconds.ToInt32());
                    }

                    // Collation
                    if (indexDocument.TryGetValue("collation", out var collationBson))
                        options.Collation = Collation.FromBsonDocument(collationBson.AsBsonDocument);

                    // Weights (text index)
                    if (indexDocument.TryGetValue("weights", out var weights))
                        options.Weights = weights.AsBsonDocument;

                    // Default language (text index)
                    if (indexDocument.TryGetValue("default_language", out var defaultLanguage))
                        options.DefaultLanguage = defaultLanguage.AsString;

                    // Language override (text index)
                    if (indexDocument.TryGetValue("language_override", out var languageOverride))
                        options.LanguageOverride = languageOverride.AsString;

                    // Text index version
                    if (indexDocument.TryGetValue("textIndexVersion", out var textIndexVersion))
                        options.TextIndexVersion = textIndexVersion.ToInt32();

                    // Storage engine options
                    if (indexDocument.TryGetValue("storageEngine", out var storageEngine))
                        options.StorageEngine = storageEngine.AsBsonDocument;

                    // 2dsphere index options:
                    if (indexDocument.TryGetValue("2dsphereIndexVersion", out var sphereVersion))
                        options.SphereIndexVersion = sphereVersion.ToInt32();

                    // Bits (for geo indexes)
                    if (indexDocument.TryGetValue("bits", out var bits))
                        options.Bits = bits.ToInt32();

                    // Min & Max (for geo indexes)
                    if (indexDocument.TryGetValue("min", out var min))
                        options.Min = min.ToDouble();

                    if (indexDocument.TryGetValue("max", out var max))
                        options.Max = max.ToDouble();

                    // Bucket size (for geo 2d indexes)
                    if (indexDocument.TryGetValue("bucketSize", out var bucketSize))
                    {
                        // GeoHaystack bucketSize is obsolete in modern MongoDB; suppress warning for backward compatibility.
#pragma warning disable CS0618
                        options.BucketSize = bucketSize.ToDouble();
#pragma warning restore CS0618
                    }

                    if (indexDocument.TryGetValue("wildcardProjection", out var wildcardProjection))
                    {
                        // options.WildcardProjection = wildcardProjection.AsBsonDocument; // Not supported in .NET driver
                        log.WriteLine($"Warning: Wildcard projection skipped in {collectionName}. Create Manually on target.");
                    }

                    var indexModel = new CreateIndexModel<BsonDocument>(keys, options);

                    await targetCollection.Indexes.CreateOneAsync(indexModel);
                    counter++;
                }
                catch (Exception ex)
                {
                    log.WriteLine($"Failed to create index '{indexName}' in {collectionName}: {ex.Message}",LogType.Error);
                    // You may want to log or handle the error appropriately
                }
            }

            return counter;
        }
    }

}
