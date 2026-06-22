using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    class IndexCopier
    {
        private static readonly HashSet<string> UnsupportedIndexOptions = new(StringComparer.OrdinalIgnoreCase)
        {
            "collation",
            "hidden"
        };

        private static readonly HashSet<string> SupportedPartialFilterOperators = new(StringComparer.Ordinal)
        {
            "$eq", "$gt", "$gte", "$lt", "$lte", "$type", "$exists"
        };

        private static readonly HashSet<string> UnsupportedPartialFilterFieldOperators = new(StringComparer.Ordinal)
        {
            "$ne", "$nin", "$in", "$all", "$elemMatch", "$size", "$regex", "$not",
            "$mod", "$text", "$where", "$geoWithin", "$geoIntersects", "$near", "$nearSphere"
        };

        private static readonly HashSet<string> SupportedPartialFilterLogicalOperators = new(StringComparer.Ordinal)
        {
            "$and"
        };

        private static readonly HashSet<string> UnsupportedPartialFilterLogicalOperators = new(StringComparer.Ordinal)
        {
            "$or", "$nor"
        };

        /// <summary>
        /// Index filter mode for selective index copying.
        /// </summary>
        public enum IndexFilter
        {
            All,
            UniqueOnly,
            NonUniqueOnly
        }

        /// <summary>
        /// Copies all indexes from source to target (original behavior).
        /// </summary>
        public async Task<int> CopyIndexesAsync(IMongoCollection<BsonDocument> sourceCollection,
            MongoClient _targetClient,
            string databaseName,
            string collectionName,
            Log log)
        {
            return await CopyIndexesInternalAsync(sourceCollection, _targetClient, databaseName, collectionName, log, IndexFilter.All);
        }

        /// <summary>
        /// Copies only unique indexes from source to target.
        /// Call this before offline data copy to enforce uniqueness constraints during insertion.
        /// </summary>
        public async Task<int> CopyUniqueIndexesAsync(IMongoCollection<BsonDocument> sourceCollection,
            MongoClient _targetClient,
            string databaseName,
            string collectionName,
            Log log)
        {
            return await CopyIndexesInternalAsync(sourceCollection, _targetClient, databaseName, collectionName, log, IndexFilter.UniqueOnly);
        }

        /// <summary>
        /// Copies only non-unique indexes from source to target.
        /// Call this after offline data copy completes for better performance.
        /// </summary>
        public async Task<int> CopyNonUniqueIndexesAsync(IMongoCollection<BsonDocument> sourceCollection,
            MongoClient _targetClient,
            string databaseName,
            string collectionName,
            Log log,
            bool useBlockingBuilds = false)
        {
            return await CopyIndexesInternalAsync(sourceCollection, _targetClient, databaseName, collectionName, log, IndexFilter.NonUniqueOnly, useBlockingBuilds);
        }

        /// <summary>
        /// Returns the count of non-unique indexes on the source collection (excluding _id_).
        /// Used for calculating IndexPercent progress.
        /// </summary>
        public async Task<int> CountNonUniqueIndexesAsync(IMongoCollection<BsonDocument> sourceCollection, Log log)
        {
            var indexDocuments = await sourceCollection.Indexes.List().ToListAsync();
            int count = 0;
            foreach (var indexDocument in indexDocuments)
            {
                var indexName = indexDocument.GetValue("name", null)?.AsString;
                if (indexName == "_id_") continue;
                bool isUnique = indexDocument.TryGetValue("unique", out var unique) && unique.ToBoolean();
                if (!isUnique) count++;
            }
            return count;
        }

        /// <summary>
        /// Returns the count of non-unique indexes currently present on the target collection
        /// (excluding _id_). Used to verify blocking/non-blocking index-build completion instead of
        /// relying on currentOp activity, which can briefly report zero while builds are still queued.
        /// Returns -1 on failure so callers can keep waiting rather than declaring completion.
        /// </summary>
        public static async Task<int> CountNonUniqueIndexesOnTargetAsync(MongoClient targetClient, string databaseName, string collectionName, Log log)
        {
            try
            {
                var targetCollection = targetClient
                    .GetDatabase(databaseName)
                    .GetCollection<BsonDocument>(collectionName);
                var indexDocuments = await targetCollection.Indexes.List().ToListAsync();
                int count = 0;
                foreach (var indexDocument in indexDocuments)
                {
                    var indexName = indexDocument.GetValue("name", null)?.AsString;
                    if (indexName == "_id_") continue;
                    bool isUnique = indexDocument.TryGetValue("unique", out var unique) && unique.ToBoolean();
                    if (!isUnique) count++;
                }
                return count;
            }
            catch (Exception ex)
            {
                log?.WriteLine($"CountNonUniqueIndexesOnTargetAsync failed for {databaseName}.{collectionName}: {ex.Message}", LogType.Warning);
                return -1;
            }
        }

        private async Task<int> CopyIndexesInternalAsync(IMongoCollection<BsonDocument> sourceCollection,
            MongoClient _targetClient,
            string databaseName,
            string collectionName,
            Log log,
            IndexFilter filter,
            bool useBlockingBuilds = false)
        {
 
            var targetCollection = _targetClient
                .GetDatabase(databaseName)
                .GetCollection<BsonDocument>(collectionName);

            var indexDocuments = await sourceCollection.Indexes.List().ToListAsync();

            int counter = 0;
            bool hasTextIndex = false;
            var createdIndexNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var indexDocument in indexDocuments)
            {
                var indexName = indexDocument.GetValue("name", null)?.AsString;

                // Skip the default _id_ index since it is created automatically
                if (indexName == "_id_")
                {
                    log.WriteLine($"Skipping default _id index for {databaseName}.{collectionName}", LogType.Debug);
                    continue;
                }

                try
                {
                    var keys = indexDocument["key"].AsBsonDocument;

                    // Determine uniqueness for filtering
                    bool isUnique = indexDocument.TryGetValue("unique", out var uniqueVal) && uniqueVal.ToBoolean();
                    if (filter == IndexFilter.UniqueOnly && !isUnique) continue;
                    if (filter == IndexFilter.NonUniqueOnly && isUnique) continue;

                    if (HasUnsupportedIndexOption(indexDocument, indexName, databaseName, collectionName, log))
                    {
                        continue;
                    }

                    if (IsTextIndex(keys))
                    {
                        if (hasTextIndex)
                        {
                            log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: only one text index is supported.", LogType.Warning);
                            continue;
                        }

                        if (indexDocument.TryGetValue("textIndexVersion", out var tiv) && tiv.ToInt32() != 2)
                        {
                            log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: textIndexVersion {tiv} is not supported (only 2).", LogType.Warning);
                            continue;
                        }

                        hasTextIndex = true;
                    }

                    if (IsCompoundGeospatialIndex(keys))
                    {
                        log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: compound geospatial index is not supported.", LogType.Warning);
                        continue;
                    }

                    if (IsCompoundWildcardIndex(keys))
                    {
                        log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: compound wildcard index is not supported.", LogType.Warning);
                        continue;
                    }

                    if (HasMultipleHashedFields(keys))
                    {
                        log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: multiple hashed fields are not supported.", LogType.Warning);
                        continue;
                    }

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

                    // Partial filter expression (using generic CreateIndexOptions to support it)
                    if (indexDocument.TryGetValue("partialFilterExpression", out var partialFilter))
                    {
                        var pfe = partialFilter.AsBsonDocument;

                        // Cannot combine sparse with partialFilterExpression
                        if (options.Sparse == true)
                        {
                            options.Sparse = null;
                            log.WriteLine($"Modified index '{indexName}' on {databaseName}.{collectionName}: removed 'sparse' option because partialFilterExpression is present.", LogType.Warning);
                        }

                        // Remove empty partialFilterExpression
                        if (pfe.ElementCount == 0)
                        {
                            log.WriteLine($"Modified index '{indexName}' on {databaseName}.{collectionName}: removed empty partialFilterExpression.", LogType.Warning);
                        }
                        else
                        {
                            if (!TryTransformPartialFilterExpression(pfe, out var transformedPfe, out var issues))
                            {
                                log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: incompatible partialFilterExpression ({string.Join("; ", issues)}).", LogType.Warning);
                                continue;
                            }

                            options.PartialFilterExpression = transformedPfe;
                        }
                    }

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
                        log.WriteLine($"Warning: Wildcard projection skipped in {collectionName}. Create Manually on target.", LogType.Warning);
                    }

                    options.Name = GetUniqueIndexName(options.Name ?? indexName ?? "unnamed", createdIndexNames, databaseName, collectionName, log);

                    var indexModel = new CreateIndexModel<BsonDocument>(keys, options);

                    if (filter == IndexFilter.NonUniqueOnly)
                    {
                        // One createIndexes command per index. We never wait for the build to
                        // complete here — server-side build progress is monitored separately.
                        // The `blocking` field is forwarded from the configured indexing strategy.
                        bool submitted = await SubmitNonUniqueIndexBuildAsync(_targetClient, databaseName, collectionName, keys, options, useBlockingBuilds, log);
                        if (submitted)
                            counter++;
                    }
                    else
                    {
                        await targetCollection.Indexes.CreateOneAsync(indexModel);
                        counter++;
                    }
                }
                catch (Exception ex)
                {
                    log.WriteLine($"Failed to create index '{indexName}' in {collectionName}. Details: {ex}",LogType.Error);
                    // You may want to log or handle the error appropriately
                }
            }

            return counter;
        }

        private static async Task<bool> SubmitNonUniqueIndexBuildAsync(
            MongoClient targetClient,
            string databaseName,
            string collectionName,
            BsonDocument keys,
            CreateIndexOptions<BsonDocument> options,
            bool blocking,
            Log log)
        {
            var targetDb = targetClient.GetDatabase(databaseName);

            var indexDoc = new BsonDocument
            {
                { "key", keys }
            };

            if (!string.IsNullOrWhiteSpace(options.Name))
                indexDoc["name"] = options.Name;
            if (options.Unique.HasValue)
                indexDoc["unique"] = options.Unique.Value;
            if (options.Sparse.HasValue)
                indexDoc["sparse"] = options.Sparse.Value;
            if (options.PartialFilterExpression != null)
                indexDoc["partialFilterExpression"] = options.PartialFilterExpression.ToBsonDocument();
            if (options.ExpireAfter.HasValue)
                indexDoc["expireAfterSeconds"] = (int)options.ExpireAfter.Value.TotalSeconds;
            if (options.StorageEngine != null)
                indexDoc["storageEngine"] = options.StorageEngine;

            var command = new BsonDocument
            {
                { "createIndexes", collectionName },
                { "indexes", new BsonArray { indexDoc } },
                { "blocking", blocking },
                { "maxTimeMS", 5000 }
            };

            try
            {
                await targetDb.RunCommandAsync<BsonDocument>(command);
                return true;
            }
            catch (MongoCommandException ex) when (IsExpectedBlockingTimeout(ex))
            {
                // Timeout is expected; server-side index build continues and queues.
                return true;
            }
            catch (MongoExecutionTimeoutException ex) when (IsExpectedBlockingTimeout(ex))
            {
                // Timeout is expected; server-side index build continues and queues.
                return true;
            }
            catch (TimeoutException ex) when (IsExpectedBlockingTimeout(ex))
            {
                // Timeout is expected; server-side index build continues and queues.
                return true;
            }
            catch (Exception ex)
            {
                if (IsExpectedBlockingTimeout(ex))
                    return true;

                var indexName = options.Name ?? "unnamed";
                log.WriteLine($"Failed to submit non-unique index build '{indexName}' on {databaseName}.{collectionName}. Details: {ex.Message}", LogType.Error);
                return false;
            }
        }

        private static bool IsExpectedBlockingTimeout(Exception ex)
        {
            var message = ex.Message ?? string.Empty;
            return message.Contains("MaxTimeMS expired", StringComparison.OrdinalIgnoreCase)
                || message.Contains("operation exceeded time limit", StringComparison.OrdinalIgnoreCase)
                || message.Contains("exceeded time limit", StringComparison.OrdinalIgnoreCase)
                || (ex is MongoCommandException mce && mce.Code == 50);
        }

        private static bool HasUnsupportedIndexOption(BsonDocument indexDocument, string? indexName, string databaseName, string collectionName, Log log)
        {
            foreach (var unsupported in UnsupportedIndexOptions)
            {
                if (indexDocument.Contains(unsupported))
                {
                    log.WriteLine($"Skipping index '{indexName}' on {databaseName}.{collectionName}: '{unsupported}' option is not supported.", LogType.Warning);
                    return true;
                }
            }

            return false;
        }

        private static string GetUniqueIndexName(string desiredName, HashSet<string> createdNames, string databaseName, string collectionName, Log log)
        {
            if (createdNames.Add(desiredName))
            {
                return desiredName;
            }

            int suffix = 1;
            string renamed = $"{desiredName}_dup{suffix}";
            while (!createdNames.Add(renamed))
            {
                suffix++;
                renamed = $"{desiredName}_dup{suffix}";
            }

            log.WriteLine($"Renaming duplicate index name '{desiredName}' to '{renamed}' on {databaseName}.{collectionName}.", LogType.Warning);
            return renamed;
        }

        private static bool IsTextIndex(BsonDocument keys)
        {
            foreach (var element in keys.Elements)
            {
                if (element.Value.BsonType == BsonType.String && string.Equals(element.Value.AsString, "text", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsCompoundGeospatialIndex(BsonDocument keys)
        {
            if (keys.ElementCount <= 1)
            {
                return false;
            }

            bool hasGeo = false;
            bool hasRegular = false;

            foreach (var element in keys.Elements)
            {
                string direction = element.Value.ToString();
                bool isGeo = string.Equals(direction, "2dsphere", StringComparison.OrdinalIgnoreCase)
                             || string.Equals(direction, "2d", StringComparison.OrdinalIgnoreCase);

                if (isGeo)
                {
                    hasGeo = true;
                    continue;
                }

                bool isText = string.Equals(direction, "text", StringComparison.OrdinalIgnoreCase);
                bool isHashed = string.Equals(direction, "hashed", StringComparison.OrdinalIgnoreCase);

                if (!isText && !isHashed)
                {
                    hasRegular = true;
                }
            }

            return hasGeo && hasRegular;
        }

        private static bool IsCompoundWildcardIndex(BsonDocument keys)
        {
            if (keys.ElementCount <= 1)
            {
                return false;
            }

            return keys.Elements.Any(e => e.Name == "$**" || e.Name.EndsWith(".$**", StringComparison.Ordinal));
        }

        private static bool HasMultipleHashedFields(BsonDocument keys)
        {
            int hashedCount = keys.Elements.Count(e => string.Equals(e.Value.ToString(), "hashed", StringComparison.OrdinalIgnoreCase));
            return hashedCount > 1;
        }

        private static bool TryTransformPartialFilterExpression(BsonDocument partialFilter, out BsonDocument transformed, out List<string> issues)
        {
            transformed = new BsonDocument();
            issues = new List<string>();

            foreach (var element in partialFilter.Elements)
            {
                string field = element.Name;
                BsonValue condition = element.Value;

                if (UnsupportedPartialFilterLogicalOperators.Contains(field))
                {
                    issues.Add($"Logical operator '{field}' is not supported.");
                    continue;
                }

                if (SupportedPartialFilterLogicalOperators.Contains(field))
                {
                    if (!condition.IsBsonArray)
                    {
                        issues.Add($"Logical operator '{field}' expects an array of conditions.");
                        continue;
                    }

                    var transformedClauses = new BsonArray();
                    foreach (var clause in condition.AsBsonArray)
                    {
                        if (!clause.IsBsonDocument)
                        {
                            issues.Add($"Logical operator '{field}' has a non-document clause.");
                            continue;
                        }

                        if (TryTransformPartialFilterExpression(clause.AsBsonDocument, out var transformedClause, out var subIssues))
                        {
                            if (transformedClause.ElementCount > 0)
                            {
                                transformedClauses.Add(transformedClause);
                            }
                        }
                        else
                        {
                            issues.AddRange(subIssues);
                        }
                    }

                    if (transformedClauses.Count > 0)
                    {
                        transformed[field] = transformedClauses;
                    }

                    continue;
                }

                if (condition.IsBsonDocument)
                {
                    var newCondition = new BsonDocument();
                    bool fieldCompatible = true;

                    foreach (var opElement in condition.AsBsonDocument.Elements)
                    {
                        string op = opElement.Name;
                        var value = opElement.Value;

                        if (op == "$in")
                        {
                            if (value.IsBsonArray && value.AsBsonArray.Count == 1)
                            {
                                newCondition["$eq"] = value.AsBsonArray[0];
                            }
                            else
                            {
                                issues.Add($"Field '{field}' uses unsupported $in with multiple/invalid values.");
                                fieldCompatible = false;
                            }
                            continue;
                        }

                        if (op == "$exists")
                        {
                            bool existsTrue = (value.IsBoolean && value.AsBoolean) || (value.IsInt32 && value.AsInt32 == 1);
                            if (!existsTrue)
                            {
                                issues.Add($"Field '{field}' uses unsupported $exists value (only true is supported).");
                                fieldCompatible = false;
                            }
                            else
                            {
                                newCondition[op] = value;
                            }
                            continue;
                        }

                        if (op == "$not" || UnsupportedPartialFilterFieldOperators.Contains(op) || (op.StartsWith("$", StringComparison.Ordinal) && !SupportedPartialFilterOperators.Contains(op)))
                        {
                            issues.Add($"Field '{field}' uses unsupported operator '{op}'.");
                            fieldCompatible = false;
                            continue;
                        }

                        newCondition[op] = value;
                    }

                    if (fieldCompatible && newCondition.ElementCount > 0)
                    {
                        transformed[field] = newCondition;
                    }

                    continue;
                }

                if (condition.IsBsonArray)
                {
                    if (condition.AsBsonArray.Count == 0)
                    {
                        transformed[field] = condition;
                    }
                    else
                    {
                        issues.Add($"Field '{field}' uses unsupported non-empty array equality.");
                    }
                    continue;
                }

                // Direct value comparison (implicit $eq) is supported
                transformed[field] = condition;
            }

            return issues.Count == 0;
        }
    }

}
