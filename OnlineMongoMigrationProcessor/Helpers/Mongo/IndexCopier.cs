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

                    await targetCollection.Indexes.CreateOneAsync(indexModel);
                    counter++;
                }
                catch (Exception ex)
                {
                    log.WriteLine($"Failed to create index '{indexName}' in {collectionName}. Details: {ex}",LogType.Error);
                    // You may want to log or handle the error appropriately
                }
            }

            return counter;
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
