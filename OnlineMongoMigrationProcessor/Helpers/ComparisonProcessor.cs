using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public class ComparisonHelper
    {
        public async Task CompareRandomDocumentsAsync(
        Log log,
        MigrationJob job,
        MigrationSettings config,
        CancellationToken cancellationToken = default
        )
        {
            MigrationJobContext.AddVerboseLog($"ComparisonHelper.CompareRandomDocumentsAsync: jobId={job.Id}, sampleSize={config.CompareSampleSize}");
            MongoClient sourceClient;
            MongoClient targetClient;

            if (job.IsSimulatedRun)
            {
                log.WriteLine("Skipping comparison for simulated run.");
                return;
            }

            try
            {

                log.WriteLine($"Running hash comparison using {config.CompareSampleSize} sample documents.");

                sourceClient = MongoClientFactory.Create(log, MigrationJobContext.SourceConnectionString[job.Id] ?? string.Empty, false, config.CACertContentsForSourceServer);
                targetClient = MongoClientFactory.Create(log, MigrationJobContext.TargetConnectionString[job.Id] ?? string.Empty);


                foreach (var mu in Helper.GetMigrationUnitsToMigrate(job) ?? new List<MigrationUnit>())
                {

                    log.WriteLine($"Processing {mu.DatabaseName}.{mu.CollectionName}.");

                    cancellationToken.ThrowIfCancellationRequested();

                    if (!Helper.IsMigrationUnitValid(mu))
                    {
                        log.WriteLine($"Skipping {mu.DatabaseName}.{mu.CollectionName} as source collection status is empty.");
                        continue; //skip if collection is not OK
                    }

                    var sourceDb = sourceClient.GetDatabase(mu.DatabaseName);
                    var targetDb = targetClient.GetDatabase(mu.GetEffectiveTargetDatabaseName());

                    var sourceCollection = sourceDb.GetCollection<RawBsonDocument>(mu.CollectionName);
                    var targetCollection = targetDb.GetCollection<RawBsonDocument>(mu.GetEffectiveTargetCollectionName());

                    DateTime currTime = DateTime.UtcNow;


                    var userFilterDoc = string.IsNullOrWhiteSpace(mu.UserFilter)
                        ? new BsonDocument()
                        : MongoHelper.GetFilterDoc(mu.UserFilter);

                    var agg = sourceCollection.Aggregate();

                    if (userFilterDoc.ElementCount > 0)
                    {
                        agg = agg.Match(new BsonDocumentFilterDefinition<RawBsonDocument>(userFilterDoc));
                    }

#if LEGACY_MONGODB_DRIVER
                    // Sample() not available in legacy driver; use $sample aggregation stage directly
                    var randomDocsCursor = await agg
                        .AppendStage<RawBsonDocument>(new BsonDocument("$sample", new BsonDocument("size", config.CompareSampleSize)))
                        .ToCursorAsync(cancellationToken);
#else
                    var randomDocsCursor = await agg
                        .Sample(config.CompareSampleSize)
                        .ToCursorAsync(cancellationToken);
#endif


                    var randomDocs = await randomDocsCursor.ToListAsync(cancellationToken);

                    int mismatched = 0;
                    foreach (var sourceDoc in randomDocs)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (!sourceDoc.Contains("_id"))
                        {
                            log.WriteLine($"Error found in {mu.DatabaseName}.{mu.CollectionName}: Sampled document missing _id.", LogType.Error);
                            mismatched++;
                            continue;
                        }

                        var id = sourceDoc.GetValue("_id");
                        var filter = Builders<RawBsonDocument>.Filter.Eq("_id", id);

                        var targetDoc = await targetCollection.Find(filter)
                            .FirstOrDefaultAsync(cancellationToken);

                        if (targetDoc == null)
                        {
                            log.WriteLine($"Error found in {mu.DatabaseName}.{mu.CollectionName}: Document with _id {id} missing in target.", LogType.Error);
                            mismatched++;
                            continue;
                        }

                        var sourceHash = ComputeHash(sourceDoc);
                        var targetHash = ComputeHash(targetDoc);

                        if (sourceHash != targetHash)
                        {
                            log.WriteLine($"Error found in {mu.DatabaseName}.{mu.CollectionName}: Hash mismatch for _id {id}.", LogType.Error);
                            mismatched++;
                            continue;
                        }
                    }

                    if (mismatched == 0)
                    {
                        log.WriteLine($"No mismatch found in {mu.DatabaseName}.{mu.CollectionName}");
                    }

                    mu.VarianceCount = mismatched;
                    mu.ComparedOn = currTime;
                    MigrationJobContext.SaveMigrationUnit(mu,false);
                }
            }           
            catch (Exception ex)
            {
                log.WriteLine($"Error during comparison. Details: {ex}", LogType.Error);
            }
        }

        private static string ComputeHash(RawBsonDocument doc)
        {
            using (var sha = SHA256.Create())
            {
                var bytes = doc.ToBson();
                var hashBytes = sha.ComputeHash(bytes);
                return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            }
        }
    }
}

