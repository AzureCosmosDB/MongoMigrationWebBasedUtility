using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public class ComparisonHelper
    {
        public async Task CompareRandomDocumentsAsync(
        Log log,
        JobList joblist,
        MigrationJob job,
        MigrationSettings config,
        CancellationToken cancellationToken = default
        )
        {
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

                sourceClient = MongoClientFactory.Create(log, job.SourceConnectionString ?? string.Empty, false, config.CACertContentsForSourceServer);
                targetClient = MongoClientFactory.Create(log, job.TargetConnectionString ?? string.Empty);

                foreach (var mu in job.MigrationUnits ?? new List<MigrationUnit>())
                {

                    log.WriteLine($"Processing {mu.DatabaseName}.{mu.CollectionName}.");

                    cancellationToken.ThrowIfCancellationRequested();

                    if (mu.SourceStatus != CollectionStatus.OK)
                    {
                        log.WriteLine($"Skipping {mu.DatabaseName}.{mu.CollectionName} as source collection status is empty.");
                        continue; //skip if collection is not OK
                    }

                    var sourceDb = sourceClient.GetDatabase(mu.DatabaseName);
                    var targetDb = targetClient.GetDatabase(mu.DatabaseName);

                    var sourceCollection = sourceDb.GetCollection<BsonDocument>(mu.CollectionName);
                    var targetCollection = targetDb.GetCollection<BsonDocument>(mu.CollectionName);

                    DateTime currTime = DateTime.UtcNow;


                    var userFilterDoc = string.IsNullOrWhiteSpace(mu.UserFilter)
                        ? new BsonDocument()
                        : BsonDocument.Parse(mu.UserFilter);

                    var agg = sourceCollection.Aggregate();

                    if (userFilterDoc.ElementCount > 0)
                    {
                        agg = agg.Match(userFilterDoc);
                    }

                    var randomDocsCursor = await agg
                        .Sample(config.CompareSampleSize)
                        .ToCursorAsync(cancellationToken);


                    var randomDocs = await randomDocsCursor.ToListAsync(cancellationToken);

                    int mismatched = 0;
                    foreach (var sourceDoc in randomDocs)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var id = sourceDoc.GetValue("_id");
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);

                        var targetDoc = await targetCollection.Find(filter)
                            .FirstOrDefaultAsync(cancellationToken);

                        if (targetDoc == null)
                        {
                            log.WriteLine($"Error found in {mu.DatabaseName}.{mu.CollectionName}: Document with _id {id} missing in target.");
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
                    joblist.Save();
                }
            }           
            catch (Exception ex)
            {
                log.WriteLine($"Error during comparison: {ex.Message}", LogType.Error);
            }
        }

        private static string ComputeHash(BsonDocument doc)
        {
            using (var sha = SHA256.Create())
            {
                var json = doc.ToJson();
                var bytes = Encoding.UTF8.GetBytes(json);
                var hashBytes = sha.ComputeHash(bytes);
                return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            }
        }
    }
}

