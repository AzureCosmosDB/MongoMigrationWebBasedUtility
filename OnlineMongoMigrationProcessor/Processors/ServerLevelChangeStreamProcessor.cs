using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;
using static System.Net.Mime.MediaTypeNames;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class ServerLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        // Server-level processors use MigrationJob properties directly for global resume tokens
        protected override bool UseResumeTokenCache => false;
        protected OrderedUniqueList<string> _uniqueCollectionKeys;


        private bool _monitorAllCollections = false;
        public ServerLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, targetClient, muCache, config, syncBack, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog($"ServerLevelChangeStreamProcessor: Constructor called, syncBack={syncBack}");
            _uniqueCollectionKeys = new OrderedUniqueList<string>();
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog("ServerLevelChangeStreamProcessor.ProcessChangeStreamsAsync: starting");
            long loops = 0;
            bool oplogSuccess = true;

            bool isVCore = (_syncBack ? MigrationJobContext.CurrentlyActiveJob.TargetEndpoint : MigrationJobContext.CurrentlyActiveJob.SourceEndpoint)
               .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            // RUOptimizedCopy jobs should not use server-level change streams
            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
            {
                _log.WriteLine($"{_syncBackPrefix}RUOptimizedCopy jobs do not support server-level change streams. This processor should not be used for such jobs.", LogType.Error);
                return;

            }

            //temp override to monitor all collections.
            bool found = _migrationUnitsToProcess.TryGetValue("DUMMY.DUMMY", out var dummyMu);
            if (_migrationUnitsToProcess.Count == 1 && MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && found && dummyMu != null)
            {
                _monitorAllCollections = true;
                _log.WriteLine($"{_syncBackPrefix}Special mode: Starting server-level change stream processing for all collections.", LogType.Warning);
            }
            else
            {
                _log.WriteLine($"{_syncBackPrefix}Starting server-level change stream processing for {_migrationUnitsToProcess.Count} collection(s).");
            }

            long loop = 0;
            DateTime lastResumeTokenCheck = DateTime.MinValue;
            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                try
                {
                    loop++;
                    int seconds = GetBatchDurationInSeconds(1.0f); // Use full duration for server-level
                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.WriteLine($"{_syncBackPrefix}Processing round {loop} for server - level change stream. Batch Duration {seconds} seconds");

                    string resumeToken = GetResumeToken();

                    if(!string.IsNullOrEmpty(resumeToken))
                    {
                        //await Task.Delay(60 * 1000, cancellationToken);
                        await WatchServerLevelChangeStream(cancellationToken);
                    }
                    else
                    {
                        _log.WriteLine($"{_syncBackPrefix}No resume token found for server-level change stream. Waiting for 60 seconds before retrying.", LogType.Warning);

                        // Wait for 60 seconds before checking again
                        
                        await InitializeResumeTokensAsync(cancellationToken);
                        await Task.Delay(60 * 1000, cancellationToken);
                        lastResumeTokenCheck = DateTime.UtcNow;
                    }

                    //cleanup for aggressive CS mode
                    if (loops == 1 || loops % 4 == 0)
                    {
                        await AggressiveCSCleanupAsync();
                    }

                }
                catch (OperationCanceledException)
                {
                    // Expected when batch times out, continue to next batch
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error in server-level change stream processing: {ex}", LogType.Error);
                    // Continue processing on errors
                }
            }
        }

        private async Task InitializeResumeTokensAsync(CancellationToken token)
        {
            _log.WriteLine($"{_syncBackPrefix}Rechecking server for resume token.", LogType.Info);

            try
            {

                await MongoHelper.SetChangeStreamResumeTokenAsync(
                    _log,
                    _syncBack ? _targetClient : _sourceClient,
                    MigrationJobContext.CurrentlyActiveJob,
                    null,
                    30,
                    _syncBack,
                    token);
            }
            catch (Exception ex)
            {
                // do nothing
            }
        }


        private DateTime GetChangeTime(ChangeStreamDocument<BsonDocument> change)
        {
            if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
            {
                return  MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
            }
            else if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.WallTime != null)
            {
                return change.WallTime.Value;
            }
            return DateTime.MinValue;
        }

        private async Task WatchServerLevelChangeStream(CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog("ServerLevelChangeStreamProcessor.WatchServerLevelChangeStream: starting");

            long counter = 0;            
            string collectionKey = string.Empty;
            string latestResumeToken = string.Empty;
            DateTime latestTimestamp = DateTime.MinValue;

            try
            {
                // Calculate MaxAwaitTime from the cancellation token timeout
                // Use 80% of cancellation timeout or 5 seconds minimum to ensure cursor returns before cancellation
                int maxAwaitSeconds = 5;
                if (cancellationToken.CanBeCanceled)
                {
                    try
                    {
                        // Try to get timeout from CancellationTokenSource if available
                        // Fall back to 5 seconds if we can't determine it
                        var timeout = GetBatchDurationInSeconds(1.0f);
                        maxAwaitSeconds = Math.Max(5, (int)(timeout * 0.8));
                    }
                    catch
                    {
                        maxAwaitSeconds = 5;
                    }
                }

                // Create pipeline for server-level change stream
                List<BsonDocument> pipeline = new List<BsonDocument>();

                // Set up options - use global resume token from MigrationJob
                var options = new ChangeStreamOptions
                {
                    BatchSize = 500,
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds)
                };

                // Get resume information from MigrationJob for server-level streams

                DateTime timeStamp = GetCursorUtcTimestamp();
                string tokenJson = GetResumeToken();
                DateTime startedOn = GetChangeStreamStartedOn();
                string? version = string.Empty;

                if (!_syncBack)
                {
                    version = MigrationJobContext.CurrentlyActiveJob.SourceServerVersion;
                }
                else
                {
                    version = "8"; // hard code for target
                }

                // Handle initial document replay for server-level streams
                bool initialReplayCompleted = GetInitialDocumentReplayedStatus();
                if (!initialReplayCompleted && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive)
                {
                    if (!AutoReplayFirstChangeInResumeToken())
                    {
                        _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for server-level change stream. Skipping server-level processing.", LogType.Error);

                        // Reset CSUpdatesInLastBatch before early return to prevent stale values
                        foreach (var kvp in _migrationUnitsToProcess.Keys)
                        {
                            _migrationUnitsToProcess[kvp] = 0;
                        }
                        
                        return;
                    }
                    SetInitialDocumentReplayedStatus(true);
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob); ;
                }

                //if (timeStamp > DateTime.MinValue && resumeToken == null && !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack)) //skip CursorUtcTimestamp if its reset 
                //{
                //    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                //    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                //}
                //else if (!string.IsNullOrEmpty(resumeToken))  //both version  having resume token
                //{
                //    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken), MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                //}
                //else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3")) //for Mongo 3.6
                //{
                //    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                //}
                //else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))  //newer version
                //{
                //    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                //    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                //}
                var resumeToken = BsonSerializer.Deserialize<BsonDocument>(tokenJson);

                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter =resumeToken, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };

                var pipelineArray = pipeline.ToArray();

                // Watch at client level (server-level)
                using var cursor = _sourceClient.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);

                long changeCount = 0;
                long ignoreCount = 0;

                if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
                {
                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        latestResumeToken=change.ResumeToken.ToJson();
                        latestTimestamp = GetChangeTime(change);
                        collectionKey = change.CollectionNamespace.ToString();
                        var id = Helper.GenerateMigrationUnitId(collectionKey);
                        if (_migrationUnitsToProcess.ContainsKey(id) || _monitorAllCollections)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            if (ExecutionCancelled) break;

                            var result = await PreProcessChange(change, counter);
                            if (!result.success)
                                break;
                            counter = result.counter;
                            changeCount=counter;
                        }
                        else
                        {
                            ignoreCount++;
                            ShowInMonitor(change, collectionKey, GetChangeTime(change), changeCount+ ignoreCount, true);
                        }
                        if (ExecutionCancelled)
                            break;
                    }

                    try
                    {
                        if (changeCount > _config.ChangeStreamMaxDocsInBatch)
                        {
                            await BulkProcessAllChangesAsync(_accumulatedChangesPerCollection);
                            SetJobResumeToken(latestResumeToken, latestTimestamp);
                            changeCount = 0;
                        }
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL error during BulkProcessAllChangesAsync. Details: {ex}", LogType.Error);
                        StopJob($"CRITICAL error in BulkProcessAllChangesAsync. Details: {ex}");
                        throw; // Re-throw to stop processing
                    }

                }
                else
                {
                    while (cursor.MoveNext(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) break;

                        foreach (var change in cursor.Current)
                        {
                            latestResumeToken = change.ResumeToken.ToJson();
                            latestTimestamp = GetChangeTime(change);
                            collectionKey = change.CollectionNamespace.ToString();

                            var id= Helper.GenerateMigrationUnitId(collectionKey);

                            if (_migrationUnitsToProcess.ContainsKey(id) || _monitorAllCollections)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                if (ExecutionCancelled) break;

                               
                                var result = await PreProcessChange(change, counter);
                                if (!result.success)
                                    break;
                                counter = result.counter;
                                changeCount = counter;


                            }
                            else
                            {
                                ignoreCount++;
                                ShowInMonitor(change, collectionKey, GetChangeTime(change), changeCount + ignoreCount, true);
                            }

                            if (ExecutionCancelled)
                                break;

                        }
                        
                        try
                        {
                            if (counter > _config.ChangeStreamMaxDocsInBatch)
                            {
                                await BulkProcessAllChangesAsync(_accumulatedChangesPerCollection);
                                SetJobResumeToken(latestResumeToken, latestTimestamp);
                                changeCount = 0;
                            }
                        }
                        catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                        {
                            _log.WriteLine($"{_syncBackPrefix}CRITICAL error during BulkProcessAllChangesAsync. Details: {ex}", LogType.Error);
                            StopJob($"CRITICAL error in BulkProcessAllChangesAsync. Details: {ex}");
                            throw; // Re-throw to stop processing
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when batch timeout occurs
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error watching server-level change stream: {ex}", LogType.Error);
                throw;
            }
            finally
            {
                try
                {
                    if (_accumulatedChangesPerCollection != null)
                    {
                        try
                        {
                            await BulkProcessAllChangesAsync(_accumulatedChangesPerCollection);                            
                        }
                        catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                        {
                            _log.WriteLine($"{_syncBackPrefix}CRITICAL error during final BulkProcessAllChangesAsync. Details: {ex}", LogType.Error);
                            StopJob($"CRITICAL error in final BulkProcessAllChangesAsync. Details: {ex}");
                            throw; // Re-throw to stop processing
                        }
                    }
                    SetJobResumeToken(latestResumeToken, latestTimestamp);

                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {collectionKey}. Details: {ex}", LogType.Error);
                }
            }
        }

        private void SetJobResumeToken(string latestResumeToken, DateTime latestTimestamp)
        {
            MigrationJobContext.CurrentlyActiveJob.SyncBackOriginalResumeToken = latestResumeToken;
            MigrationJobContext.CurrentlyActiveJob.SyncBackCursorUtcTimestamp = latestTimestamp;
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
        }

        private async Task<(bool success, long counter)> PreProcessChange(ChangeStreamDocument<BsonDocument> change, long counter)
        {
            try
            {
                // Extract namespace information
                if (change.CollectionNamespace == null)
                {
                    return (true, counter); // Skip if no namespace info
                }

                var databaseName = change.CollectionNamespace.DatabaseNamespace.DatabaseName;
                var collectionName = change.CollectionNamespace.CollectionName;
                var collectionKey = $"{databaseName}.{collectionName}";

                MigrationUnit migrationUnit=null;

                //if monitoring all collections, use a dummy key to report all changes, no filtering of collections and data
                if (_monitorAllCollections)
                {
                    //add to _allCollectionsAsMigrationUnit dynamically
                    _uniqueCollectionKeys.Add(collectionKey);
                }
                else
                {
                    // Check if this change belongs to one of our collections with SourceStatus.OK
                    var id = Helper.GenerateMigrationUnitId(collectionKey);
                    if (!_migrationUnitsToProcess.ContainsKey(id))
                    {
                        return (true, counter); // Skip changes for collections not in our job
                    }

                    migrationUnit = MigrationJobContext.GetMigrationUnit(Helper.GenerateMigrationUnitId(databaseName, collectionName));
                    migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    // Check user filter condition               
                    var userFilterDoc = MongoHelper.GetFilterDoc(migrationUnit.UserFilter);

                    if (change.OperationType != ChangeStreamOperationType.Delete)
                    {
                        if (userFilterDoc.Elements.Count() > 0
                            && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                            return (true, counter); // Skip if doesn't match user filter
                    }
                }
                counter++;

                DateTime timeStamp = DateTime.MinValue;
                if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
                }
                else if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.WallTime != null)
                {
                    timeStamp = change.WallTime.Value;
                }

                _ = Task.Run(() => ShowInMonitor(change, collectionKey, timeStamp, counter));


                // Get target collection
                IMongoCollection<BsonDocument> targetCollection = GetTargetCollection(databaseName, collectionName);

                
                if (_monitorAllCollections)
                {
                    migrationUnit = MigrationJobContext.GetMigrationUnit(Helper.GenerateMigrationUnitId("DUMMY", "DUMMY"));
                    migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    
                }

                var keyForUI= $"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}";
                InitializeAccumulatedChangesTracker(keyForUI);
                PreProcessChangeEvent(change, targetCollection, collectionKey, _accumulatedChangesPerCollection[keyForUI], MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun, migrationUnit);

                migrationUnit.CSUpdatesInLastBatch++;

                if (ExecutionCancelled)
                    return (false, counter);

                return (true, counter);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing server-level change: {ex}", LogType.Error);
                return (false, counter);
            }
        }

        private async Task BulkProcessAllChangesAsync(Dictionary<string, AccumulatedChangesTracker> accumulatedChangesInColl)
        {
            MigrationUnit migrationUnit = null;
            foreach (var kvp in accumulatedChangesInColl)
            {
                var collectionKey = kvp.Key;
                var docs = kvp.Value;
                //MigrationUnit migrationUnit;
                bool found = false;
                int totalChanges = docs.DocsToBeInserted.Count + docs.DocsToBeUpdated.Count + docs.DocsToBeDeleted.Count;

                if (totalChanges > 0)
                {

                    var muId = Helper.GenerateMigrationUnitId(collectionKey);
                    var mu = MigrationJobContext.GetMigrationUnit(muId);
                    mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    if (mu != null)
                    {
                        migrationUnit = mu;
                        var targetCollection = GetTargetCollection(migrationUnit.DatabaseName, migrationUnit.CollectionName);

                        if (_monitorAllCollections)
                        {
                            //since we want the chnages to  be reported to this dummy collection.
                            muId = Helper.GenerateMigrationUnitId("DUMMY.DUMMY");
                            migrationUnit = MigrationJobContext.GetMigrationUnit(muId);
                            migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                        }

                        try
                        {
                            await BulkProcessChangesAsync(
                            migrationUnit,
                            targetCollection,
                            insertEvents: docs.DocsToBeInserted.Values.ToList(),
                            updateEvents: docs.DocsToBeUpdated.Values.ToList(),
                            deleteEvents: docs.DocsToBeDeleted.Values.ToList(),
                            accumulatedChangesInColl: docs);

                            // Only update resume token AFTER successful batch write
                            // This ensures we can recover from the last successful checkpoint on failure
                            if (!string.IsNullOrEmpty(docs.LatestResumeToken))
                            {
                                UpdateResumeToken(docs.LatestResumeToken, docs.LatestOperationType, docs.LatestDocumentKey, collectionKey);
                                if (!_syncBack)
                                    migrationUnit.CursorUtcTimestamp = docs.LatestTimestamp;
                                else
                                    migrationUnit.SyncBackCursorUtcTimestamp = docs.LatestTimestamp;     
                                
                                MigrationJobContext.SaveMigrationUnit(migrationUnit,true);

                                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Checkpoint updated for {collectionKey}: Resume token persisted after successful batch write");
                            }
                        }
                        catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                        {
                            _log.WriteLine($"{_syncBackPrefix}CRITICAL error in BulkProcessAllChangesAsync for {collectionKey}.Details: {ex}", LogType.Error);
                            StopJob($"CRITICAL error processing {collectionKey}. Details: {ex}");
                            throw; // Re-throw to stop processing
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Error processing changes for {collectionKey}. Details: {ex}", LogType.Error);
                            throw; // Re-throw to ensure error is handled upstream
                        }

                    }
                }
            }

            MigrationJobContext.SaveMigrationUnit(migrationUnit,false);
        }

        // Server-level equivalent of AutoReplayFirstChangeInResumeToken
        private bool AutoReplayFirstChangeInResumeToken()
        {
            string documentId = GetResumeDocumentId();
            ChangeStreamOperationType operationType = GetResumeTokenOperation();
            string collectionKey = GetResumeCollectionKey();

            if (string.IsNullOrEmpty(documentId))
            {
                _log.WriteLine($"Auto replay is empty for server-level change stream.");
                return true; // Skip if no document ID is provided
            }

            if (string.IsNullOrEmpty(collectionKey))
            {
                _log.WriteLine($"Auto replay collection key is empty for server-level change stream. Cannot determine target collection.");
                return true; // Skip if no collection key is provided
            }

            _log.WriteLine($"Auto replay for {operationType} operation with document key {documentId} in collection {collectionKey} for server-level change stream.");

            var bsonDoc = BsonDocument.Parse(documentId);
            var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);

            // Validate that the collection key is in our migration units
            var id = Helper.GenerateMigrationUnitId(collectionKey);
            if (!_migrationUnitsToProcess.ContainsKey(id))
            {
                _log.WriteLine($"Collection {collectionKey} for server-level auto replay is not in migration units. Skipping replay.");
                return true;
            }

            var parts = collectionKey.Split('.');
            if (parts.Length != 2)
            {
                _log.WriteLine($"Invalid collection key format for server-level auto replay: {collectionKey}. Expected format: database.collection");
                return true;
            }

            var databaseName = parts[0];
            var collectionName = parts[1];

            IMongoDatabase sourceDb;
            IMongoDatabase targetDb;
            IMongoCollection<BsonDocument> sourceCollection;
            IMongoCollection<BsonDocument> targetCollection;

            if (!_syncBack)
            {
                sourceDb = _sourceClient.GetDatabase(databaseName);
                sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    targetDb = _targetClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                }
                else
                {
                    targetCollection = sourceCollection; // Use source as placeholder for simulated runs
                }
            }
            else
            {
                // For sync back, target is source and vice versa
                targetDb = _sourceClient.GetDatabase(databaseName);
                targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                sourceDb = _targetClient.GetDatabase(databaseName);
                sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
            }

            var result = sourceCollection.Find(filter).FirstOrDefault();

            try
            {
                var migrationUnit = MigrationJobContext.GetMigrationUnit(Helper.GenerateMigrationUnitId(databaseName, collectionName));
                migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                IncrementEventCounter(migrationUnit, operationType);

                switch (operationType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with document key {documentId} in {collectionKey}. Skipping insert.");
                            return true;
                        }
                        targetCollection.InsertOne(result);
                        IncrementDocCounter(migrationUnit, operationType);
                        return true;

                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {operationType} operation for {collectionKey} with document key {documentId}. No document found on source, deleting it from target.");
                            try
                            {
                                targetCollection.DeleteOne(filter);
                                IncrementDocCounter(migrationUnit, ChangeStreamOperationType.Delete);
                            }
                            catch { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(migrationUnit, operationType);
                            return true;
                        }

                    case ChangeStreamOperationType.Delete:
                        targetCollection.DeleteOne(filter);
                        IncrementDocCounter(migrationUnit, operationType);
                        return true;

                    default:
                        _log.WriteLine($"Unhandled operation type: {operationType}", LogType.Error);
                        return false;
                }
            }
            catch (MongoException mex) when (operationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {operationType} in server-level auto replay with document key {documentId}. Details: {ex}", LogType.Error);
                return false;
            }
        }

        private void PreProcessChangeEvent(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, AccumulatedChangesTracker accumulatedChangesInColl, bool isWriteSimulated, MigrationUnit mu)
        {
            BsonValue idValue = BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace}. Change stream event is missing _id in DocumentKey.", LogType.Error);
                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate inserts even in simulation mode so counters get updated
                        if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                            accumulatedChangesInColl.AddInsert(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        IncrementEventCounter(mu, change.OperationType);
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            // Skip actual delete operation in simulation mode
                            if (!isWriteSimulated)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} with _id {idValue}. No document found on source, deleting it from target.");
                                var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                                try
                                {
                                    targetCollection.DeleteOne(deleteTTLFilter);
                                    IncrementEventCounter(mu, ChangeStreamOperationType.Delete);
                                }
                                catch
                                { }
                            }
                        }
                        else
                        {
                            // Accumulate updates even in simulation mode so counters get updated
                            accumulatedChangesInColl.AddUpdate(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate deletes even in simulation mode so counters get updated
                        accumulatedChangesInColl.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}", LogType.Error);
                        break;
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details: {ex}", LogType.Error);
            }
        }

        #region Server-Level Resume Token Management

        private DateTime GetCursorUtcTimestamp()
        {
            if (!_syncBack)
                return MigrationJobContext.CurrentlyActiveJob.SyncBackCursorUtcTimestamp;
            else
                return MigrationJobContext.CurrentlyActiveJob.CursorUtcTimestamp;
        }

        private DateTime GetChangeStreamStartedOn()
        {
            if (!_syncBack)
            {
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn.HasValue)
                    return MigrationJobContext.CurrentlyActiveJob.ChangeStreamStartedOn.Value;
                else
                    return DateTime.MinValue;
            }
            else
            {
                if (MigrationJobContext.CurrentlyActiveJob.SyncBackChangeStreamStartedOn.HasValue)
                    return MigrationJobContext.CurrentlyActiveJob.SyncBackChangeStreamStartedOn.Value;
                else
                    return DateTime.MinValue;

            }
        }

        private string GetResumeToken()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeToken ?? string.Empty;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeToken ?? string.Empty;
            }
        }

        private bool GetInitialDocumentReplayedStatus()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.InitialDocumenReplayed;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackInitialDocumenReplayed;
            }
        }

        private void SetInitialDocumentReplayedStatus(bool value)
        {
            if (!_syncBack)
            {
                MigrationJobContext.CurrentlyActiveJob.InitialDocumenReplayed = value;
            }
            else
            {
                MigrationJobContext.CurrentlyActiveJob.SyncBackInitialDocumenReplayed = value;
            }
        }

        private ChangeStreamOperationType GetResumeTokenOperation()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeTokenOperation;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeTokenOperation;
            }
        }

        private string GetResumeDocumentId()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeDocumentId ?? string.Empty;
            }
            else
            {
                return MigrationJobContext. CurrentlyActiveJob.SyncBackResumeDocumentId ?? string.Empty;
            }
        }

        private string GetResumeCollectionKey()
        {
            if (!_syncBack)
            {
                return MigrationJobContext.CurrentlyActiveJob.ResumeCollectionKey ?? string.Empty;
            }
            else
            {
                return MigrationJobContext.CurrentlyActiveJob.SyncBackResumeCollectionKey ?? string.Empty;
            }
        }

        private void UpdateResumeToken(string resumeToken, ChangeStreamOperationType operationType, string documentId, string collectionKey)
        {
            if (!_syncBack)
            {
                MigrationJobContext.CurrentlyActiveJob.ResumeToken = resumeToken;
                if (string.IsNullOrEmpty(MigrationJobContext.CurrentlyActiveJob.OriginalResumeToken))
                {
                    MigrationJobContext.CurrentlyActiveJob.OriginalResumeToken = resumeToken;
                }
                MigrationJobContext.CurrentlyActiveJob.ResumeTokenOperation = operationType;
                MigrationJobContext.CurrentlyActiveJob.ResumeDocumentId = documentId;
                MigrationJobContext.CurrentlyActiveJob.ResumeCollectionKey = collectionKey;
            }
            else
            {
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeToken = resumeToken;
                if (string.IsNullOrEmpty(MigrationJobContext.CurrentlyActiveJob.SyncBackOriginalResumeToken))
                {
                    MigrationJobContext.CurrentlyActiveJob.SyncBackOriginalResumeToken = resumeToken;
                }
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeTokenOperation = operationType;
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeDocumentId = documentId;
                MigrationJobContext.CurrentlyActiveJob.SyncBackResumeCollectionKey = collectionKey;
            }
        }

        #endregion
    }
}