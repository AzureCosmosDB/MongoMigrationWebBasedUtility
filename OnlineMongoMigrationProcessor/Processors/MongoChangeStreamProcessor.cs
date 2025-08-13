using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using static OnlineMongoMigrationProcessor.MongoHelper;


#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class MongoChangeStreamProcessor
    {
        private int _concurrentProcessors;
        private int _processorRunMaxDurationInSec; // Max Duration to watch the change stream in seconds
        private int _processorRunMinDurationInSec; // Max Duration to watch the change stream in seconds

        private MongoClient _sourceClient;
        private MongoClient _targetClient;
        private JobList? _jobList;
        private MigrationJob? _job;
        private MigrationSettings? _config;
        private bool _syncBack = false;
        private string _syncBackPrefix = string.Empty;
        private bool _isCSProcessing = false;
        private Log _log;

        private ConcurrentDictionary<string, string> _resumeTokenCache = new ConcurrentDictionary<string, string>();
        private ConcurrentDictionary<string, MigrationUnit> _migrationUnitsToProcess = new ConcurrentDictionary<string, MigrationUnit>();

        private static readonly object _processingLock = new object();

        public bool ExecutionCancelled { get; set; }        

        public MongoChangeStreamProcessor(Log log,MongoClient sourceClient, MongoClient targetClient, JobList jobList,MigrationJob job, MigrationSettings config, bool syncBack = false)
        {
            _log = log;
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _jobList = jobList;
            _job = job;
            _config = config;
            _syncBack = syncBack;
            if (_syncBack)
                _syncBackPrefix = "Sync Back: ";

            _concurrentProcessors = _config?.ChangeStreamMaxCollsInBatch ?? 5;
            _processorRunMaxDurationInSec = _config?.ChangeStreamBatchDuration ?? 120;
            _processorRunMinDurationInSec= _config?.ChangeStreamBatchDurationMin ?? 30;
        }

        public bool AddCollectionsToProcess(MigrationUnit mu, CancellationTokenSource cts)
        {
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            if (mu.SourceStatus != CollectionStatus.OK || mu.DumpComplete != true || mu.RestoreComplete != true)
            {
                _log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams for processing.", LogType.Error);
                return false;
            }            
            if (!_migrationUnitsToProcess.ContainsKey(key))
            {
                _migrationUnitsToProcess.TryAdd(key, mu);
                _log.WriteLine($"{_syncBackPrefix}Change stream for {key} added to queue.");
                
                _ = RunCSPostProcessingAsync(cts); // fire-and-forget by design
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task RunCSPostProcessingAsync(CancellationTokenSource cts)
        {
            lock (_processingLock)
            {
                if (_isCSProcessing)
                {
                    return; //already processing    
                }
                _isCSProcessing = true;
            }

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
               .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            try
            {               
                cts = new CancellationTokenSource();
                var token = cts.Token;

                int index = 0;

                _migrationUnitsToProcess.Clear();
                foreach (var migrationUnit in _job.MigrationUnits)
                {
                    if (migrationUnit.SourceStatus == CollectionStatus.OK && migrationUnit.DumpComplete == true && migrationUnit.RestoreComplete == true)
                    {
                        _migrationUnitsToProcess[$"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}"] = migrationUnit;

                    }                  
                }

                _job.CSPostProcessingStarted = true;
                _jobList?.Save(); // persist state

                if (_migrationUnitsToProcess.Count == 0)
                {
                    _log.WriteLine($"{_syncBackPrefix}No change streams to process.");
                    
                    _isCSProcessing = false;
                    return;
                }
                                

                // Get the latest sorted keys
                var sortedKeys = _migrationUnitsToProcess
                    .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                    .Select(kvp => kvp.Key)
                    .ToList();

                _log.WriteLine($"{_syncBackPrefix}Starting change stream processing for {sortedKeys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, sortedKeys.Count)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds.");

                

                long loops=0;
                bool oplogSucess=true;

                while (!token.IsCancellationRequested && !ExecutionCancelled)
                {
                    //_job.CurrentlyActive = true;
                    var totalKeys = sortedKeys.Count;

                    while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                    {
                        var tasks = new List<Task>();
                        var collectionProcessed = new List<string>();

                        // Determine the batch
                        var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                        var batchUnits = batchKeys
                            .Select(k => _migrationUnitsToProcess.TryGetValue(k, out var unit) ? unit : null)
                            .Where(u => u != null)
                            .ToList();


                        //total of batchUnits.All(u => u.CSUpdatesInLastBatch)
                        long totalUpdatesInBatch = batchUnits.Sum(u => u.CSNormalizedUpdatesInLastBatch);

                        //total of  _migrationUnitsToProcess
                        long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch);
                                                
                        float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;

                        int seconds = GetBatchDurationInSeconds(timeFactor);
                        foreach (var key in batchKeys)
                        {
                            if (_migrationUnitsToProcess.TryGetValue(key, out var unit))
                            {
                                collectionProcessed.Add(key);
                                unit.CSLastBatchDurationSeconds = seconds; // Store the factor for each unit
                                tasks.Add(Task.Run(() => ProcessCollectionChangeStream(unit, true, seconds), token));
                            }
                        }                                                

                        _log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds");

                        await Task.WhenAll(tasks);

                        index += _concurrentProcessors;
                        
                        // Pause briefly before next batch
                        Thread.Sleep(100);
                    }

                    index = 0;
                    // Sort the dictionary after all processing is complete
                    sortedKeys = _migrationUnitsToProcess
                        .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                        .Select(kvp => kvp.Key)
                        .ToList();

                    loops++;
                    // every 4 loops, check for oplog count, doesn't work on vcore
                    if (loops%4==0 && oplogSucess && !isVCore && !_syncBack)
                    {
                        foreach (var unit in _migrationUnitsToProcess)
                        {
                            if (unit.Value.CursorUtcTimestamp > DateTime.MinValue)
                            {
                                // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
                                long secondsSinceEpoch = new DateTimeOffset(unit.Value.CursorUtcTimestamp.ToLocalTime()).ToUnixTimeSeconds();

                                _ = Task.Run(() =>
                                {
                                    oplogSucess = MongoHelper.GetPendingOplogCountAsync(_log, _sourceClient, secondsSinceEpoch, unit.Key);
                                });
                                if (!oplogSucess)
                                    break;
                            }
                        }
                    }
                }

                _log.WriteLine($"{_syncBackPrefix}Change stream processing completed.");                
                //_job.CurrentlyActive = false;//causes failure do not undo
                _jobList?.Save();

            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"{_syncBackPrefix}Change stream processing was cancelled.");

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error during change stream processing: {ex.ToString()}", LogType.Error);

            }
            finally
            {
                lock (_processingLock)
                {
                    _isCSProcessing = false;
                }

            }
        }

        private void ResetCounters(MigrationUnit mu)
        {
            if (!_syncBack)
            {
                mu.CSDocsUpdated = 0;
                mu.CSDocsInserted = 0;
                mu.CSDocsDeleted = 0;
                mu.CSDuplicateDocsSkipped = 0;

                mu.CSDInsertEvents = 0;
                mu.CSDeleteEvents = 0;
                mu.CSUpdatedEvents = 0;
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

        private void ProcessCollectionChangeStream(MigrationUnit mu, bool IsCSProcessingRun=false, int seconds = 0)
        {
            try
            {                    

                string databaseName = mu.DatabaseName;
                string collectionName = mu.CollectionName;

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument>? sourceCollection=null;
                IMongoCollection<BsonDocument>? targetCollection=null;

                if (!_syncBack)
                {
                    sourceDb = _sourceClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                    if (!_job.IsSimulatedRun)
                    {
                        targetDb = _targetClient.GetDatabase(databaseName);
                        targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                    }
                }
                else
                {
                    // For sync back, we use the source collection as the target and vice versa
                    targetDb = _sourceClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                    sourceDb = _targetClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
                }                

                try
                {
                    // Default options; will be overridden based on resume strategy
                    ChangeStreamOptions options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };

                    DateTime startedOn;
                    DateTime timeStamp;
                    string resumeToken = string.Empty;
                    string? version = string.Empty;
                    if (!_syncBack)
                    {
                        timeStamp = mu.CursorUtcTimestamp;
                        resumeToken= mu.ResumeToken ?? string.Empty;
                        version = _job.SourceServerVersion;
                        if (mu.ChangeStreamStartedOn.HasValue)
                        {
                            startedOn = mu.ChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }
                    else
                    {
                        timeStamp= mu.SyncBackCursorUtcTimestamp;
                        resumeToken = mu.SyncBackResumeToken ?? string.Empty;
                        version = "8"; //hard code for target
                        if (mu.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            startedOn = mu.SyncBackChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }

                    if (!mu.InitialDocumenReplayed && !_job.IsSimulatedRun)
                    {
                        // Guard targetCollection for non-simulated runs
                        if (targetCollection == null)
                        {
                            var targetDb2 = _targetClient.GetDatabase(databaseName);
                            targetCollection = targetDb2.GetCollection<BsonDocument>(collectionName);
                        }
                        if (AutoReplayFirstChangeInResumeToken(mu.ResumeDocumentId, mu.ResumeTokenOperation, sourceCollection!, targetCollection!, mu))
                        {
                            // If the first change was replayed, we can proceed
                            mu.InitialDocumenReplayed = true;
                            _jobList?.Save();

                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.", LogType.Error);
                            throw new Exception($"Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.");
                        }
                    }

                    if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && resumeToken == null && !(_job.JobType==JobType.RUOptimizedCopy && !_job.ProcessingSyncBack)) //skip CursorUtcTimestamp if its reset 
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                    }
                    else if (!string.IsNullOrEmpty(resumeToken) && !mu.ResetChangeStream) //skip resume token if its reset
                    {
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken) };

                    }
                    else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3"))
                    {
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
                    }
                    else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        if(mu.ResetChangeStream)
                        {
                            ResetCounters(mu);
                        }

                        mu.ResetChangeStream = false; //reset the start time after setting resume token

                    }
                   

                    if(seconds==0)
                        seconds = GetBatchDurationInSeconds(.5f); //get seconds from config or use default

                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.AddVerboseMessage($"{_syncBackPrefix}Monitoring change stream with new batch for {sourceCollection!.CollectionNamespace}. Batch Duration {seconds} seconds");
                    // In simulated runs, use source collection as a placeholder to avoid null target warnings
                    if (_job.IsSimulatedRun && targetCollection == null)
                    {
                        targetCollection = sourceCollection;
                    }
                    
                    WatchCollection(mu, options, sourceCollection!, targetCollection!, cancellationToken);

                                           
                }
                catch (OperationCanceledException)
                {
                    // A new batch will be started. do nothing
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    // Handle other potential exceptions
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex.ToString()}", LogType.Error);
                    _log.AddVerboseMessage($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex.ToString()}");

                    //ExecutionCancelled= true; // do not cancel as some collections may not be having any chnages annd  others may be processing.
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.", LogType.Error);
                    _log.AddVerboseMessage($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.");

                    //ExecutionCancelled = true; // do not cancel as some collections may not be having any chnages annd  others may be processing.              
                }
                finally
                {
                    
                }

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex.ToString()}", LogType.Error);
                
            }
        }

        private int GetBatchDurationInSeconds(float timeFactor=1)
        {
            // Create a CancellationTokenSource with a timeout (e.g., 120 seconds)
            int seconds = (int)(_processorRunMaxDurationInSec * timeFactor);
            if (seconds < _processorRunMinDurationInSec)
                seconds = _processorRunMinDurationInSec; // Ensure at least 15 second
            return seconds;
        }

        private void WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection,IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken)
        {
            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            long counter = 0;
            BsonDocument userFilterDoc = new BsonDocument();

            if (!string.IsNullOrWhiteSpace(mu.UserFilter))
            {
                userFilterDoc = BsonDocument.Parse(mu.UserFilter);
                userFilterDoc??= new BsonDocument(); // Ensure it's not null
            }

            ChangeStreamDocuments changeStreamDocuments = new ChangeStreamDocuments();

            try
            {
                List<BsonDocument> pipeline;
                if (_job.JobType == JobType.RUOptimizedCopy)
                {
                    pipeline = new List<BsonDocument>()
                    {
                    new BsonDocument("$match", new BsonDocument("operationType",
                        new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" }))),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "operationType", 1 },
                        { "_id", 1 },
                        { "fullDocument", 1 },
                        { "ns", 1 },
                        { "documentKey", 1 }
                    })
                    };                    
                }
                else
                {
                    pipeline = new List<BsonDocument>();                  

                }

                var pipelineArray = pipeline.ToArray();
                using var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);
                string lastProcessedToken = string.Empty;

                

                if (_job.SourceServerVersion.StartsWith("3"))
                {

                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        lastProcessedToken = string.Empty;
                        _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token1);
                        lastProcessedToken = token1 ?? string.Empty;

                        if (lastProcessedToken == change.ResumeToken.ToJson())
                        {
                            mu.CSUpdatesInLastBatch = 0;
                            mu.CSNormalizedUpdatesInLastBatch = 0;
                            return; // Skip processing if the event has already been processed
                        }                        
                       

                        if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, changeStreamDocuments, ref counter, userFilterDoc))
                            return;                       
                    }
                }
                else
                {
                    while (cursor.MoveNext(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        foreach (var change in cursor.Current)
                        {

                            cancellationToken.ThrowIfCancellationRequested();
                            if (ExecutionCancelled) return;

                            lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token2);
                            lastProcessedToken = token2 ?? string.Empty;
                            
                            if (lastProcessedToken == change.ResumeToken.ToJson() && _job.JobType!=JobType.RUOptimizedCopy)
                            {
                                mu.CSUpdatesInLastBatch = 0;
                                mu.CSNormalizedUpdatesInLastBatch = 0;
                                return; // Skip processing if the event has already been processed
                            }

                            if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, changeStreamDocuments, ref counter, userFilterDoc))
                                return;

                        }

                        if (ExecutionCancelled)
                            return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // A new batch will be started. do nothing
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                try
                {
                    BulkProcessChangesAsync(
                        mu,
                        targetCollection,
                        insertEvents: changeStreamDocuments.DocsToBeInserted,
                        updateEvents: changeStreamDocuments.DocsToBeUpdated,
                        deleteEvents: changeStreamDocuments.DocsToBeDeleted).GetAwaiter().GetResult();

                    mu.CSUpdatesInLastBatch = counter;
                    mu.CSNormalizedUpdatesInLastBatch = (long)(counter / (mu.CSLastBatchDurationSeconds > 0 ? mu.CSLastBatchDurationSeconds : 1));
                    _jobList?.Save();
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {sourceCollection.CollectionNamespace}. Details: {ex.ToString()}", LogType.Error);

                }
            }
        }


        private void IncrementSkippedCounter(MigrationUnit mu, int incrementBy = 1)
        {
            if (!_syncBack)
                mu.CSDuplicateDocsSkipped = mu.CSDuplicateDocsSkipped + incrementBy;
            else
                mu.SyncBackDuplicateDocsSkipped = mu.SyncBackDuplicateDocsSkipped + incrementBy;
        }

        private void IncrementDocCounter(MigrationUnit mu, ChangeStreamOperationType op, int incrementBy=1)
        {
            if(op== ChangeStreamOperationType.Insert)
            {
                if (!_syncBack)
                    mu.CSDocsInserted= mu.CSDocsInserted +incrementBy ;
                else
                    mu.SyncBackDocsInserted = mu.SyncBackDocsInserted + incrementBy;
            }
            else if(op== ChangeStreamOperationType.Update || op == ChangeStreamOperationType.Replace)
            {
                if (!_syncBack)
                    mu.CSDocsUpdated= mu.CSDocsUpdated + incrementBy;
                else
                    mu.SyncBackDocsUpdated = mu.SyncBackDocsUpdated + incrementBy;
            }
            else if(op== ChangeStreamOperationType.Delete)
            {
                if (!_syncBack)
                    mu.CSDocsDeleted= mu.CSDocsDeleted + incrementBy;
                else
                    mu.SyncBackDocsDeleted = mu.SyncBackDocsDeleted + incrementBy;

            }
        }


        private void IncrementEventCounter(MigrationUnit mu, ChangeStreamOperationType op)
        {
            if (op == ChangeStreamOperationType.Insert)
            {
                if (!_syncBack)
                    mu.CSDInsertEvents++;
                else
                    mu.SyncBackInsertEvents++;
            }
            else if (op == ChangeStreamOperationType.Update || op == ChangeStreamOperationType.Replace)
            {
                if (!_syncBack)
                    mu.CSUpdatedEvents++;
                else
                    mu.SyncBackUpdateEvents++;
                
            }
            else if (op == ChangeStreamOperationType.Delete)
            {
                if (!_syncBack)
                    mu.CSDeleteEvents++;
                else
                    mu.SyncBackDeleteEvents++;

            }
        }

        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(BsonDocument? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, MigrationUnit mu)
        {
            if(documentId == null || documentId.IsBsonNull)
            {
                _log.WriteLine($"Auto replay is empty for {sourceCollection.CollectionNamespace}.");
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.WriteLine($"Auto replay for {opType} operation with _id {documentId} in {sourceCollection.CollectionNamespace}.");
            }
            var filter = MongoHelper.BuildFilterFromDocumentKey(documentId);
            //var filter = Builders<BsonDocument>.Filter.Eq("_id", documentId); // Assuming _id is your resume token
            var result = sourceCollection.Find(filter).FirstOrDefault(); // Retrieve the document for the resume token

            try
            {
                switch (opType)
                {
                    case ChangeStreamOperationType.Insert:
                        if(result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with _id {documentId} in {sourceCollection.CollectionNamespace}. Skipping insert.");
                            return true; // Skip if no document found
                        }
                        targetCollection.InsertOne(result);

                        IncrementDocCounter(mu, opType);

                        return true;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {opType} operation for {sourceCollection.CollectionNamespace} with _id {documentId}. No document found on source, deleting it from target.");
                            var deleteTTLFilter = MongoHelper.BuildFilterFromDocumentKey(documentId);
                            //var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                                IncrementDocCounter(mu, ChangeStreamOperationType.Delete);

                            }
                            catch
                            { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(mu, opType);

                            return true;
                        }
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                        targetCollection.DeleteOne(deleteFilter);
                        IncrementDocCounter(mu, opType);

                        return true;
                    default:
                        _log.WriteLine($"Unhandled operation type: {opType}");
                        return false;
                }
            }


            catch (MongoException mex) when (opType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {opType} on {sourceCollection.CollectionNamespace} with _id {documentId}. Details: {ex.ToString()}", LogType.Error);
                return false; // Return false to indicate failure in processing
            }            
        }


        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace ,MigrationUnit mu, ChangeStreamDocuments changeStreamDocuments, ref long counter, BsonDocument userFilterDoc)
        {
            try
            {
                //check if user filter condition is met

                if (change.OperationType != ChangeStreamOperationType.Delete)
                {
                    if (userFilterDoc.Elements.Count() > 0
                        && !MongoHelper.CheckForUserFilterMatch(change, userFilterDoc))
                        return true;
                }

                counter++;

                DateTime timeStamp=DateTime.MinValue;

                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime); // Convert BsonTimestamp to DateTime
                }
                else if (!_job.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for 4.0 and above
                {
                    timeStamp = change.WallTime.Value; // Use WallTime for 4.0 and above
                }

                // Output change details to the console
                if(timeStamp == DateTime.MinValue)
                    _log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                else
                    _log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}");

                ProcessChange(change, targetCollection, collNameSpace, changeStreamDocuments, _job.IsSimulatedRun, mu);

                if (!_syncBack)
                    mu.CursorUtcTimestamp = timeStamp;
                else
                    mu.SyncBackCursorUtcTimestamp = timeStamp; //for reverse sync               

                if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
                { 
                    if (!_syncBack)
                        mu.ResumeToken = change.ResumeToken.ToJson();
                    else
                        mu.SyncBackResumeToken = change.ResumeToken.ToJson();

                    _resumeTokenCache[$"{collNameSpace}"] = change.ResumeToken.ToJson();
                }          
                
                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing cursor. Details: {ex.ToString()}", LogType.Error);
                
                return false;
            }
        }


        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, ChangeStreamDocuments changeStreamDocuments, bool isWriteSimulated, MigrationUnit mu)
        {
            if (isWriteSimulated)
                return;

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

                        if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                            changeStreamDocuments.AddInsert(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:

                        IncrementEventCounter(mu, change.OperationType);
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
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
                        else
                        {
                            changeStreamDocuments.AddUpdate(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:

                        IncrementEventCounter(mu, change.OperationType);
                        changeStreamDocuments.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}");
                        break;
                }

                if (changeStreamDocuments.DocsToBeInserted.Count + changeStreamDocuments.DocsToBeUpdated.Count + changeStreamDocuments.DocsToBeDeleted.Count > _config.ChangeStreamMaxDocsInBatch)
                {
                    _log.AddVerboseMessage($"{_syncBackPrefix}Change stream max batch size exceeded. Flushing changes for {collNameSpace}");


                    // Process the changes in bulk if the batch size exceeds the limit
                    BulkProcessChangesAsync(
                        mu,
                        targetCollection,
                        insertEvents: changeStreamDocuments.DocsToBeInserted,
                        updateEvents: changeStreamDocuments.DocsToBeUpdated,
                        deleteEvents: changeStreamDocuments.DocsToBeDeleted).GetAwaiter().GetResult();
                    
                    // Clear the lists after processing
                    changeStreamDocuments.DocsToBeInserted.Clear();
                    changeStreamDocuments.DocsToBeUpdated.Clear();
                    changeStreamDocuments.DocsToBeDeleted.Clear();
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details: {ex.ToString()}", LogType.Error);
            }
        }


        private async Task BulkProcessChangesAsync(
          MigrationUnit mu,
          IMongoCollection<BsonDocument> collection,
          List<ChangeStreamDocument<BsonDocument>> insertEvents,
          List<ChangeStreamDocument<BsonDocument>> updateEvents,
          List<ChangeStreamDocument<BsonDocument>> deleteEvents,
          int batchSize = 50)
        {
            if (_job.IsSimulatedRun)
            {
                _log.WriteLine($"{_syncBackPrefix}Skipping bulk processing for {collection.CollectionNamespace.FullName} in simulated run.");
                return;
            }

            CounterDelegate<MigrationUnit> counterDelegate = (migrationUnit, counterType, operationType, count) =>
            {
                switch (counterType)
                {
                    case CounterType.Processed:
                        if (operationType.HasValue)
                        {
                            IncrementDocCounter(migrationUnit, operationType.Value, count);
                        }
                        break;
                    case CounterType.Skipped:
                        IncrementSkippedCounter(migrationUnit, count);
                        break;
                }
            };


            try
            {
                await MongoHelper.ProcessInsertsAsync<MigrationUnit> (mu, collection, insertEvents, counterDelegate, _log, _syncBackPrefix, batchSize);

                await MongoHelper.ProcessUpdatesAsync<MigrationUnit> (mu, collection, updateEvents, counterDelegate, _log, _syncBackPrefix, batchSize);

                await MongoHelper.ProcessDeletesAsync<MigrationUnit> (mu, collection, deleteEvents, counterDelegate, _log, _syncBackPrefix, batchSize);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operations for {collection.CollectionNamespace.FullName}. Details: {ex}", LogType.Error);
            }

        }       

    }
}
