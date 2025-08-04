﻿using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using static System.Reflection.Metadata.BlobBuilder;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    internal class MongoChangeStreamProcessor
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

        public bool AddCollectionsToProcess(MigrationUnit item, CancellationTokenSource cts)
        {
            string key = $"{item.DatabaseName}.{item.CollectionName}";
            if (item.SourceStatus != CollectionStatus.OK || item.DumpComplete != true || item.RestoreComplete != true)
            {
                _log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams to process.", LogType.Error);
                return false;
            }            
            if (!_migrationUnitsToProcess.ContainsKey(key))
            {
                _migrationUnitsToProcess.TryAdd(key, item);
                _log.WriteLine($"{_syncBackPrefix}Change stream for {key} added to queue.");
                
                var result=RunCSPostProcessingAsync(cts);
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

                                Task.Run(() =>
                                {
                                    oplogSucess = MongoHelper.GetPendingOplogCountAsync(_log, _sourceClient, secondsSinceEpoch, unit.Key);
                                });
                                if (!oplogSucess)
                                    break;
                            }
                        }
                    }
                    // _log.WriteLine($"sorted keys: {string.Join(", ", sortedKeys.ToArray())}");
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

        private void ProcessCollectionChangeStream(MigrationUnit item, bool IsCSProcessingRun=false, int seconds = 0)
        {
            try
            {                    

                string databaseName = item.DatabaseName;
                string collectionName = item.CollectionName;

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument> sourceCollection=null;
                IMongoCollection<BsonDocument> targetCollection=null;

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
                    ChangeStreamOptions options=null;

                    DateTime startedOn;
                    DateTime timeStamp;
                    string resumeToken = string.Empty;
                    string? version = string.Empty;
                    if (!_syncBack)
                    {
                        timeStamp = item.CursorUtcTimestamp;
                        resumeToken= item.ResumeToken ?? string.Empty;
                        version = _job.SourceServerVersion;
                        if (item.ChangeStreamStartedOn.HasValue)
                        {
                            startedOn = item.ChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }
                    else
                    {
                        timeStamp= item.SyncBackCursorUtcTimestamp;
                        resumeToken = item.SyncBackResumeToken ?? string.Empty;
                        version = "8"; //hard code for target
                        if (item.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            startedOn = item.SyncBackChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }

                    if (!item.InitialDocumenReplayed && !_job.IsSimulatedRun)
                    {
                        if (AutoReplayFirstChangeInResumeToken(item.ResumeDocumentId, item.ResumeTokenOperation, sourceCollection, targetCollection,item))
                        {
                            // If the first change was replayed, we can proceed
                            item.InitialDocumenReplayed = true;
                            _jobList?.Save();

                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {sourceCollection.CollectionNamespace}. Skipping change stream processing for this collection.", LogType.Error);
                            throw new Exception($"Failed to replay the first change for {sourceCollection.CollectionNamespace}. Skipping change stream processing for this collection.");
                        }
                    }

                    if (timeStamp > DateTime.MinValue && !item.ResetChangeStream && resumeToken == null) //skip CursorUtcTimestamp if its reset 
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                    }
                    else if (!string.IsNullOrEmpty(resumeToken) && !item.ResetChangeStream) //skip resume token if its reset
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
                        if(item.ResetChangeStream)
                        {
                            if (!_syncBack)
                            {
                                item.CSDocsUpdated = 0;
                                item.CSDocsInserted = 0;
                                item.CSDocsDeleted = 0;
                                item.CSDuplicateDocsSkipped = 0;

                                item.CSDInsertEvents = 0;
                                item.CSDeleteEvents = 0;
                                item.CSUpdatedEvents = 0;
                            }
                            else
                            {
                                item.SyncBackDocsUpdated = 0;
                                item.SyncBackDocsInserted = 0;
                                item.SyncBackDocsDeleted = 0;
                                item.SyncBackDuplicateDocsSkipped = 0;

                                item.SyncBackInsertEvents = 0;
                                item.SyncBackDeleteEvents = 0;
                                item.SyncBackUpdateEvents = 0;
                            }
                        }

                        item.ResetChangeStream = false; //reset the start time after setting resume token

                    }
                   

                    if(seconds==0)
                        seconds = GetBatchDurationInSeconds(.5f); //get seconds from config or use default

                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.AddVerboseMessage($"{_syncBackPrefix}Monitoring change stream with new batch for {sourceCollection.CollectionNamespace}. Batch Duration {seconds} seconds");
                    
                    WatchCollection(item, options, sourceCollection, targetCollection, cancellationToken);

                                           
                }
                catch (OperationCanceledException)
                {
                    // A new batch will be started. do nothing
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    // Handle other potential exceptions
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                    _log.AddVerboseMessage($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details : {ex.ToString()}");

                    //ExecutionCancelled= true; // do not cancel as some collections may not be having any chnages annd  others may be processing.
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token expired or cursor invalid for {sourceCollection.CollectionNamespace}.", LogType.Error);
                    _log.AddVerboseMessage($"{_syncBackPrefix}Resume token expired or cursor invalid for {sourceCollection.CollectionNamespace}.");

                    //ExecutionCancelled = true; // do not cancel as some collections may not be having any chnages annd  others may be processing.              
                }
                finally
                {
                    
                }

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix} Error processing change stream for {item.DatabaseName}.{item.CollectionName}. Details : {ex.ToString()}", LogType.Error);
                
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

        private void WatchCollection(MigrationUnit item, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection,IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken)
        {
            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            long counter = 0;

            ChangeStreamDocuments changeStreamDocuments = new ChangeStreamDocuments();

            try
            {
                using var cursor = sourceCollection.Watch(options, cancellationToken);

                string lastProcessedToken = string.Empty;
                if (_job.SourceServerVersion.StartsWith("3"))
                {

                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        lastProcessedToken = string.Empty;
                        _resumeTokenCache.TryGetValue($"{sourceCollection.CollectionNamespace}", out lastProcessedToken);
                        if (lastProcessedToken == change.ResumeToken.ToJson())
                        {
                            item.CSUpdatesInLastBatch = 0;
                            item.CSNormalizedUpdatesInLastBatch = 0;
                            return; // Skip processing if the event has already been processed
                        }

                        if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(),item, changeStreamDocuments, ref counter))
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
                            _resumeTokenCache.TryGetValue($"{sourceCollection.CollectionNamespace}", out lastProcessedToken);
                            //Console.WriteLine(counter.ToString() + " : " + lastProcessedToken + " --?  " + change.ResumeToken.ToJson());
                            if (lastProcessedToken == change.ResumeToken.ToJson())
                            {
                                item.CSUpdatesInLastBatch = 0;
                                item.CSNormalizedUpdatesInLastBatch = 0;
                                return; // Skip processing if the event has already been processed
                            }

                            if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), item, changeStreamDocuments, ref counter))
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
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                try
                {
                    BulkProcessChangesAsync(
                        item,
                        targetCollection,
                        insertEvents: changeStreamDocuments.DocsToBeInserted,
                        updateEvents: changeStreamDocuments.DocsToBeUpdated,
                        deleteEvents: changeStreamDocuments.DocsToBeDeleted).GetAwaiter().GetResult();

                    item.CSUpdatesInLastBatch = counter;
                    item.CSNormalizedUpdatesInLastBatch = (long)(counter / (item.CSLastBatchDurationSeconds > 0 ? item.CSLastBatchDurationSeconds : 1));
                    _jobList?.Save();
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {sourceCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);

                }
            }
        }
        

        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(BsonValue? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, MigrationUnit item)
        {


            if(documentId == null || documentId.IsBsonNull)
            {
                _log.WriteLine($"Auto replay is empty {sourceCollection.CollectionNamespace}.");
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.WriteLine($"Auto replay for {opType} operation with _id {documentId} in {sourceCollection.CollectionNamespace}. ");
            }
            
            var filter = Builders<BsonDocument>.Filter.Eq("_id", documentId); // Assuming _id is your resume token
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
                        if (!_syncBack)
                        {
                            item.CSDocsInserted++;
                        }
                        else
                        {
                            item.SyncBackDocsInserted++;
                        }
                        return true;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {opType} operation for {sourceCollection.CollectionNamespace} having _id {documentId}. No document found on source, deleting it from target.");
                            var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                                if (!_syncBack)
                                {
                                    item.CSDocsDeleted++;
                                }
                                else
                                {
                                    item.SyncBackDocsDeleted++;
                                }
                            }
                            catch
                            { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            if (!_syncBack)
                            {
                                item.CSDocsUpdated++;
                            }
                            else
                            {
                                item.SyncBackDocsUpdated++;
                            }
                            return true;
                        }
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                        targetCollection.DeleteOne(deleteFilter);
                        if (!_syncBack)
                        {
                            item.CSDocsDeleted++;
                        }
                        else
                        {
                            item.SyncBackDocsDeleted++;
                        }
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
                _log.WriteLine($"Error processing operation {opType} on {sourceCollection.CollectionNamespace} with _id {documentId}. Details : {ex.ToString()}", LogType.Error);
                return false; // Return false to indicate failure in processing
            }


            
        }


        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace ,MigrationUnit item, ChangeStreamDocuments changeStreamDocuments, ref long counter)
        {

            try
            {
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
                _log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {timeStamp}.  Sequence in Batch # {counter}");
                ProcessChange(change, targetCollection, collNameSpace, changeStreamDocuments, _job.IsSimulatedRun, item);

                if (!_syncBack)
                    item.CursorUtcTimestamp = timeStamp;
                else
                    item.SyncBackCursorUtcTimestamp = timeStamp; //for reverse sync
               

                if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
                { 
                    if (!_syncBack)
                        item.ResumeToken = change.ResumeToken.ToJson();
                    else
                        item.SyncBackResumeToken = change.ResumeToken.ToJson();

                    _resumeTokenCache[$"{collNameSpace}"] = change.ResumeToken.ToJson();
                }          
                
                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing cursor. Details : {ex.ToString()}", LogType.Error);
                
                return false;
            }
        }


        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, ChangeStreamDocuments changeStreamDocuments, bool isWriteSimulated, MigrationUnit item)
        {
            if (isWriteSimulated)
                return;


            BsonValue idValue = BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace}. Change stream event missing _id in DocumentKey.", LogType.Error);

                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (_syncBack)
                            item.SyncBackInsertEvents++;
                        else
                            item.CSDInsertEvents++;
                        ;
                        if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                            changeStreamDocuments.AddInsert(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (_syncBack)
                            item.SyncBackUpdateEvents++;
                        else
                            item.CSUpdatedEvents++;
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} having _id {idValue}. No document found on source, deleting it from target.");
                            var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                                if (!_syncBack)
                                    item.CSDocsDeleted++;
                                else
                                    item.SyncBackDocsDeleted++;
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
                        if (_syncBack)
                            item.SyncBackDeleteEvents++;
                        else
                            item.CSDeleteEvents++;
                        changeStreamDocuments.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}");
                        break;
                }

                if (changeStreamDocuments.DocsToBeInserted.Count + changeStreamDocuments.DocsToBeUpdated.Count + changeStreamDocuments.DocsToBeDeleted.Count > _config.ChangeStreamMaxDocsInBatch)
                {
                    _log.AddVerboseMessage($"{_syncBackPrefix} Change Stream MaxBatchSize exceeded. Flushing changes for {collNameSpace}");



                    // Process the changes in bulk if the batch size exceeds the limit
                    BulkProcessChangesAsync(
                        item,
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
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details : {ex.ToString()}", LogType.Error);

            }
        }


        private async Task BulkProcessChangesAsync(
          MigrationUnit item,
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

            try
            {
                // Insert operations
                foreach (var batch in insertEvents.Chunk(batchSize))
                {
                    // Deduplicate inserts by _id to avoid duplicate key errors within the same batch
                    var deduplicatedInserts = batch
                        .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                        .GroupBy(e => e.FullDocument["_id"].ToString())
                        .Select(g => g.First()) // Take the first occurrence of each document
                        .ToList();

                    var insertModels = deduplicatedInserts
                        .Select(e =>
                        {
                            var doc = e.FullDocument;
                            var id = doc["_id"];

                            if (id.IsObjectId)
                                doc["_id"] = id.AsObjectId;

                            return new InsertOneModel<BsonDocument>(doc);
                        })
                        .ToList();

                    long insertCount = 0;
                    try
                    {
                        if (insertModels.Any())
                        {
                            var result = await collection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
                            insertCount += result.InsertedCount;
                        }
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        insertCount += ex.Result?.InsertedCount ?? 0;

                        var duplicateKeyErrors = ex.WriteErrors
                            .Where(err => err.Code == 11000)
                            .ToList();

                        if (!_syncBack)
                        {
                            item.CSDuplicateDocsSkipped += duplicateKeyErrors.Count;
                        }
                        else
                        {
                            item.SyncBackDuplicateDocsSkipped += duplicateKeyErrors.Count;
                        }

                        // Log non-duplicate key errors for inserts
                        var otherErrors = ex.WriteErrors
                            .Where(err => err.Code != 11000)
                            .ToList();

                        if (otherErrors.Any())
                        {
                            _log.WriteLine($"{_syncBackPrefix} Insert BulkWriteException (non-duplicate errors) in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}");
                        }
                        else if (duplicateKeyErrors.Count > 0)
                        {
                            _log.AddVerboseMessage($"{_syncBackPrefix} Skipped {duplicateKeyErrors.Count} duplicate key inserts in {collection.CollectionNamespace.FullName}");
                        }
                    }
                    finally
                    {
                        if (!_syncBack)
                        {
                            item.CSDocsInserted += insertCount;
                        }
                        else
                        {
                            item.SyncBackDocsInserted += insertCount;
                        }
                    }
                }

                // Update operations
                foreach (var batch in updateEvents.Chunk(batchSize))
                {
                    // Group by _id to handle multiple updates to the same document in the batch
                    var groupedUpdates = batch
                        .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                        .GroupBy(e => e.FullDocument["_id"].ToString())
                        .Select(g => g.OrderByDescending(e => e.ClusterTime ?? new MongoDB.Bson.BsonTimestamp(0, 0)).First()) // Take the latest update for each document
                        .ToList();

                    var updateModels = groupedUpdates
                        .Select(e =>
                        {
                            var doc = e.FullDocument;
                            var id = doc["_id"];

                            if (id.IsObjectId)
                                id = id.AsObjectId;

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", id);

                            // Use ReplaceOneModel instead of UpdateOneModel to avoid conflicts with unique indexes
                            return new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                        })
                        .ToList();

                    long updateCount = 0;
                    try
                    {
                        if (updateModels.Any())
                        {

                            var result = await collection.BulkWriteAsync(updateModels, new BulkWriteOptions { IsOrdered = false });                            
                            
                            updateCount += result.ModifiedCount + result.Upserts.Count;
                        }
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        updateCount += (ex.Result?.ModifiedCount ?? 0) + (ex.Result?.Upserts?.Count ?? 0);

                        var duplicateKeyErrors = ex.WriteErrors
                            .Where(err => err.Code == 11000)
                            .ToList();

                        if (!_syncBack)
                        {
                            item.CSDuplicateDocsSkipped += duplicateKeyErrors.Count;
                        }
                        else
                        {
                            item.SyncBackDuplicateDocsSkipped += duplicateKeyErrors.Count;
                        }

                        // Log non-duplicate key errors
                        var otherErrors = ex.WriteErrors
                            .Where(err => err.Code != 11000)
                            .ToList();

                        if (otherErrors.Any())
                        {
                            _log.WriteLine($"{_syncBackPrefix} Update BulkWriteException (non-duplicate errors) in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}");
                        }

                        // Handle duplicate key errors by attempting individual operations
                        if (duplicateKeyErrors.Any())
                        {
                            _log.AddVerboseMessage($"{_syncBackPrefix} Handling {duplicateKeyErrors.Count} duplicate key errors for updates in {collection.CollectionNamespace.FullName}");
                            
                            // Process failed operations individually
                            foreach (var error in duplicateKeyErrors)
                            {
                                try
                                {
                                    // Try to find the corresponding update model and retry as update without upsert
                                    var failedModel = updateModels[error.Index];
                                    if (failedModel is ReplaceOneModel<BsonDocument> replaceModel)
                                    {
                                        // Try update without upsert first
                                        var updateResult = await collection.ReplaceOneAsync(replaceModel.Filter, replaceModel.Replacement, new ReplaceOptions { IsUpsert = false });
                                        if (updateResult.ModifiedCount > 0)
                                        {
                                            updateCount++;
                                        }
                                        else
                                        {
                                            // Document might not exist, but we can't upsert due to unique constraint
                                            // This is expected in some scenarios - count as skipped
                                            if (!_syncBack)
                                            {
                                                item.CSDuplicateDocsSkipped++;
                                            }
                                            else
                                            {
                                                item.SyncBackDuplicateDocsSkipped++;
                                            }
                                        }
                                    }
                                }
                                catch (Exception retryEx)
                                {
                                    _log.AddVerboseMessage($"{_syncBackPrefix} Individual retry failed for update in {collection.CollectionNamespace.FullName}: {retryEx.Message}");
                                }
                            }
                        }
                    }
                    finally
                    {
                        if (!_syncBack)
                        {
                            item.CSDocsUpdated += updateCount;
                        }
                        else
                        {
                            item.SyncBackDocsUpdated += updateCount;
                        }
                    }
                }

                // Delete operations
                foreach (var batch in deleteEvents.Chunk(batchSize))
                {
                    // Deduplicate deletes by _id within the same batch
                    var deduplicatedDeletes = batch
                        .GroupBy(e => e.DocumentKey.GetValue("_id", null)?.ToString() ?? "")
                        .Where(g => !string.IsNullOrEmpty(g.Key))
                        .Select(g => g.First()) // Take the first delete for each document
                        .ToList();

                    var deleteModels = deduplicatedDeletes
                        .Select(e =>
                        {
                            try
                            {
                                if (!e.DocumentKey.Contains("_id"))
                                {
                                    _log.WriteLine($"{_syncBackPrefix} Delete event missing _id in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                    return null;
                                }

                                var id = e.DocumentKey.GetValue("_id", null);
                                if (id == null)
                                {
                                    _log.WriteLine($"{_syncBackPrefix} _id is null in DocumentKey in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                    return null;
                                }

                                if (id.IsObjectId)
                                    id = id.AsObjectId;

                                return new DeleteOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Eq("_id", id));
                            }
                            catch (Exception dex)
                            {
                                _log.WriteLine($"{_syncBackPrefix} Error building delete model in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}, Error: {dex.Message}");
                                return null;
                            }
                        })
                        .Where(model => model != null)
                        .ToList();

                    if (deleteModels.Any())
                    {
                        try
                        {
                            var result = await collection.BulkWriteAsync(deleteModels, new BulkWriteOptions { IsOrdered = false });
                            if (!_syncBack)
                            {
                                item.CSDocsDeleted += result.DeletedCount;
                            }
                            else
                            {
                                item.SyncBackDocsDeleted += result.DeletedCount;
                            }
                        }
                        catch (MongoBulkWriteException<BsonDocument> ex)
                        {
                            // Count successful deletes even when some fail
                            long deletedCount = ex.Result?.DeletedCount ?? 0;
                            
                            if (!_syncBack)
                            {
                                item.CSDocsDeleted += deletedCount;
                            }
                            else
                            {
                                item.SyncBackDocsDeleted += deletedCount;
                            }

                            // Log errors that are not "document not found" (which is expected)
                            var significantErrors = ex.WriteErrors
                                .Where(err => !err.Message.Contains("not found") && err.Code != 11000)
                                .ToList();

                            if (significantErrors.Any())
                            {
                                _log.WriteLine($"{_syncBackPrefix} Bulk delete error in {collection.CollectionNamespace.FullName}: {string.Join(", ", significantErrors.Select(e => e.Message))}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operations for {collection.CollectionNamespace.FullName}. Details: {ex}", LogType.Error);
            }

        }

    }
}
