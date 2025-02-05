﻿using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    internal class MongoChangeStreamProcessor
    {
        private MongoClient _sourceClient;
        private MongoClient _targetClient;
        private JobList? _jobs;
        private MigrationSettings? _config;

        public bool ExecutionCancelled { get; set; }

        public MongoChangeStreamProcessor(MongoClient sourceClient, MongoClient targetClient, JobList jobs, MigrationSettings config)
        {
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _jobs = jobs;
            _config = config;
        }

        public void ProcessCollectionChangeStream(MigrationJob job, MigrationUnit item)
        {
            try
            {
                string databaseName = item.DatabaseName;
                string collectionName = item.CollectionName;

                var sourceDb = _sourceClient.GetDatabase(databaseName);
                var sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                var targetDb = _targetClient.GetDatabase(databaseName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                Log.WriteLine($"Replaying change stream for {databaseName}.{collectionName}");

                while (!ExecutionCancelled && item.DumpComplete)
                {
                    try
                    {
                        ChangeStreamOptions options = new ChangeStreamOptions { };

                        if (item.CursorUtcTimestamp > DateTime.MinValue && !job.SourceServerVersion.StartsWith("3"))
                        {
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(item.CursorUtcTimestamp.ToLocalTime());
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }
                        else if (item.ResumeToken != null)
                        {
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(item.ResumeToken) };
                        }
                        else if (item.ResumeToken == null && job.SourceServerVersion.StartsWith("3"))
                        {
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
                        }
                        else if (item.ChangeStreamStartedOn.HasValue && !job.SourceServerVersion.StartsWith("3"))
                        {
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)item.ChangeStreamStartedOn);
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }

                        // Create a CancellationTokenSource with a timeout (e.g., 5 minutes)
                        var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(5));
                        CancellationToken cancellationToken = cancellationTokenSource.Token;

                        // Open a Change Stream
                        using (var cursor = sourceCollection.Watch(options))
                        {
                            int counter = 0;

                            // Continuously monitor the change stream
                            //mongo 3.6 has different implementation for change stream
                            if (job.SourceServerVersion.StartsWith("3"))
                            {
                                if (item.ResumeDocumentId != null && !item.ResumeDocumentId.IsBsonNull)                              
                                {
                                    //manually replay  the first change as the ResumeAfter skips the current item
                                    ProcessFirstChangeManuallyForResumeToken(item.ResumeDocumentId, item.ResumeTokenOperation, sourceCollection, targetCollection);
                                }

                                //proceed with the rest of the changes
                                foreach (ChangeStreamDocument<BsonDocument> change in cursor.ToEnumerable())
                                {
                                    if (ProcessCursor(job,change, cursor, targetCollection, item, ref counter) == false)
                                    {
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                while (cursor.MoveNext(cancellationToken))
                                {
                                    foreach (var change in cursor.Current)
                                    {
                                        if (ProcessCursor(job,change, cursor, targetCollection, item, ref counter) == false)
                                        {
                                            break;
                                        }
                                    }

                                    // Break the outer loop if conditions are met
                                    if (counter > _config.ChangeStreamBatchSize || ExecutionCancelled)
                                        break;
                                }
                            }
                            Log.Save();
                        }

                        // Pause briefly before next iteration (optional)
                        Thread.Sleep(100);
                    }
                    catch (OperationCanceledException)
                    {
                        // Handle cancellation gracefully
                        Log.AddVerboseMessage($"CS Batch duration expired while monitoring {targetCollection.CollectionNamespace}. CS will resume automatically");
                    }
                    catch (MongoCommandException ex) when (ex.Message.Contains("Resume of change stream was not possible"))
                    {
                        // Handle other potential exceptions
                        Log.WriteLine($"Oplog is full. Error processing change stream for {targetCollection.CollectionNamespace}. Details : {ex.Message}", LogType.Error);
                        Log.Save();
                        Thread.Sleep(1000 * 300);
                    }
                    catch (Exception ex)
                    {
                        // Handle other potential exceptions
                        Log.WriteLine($"Error processing change stream for {targetCollection.CollectionNamespace}. Details : {ex.Message}", LogType.Error);
                        Log.Save();
                    }
                    finally
                    {
                        Log.Save();
                    }
                }
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error processing change stream. Details : {ex.Message}", LogType.Error);
                Log.Save();
            }
        }

        // This method retrieves the event associated with the ResumeToken
        private void ProcessFirstChangeManuallyForResumeToken(BsonValue? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection)
        {
            if (documentId==null || documentId.IsBsonNull)
            {
                Log.WriteLine("Manual replay for {targetCollection.CollectionNamespace} resume token is null, skipping processing.");
                Log.Save();
                return;
            }

            Log.WriteLine($"Manual replay for {targetCollection.CollectionNamespace} resume token is type: {opType} ");

            var filter = Builders<BsonDocument>.Filter.Eq("_id", documentId); // Assuming _id is your resume token
            var result = sourceCollection.Find(filter).FirstOrDefault(); // Retrieve the document for the resume token

            if (result != null)
            {
                try
                {
                    switch (opType)
                    {
                        case ChangeStreamOperationType.Insert:
                            targetCollection.InsertOne(result);
                            break;
                        case ChangeStreamOperationType.Update:
                        case ChangeStreamOperationType.Replace:
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            break;
                        case ChangeStreamOperationType.Delete:
                            var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                            targetCollection.DeleteOne(deleteFilter);
                            break;
                        default:
                            Log.WriteLine($"Unhandled operation type: {opType}");
                            break;
                    }
                }
                catch (MongoException mex) when (opType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
                {
                    // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"Error processing operation {opType} on {targetCollection.CollectionNamespace} with _id {documentId}. Details : {ex.Message}", LogType.Error);
                    Log.Save();
                }
            }
            else
            {
                Log.WriteLine("Document for the ResumeToken doesn't exist (may be deleted).");
                Log.Save();
            }
            Log.Save();
        }


        private bool ProcessCursor(MigrationJob job, ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, MigrationUnit item, ref int counter)
        {
            try
            {
                if (!job.SourceServerVersion.StartsWith("3"))
                {
                    // Access the ClusterTime (timestamp) from the ChangeStreamDocument
                    var timestamp = change.ClusterTime; // Convert BsonTimestamp to DateTime

                    // Output change details to the console
                    Log.AddVerboseMessage($"{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {MongoHelper.BsonTimestampToUtcDateTime(timestamp)}");
                    ProcessChange(change, targetCollection);
                    item.CursorUtcTimestamp = MongoHelper.BsonTimestampToUtcDateTime(timestamp);
                }
                else
                {
                    // Output change details to the console
                    Log.AddVerboseMessage($"{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]}");
                    ProcessChange(change, targetCollection);
                }
                item.ResumeToken = cursor.Current.FirstOrDefault().ResumeToken.ToJson();
                _jobs?.Save(); // persists state

                counter++;

                // Break if batch size is reached
                if (counter > _config.ChangeStreamBatchSize)
                    return false;

                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error processing cursor. Details : {ex.Message}", LogType.Error);
                Log.Save();
                return false;
            }
        }


        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection)
        {
            try
            {
                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        targetCollection.InsertOne(change.FullDocument);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", change.DocumentKey["_id"]);
                        targetCollection.ReplaceOne(filter, change.FullDocument, new ReplaceOptions { IsUpsert = true });
                        break;
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", change.DocumentKey["_id"]);
                        targetCollection.DeleteOne(deleteFilter);
                        break;
                    default:
                        Log.WriteLine($"Unhandled operation type: {change.OperationType}");
                        break;
                }
            }
            catch (MongoException mex) when (change.OperationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error processing operation {change.OperationType} on {targetCollection.CollectionNamespace} with _id {change.DocumentKey["_id"]}. Details : {ex.Message}", LogType.Error);
                Log.Save();
            }
        }
    }
}


