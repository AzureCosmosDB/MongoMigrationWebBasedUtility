using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ZstdSharp.Unsafe;

namespace OnlineMongoMigrationProcessor.Persistence
{
    /// <summary>
    /// MongoDB implementation of PersistenceStorage.
    /// Persists objects (MigrationJob, MigrationUnit, JobList, MigrationSettings) 
    /// to MongoDB storage using _id as the key.
    /// </summary>
    public class DocumentDBPersistence : PersistenceStorage
    {
        private static IMongoClient? _client;
        private static IMongoDatabase? _database;
        private static IMongoCollection<BsonDocument>? _dataCollection;
        private static IMongoCollection<BsonDocument>? _logCollection;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();
        private static string _appId = string.Empty;
        private const string DATABASE_NAME = "mongomigrationwebappstorage";
        private const string DATA_Collection = "datafiles";
        private const string LOG_Collection = "logfiles";

        /// <summary>
        /// Initializes the MongoDB persistence layer with the provided connection string.
        /// This method is thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">MongoDB connection string</param>
        /// <exception cref="ArgumentException">Thrown when connection string is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when initialization fails</exception>
        public override void Initialize(string connectionStringOrPath,string appId)
        {

            if (_isInitialized)
                return;

            lock (_initLock)
            {
                if (_isInitialized)
                    return;

                if (string.IsNullOrWhiteSpace(connectionStringOrPath))
                    throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionStringOrPath));

                var sw = System.Diagnostics.Stopwatch.StartNew();
                Helper.LogToFile($"DocumentDBPersistence.Initialize starting");
                
                try
                {
                    Helper.LogToFile($"Creating MongoClient, elapsed: {sw.ElapsedMilliseconds}ms");
                    _client = new MongoClient(connectionStringOrPath);
                    Helper.LogToFile($"MongoClient created, elapsed: {sw.ElapsedMilliseconds}ms");
                    
                    Helper.LogToFile($"Getting database {DATABASE_NAME}, elapsed: {sw.ElapsedMilliseconds}ms");
                    _database = _client.GetDatabase(DATABASE_NAME);
                    Helper.LogToFile($"Database reference obtained, elapsed: {sw.ElapsedMilliseconds}ms");
                    
                    _dataCollection = _database.GetCollection<BsonDocument>(DATA_Collection);
                    _logCollection = _database.GetCollection<BsonDocument>(LOG_Collection);
                    
                    Helper.LogToFile($"Listing collection names, elapsed: {sw.ElapsedMilliseconds}ms");
                    var collectionNames = _database.ListCollectionNames().ToListAsync().GetAwaiter().GetResult();
                    Helper.LogToFile($"Collection names listed ({collectionNames.Count} collections), elapsed: {sw.ElapsedMilliseconds}ms");

                    if (!collectionNames.Contains(DATA_Collection))
                    {
                        Helper.LogToFile($"Creating {DATA_Collection} collection, elapsed: {sw.ElapsedMilliseconds}ms");
                        _database.CreateCollectionAsync(DATA_Collection).GetAwaiter().GetResult();
                        Helper.LogToFile($"{DATA_Collection} collection created, elapsed: {sw.ElapsedMilliseconds}ms");
                    }

                    if (!collectionNames.Contains(LOG_Collection))
                    {
                        Helper.LogToFile($"Creating {LOG_Collection} collection, elapsed: {sw.ElapsedMilliseconds}ms");
                        _database.CreateCollectionAsync(LOG_Collection).GetAwaiter().GetResult();
                        Helper.LogToFile($"{LOG_Collection} collection created, elapsed: {sw.ElapsedMilliseconds}ms");
                        
                        // Create composite index on JobId and _id to optimize ReadLogs queries
                        // ReadLogs filters by JobId and sorts by _id
                        Helper.LogToFile($"Creating composite index on JobId and _id, elapsed: {sw.ElapsedMilliseconds}ms");
                        var indexKeysDefinition = Builders<BsonDocument>.IndexKeys
                            .Ascending("JobId")
                            .Ascending("_id");
                        var indexModel = new CreateIndexModel<BsonDocument>(indexKeysDefinition);
                        _logCollection.Indexes.CreateOne(indexModel);
                        Helper.LogToFile($"Composite index created, elapsed: {sw.ElapsedMilliseconds}ms");
                    }

                    _appId = appId;
                    _isInitialized = true;
                    
                    sw.Stop();
                    Helper.LogToFile($"DocumentDBPersistence.Initialize completed in {sw.ElapsedMilliseconds}ms");
                }
                catch (Exception ex)
                {
                    sw.Stop();
                    Helper.LogToFile($"DocumentDBPersistence.Initialize FAILED after {sw.ElapsedMilliseconds}ms: {ex.Message}");
                    throw new InvalidOperationException($"Failed to initialize DocumentDBPersistence. Details: {ex}", ex);
                }
            }
        }

        /// <summary>
        /// Ensures the storage is initialized before operations
        /// </summary>
        private static void EnsureInitialized()
        {
            if (!_isInitialized || _dataCollection == null)
                throw new InvalidOperationException("DocumentDBPersistence is not initialized. Call Initialize() first with a valid connection string.");
        }

        /// <summary>
        /// Normalizes the ID for MongoDB by replacing backslashes and forward slashes with underscores.
        /// Handles hierarchical IDs like "job1\mu1" by converting them to "job1_mu1".
        /// </summary>
        private static string NormalizeIdForMongo(string id)
        {
            return $"{_appId}.{id.Replace('\\', '_').Replace('/', '_')}";
        }

        /// <summary>
        /// Upserts a document with the specified _id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier (_id) for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public override bool UpsertDocument(string id, string jsonContent)
        {
            return Upsert(id, jsonContent, false);
        }

        private bool Upsert(string id, string jsonContent, bool IsLog)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (string.IsNullOrWhiteSpace(jsonContent))
                throw new ArgumentException("JSON content cannot be null or empty", nameof(jsonContent));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var document = BsonDocument.Parse(jsonContent);
                document["_id"] = normalizedId;

                // When IsLog is false, extract JobId and add as attribute
                if (!IsLog)
                {
                    // Extract JobId using regex
                    // Format: "aca_server1.migrationjobs_<jobId>_..."
                    var jobId = ExtractJobIdFromNormalizedId(normalizedId);
                    if (!string.IsNullOrEmpty(jobId))
                    {
                        document["JobId"] = jobId;
                    }
                }

                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);

                if (IsLog)
                    _logCollection!.ReplaceOne(filter, document, new ReplaceOptions { IsUpsert = true });
                else
                    _dataCollection!.ReplaceOne(filter, document, new ReplaceOptions { IsUpsert = true });

                return true;
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error upserting document {id}. Details: {ex}", "DocumentDBPersistence.txt");
                return false;
            }
        }

        // Helper method to extract JobId from normalizedId
        private static string ExtractJobIdFromNormalizedId(string normalizedId)
        {
            // Example: aca_server1.migrationjobs_4f007573-9b88-472b-9229-3b4657713193_4A47FD3...
            // Regex: migrationjobs_([0-9a-fA-F\-]+)
            var match = System.Text.RegularExpressions.Regex.Match(normalizedId, @"migrationjobs_([0-9a-fA-F\-]+)");
            return match.Success ? match.Groups[1].Value : string.Empty;
        }

        /// <summary>
        /// Reads a document by its _id and returns it with Id property restored
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document</param>
        /// <returns>JSON content with Id property if found, null otherwise</returns>
        public override string? ReadDocument(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var document = _dataCollection!.Find(filter).FirstOrDefault();
                
                if (document == null)
                    return null;

                
                return document.ToJson();
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error reading document {id}.Details: {ex}", "DocumentDBPersistence.txt");
                return null;
            }
        }

        /// <summary>
        /// Checks if a document exists by its _id
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document</param>
        /// <returns>True if document exists, false otherwise</returns>
        public override bool DocumentExists(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                return false;

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var count = _dataCollection!.CountDocuments(filter);
                return count > 0;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Deletes a document by its _id and all hierarchical child documents
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document to delete</param>
        /// <returns>True if at least one document was deleted, false otherwise</returns>
        public override bool DeleteDocument(string id)
        {
            return Delete(id);
        }
        private bool Delete(string id, bool isLog = false)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var regexFilter = Builders<BsonDocument>.Filter.Regex("_id", new BsonRegularExpression($"^{System.Text.RegularExpressions.Regex.Escape(normalizedId)}"));
                var jobIdFilter = Builders<BsonDocument>.Filter.Eq("JobId", id);
                var filter = Builders<BsonDocument>.Filter.Or(regexFilter, jobIdFilter);

                IMongoCollection<BsonDocument> collectionToUse = isLog ? _logCollection! : _dataCollection!;
                var result = collectionToUse.DeleteMany(filter);
                return result.DeletedCount > 0;
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error deleting document {id}.Details: {ex}", "DocumentDBPersistence.txt");
                return false;
            }
        }

        /// <summary>
        /// Lists all document IDs in the collection
        /// </summary>
        /// <returns>List of document IDs</returns>
        public override List<string> ListDocumentIds()
        {
            EnsureInitialized();

            try
            {
                var projection = Builders<BsonDocument>.Projection.Include("_id");
                var documents = _dataCollection!.Find(Builders<BsonDocument>.Filter.Empty)
                    .Project(projection)
                    .ToList();

                return documents.Select(doc => doc["_id"].AsString).ToList();
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error listing document IDs. Details: {ex}", "DocumentDBPersistence.txt");
                return new List<string>();
            }
        }

        public override int GetLogCount(string id)
        {

            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(id))
                return 0;
            
            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("JobId", normalizedId);
                var count = (int)_logCollection!.CountDocuments(filter);
                return count;
            }
            catch
            {
                return 0;
            }
        }

        public override byte[] DownloadLogsPaginated(string id, int skip, int take)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(id))
                return Array.Empty<byte>();
            
            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("JobId", normalizedId);
                var sort = Builders<BsonDocument>.Sort.Ascending("_id");
                
                // Use StringBuilder to stream format directly without loading all docs into a list
                var sb = new System.Text.StringBuilder();
                
                // Stream process documents using ForEachAsync to avoid loading all into memory at once
                // Skip and Limit are applied on the server side before any data is transferred
                using (var cursor = _logCollection!.Find(filter).Sort(sort).Skip(skip).Limit(take).ToCursor())
                {
                    while (cursor.MoveNext())
                    {
                        // Process each batch as it arrives, don't accumulate
                        foreach (var doc in cursor.Current)
                        {
                            try
                            {
                                LogType logType = Enum.Parse<LogType>(doc.GetValue("Type").AsString);
                                
                                char typeChar = logType switch
                                {
                                    LogType.Error => 'E',
                                    LogType.Warning => 'W',
                                    LogType.Info => 'I',
                                    LogType.Message => 'L',
                                    LogType.Debug => 'D',
                                    LogType.Verbose => 'V',
                                    _ => '?'
                                };
                                
                                DateTime dateTime = doc.GetValue("Datetime").ToUniversalTime();
                                string dateTimeStr = dateTime.ToString("MM/dd/yyyy HH:mm:ss");
                                string message = doc.GetValue("Message").AsString;
                                
                                sb.AppendLine($"{typeChar}|{dateTimeStr}|{message}");
                            }
                            catch (Exception docEx)
                            {
                                // Log individual document parsing errors but continue
                                Helper.LogToFile($"[DocumentDBPersistence] Error parsing log document.Details: {docEx}", "DocumentDBPersistence.txt");
                            }
                        }
                    }
                }
                
                return System.Text.Encoding.UTF8.GetBytes(sb.ToString());
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error downloading paginated logs for {id}. Details: {ex}", "DocumentDBPersistence.txt");
                return Array.Empty<byte>();
            }
        }

        public override byte[] DownloadLogsAsJsonBytes(string Id, int topEntries = 20, int bottomEntries = 230)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(Id))
                throw new ArgumentException("Path cannot be null or empty", nameof(Id));
            try
            {
                var normalizedId = NormalizeIdForMongo(Id);
                var filter = Builders<BsonDocument>.Filter.Eq("JobId", normalizedId);
                var sort = Builders<BsonDocument>.Sort.Ascending("_id");
                
                // First, get the total count efficiently
                var totalLogs = (int)_logCollection!.CountDocuments(filter);
                
                if (totalLogs > 0)
                {
                    var selectedLogs = new List<BsonDocument>();

                    // If topEntries=0 and bottomEntries=0, get all logs
                    if (topEntries == 0 && bottomEntries == 0)
                    {
                        using (var cursor = _logCollection!.Find(filter).Sort(sort).ToCursor())
                        {
                            while (cursor.MoveNext())
                            {
                                selectedLogs.AddRange(cursor.Current);
                            }
                        }
                    }
                    else
                    {
                        // Get top entries using cursor
                        if (topEntries > 0)
                        {
                            using (var cursor = _logCollection!.Find(filter).Sort(sort).Limit(topEntries).ToCursor())
                            {
                                while (cursor.MoveNext())
                                {
                                    selectedLogs.AddRange(cursor.Current);
                                }
                            }
                        }
                        
                        // Get bottom entries if needed (skip overlapping with top)
                        if (bottomEntries > 0 && totalLogs > topEntries)
                        {
                            int skipCount = Math.Max(totalLogs - bottomEntries, topEntries);
                            int takeCount = totalLogs - skipCount;
                            
                            if (takeCount > 0)
                            {
                                using (var cursor = _logCollection!.Find(filter).Sort(sort).Skip(skipCount).Limit(takeCount).ToCursor())
                                {
                                    while (cursor.MoveNext())
                                    {
                                        selectedLogs.AddRange(cursor.Current);
                                    }
                                }
                            }
                        }
                    }
                    
                    // Format logs as multi-line string with Type|DateTime|Message format
                    var sb = new System.Text.StringBuilder();
                    
                    foreach (var doc in selectedLogs)
                    {
                        // Parse LogType from document
                        LogType logType = Enum.Parse<LogType>(doc.GetValue("Type").AsString);
                        
                        // Convert LogType to single character (E=Error, W=Warning, I=Info, D=Debug, V=Verbose)
                        char typeChar = logType switch
                        {
                            LogType.Error => 'E',
                            LogType.Warning => 'W',
                            LogType.Info => 'I',
                            LogType.Message => 'L', // Legacy - use 'L' for old Message type
                            LogType.Debug => 'D',
                            LogType.Verbose => 'V',
                            _ => '?'
                        };
                        
                        // Get DateTime and format as short format (MM/dd/yyyy HH:mm:ss)
                        DateTime dateTime = doc.GetValue("Datetime").ToUniversalTime();
                        string dateTimeStr = dateTime.ToString("MM/dd/yyyy HH:mm:ss");
                        
                        // Get message
                        string message = doc.GetValue("Message").AsString;
                        
                        // Build the line: Type|DateTime|Message
                        sb.AppendLine($"{typeChar}|{dateTimeStr}|{message}");
                    }
                    
                    return System.Text.Encoding.UTF8.GetBytes(sb.ToString());
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error downloading logs for {Id}.Details: {ex}", "DocumentDBPersistence.txt");
            }
            return Array.Empty<byte>();
        }


        public override LogBucket ReadLogs(string id, out string fileName)
        {

            fileName = id;
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            //const int MAX_LOGS_TO_READ = 5000; // Prevent OOM by limiting max logs

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("JobId", normalizedId);
                
                // Sort by _id (ObjectId contains timestamp, so this gives chronological order)
                var sort = Builders<BsonDocument>.Sort.Ascending("_id");
                
                // Use cursor to avoid loading all documents at once
                var logObjects = new List<LogObject>();
                using (var cursor = _logCollection!.Find(filter).Sort(sort).ToCursor())
                {
                    while (cursor.MoveNext())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            var logObject = new LogObject(
                                Enum.Parse<LogType>(doc.GetValue("Type").AsString),
                                doc.GetValue("Message").AsString
                            )
                            {
                                Datetime = doc.GetValue("Datetime").ToUniversalTime()
                            };
                            logObjects.Add(logObject);
                        }
                    }
                }

                if (logObjects.Count > 0)
                {
                    var logBucket = new LogBucket
                    {
                        Logs = logObjects
                    };
                    
                    return logBucket;
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error reading logs for job {id}.Details:  {ex}", "DocumentDBPersistence.txt");
            }

            return new LogBucket();
        }

        public override void PushLogEntry(string jobId, LogObject logObject)
        {

            // Store one document per log entry with ObjectId as _id
            // Each document contains JobId field for querying all logs for a specific job

            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));
            try
            {
                var normalizedJobId = NormalizeIdForMongo(jobId);
                
                // Create a new document with ObjectId as _id
                var logDocument = new BsonDocument
                {
                    { "_id", ObjectId.GenerateNewId() },
                    { "JobId", normalizedJobId },
                    { "Message", logObject.Message },
                    { "Type", logObject.Type.ToString() },
                    { "Datetime", logObject.Datetime }
                };
                
                _logCollection!.InsertOne(logDocument);
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error pushing log entry for job {jobId}. Details: {ex}", "DocumentDBPersistence.txt");
            }
        }

        /// <summary>
        /// Deletes all log entries for a given JobId
        /// </summary>
        /// <param name="jobId">Job ID to delete logs for</param>
        /// <returns>Number of log entries deleted, or -1 if error occurred</returns>
        public override long DeleteLogs(string jobId)
        {

            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));

            try
            {
                var normalizedJobId = NormalizeIdForMongo(jobId);
                var filter = Builders<BsonDocument>.Filter.Eq("JobId", normalizedJobId);
                var result = _logCollection!.DeleteMany(filter);
                return result.DeletedCount;
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DocumentDBPersistence] Error deleting logs for job {jobId}.Details:  {ex}", "DocumentDBPersistence.txt");
                return -1;
            }
        }

        /// <summary>
        /// Tests the connection to MongoDB storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public override bool TestConnection()
        {

            if (!_isInitialized)
                return false;

            try
            {
                _database!.ListCollectionNames();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public override bool IsInitialized => _isInitialized;
    }
}
