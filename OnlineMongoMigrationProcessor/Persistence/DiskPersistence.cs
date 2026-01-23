using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.Marshalling;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace OnlineMongoMigrationProcessor.Persistence
{
    /// <summary>
    /// Disk-based implementation of PersistenceStorage.
    /// Stores documents as JSON files on the local file system.
    /// </summary>
    public class DiskPersistence : PersistenceStorage
    {
        private static string? _storagePath;
        private static string? _appId;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();

        private const string FILE_EXTENSION = ".json";

        /// <summary>
        /// Initializes the disk persistence layer with the provided storage path.
        /// This method is thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">Directory path where files will be stored</param>
        /// <exception cref="ArgumentException">Thrown when path is null or empty</exception>
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
                    throw new ArgumentException("Storage path cannot be null or empty", nameof(connectionStringOrPath));

                try
                {
                    _storagePath = connectionStringOrPath;
                    _appId= appId;
                    // Create directory if it doesn't exist (no-op for blob storage)
                    StorageStreamFactory.EnsureDirectoryExists(_storagePath);

                    _isInitialized = true;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to initialize DiskPersistence. Details: {ex}", ex);
                }
            }
        }

        /// <summary>
        /// Ensures the storage is initialized before operations
        /// </summary>
        private static void EnsureInitialized()
        {
            if (!_isInitialized || string.IsNullOrEmpty(_storagePath))
                throw new InvalidOperationException("DiskPersistence is not initialized. Call Initialize() first with a valid path.");
        }

        /// <summary>
        /// Gets the file path for a document id.
        /// Handles hierarchical IDs like "job1\mu1.json" by creating folder structure.
        /// The ID should include the .json extension for files.
        /// </summary>
        private static string GetFilePath(string id)
        {
            // Split by backslash or forward slash to handle hierarchical structure
            var parts = id.Split('\\', '/');
            
            if (parts.Length == 1)
            {
                // Simple ID, just use as filename (already has .json extension)
                var sanitizedId = SanitizeFileName(parts[0]);
                return Path.Combine(_storagePath!, sanitizedId);
            }
            else
            {
                // Hierarchical ID like "job1\mu1.json"
                // Create folder structure: storagePath/job1/mu1.json
                var pathParts = new List<string> { _storagePath! };
                
                // Add all parts except the last as directories
                for (int i = 0; i < parts.Length - 1; i++)
                {
                    pathParts.Add(SanitizeFileName(parts[i]));
                }
                
                // Create the directory structure if it doesn't exist (no-op for blob storage)
                var directoryPath = Path.Combine(pathParts.ToArray());
                StorageStreamFactory.EnsureDirectoryExists(directoryPath);
                
                // Add the last part as the filename (already has .json extension)
                var fileName = SanitizeFileName(parts[^1]);
                pathParts.Add(fileName);
                
                return Path.Combine(pathParts.ToArray());
            }
        }

        /// <summary>
        /// Gets the directory path for a folder id (id without .json extension).
        /// </summary>
        private static string GetDirectoryPath(string id)
        {

            // Split by backslash or forward slash
            var parts = id.Split('\\', '/');
            
            var pathParts = new List<string> { _storagePath! };
            
            // Add all parts as directories
            foreach (var part in parts)
            {
                pathParts.Add(SanitizeFileName(part));
            }
            
            return Path.Combine(pathParts.ToArray());
        }

        /// <summary>
        /// Sanitizes a string to be used as a filename or folder name
        /// </summary>
        private static string SanitizeFileName(string fileName)
        {
            var invalidChars = Path.GetInvalidFileNameChars();
            return string.Join("_", fileName.Split(invalidChars, StringSplitOptions.RemoveEmptyEntries));
        }

        /// <summary>
        /// Upserts a document with the specified id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier for the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public override bool UpsertDocument(string id, string jsonContent)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (string.IsNullOrWhiteSpace(jsonContent))
                throw new ArgumentException("JSON content cannot be null or empty", nameof(jsonContent));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);
                // Use StorageStreamFactory for blob storage support
                return StorageStreamFactory.WriteAllText(filePath, jsonContent);
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DiskPersistence] Error upserting document {id}. Details: {ex}", "DiskPersistence.txt");
                return false;
            }
        }

        /// <summary>
        /// Reads a document by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public override string ReadDocument(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);
                // Use StorageStreamFactory for blob storage support
                return StorageStreamFactory.ReadAllText(filePath);
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DiskPersistence] Error reading document {id}. Details: {ex}", "DiskPersistence.txt");
                return null;
            }
        }

        /// <summary>
        /// Checks if a document exists by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <returns>True if document exists, false otherwise</returns>
        public override bool DocumentExists(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                return false;

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);
                return StorageStreamFactory.Exists(filePath);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Deletes a document or folder by its id.
        /// If id ends with .json, deletes the file.
        /// If id doesn't end with .json, deletes the entire folder.
        /// </summary>
        /// <param name="id">Unique identifier of the document/folder (e.g., "job1\mu1.json" for file, "job1" for folder)</param>
        /// <returns>True if deleted, false otherwise</returns>
        public override bool DeleteDocument(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                if (id.EndsWith(FILE_EXTENSION))
                {
                    // Delete file
                    var filePath = GetFilePath(id);
                    
                    if (!StorageStreamFactory.Exists(filePath))
                        return false;

                    StorageStreamFactory.DeleteIfExists(filePath);
                    return true;
                }
                else
                {
                    // Delete folder (and all contents)
                    var directoryPath = GetDirectoryPath(id);
                    return StorageStreamFactory.DeleteDirectory(directoryPath, recursive: true);
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DiskPersistence] Error deleting document/folder {id}. Details: {ex}", "DiskPersistence.txt");
                return false;
            }
        }

        /// <summary>
        /// Lists all document IDs in the storage.
        /// Returns IDs with .json extension included.
        /// </summary>
        /// <returns>List of document IDs (e.g., "job1\mu1.json", "settings.json")</returns>
        public override List<string> ListDocumentIds()
        {
            EnsureInitialized();

            try
            {
                var ids = new List<string>();
                
                // Recursively find all .json files using StorageStreamFactory
                var files = StorageStreamFactory.ListFiles(_storagePath!, "*" + FILE_EXTENSION, recursive: true);
                
                foreach (var file in files)
                {
                    // Get relative path from storage root
                    string relativePath;
                    if (StorageStreamFactory.UseBlobStorage)
                    {
                        // For blob storage, the file is already a relative blob name
                        relativePath = file;
                    }
                    else
                    {
                        relativePath = Path.GetRelativePath(_storagePath!, file);
                    }
                    
                    // Convert path separators to backslash for consistency (keep .json extension)
                    var id = relativePath.Replace('/', '\\').Replace(Path.DirectorySeparatorChar, '\\');
                    
                    ids.Add(id);
                }
                
                return ids;
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DiskPersistence] Error listing document IDs. Details: {ex}", "DiskPersistence.txt");
                return new List<string>();
            }
        }


        /// <summary>
        /// Tests the connection to the storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public override bool TestConnection()
        {
            if (!_isInitialized || string.IsNullOrEmpty(_storagePath))
                return false;

            try
            {
                // For blob storage, assume connection is valid if initialized
                if (StorageStreamFactory.UseBlobStorage)
                    return true;
                return Directory.Exists(_storagePath);
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

        private static readonly object _readLock = new object();
        

        public override void PushLogEntry(string jobId, LogObject logObject)
        {           
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("ID cannot be null or empty", nameof(jobId));

            if (logObject == null)
                throw new ArgumentNullException(nameof(logObject));

            try
            {
                var folder = Path.Combine(_storagePath, "migrationlogs");
                var binPath = Path.Combine(folder, $"{jobId}.bin");

                StorageStreamFactory.EnsureDirectoryExists(folder);

                using var fs = StorageStreamFactory.OpenAppend(binPath);
                using var bw = new BinaryWriter(fs);

                var messageBytes = Encoding.UTF8.GetBytes(logObject.Message);
                bw.Write(messageBytes.Length);
                bw.Write(messageBytes);
                bw.Write((byte)logObject.Type);
                bw.Write(logObject.Datetime.ToBinary());
            }
            catch { }
        }

        public override int GetLogCount(string id)
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");
            
            if (!StorageStreamFactory.Exists(binPath))
                return 0;
                
            int count = 0;
            try
            {
                using var fs = StorageStreamFactory.OpenReadShared(binPath);
                if (fs == null) return 0;
                using var br = new BinaryReader(fs);
                
                while (fs.Position < fs.Length)
                {
                    try
                    {
                        if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                            break;
                            
                        int msgLen = br.ReadInt32();
                        if (msgLen <= 0 || msgLen > 1_000_000)
                            break;
                            
                        long bytesToSkip = msgLen + 1 + 8;
                        if (br.BaseStream.Position + bytesToSkip > br.BaseStream.Length)
                            break;
                            
                        br.BaseStream.Seek(bytesToSkip, SeekOrigin.Current);
                        count++;
                    }
                    catch
                    {
                        break;
                    }
                }
            }
            catch
            {
                return 0;
            }
            
            return count;
        }

        public override byte[] DownloadLogsPaginated(string id, int skip, int take)
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");
            var logBucket = new LogBucket { Logs = new List<LogObject>() };
            var offsets = new List<long>();

            if (!StorageStreamFactory.Exists(binPath))
                return Array.Empty<byte>();

            try
            {
                using var fs = StorageStreamFactory.OpenReadShared(binPath);
                if (fs == null) return Array.Empty<byte>();
                using var br = new BinaryReader(fs);

                // First pass: collect all offsets
                while (fs.Position < fs.Length)
                {
                    long offset = fs.Position;
                    try
                    {
                        if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                            break;

                        int msgLen = br.ReadInt32();
                        if (msgLen <= 0 || msgLen > 1_000_000)
                            break;

                        long bytesToSkip = msgLen + 1 + 8;
                        if (br.BaseStream.Position + bytesToSkip > br.BaseStream.Length)
                            break;

                        br.BaseStream.Seek(bytesToSkip, SeekOrigin.Current);
                        offsets.Add(offset);
                    }
                    catch
                    {
                        break;
                    }
                }

                // Apply skip/take pagination
                var selectedOffsets = offsets.Skip(skip).Take(take).ToList();

                // Second pass: read selected entries
                foreach (var offset in selectedOffsets)
                {
                    fs.Position = offset;
                    var log = TryReadLogEntry(br);
                    if (log != null)
                        logBucket.Logs!.Add(log);
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Error reading paginated logs. Details: {ex}", "DiskPersistence.txt");
            }

            // Format logs
            var sb = new System.Text.StringBuilder();
            foreach (var log in logBucket.Logs)
            {
                char typeChar = log.Type switch
                {
                    LogType.Error => 'E',
                    LogType.Warning => 'W',
                    LogType.Info => 'I',
                    LogType.Message => 'L',
                    LogType.Debug => 'D',
                    LogType.Verbose => 'V',
                    _ => '?'
                };
                string dateTime = log.Datetime.ToString("MM/dd/yyyy HH:mm:ss");
                sb.AppendLine($"{typeChar}|{dateTime}|{log.Message}");
            }
            
            return System.Text.Encoding.UTF8.GetBytes(sb.ToString());
        }

        public override byte[] DownloadLogsAsJsonBytes(string id, int topEntries = 20, int bottomEntries = 230)
        {
            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");
            var logs = ParseLogBinFile(binPath, topEntries, bottomEntries);
            
            // Format logs as multi-line string with Type|DateTime|Message format
            var sb = new System.Text.StringBuilder();
            
            foreach (var log in logs.Logs)
            {
                // Convert LogType to single character (E=Error, W=Warning, I=Info, D=Debug, V=Verbose)
                char typeChar = log.Type switch
                {
                    LogType.Error => 'E',
                    LogType.Warning => 'W',
                    LogType.Info => 'I',
                    LogType.Message => 'L', // Legacy - use 'L' for old Message type
                    LogType.Debug => 'D',
                    LogType.Verbose => 'V',
                    _ => '?'
                };
                
                // Format DateTime as short format (MM/dd/yyyy HH:mm:ss)
                string dateTime = log.Datetime.ToString("MM/dd/yyyy HH:mm:ss");
                
                // Build the line: Type|DateTime|Message
                sb.AppendLine($"{typeChar}|{dateTime}|{log.Message}");
            }
            
            return System.Text.Encoding.UTF8.GetBytes(sb.ToString());
        }
        public override LogBucket ReadLogs(string id, out string fileName)
        {
            fileName = id;

            try
            {
                lock (_readLock)
                {
                    Console.WriteLine($"Reading log file for ID: {id}");
                    var folder = Path.Combine(_storagePath, "migrationlogs");
                    var binPath = Path.Combine(folder, $"{id}.bin");

                    if (StorageStreamFactory.Exists(binPath))
                    {
                        var logBucket = ParseLogBinFile(binPath);
                        if (logBucket.Logs == null || logBucket.Logs.Count == 0)
                        {
                            return HandleError(id, binPath, binPath, out fileName);
                        }
                        return logBucket;
                    }

                    return new LogBucket();
                }
            }
            catch
            {
                throw new Exception("Log Init failed");
            }
        }

        private LogBucket HandleError(string jobId, string binPath, string currentLogFilePath, out string backupFileName)
        {
            backupFileName = CreateFileCopyWithTimestamp(currentLogFilePath);

            StorageStreamFactory.DeleteIfExists(currentLogFilePath);

            var logBucket = new LogBucket();
            logBucket.Logs ??= new List<LogObject>();
            logBucket.Logs.Add(new LogObject(LogType.Error, $"Unable to load the log file; original file backed up as {backupFileName}"));
            WriteBinaryLog(jobId, logBucket.Logs);
            return ParseLogBinFile(binPath);
        }


        private void WriteBinaryLog(string id, List<LogObject> logs)
        {
            if (logs == null || logs.Count == 0)
                return;

            var folder = Path.Combine(_storagePath, "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");

            try
            {
                StorageStreamFactory.EnsureDirectoryExists(folder);

                using var fs = StorageStreamFactory.OpenAppend(binPath);
                using var bw = new BinaryWriter(fs);

                foreach (var log in logs)
                {
                    try
                    {
                        var message = log.Message ?? string.Empty;
                        var messageBytes = Encoding.UTF8.GetBytes(message);

                        bw.Write(messageBytes.Length);
                        bw.Write(messageBytes);
                        bw.Write((byte)log.Type);
                        bw.Write(log.Datetime.ToBinary());
                    }
                    catch (Exception ex)
                    {
                        Helper.LogToFile($"Failed to write log entry. Details: {ex}", "DiskPersistence.txt");

                        // Continue writing other logs
                    }
                }

                bw.Flush();
                fs.Flush();
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Error writing binary log. Details: {ex}", "DiskPersistence.txt");
                throw;
            }
        }


        private LogBucket ParseLogBinFile(string binPath, int topCount = 20, int bottomCount = 280)
        {
            var logBucket = new LogBucket { Logs = new List<LogObject>() };
            var offsets = new List<long>();

            if (!StorageStreamFactory.Exists(binPath))
                return logBucket;

            try
            {
                using var fs = StorageStreamFactory.OpenReadShared(binPath);
                if (fs == null) return logBucket;
                using var br = new BinaryReader(fs);

                // First pass: collect offsets of valid log entries
                while (fs.Position < fs.Length)
                {
                    long offset = fs.Position;

                    try
                    {
                        if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                            break;

                        int msgLen = br.ReadInt32();

                        if (msgLen <= 0 || msgLen > 1_000_000)
                            break;

                        long bytesToSkip = msgLen + 1 + 8;
                        if (br.BaseStream.Position + bytesToSkip > br.BaseStream.Length)
                            break;

                        br.BaseStream.Seek(bytesToSkip, SeekOrigin.Current);
                        offsets.Add(offset);
                    }
                    catch
                    {
                        break;
                    }
                }

                // Select top N and bottom M
                List<long> selectedOffsets;
                if (offsets.Count > topCount + bottomCount)
                {
                    if (topCount > 0 && bottomCount > 0)
                    {
                        selectedOffsets = offsets
                            .Take(topCount)
                            .Concat(offsets.Skip(Math.Max(0, offsets.Count - bottomCount)))
                            .Distinct()
                            .OrderBy(i => i)
                            .ToList();
                    }
                    else
                    {
                        // Return full list without filtering
                        selectedOffsets = offsets
                            .Distinct()
                            .OrderBy(i => i)
                            .ToList();
                    }
                }
                else
                {
                    selectedOffsets = offsets;
                }

                // Second pass: read selected entries
                foreach (var offset in selectedOffsets)
                {
                    fs.Position = offset;
                    var log = TryReadLogEntry(br);
                    if (log != null)
                        logBucket.Logs!.Add(log);
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Error parsing binary log file. Details: {ex}", "DiskPersistence.txt");
            }

            return logBucket;
        }

        private LogObject? TryReadLogEntry(BinaryReader br)
        {
            const int MaxReasonableLength = 1_000_000;

            try
            {
                if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                    return null;

                int len = br.ReadInt32();

                if (len <= 0 || len > MaxReasonableLength)
                {
                    Helper.LogToFile($"Invalid message length: {len}", "DiskPersistence.txt");
                    return null;
                }

                long requiredBytes = len + 1 + 8;
                if (br.BaseStream.Position + requiredBytes > br.BaseStream.Length)
                {
                    Helper.LogToFile($"Incomplete log entry. Message length={len}, remaining={br.BaseStream.Length - br.BaseStream.Position}", "DiskPersistence.txt");
                    return null;
                }

                byte[] bytes = br.ReadBytes(len);
                if (bytes.Length != len)
                {
                    Helper.LogToFile($"ReadBytes returned {bytes.Length}, expected {len}", "DiskPersistence.txt");
                    return null;
                }

                string msg = Encoding.UTF8.GetString(bytes);
                byte typeByte = br.ReadByte();
                var type = (LogType)typeByte;
                long dateBinary = br.ReadInt64();
                DateTime datetime = DateTime.FromBinary(dateBinary);

                return new LogObject(type, msg) { Datetime = datetime };
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Exception while reading log entry. Details: {ex}", "DiskPersistence.txt");
                return null;
            }
        }

        private string CreateFileCopyWithTimestamp(string sourceFilePath)
        {

            if (string.IsNullOrEmpty(sourceFilePath))
                throw new ArgumentException("Source file path cannot be null or empty.", nameof(sourceFilePath));

            if (!StorageStreamFactory.Exists(sourceFilePath))
                throw new FileNotFoundException("Source file not found.", sourceFilePath);

            string directory = Path.GetDirectoryName(sourceFilePath) ?? string.Empty;
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(sourceFilePath);
            string extension = Path.GetExtension(sourceFilePath);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
            string newFilePath = Path.Combine(directory, newFileName);

            if (!StorageStreamFactory.Exists(newFilePath))
            {
                StorageStreamFactory.CopyFile(sourceFilePath, newFilePath);
            }

            return newFileName;
        }

        /// <summary>
        /// Deletes all log entries for a given JobId by deleting the binary log file
        /// </summary>
        /// <param name="jobId">Job ID to delete logs for</param>
        /// <returns>1 if file was deleted, 0 if file didn't exist, -1 if error occurred</returns>
        public override long DeleteLogs(string jobId)
        {

            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));

            try
            {
                var folder = Path.Combine(_storagePath!, "migrationlogs");
                var binPath = Path.Combine(folder, $"{jobId}.bin");

                if (StorageStreamFactory.Exists(binPath))
                {
                    StorageStreamFactory.DeleteIfExists(binPath);
                    Helper.LogToFile($"[DiskPersistence] Deleted log file for job {jobId}", "DiskPersistence.txt");
                    return 1;
                }
                else
                {
                    Helper.LogToFile($"[DiskPersistence] Log file for job {jobId} does not exist", "DiskPersistence.txt");
                    return 0;
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"[DiskPersistence] Error deleting logs for job {jobId}. Details: {ex}", "DiskPersistence.txt");
                return -1;
            }
        }

    }
}
