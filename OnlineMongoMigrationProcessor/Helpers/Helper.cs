using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using SharpCompress.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using System.Xml.Linq;
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8604 // Possible null reference argument.

namespace OnlineMongoMigrationProcessor
{
    public static class Helper
    {

        static string _workingFolder = string.Empty;


        public static bool IsOnline(MigrationJob job)
        {
            if (job == null)
            {
                return false;
            }
            if (job.CDCMode==CDCMode.Offline)
            {
                return false;
            }
            return true;
        }
        private static double GetFolderSizeInGB(string folderPath)
        {
            if (!Directory.Exists(folderPath))
            {
                //Console.WriteLine("Folder does not exist.");
                Helper.LogToFile($"Folder {folderPath} does not exist", "HelperLogs.txt");
                return 0;
            }

            try
            {
                long totalSizeBytes = Directory.EnumerateFiles(folderPath, "*", SearchOption.AllDirectories)
                                               .Sum(file => new FileInfo(file).Length);

                return totalSizeBytes / (1024.0 * 1024 * 1024); // Convert bytes to GB
            }
            catch (UnauthorizedAccessException e)
            {
                //Console.WriteLine($"Access denied: {e.ToString()}");
                Helper.LogToFile($"Access denied: {e.ToString()}", "HelperLogs.txt");
                return 0;
            }
            catch (Exception e)
            {
                Helper.LogToFile($"Unknown exception: {e.ToString()}", "HelperLogs.txt");
                return 0;
            }
        }

        

        public static bool IsMigrationUnitValid(MigrationUnitBasic mu)
        {
            return mu.SourceStatus == CollectionStatus.OK;
        }

        public static bool CanProceedWithDownloads(string directoryPath,long spaceRequiredInMb, out double folderSizeInGB, out double freeSpaceGB)
        {
            
            freeSpaceGB = 0;
            folderSizeInGB = 0;

            // When using Azure Blob Storage with Entra ID, skip disk space check
            // as blob storage has virtually unlimited capacity
            if (StorageStreamFactory.UseBlobStorage)
            {
                freeSpaceGB = double.MaxValue;
                return true;
            }

            if (!Directory.Exists(directoryPath)) 
            {
                return true;
            }


            double freeSpaceInMb= 0;
            if (IsWindows())
            {
                DriveInfo drive = new DriveInfo(Path.GetPathRoot(directoryPath));
                freeSpaceInMb = drive.AvailableFreeSpace / (1024.0 * 1024);          
            }
            else
            {
                double maxAllowedMb = 100 *1024;
                try
                {
                    var quotaEnv = Environment.GetEnvironmentVariable("STORAGE_QUOTA_GB");
                    maxAllowedMb=double.Parse(quotaEnv) * 1024;
                }
                catch
                {
                    //use default
                }   

                long currentUsageBytes = Directory
                    .EnumerateFiles(directoryPath, "*", SearchOption.AllDirectories)
                    .Sum(f => new FileInfo(f).Length);

                freeSpaceInMb = maxAllowedMb - (currentUsageBytes / (1024.0 * 1024));
            }
            freeSpaceGB = Math.Round(freeSpaceInMb / 1024, 2);
            

            // Check if the total disk available is less than spaceRequiredInMb
            if (freeSpaceInMb < spaceRequiredInMb)
            {
                // Get disk space info
                DirectoryInfo dirInfo = Directory.GetParent(directoryPath)?.Parent.Parent;
                folderSizeInGB = Math.Round(GetFolderSizeInGB(dirInfo.FullName), 2);
                MigrationJobContext.AddVerboseLog($"Checking for Disk Space returned false: directoryPath={directoryPath}, spaceRequiredInMb={spaceRequiredInMb}, freeSpaceInMb={freeSpaceInMb}");
                return false;
            }
            else
            {
                MigrationJobContext.AddVerboseLog($"Checking for Disk Space returned true: directoryPath={directoryPath}, spaceRequiredInMb={spaceRequiredInMb}, freeSpaceInMb={freeSpaceInMb}");
                return true;
            }
              
        }

        public static string EmbedDatabaseNameInConnectionString(string connectionString, string database)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));

            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Database name cannot be null or empty.", nameof(database));

            // Pattern:
            // Capture everything up to host[:port]
            // Optional existing database
            // Optional query string
            var pattern = @"^(mongodb(?:\+srv)?://[^/]+)(?:/[^?]*)?(.*)$";

            return Regex.Replace(
                connectionString,
                pattern,
                $"$1/{database}$2",
                RegexOptions.IgnoreCase
            );
        }

        public static string EncodeMongoPasswordInConnectionString(string connectionString)
        {
       
            // Regex pattern to capture the password part (assuming mongodb://user:password@host)
            string pattern = @"(mongodb(?:\+srv)?:\/\/[^:]+:)(.*)@([^@]+)$";

            Match match = Regex.Match(connectionString, pattern);

            if (match.Success)
            {
                string decodedPassword = Uri.UnescapeDataString(match.Groups[2].Value); //decode if user gave encoded password

                string encodedPassword = Uri.EscapeDataString(decodedPassword); // URL-encode password
                return match.Groups[1].Value + encodedPassword + "@" + match.Groups[3].Value; // Reconstruct the connection string
            }

            // Return the original string if no password is found
            return connectionString;
        }

        public static async Task<bool> ValidateMongoToolsAvailableAsync(Log log)
        {
            MigrationJobContext.AddVerboseLog("ValidateMongoToolsAvailableAsync: checking mongodump/mongorestore");
            try
            {
                var mongodumpCheck = Process.Start(new ProcessStartInfo
                {
                    FileName = "mongodump",
                    Arguments = "--version",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                });
                mongodumpCheck?.WaitForExit();

                if (mongodumpCheck?.ExitCode != 0)
                {
                    throw new Exception("mongodump not found in PATH");
                }

                log.WriteLine("MongoDB tools validated successfully");
                return true;
            }
            catch (Exception ex)
            {
                log.WriteLine($"MongoDB tools not available: {ex.ToString()}", LogType.Error);
                return false;
            }
        }
        public static async Task<string> EnsureMongoToolsAvailableAsync(Log log,string toolsDestinationFolder, MigrationSettings config)
        {
            MigrationJobContext.AddVerboseLog($"EnsureMongoToolsAvailableAsync: toolsDestinationFolder={toolsDestinationFolder}");
            string toolsDownloadUrl = config.MongoToolsDownloadUrl;

            try
            {
                string toolsLaunchFolder = Path.Combine(toolsDestinationFolder, Path.GetFileNameWithoutExtension(toolsDownloadUrl), "bin");

                string mongodumpPath = Path.Combine(toolsLaunchFolder, "mongodump.exe");
                string mongorestorePath = Path.Combine(toolsLaunchFolder, "mongorestore.exe");

                // Check if tools exist
                if (File.Exists(mongodumpPath) && File.Exists(mongorestorePath))
                {
                    log.WriteLine("Environment is ready to use.");
                    
                    return toolsLaunchFolder;
                }

                log.WriteLine("Downloading tools...");

                // Download ZIP file
                string zipFilePath = Path.Combine(toolsDestinationFolder, "mongo-tools.zip");
                Directory.CreateDirectory(toolsDestinationFolder);

                using (HttpClient client = new HttpClient())
                {
                    using (var response = await client.GetAsync(toolsDownloadUrl))
                    {
                        response.EnsureSuccessStatusCode();
                        await using (var fs = new FileStream(zipFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
                        {
                            await response.Content.CopyToAsync(fs);
                        }
                    }
                }

                // Extract ZIP file
                ZipFile.ExtractToDirectory(zipFilePath, toolsDestinationFolder, overwriteFiles: true);
                File.Delete(zipFilePath);

                if (File.Exists(mongodumpPath) && File.Exists(mongorestorePath))
                {
                    log.WriteLine("Environment is ready to use.");
                    
                    return toolsLaunchFolder;
                }
                log.WriteLine("Environment setup failed.", LogType.Error);
                
                return string.Empty;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error: {ex}", LogType.Error);
                
                return string.Empty;
            }
        }

        #region Logging

        /// <summary>
        /// Logs a message to the debug log file with timestamp
        /// </summary>
        /// <param name="message">The message to log</param>
        public static void LogToFile(string message, string striFileName = "AutoStartLog.txt")
        {
            try
            {
                string path = Path.Combine(Helper.GetWorkingFolder(), striFileName);
                string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                string logEntry = $"[{timestamp} UTC] {message}{Environment.NewLine}";
                
                if (StorageStreamFactory.UseBlobStorage)
                {
                    // Append to Blob Storage
                    StorageStreamFactory.AppendText(path, logEntry);
                }
                else
                {
                    // Append to local file
                    System.IO.File.AppendAllText(path, logEntry);
                }
            }
            catch
            {
                // Silently ignore logging errors to prevent application crashes
            }
        }

        #endregion 

        public static string GetWorkingFolder()
        {
            MigrationJobContext.AddVerboseLog($"GetWorkingFolder: _workingFolder={_workingFolder}");

            if (!string.IsNullOrEmpty(_workingFolder))
            {
                return _workingFolder;
            }

            if (!IsWindows())
            {
                _workingFolder = $"{Environment.GetEnvironmentVariable("ResourceDrive")}/{MigrationJobContext.AppId}/";

                // Only create local directory if not using Blob Storage
                if (!StorageStreamFactory.UseBlobStorage)
                {
                    if (!System.IO.Directory.Exists(_workingFolder))
                        System.IO.Directory.CreateDirectory(_workingFolder);
                }

                return _workingFolder;
            }

            //back ward compatibility, old code used to create a folder in temp path
            if (System.IO.Directory.Exists($"{Path.GetTempPath()}migrationjobs"))
            {
                _workingFolder = Path.GetTempPath();
                return _workingFolder;
            }
            //back ward compatibility end


            string homePath = Environment.GetEnvironmentVariable("ResourceDrive");

            if (string.IsNullOrEmpty(homePath))
            {
                _workingFolder = Path.GetTempPath();
            }


            if (! string.IsNullOrEmpty(homePath) && System.IO.Directory.Exists(Path.Combine(homePath, "home\\")))
            {
                _workingFolder = Path.Combine(homePath, "home\\");
            }
            return _workingFolder;
        }

        public static string UpdateAppName(string connectionString, string appName)
        {
            MigrationJobContext.AddVerboseLog($"UpdateAppName: appName={appName}");
            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));

                if (string.IsNullOrWhiteSpace(appName))
                    throw new ArgumentException("App name cannot be null or empty.", nameof(appName));

                var uri = new Uri(connectionString);
                var queryParams = HttpUtility.ParseQueryString(uri.Query);

                // Set or update the appName parameter
                queryParams["appName"] = appName;

                // Reconstruct the connection string with updated parameters
                var newQuery = queryParams.ToString();
                var updatedConnectionString = connectionString.Replace(uri.Query.ToString(), "?" + newQuery);

                return updatedConnectionString;
            }
            catch (Exception)
            {                
                return connectionString; // Return the original connection string in case of error
            }
        }

        public static long GetMigrationUnitDocCount(MigrationUnit mu)
        {
            if (mu.UserFilter != null && mu.UserFilter.Any())
                return mu.ActualDocCount;
            else
               return Math.Max(mu.ActualDocCount, mu.EstimatedDocCount);
        }

        public static (long Total, long Inserted, long Skipped, long Failed) GetProcessedTotals(MigrationUnit mu)
        {
            long skipped = mu.MigrationChunks?.Sum(c => c.SkippedAsDuplicateCount) ?? 0;
            long inserted = (mu.MigrationChunks?.Sum(c => c.RestoredSuccessDocCount) ?? 0) - skipped;
            long failed = mu.MigrationChunks?.Sum(c => c.RestoredFailedDocCount) ?? 0;
            long total = inserted + skipped + failed;
            return (total, inserted, skipped, failed);
        }

        public static string GetTimestampDiff(MigrationUnitBasic mu, bool isSyncBack)
        {
            DateTime timestamp = isSyncBack ? mu.SyncBackCursorUtcTimestamp : mu.CursorUtcTimestamp;
            if (timestamp == DateTime.MinValue || mu.ResetChangeStream)
                return "NA";
            
            return GetTimestampDiff(timestamp);
        }

        public static string GetTimestampDiff(DateTime timestamp)
        {
            var lag = DateTime.UtcNow - timestamp;
            if (lag.TotalSeconds < 0) return "Invalid";

            // Enhanced lag reporting with more granular information
            if (lag.TotalSeconds < 60)
                return $"{(int)lag.TotalSeconds} sec";
            else if (lag.TotalMinutes < 60)
                return $"{(int)lag.TotalMinutes} min {(int)lag.Seconds} sec";
            else
                return $"{(int)lag.TotalHours}h {(int)lag.Minutes}m";
        }


        public static string GetChangeStreamMode(MigrationJob job)
        {
            if (job == null || !Helper.IsOnline(job))
                return "N/A";

            return job.ChangeStreamMode.ToString();
        }

       
        public static bool IsRU(string connectionString)
        {
            return connectionString.Contains("mongo.cosmos.azure.com");
        }         

        public static async Task<List<MigrationUnit>> PopulateJobCollectionsAsync(MigrationJob job,string namespacesToMigrate, string connectionString, bool allCollectionsUseObjectId = false)
        {
            MigrationJobContext.AddVerboseLog($"PopulateJobCollectionsAsync: jobId={job?.Id}, allCollectionsUseObjectId={allCollectionsUseObjectId}");

            List<MigrationUnit> unitsToAdd = new List<MigrationUnit>();
            if (string.IsNullOrWhiteSpace(namespacesToMigrate))
            {
                return new List<MigrationUnit>();
            }

            //desrialize  input into  List of CollectionInfo
            List<CollectionInfo>? loadedObject = null;
            try
            {
                if (!string.IsNullOrEmpty(namespacesToMigrate))
                {
                    loadedObject = JsonConvert.DeserializeObject<List<CollectionInfo>>(namespacesToMigrate)!;
                }
            }
            catch
            {
                //do nothing
            }
            if (loadedObject != null)
            {
                foreach (var item in loadedObject)
                {

                    var tmpList = await PopulateJobCollectionsFromCSVAsync(job,$"{item.DatabaseName.Trim()}.{item.CollectionName.Trim()}", connectionString,false);
                    if (tmpList.Count > 0)
                    {
                        foreach (var mu in tmpList)
                        {
                            // Ensure no duplicates based on DatabaseName.CollectionName
                            if (!unitsToAdd.Any(x => x.DatabaseName == mu.DatabaseName && x.CollectionName == mu.CollectionName))
                            {
                                mu.UserFilter = item.Filter;


                                if (!string.IsNullOrEmpty(item.DataTypeFor_Id) && Enum.TryParse<DataType>(item.DataTypeFor_Id, out var parsedDataType))
                                {
                                    mu.DataTypeFor_Id = parsedDataType;
                                }
                                else
                                {
                                    mu.DataTypeFor_Id = null;
                                }
                                unitsToAdd.Add(mu);
                            }
                        }
                    }
                }
            }
            else
            {
                unitsToAdd = await PopulateJobCollectionsFromCSVAsync(job,namespacesToMigrate, connectionString);
                
                
            }
            // If allCollectionsUseObjectId is true, set DataTypeFor_Id to ObjectId for all units
            if (allCollectionsUseObjectId)
            {
                foreach (var mu in unitsToAdd)
                {
                    mu.DataTypeFor_Id = DataType.ObjectId;
                }
            }


            return unitsToAdd;
        }

        //public static bool CreateFolderIfNotExists(string folderPath)
        //{
        //    if (!Directory.Exists(folderPath))
        //    {
        //        Directory.CreateDirectory(folderPath);
        //    }
        //    return true;
        //}

        public static bool  AddMigrationUnits(List<MigrationUnit> unitsToAdd, MigrationJob job, Log log=null)
        {
            var newUnits = unitsToAdd
                .Where(mu => !job.MigrationUnitBasics
                .Any(mub => mub.Id == Helper.GenerateMigrationUnitId(mu.DatabaseName, mu.CollectionName)))
                .ToList();

            if (newUnits.Count > 0)
            {
                if(log!=null)
                    log.WriteLine($"Adding {newUnits.Count} migration units to job", LogType.Debug);

                foreach (var mu in newUnits)
                {
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    AddMigrationUnit(mu,job);
                }
                MigrationJobContext.SaveMigrationJob(job);
            }
            return true;
        }

        private static void AddMigrationUnit(MigrationUnit mu, MigrationJob job)
        {
            if (job == null)
            {
                return;
            }
            if (job?.MigrationUnitBasics == null)
            {
                job!.MigrationUnitBasics = new List<MigrationUnitBasic>();
            }

            // Check if the MigrationUnit already exists
            if (job.MigrationUnitBasics.Find(m => m.Id == mu.Id) != null)
            {
                return;
            }
            mu.ParentJob = job;
            job.MigrationUnitBasics.Add(mu.GetBasic());
        }

        private static async Task<List<MigrationUnit>> PopulateJobCollectionsFromCSVAsync(MigrationJob job,string namespacesToMigrate, string connectionString, bool split=true)
        {
            List<MigrationUnit> unitsToAdd = new List<MigrationUnit>();

            string[] collectionsInput;
            if (split)
            {
                collectionsInput = namespacesToMigrate
                    .Split(',')
                    .Select(mu => mu.Trim())
                    .ToArray();
            }
            else
            {
                collectionsInput= new string[] { namespacesToMigrate.Trim() };
            }

            foreach (var fullName in collectionsInput)
            {
                int firstDotIndex = fullName.IndexOf('.');
                if (firstDotIndex <= 0 || firstDotIndex == fullName.Length - 1) continue;

                string dbName = fullName.Substring(0, firstDotIndex).Trim();
                string colName = fullName.Substring(firstDotIndex + 1).Trim();

                // Handle wildcards - require connection string
                if ((dbName == "*" || colName == "*") && string.IsNullOrWhiteSpace(connectionString))
                {
                    // Cannot process wildcards without connection string - skip this entry
                    continue;
                }

                // Handle wildcards with connection string
                if (dbName == "*" && colName == "*")
                {
                    // Get all databases and all collections from each database
                    var databases = await MongoHelper.ListDatabasesAsync(connectionString);
                    foreach (var database in databases)
                    {
                        var collections = await MongoHelper.ListCollectionsAsync(connectionString, database);
                        foreach (var collection in collections)
                        {
                            if (!unitsToAdd.Any(x => x.DatabaseName == database && x.CollectionName == collection))
                            {
                                var migrationUnit = new MigrationUnit( job, database, collection, new List<MigrationChunk>());
                                unitsToAdd.Add(migrationUnit);
                            }
                        }
                    }
                }
                else if (dbName == "*" && colName != "*")
                {
                    // Get all databases and find the specific collection in each
                    var databases = await MongoHelper.ListDatabasesAsync(connectionString);
                    foreach (var database in databases)
                    {
                        var collections = await MongoHelper.ListCollectionsAsync(connectionString, database);
                        if (collections.Contains(colName, StringComparer.OrdinalIgnoreCase))
                        {
                            if (!unitsToAdd.Any(x => x.DatabaseName == database && x.CollectionName == colName))
                            {
                                var migrationUnit = new MigrationUnit( job, database, colName, new List<MigrationChunk>());
                                unitsToAdd.Add(migrationUnit);
                            }
                        }
                    }
                }
                else if (dbName != "*" && colName == "*")
                {
                    // Get all collections from the specific database
                    var collections = await MongoHelper.ListCollectionsAsync(connectionString, dbName);
                    foreach (var collection in collections)
                    {
                        if (!unitsToAdd.Any(x => x.DatabaseName == dbName && x.CollectionName == collection))
                        {
                            var migrationUnit = new MigrationUnit( job, dbName, collection, new List<MigrationChunk>());
                            unitsToAdd.Add(migrationUnit);
                        }
                    }
                }
                else
                {
                    // No wildcards, use as-is
                    if (!unitsToAdd.Any(x => x.DatabaseName == dbName && x.CollectionName == colName))
                    {
                        var migrationUnit = new MigrationUnit( job, dbName, colName, new List<MigrationChunk>());
                        unitsToAdd.Add(migrationUnit);
                    }
                }
            }

            return unitsToAdd;
        }

        public static Tuple<bool, string,string> ValidateNamespaceFormat(string input, JobType jobType)
        {
            
            string  errorMessage = string.Empty;
            if (string.IsNullOrWhiteSpace(input))
            {
                errorMessage="Namespaces cannot be null or empty.";
                return new Tuple<bool, string, string>(false, string.Empty, errorMessage);
            }

            //input can  be CSV or JSON format

            //desrialize  input into  List of CollectionInfo
            List<CollectionInfo>? loadedObject=null;
            try
            {
                 loadedObject = JsonConvert.DeserializeObject<List<CollectionInfo>>(input);
            }
            catch
            {
                //do nothing
            }
            if (loadedObject != null)
            {
                if(jobType==JobType.RUOptimizedCopy)
                {
                    if (loadedObject.Any(x => x.Filter != null))
                    {
                        errorMessage = "Filter is not supported in RU Optimized Copy job type.";
                        return new Tuple<bool, string, string>(false, string.Empty, errorMessage);
                    }                  
                }

                foreach (var item in loadedObject)
                {                   

                    if(!string.IsNullOrEmpty(item.Filter) && !FilterInspector.HasValidIdFilter(item.Filter))
                    {
                        errorMessage = $"{item.DatabaseName}.{item.CollectionName} filter uses unsupported operators for _id. Only $gte and $lt are supported.";
                        return new Tuple<bool, string, string>(false, string.Empty, errorMessage);
                    }

                    var validationResult = ValidateNamespaceFormatfromCSV($"{item.DatabaseName.Trim()}.{item.CollectionName.Trim()}",false);
                    if (!validationResult.Item1)
                    {
                        errorMessage = validationResult.Item2;
                        return new Tuple<bool, string,string>(false, string.Empty, errorMessage);
                    }                     
                }
                return new Tuple<bool, string,string >(true, input, errorMessage);
            }
            else
            {
                return ValidateNamespaceFormatfromCSV(input);
            }
        }
        private static Tuple<bool, string, string> ValidateNamespaceFormatfromCSV(string input, bool split=true)
        {
            // Regular expression pattern to match db1.col1, db2.col2, db3.col4 format, with support for wildcards
            // Allow * for database name or collection name
            string pattern = @"^([^\/\\\.\x00\""\<\>\|\?\s]+|\*)\.{1}([^\/\\\x00\""\<\>\|\?]+|\*)$";
            string[] items;

            if (!split)            
                items = new string[] { input.Trim() };
            else
                items = input.Split(',');

            // Use a HashSet to ensure no duplicates
            HashSet<string> validItems = new HashSet<string>();

            foreach (string mu in items)
            {
                string trimmedItem = mu.Trim(); // Remove any extra whitespace
                if (Regex.IsMatch(trimmedItem, pattern))
                {
                    //Console.WriteLine($"'{trimmedItem}' matches the pattern.");
                    validItems.Add(trimmedItem); // HashSet ensures uniqueness
                }
                else
                {
                    string errorMessage = $"Invalid namespace format: '{trimmedItem}'. Use 'database.collection', '*.collection', 'database.*', or '*.*' for wildcards.";
                    return new Tuple<bool, string,string>(false, string.Empty,errorMessage);
                }
            }

            // Join valid items into a cleaned comma-separated list
            var cleanedNamespace = string.Join(",", validItems);
            return new Tuple<bool, string,string>(true, cleanedNamespace,string.Empty);
        }

        public static string RedactPii(string input)
        {
            string pattern = @"(?<=://)([^:]+):([^@]+)";
            string replacement = "[REDACTED]:[REDACTED]";

            // Redact the user ID and password
            return Regex.Replace(input, pattern, replacement);
        }

        public static string SafeFileName(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
            {
                return string.Empty;
            }
            // Remove invalid characters and trim whitespace
            string sanitizedFileName = Regex.Replace(fileName, @"[<>:""/\\|?*]", "_").Trim();
            sanitizedFileName = sanitizedFileName.Replace(" ","_-sp-_");

            // Ensure the file name is not too long
            if (sanitizedFileName.Length > 255)
            {
                sanitizedFileName = sanitizedFileName.Substring(0, 255);
            }
            return sanitizedFileName;
        }

        //public static bool WriteAtomicFile(string filePath, string content, int maxRetries = 5)
        //{
        //    string tempFile = filePath + ".tmp";
        //    bool isNewFile = false;
        //    if (!System.IO.File.Exists(filePath))
        //    {
        //        tempFile = filePath;
        //        isNewFile = true;
        //    }
                        

        //    //Log($"WriteAtomicFile: Writing to {filePath}");

        //    // Write to temp file (fully flushed)
        //    using (var fs = new FileStream(
        //        tempFile,
        //        FileMode.Create,
        //        FileAccess.Write,
        //        FileShare.None,
        //        4096,
        //        FileOptions.WriteThrough))

        //    using (var sw = new StreamWriter(fs))
        //    {
        //        sw.Write(content);
        //        sw.Flush();
        //        fs.Flush(true);
        //    }

        //    //Log($"Flush complete {filePath}");

        //    if (isNewFile)
        //    {
        //        return true;
        //    }

        //    int attempt = 0;

        //    while (attempt < maxRetries)
        //    {
        //        try
        //        {
        //            string backupFile = $"{filePath}.backup";

        //            // If original exists → back it up
        //            if (File.Exists(filePath))
        //            {
        //                SafeDelete(backupFile);
        //                SafeMove(filePath, backupFile);
        //            }

        //            // Move new file into place
        //            SafeMove(tempFile, filePath);

        //            // Cleanup backup (best effort)
        //            SafeDelete(backupFile);

        //            return true; // DONE
        //        }
        //        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        //        {
        //            attempt++;


        //            if (attempt >= maxRetries)
        //            {
        //                SafeDelete(tempFile);
        //                throw new IOException(
        //                    $"Failed to write atomic file '{filePath}' after {maxRetries} attempts.",
        //                    ex);
        //            }

        //            Thread.Sleep(1000);
        //        }
        //    }

        //    return false;
        //}

        //private static void SafeDelete(string path)
        //{
        //    try
        //    {
        //        if (File.Exists(path))
        //            File.Delete(path);
        //    }
        //    catch { }
        //}

        //private static void SafeMove(string src, string dest)
        //{
        //    File.Move(src, dest); // keep exceptions — this is the "atomic" part
        //}


        public static string GenerateMigrationUnitId(string databaseName, string collectionName)
        {
            using var sha256 = SHA256.Create();
            var input = $"{databaseName}.{collectionName}";
            return GenerateMigrationUnitId(input);
        }

        public static string GenerateMigrationUnitId(string collectionKey)
        {
            using var sha256 = SHA256.Create();
            var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(collectionKey));

            // Take first 8 bytes (64 bits) -> convert to 16-digit hex
            ulong part = BitConverter.ToUInt64(hashBytes, 0);
            return part.ToString("X16"); // 16 hex digits
        }


        public static List<MigrationUnit> GetMigrationUnitsToMigrate(MigrationJob job)
        {
            var unitsForMigrate = new List<MigrationUnit>();

            if(job.MigrationUnitBasics==null || job.MigrationUnitBasics.Count==0)
            {
                return unitsForMigrate;
            }
            foreach (var mub in job.MigrationUnitBasics!)
            {

                var mu = MigrationJobContext.GetMigrationUnit(mub.Id,job.Id);
                if (mu != null)
                {
                    unitsForMigrate.Add((MigrationUnit)mu);
                }
            }
            return unitsForMigrate;
        }

        public static bool IsOfflineJobCompleted(MigrationJob migrationJob)
        {
            if (migrationJob == null) return true;

            if (migrationJob.IsSimulatedRun)
            {
                foreach (var mu in migrationJob.MigrationUnitBasics)
                {
                    //var mu = jobList.GetMigrationUnit(migrationJob.Id, id);
                    if (Helper.IsMigrationUnitValid(mu))
                    {
                        if (!mu.DumpComplete)
                            return false;

                    }
                }
                return true;
            }
            else
            {

                foreach (var mu  in migrationJob.MigrationUnitBasics)
                {
                    //var mu = jobList.GetMigrationUnit(migrationJob.Id, id);
                    if (Helper.IsMigrationUnitValid(mu))
                    {
                        if (!mu.RestoreComplete || !mu.DumpComplete)
                            return false;
                    }
                }
                return true;
            }
        }


        public static bool AnyValidCollection(MigrationJob migrationJob)
        {
            if (migrationJob == null) return false;

            foreach (var mu in migrationJob.MigrationUnitBasics)
            {
                if (Helper.IsMigrationUnitValid(mu))
                {
                    return true;
                }
            }
            return false;

        }

        public static string ExtractHost(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return string.Empty;
            }

            try
            {
                // Find the starting position of the host (after "://")
                var startIndex = EncodeMongoPasswordInConnectionString(connectionString).IndexOf("://") + 3;
                if (startIndex < 3 || startIndex >= connectionString.Length)
                    return string.Empty;

                // Find the end position of the host (before "/" or "?")
                var endIndex = connectionString.IndexOf("/", startIndex);
                if (endIndex == -1)
                    endIndex = connectionString.IndexOf("?", startIndex);
                if (endIndex == -1)
                    endIndex = connectionString.Length;

                // Extract and return the host
                return connectionString.Substring(startIndex, endIndex - startIndex).Split('@')[1];
            }
            catch
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Checks if the current operating system is Windows.
        /// </summary>
        /// <returns>True if running on Windows, false otherwise</returns>
        public static bool IsWindows()
        {
            return OperatingSystem.IsWindows();
        }
    }
}