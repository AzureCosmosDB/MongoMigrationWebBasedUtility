﻿using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Models;
using SharpCompress.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using System.Xml.Linq;
using MongoDB.Driver;
using MongoDB.Bson;
using OnlineMongoMigrationProcessor.Helpers;
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
                Console.WriteLine("Folder does not exist.");
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
                Console.WriteLine($"Access denied: {e.ToString()}");
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.ToString()}");
                return 0;
            }
        }

        

        public static bool IsMigrationUnitValid(MigrationUnit mu)
        {
            return mu.SourceStatus == CollectionStatus.OK;
        }

        public static bool CanProceedWithDownloads(string directoryPath,long spaceRequiredInMb, out double folderSizeInGB, out double freeSpaceGB)
        {
            freeSpaceGB = 0;
            folderSizeInGB = 0;

            if (!Directory.Exists(directoryPath)) 
            {
                return true;
            }


            DriveInfo drive = new DriveInfo(Path.GetPathRoot(directoryPath));
            double freeSpaceInMb = drive.AvailableFreeSpace / (1024.0 * 1024);

            // Check if the total disk available is less than 5 GB
            if (freeSpaceInMb < spaceRequiredInMb)
            {
                // Get disk space info


                DirectoryInfo dirInfo = Directory.GetParent(directoryPath)?.Parent.Parent;

                folderSizeInGB = Math.Round(GetFolderSizeInGB(dirInfo.FullName), 2);
                freeSpaceGB = Math.Round(freeSpaceInMb /1024, 2);

                return false;
            }
            else
            {
                return true;
            }
              
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


        public static async Task<string> EnsureMongoToolsAvailableAsync(Log log,string toolsDestinationFolder, MigrationSettings config)
        {
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



        public static string GetWorkingFolder()
        {

            if (!string.IsNullOrEmpty(_workingFolder))
            {
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
            
            if(! string.IsNullOrEmpty(homePath) && System.IO.Directory.Exists(Path.Combine(homePath, "home//")))
            {
                _workingFolder = Path.Combine(homePath, "home//");
            }
            return _workingFolder;
        }

        public static string UpdateAppName(string connectionString, string appName)
        {
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

        public static string GetChangeStreamLag(MigrationUnit unit, bool isSyncBack)
        {
            DateTime timestamp = isSyncBack ? unit.SyncBackCursorUtcTimestamp : unit.CursorUtcTimestamp;
            if (timestamp == DateTime.MinValue || unit.ResetChangeStream)
                return "NA";
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

        public static double GetChangeStreamLagSeconds(MigrationUnit unit, bool isSyncBack)
        {
            DateTime timestamp = isSyncBack ? unit.SyncBackCursorUtcTimestamp : unit.CursorUtcTimestamp;
            if (timestamp == DateTime.MinValue || unit.ResetChangeStream)
                return 0;
            var lag = DateTime.UtcNow - timestamp;
            return lag.TotalSeconds < 0 ? 0 : lag.TotalSeconds;
        }

        public static (string Display, double Seconds, bool IsHighLag) GetChangeStreamLagMetrics(MigrationUnit unit, bool isSyncBack, double maxAcceptableLagSeconds = 30)
        {
            var lagSeconds = GetChangeStreamLagSeconds(unit, isSyncBack);
            var lagDisplay = GetChangeStreamLag(unit, isSyncBack);
            var isHighLag = lagSeconds > maxAcceptableLagSeconds;
            
            return (lagDisplay, lagSeconds, isHighLag);
        }

        public static string GetChangeStreamMode(MigrationJob job)
        {
            if (job == null || !Helper.IsOnline(job))
                return "N/A";

            if (job.AggresiveChangeStream)
                return "Aggressive";
            else if (job.CSStartsAfterAllUploads)
                return "Delayed";
            else
                return "Immediate";
        }

       
        public static bool IsRU(string connectionString)
        {
            return connectionString.Contains("mongo.cosmos.azure.com");
        }         

        public static async Task<List<MigrationUnit>> PopulateJobCollectionsAsync(string namespacesToMigrate, string connectionString)
        {
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
                    var tmpList = await PopulateJobCollectionsFromCSVAsync($"{item.DatabaseName.Trim()}.{item.CollectionName.Trim()}", connectionString);
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
                unitsToAdd = await PopulateJobCollectionsFromCSVAsync(namespacesToMigrate, connectionString);                
            }

           

            return unitsToAdd;
        }



        public static void AddMigrationUnit(MigrationUnit mu, MigrationJob job)
        {
            if (job == null)
            {
                return;
            }
            if (job?.MigrationUnits == null)
            {
                job!.MigrationUnits = new List<MigrationUnit>();
            }

            // Check if the MigrationUnit already exists
            if (job.MigrationUnits.Any(existingMu => existingMu.DatabaseName == mu.DatabaseName && existingMu.CollectionName == mu.CollectionName))
            {
                return;
            }
            job.MigrationUnits.Add(mu);
        }

        private static async Task<List<MigrationUnit>> PopulateJobCollectionsFromCSVAsync(string namespacesToMigrate, string connectionString)
        {
            List<MigrationUnit> unitsToAdd = new List<MigrationUnit>();

            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(mu => mu.Trim())
                .ToArray();

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
                                var migrationUnit = new MigrationUnit(database, collection, new List<MigrationChunk>());
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
                                var migrationUnit = new MigrationUnit(database, colName, new List<MigrationChunk>());
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
                            var migrationUnit = new MigrationUnit(dbName, collection, new List<MigrationChunk>());
                            unitsToAdd.Add(migrationUnit);
                        }
                    }
                }
                else
                {
                    // No wildcards, use as-is
                    if (!unitsToAdd.Any(x => x.DatabaseName == dbName && x.CollectionName == colName))
                    {
                        var migrationUnit = new MigrationUnit(dbName, colName, new List<MigrationChunk>());
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
                        errorMessage = $"Filter for {item.DatabaseName}.{item.CollectionName} uses unsupported operators. Only $gte and $lt are supported.";
                        return new Tuple<bool, string, string>(false, string.Empty, errorMessage);
                    }

                    var validationResult = ValidateNamespaceFormatfromCSV($"{item.DatabaseName.Trim()}.{item.CollectionName.Trim()}");
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
        private static Tuple<bool, string, string> ValidateNamespaceFormatfromCSV(string input)
        {
            // Regular expression pattern to match db1.col1, db2.col2, db3.col4 format, with support for wildcards
            // Allow * for database name or collection name
            string pattern = @"^([^\/\\\.\x00\""\<\>\|\?\s]+|\*)\.{1}([^\/\\\x00\""\<\>\|\?]+|\*)$";

            // Split the input by commas
            string[] items = input.Split(',');

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

        public static bool IsOfflineJobCompleted(MigrationJob migrationJob)
        {
            if (migrationJob == null) return true;

            if (migrationJob.IsSimulatedRun)
            {
                foreach (var mu in migrationJob.MigrationUnits)
                {
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

                foreach (var mu in migrationJob.MigrationUnits)
                {
                    if (Helper.IsMigrationUnitValid(mu))
                    {
                        if (!mu.RestoreComplete || !mu.DumpComplete)
                            return false;
                    }
                }
                return true;
            }
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
    }
}

















































































