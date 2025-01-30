using SharpCompress.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    public static class Helper
    {

       

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
                Console.WriteLine($"Access denied: {e.Message}");
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");
                return 0;
            }
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


        public static async Task<string> EnsureMongoToolsAvailableAsync(string toolsDestinationFolder, MigrationSettings config)
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
                    Log.WriteLine("Environment ready to use.");
                    Log.Save();
                    return toolsLaunchFolder;
                }

                Log.WriteLine("Downloading tools...");

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
                    Log.WriteLine("Environment ready to use.");
                    Log.Save();
                    return toolsLaunchFolder;
                }
                Log.WriteLine("Environment failed.", LogType.Error);
                Log.Save();
                return string.Empty;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error: {ex}", LogType.Error);
                Log.Save();
                return string.Empty;
            }
        }

        public static Tuple<bool, string> ValidateNamespaceFormat(string input)
        {
            // Regular expression pattern to match db1.col1, db2.col2, db3.col4 format
            string pattern = @"^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+$";

            // Split the input by commas
            string[] items = input.Split(',');

            // Use a HashSet to ensure no duplicates
            HashSet<string> validItems = new HashSet<string>();

            foreach (string item in items)
            {
                string trimmedItem = item.Trim(); // Remove any extra whitespace
                if (Regex.IsMatch(trimmedItem, pattern))
                {
                    Console.WriteLine($"'{trimmedItem}' matches the pattern.");
                    validItems.Add(trimmedItem); // HashSet ensures uniqueness
                }
                else
                {
                    return new Tuple<bool, string>(false, string.Empty);
                }
            }

            // Join valid items into a cleaned comma-separated list
            var cleanedNamespace = string.Join(",", validItems);
            return new Tuple<bool, string>(true, cleanedNamespace);
        }

        public static string RedactPii(string input)
        {
            string pattern = @"(?<=://)([^:]+):([^@]+)";
            string replacement = "[REDACTED]:[REDACTED]";

            // Redact the user ID and password
            return Regex.Replace(input, pattern, replacement);
        }

        public static bool IsOfflineJobCompleted(MigrationJob migrationJob)
        {
            if (migrationJob == null) return true;

            foreach (var mu in migrationJob.MigrationUnits)
            {
                if (!mu.RestoreComplete || !mu.DumpComplete)
                    return false;
            }
            return true;
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


