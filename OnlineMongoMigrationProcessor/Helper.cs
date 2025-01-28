using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    public static class Helper
    {
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
    }
}


