using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8600

    internal class ProcessExecutor
    {
        private static bool _migrationCancelled = false;

        /// <summary>
        /// Executes a process with the given executable path and arguments.
        /// </summary>
        /// <param name="exePath">The full path to the executable file.</param>
        /// <param name="arguments">The arguments to pass to the executable.</param>
        /// <returns>True if the process completed successfully, otherwise false.</returns>
        public bool Execute(JobList jobList, MigrationUnit item, MigrationChunk chunk, double basePercent, double contribFactor, long targetCount, string exePath, string arguments)
        {
            int pid;
            string processType = string.Empty;
            try
            {
                // Determine process type and active process ID
                if (exePath.ToLower().Contains("restore"))
                {
                    processType = "MongoRestore";
                    pid = jobList.ActiveRestoreProcessId;
                }
                else
                {
                    processType = "MongoDump";
                    pid = jobList.ActiveDumpProcessId;
                }

                // Kill any existing process
                if (pid > 0)
                {
                    try
                    {
                        var existingProcess = Process.GetProcessById(pid);
                        existingProcess?.Kill();
                    }
                    catch { }
                }

                using (var process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = exePath,
                        Arguments = arguments,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    }
                })
                {
                    // Capture output and error data synchronously
                    StringBuilder outputBuffer = new StringBuilder();
                    StringBuilder errorBuffer = new StringBuilder();

                    process.OutputDataReceived += (sender, args) =>
                    {
                        if (!string.IsNullOrEmpty(args.Data))
                        {
                            outputBuffer.AppendLine(args.Data);
                            Log.WriteLine(Helper.RedactPii(args.Data));
                        }
                    };

                    process.ErrorDataReceived += (sender, args) =>
                    {
                        if (!string.IsNullOrEmpty(args.Data))
                        {
                            errorBuffer.AppendLine(args.Data);
                            ProcessErrorData(args.Data, processType, item, chunk, basePercent, contribFactor, targetCount, jobList);
                        }
                    };

                    process.Start();
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    if (processType == "MongoRestore")
                        jobList.ActiveRestoreProcessId = process.Id;
                    else
                        jobList.ActiveDumpProcessId = process.Id;

                    while (!process.WaitForExit(1000)) // Poll every sec
                    {
                        if (_migrationCancelled)
                        {
                            try
                            {
                                process.Kill();
                                Log.WriteLine($"{processType} Process terminated due to cancellation.");
                                _migrationCancelled = false;
                                break;
                            }
                            catch (Exception ex)
                            {
                                Log.WriteLine($"Error terminating process {processType}: {Helper.RedactPii(ex.Message)}", LogType.Error);
                            }
                        }
                    }

                    if (processType == "MongoRestore")
                        jobList.ActiveRestoreProcessId = 0;
                    else
                        jobList.ActiveDumpProcessId = 0;

                    Log.Save();
                    return process.ExitCode == 0;
                }
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error executing process {processType}: {Helper.RedactPii(ex.Message)}", LogType.Error);
                Log.Save();
                return false;
            }
        }

        private void ProcessErrorData(string data, string processType, MigrationUnit item, MigrationChunk chunk, double basePercent, double contribFactor, long targetCount, JobList jobList)
        {
            string percentValue = ExtractPercentage(data);
            string docsProcessed = ExtractDocCount(data, string.Empty);

            double percent = 0;
            int count;

            if (!string.IsNullOrEmpty(percentValue))
                double.TryParse(percentValue, out percent);

            if (!string.IsNullOrEmpty(docsProcessed) && int.TryParse(docsProcessed, out count) && count > 0)
            {
                percent = Math.Round(((double)count / targetCount) * 100, 3);
            }

            if (percent > 0)
            {
                Log.AddVerboseMessage($"{processType} Chunk Percentage: {percent}");
                if (processType == "MongoRestore")
                {
                    item.RestorePercent = basePercent + (percent * contribFactor);
                    if (item.RestorePercent == 100)
                        item.RestoreComplete = true;
                }
                else
                {
                    item.DumpPercent = basePercent + (percent * contribFactor);
                    if (item.DumpPercent == 100)
                        item.DumpComplete = true;
                }
                jobList.Save();
            }
            else
            {
                if (processType == "MongoRestore")
                {
                    var (restoredCount, failedCount) = ExtractRestoreCounts(data);
                    if (restoredCount > 0 || failedCount > 0)
                    {
                        chunk.RestoredSuccessDocCount = restoredCount;
                        chunk.RestoredFailedDocCount = failedCount;
                    }
                }
                else
                {
                    var dumpedDocCount = ExtractDumpedDocumentCount(data);
                    if (dumpedDocCount > 0)
                    {
                        chunk.DumpResultDocCount = dumpedDocCount;
                    }
                }
                if (!data.Contains("continuing through error: Duplicate key violation on the requested collection"))
                {
                    Log.WriteLine($"{processType} Response: {Helper.RedactPii(data)}");
                }
            }
        }

        private string ExtractPercentage(string input)
        {
            // Regular expression to match the percentage value in the format (x.y%)
            var match = Regex.Match(input, @"\(([\d.]+)%\)");
            if (match.Success)
            {
                return match.Groups[1].Value; // Extract the percentage value without the parentheses and %
            }
            return string.Empty;
        }

        private string ExtractDocCount(string input, string prefix)
        {
            // Regular expression to match the percentage value in the format (x.y%)
            var match = Regex.Match(input, @"\s+(\d+)$");
            if (match.Success)
            {
                return match.Groups[1].Value; // Extract the doc count value 
            }
            return string.Empty;
        }

        public (int RestoredCount, int FailedCount) ExtractRestoreCounts(string input)
        {
            // Regular expressions to capture the counts
            var restoredMatch = Regex.Match(input, @"(\d+)\s+document\(s\)\s+restored\s+successfully");
            var failedMatch = Regex.Match(input, @"(\d+)\s+document\(s\)\s+failed\s+to\s+restore");

            // Extract counts with default value of 0 if no match
            int restoredCount = restoredMatch.Success ? int.Parse(restoredMatch.Groups[1].Value) : 0;
            int failedCount = failedMatch.Success ? int.Parse(failedMatch.Groups[1].Value) : 0;

            return (restoredCount, failedCount);
        }

        public int ExtractDumpedDocumentCount(string input)
        {
            // Define the regex pattern to match "done" followed by document count
            string pattern = @"\bdone dumping.*\((\d+)\s+documents\)";
            var match = Regex.Match(input, pattern);

            // Check if the regex matched
            if (match.Success)
            {
                // Parse and return the document count
                return int.Parse(match.Groups[1].Value);
            }

            // Return 0 if no match found
            return 0;
        }

        /// <summary>
        /// Terminates the currently running process, if any.
        /// </summary>
        public void Terminate()
        {
            _migrationCancelled = true;
        }
    }
}

