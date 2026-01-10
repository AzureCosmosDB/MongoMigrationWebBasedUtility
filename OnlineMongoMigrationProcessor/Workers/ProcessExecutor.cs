using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml.Linq;
using static System.Net.WebRequestMethods;

#pragma warning disable CS8600
namespace OnlineMongoMigrationProcessor.Workers
{
    internal class ProcessExecutor
    {
        private Log _log;
        private Process? _process = null;
        private readonly object _processLock = new object();
        private CancellationToken _cancellationToken;
        
        public ProcessExecutor(Log log)
        {
            _log = log;
		}

        /// <summary>
        /// Executes a process with the given executable path and arguments.
        /// </summary>
        /// <param name="jobList">The MigrationJobContext.MigrationJob list for saving state.</param>
        /// <param name="mu">Migration mu.</param>
        /// <param name="chunk">Migration chunk.</param>
        /// <param name="chunkIndex">Index of the chunk.</param>
        /// <param name="basePercent">Base percentage for progress calculation.</param>
        /// <param name="contribFactor">Contribution factor for progress calculation.</param>
        /// <param name="targetCount">Target document count.</param>
        /// <param name="exePath">The full path to the executable file.</param>
        /// <param name="arguments">The arguments to pass to the executable.</param>
        /// <param name="cancellationToken">Cancellation token for graceful shutdown.</param>
        /// <param name="onProcessStarted">Callback when process starts with PID.</param>
        /// <param name="onProcessEnded">Callback when process ends with PID.</param>
        /// <returns>True if the process completed successfully, otherwise false.</returns>
        public bool Execute(
            MigrationUnit mu,
            MigrationChunk chunk,
            int chunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            string exePath,
            string arguments,
            string outputFilePath,
            CancellationToken cancellationToken,
            Action<int>? onProcessStarted = null,
            Action<int>? onProcessEnded = null)
        {
            MigrationJobContext.AddVerboseLog($"ProcessExecutor.Execute: mu={mu.DatabaseName}.{mu.CollectionName}, chunkIndex={chunkIndex}, exePath={exePath}");
            _cancellationToken = cancellationToken;
            string processType = exePath.ToLower().Contains("restore") ? "MongoRestore" : "MongoDump";


            try
            {
                lock (_processLock)
                {
                    _process = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            FileName = exePath,
                            Arguments = arguments,
                            RedirectStandardOutput = true,
                            RedirectStandardError = true,
                            RedirectStandardInput = true,   // needed for restore streaming
                            UseShellExecute = false,
                            CreateNoWindow = true
                        }
                    };
                }

                StringBuilder outputBuffer = new StringBuilder();
                StringBuilder errorBuffer = new StringBuilder();

                _process.OutputDataReceived += (sender, args) =>
                {
                    if (!string.IsNullOrEmpty(args.Data))
                    {
                        outputBuffer.AppendLine(args.Data);
                        MigrationJobContext.AddVerboseLog($"{processType} Log: {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] {Helper.RedactPii(args.Data)}");
                    }
                };

                _process.ErrorDataReceived += (sender, args) =>
                {
                    if (!string.IsNullOrEmpty(args.Data))
                    {
                        errorBuffer.AppendLine(args.Data);
                        ProcessConsoleOutput(args.Data, processType, mu, chunk, chunkIndex, basePercent, contribFactor, targetCount);
                    }
                };

                _process.Start();
                int processId = _process.Id;
                onProcessStarted?.Invoke(processId);
                _log.WriteLine($"{processType} process started: PID {processId} for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}]", LogType.Debug);

                if (processType == "MongoDump")
                {
                    // stdout is streamed to file → do NOT call BeginOutputReadLine
                    _process.BeginErrorReadLine();   // we still need progress messages from stderr
                }
                else
                {
                    // restore → read console normally
                    _process.BeginOutputReadLine();
                    _process.BeginErrorReadLine();
                }

                // -------------------------
                // Dump => write stdout -> file
                // Restore => read file -> stdin
                // -------------------------
                if (processType == "MongoDump")
                {
                    using var fileStream = new FileStream(
                        outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None,
                        bufferSize: 81920, useAsync: true);

                    _process.StandardOutput.BaseStream.CopyToAsync(fileStream, cancellationToken).Wait(cancellationToken);
                    fileStream.Flush();
                }
                else // MongoRestore
                {
                    using var fileStream = new FileStream(
                        outputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                        bufferSize: 81920, useAsync: true);

                    fileStream.CopyToAsync(_process.StandardInput.BaseStream, cancellationToken).Wait(cancellationToken);
                    _process.StandardInput.Close(); // signal EOF to mongo restore
                }

                // cancellation-aware process wait
                while (!_process.WaitForExit(1000))
                {
                    if (_cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            _process.Kill(entireProcessTree: true);
                            _log.WriteLine($"{processType} process {processId} terminated due to cancellation.");
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Error killing process {processId}: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                        }

                        onProcessEnded?.Invoke(processId);
                        return false;
                    }
                }

                onProcessEnded?.Invoke(processId);

                bool success = _process.ExitCode == 0;
                if (!success)
                    _log.WriteLine($"{processType} process {processId} exited with code {_process.ExitCode}", LogType.Error);

                return success;
            }
            catch (Exception ex) when (ex.Message.Contains("canceled"))
            {
                _log.WriteLine($"{processType} process {_process.Id} canceled", LogType.Debug);
                return false;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error executing {processType}: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return false;
            }
        }


        private void ProcessConsoleOutput(string data, string processType, MigrationUnit mu, MigrationChunk chunk,int chunkIndex, double basePercent, double contribFactor, long targetCount)
        {

            if (processType == "MongoDump")
            {
                string percentValue = ExtractPercentage(data);
                string docsProcessed = ExtractDocCount(data);


                double percent = 0;
                int count=0;

                if (!string.IsNullOrEmpty(percentValue))
                    double.TryParse(percentValue, out percent);

                if (!string.IsNullOrEmpty(docsProcessed) && int.TryParse(docsProcessed, out count) && count > 0)
                {
                    percent = Math.Min(100, Math.Round((double)count / targetCount * 100, 3));
                    chunk.DumpResultDocCount = count;
                    MigrationJobContext.AddVerboseLog($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] Dumped Documents Count: {count}");
                }
                else if (percent > 0 && targetCount > 0 && count == 0)
                {
                    long calculatedCount = (long)(percent / 100.0 * targetCount);
                    chunk.DumpResultDocCount = calculatedCount;
                    MigrationJobContext.AddVerboseLog($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] Calculated Dumped Documents Count: {calculatedCount}");
                }              

                if (percent > 0 && targetCount > 0)
                {
                    _log.ShowInMonitor($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] : {percent}%");
                                        
                    mu.DumpPercent = PercentageUpdater.CalculateOverallPercentFromAllChunks(mu, isRestore: false, log: _log);
                    if (mu.DumpPercent >= 99.99)
                    {
                        mu.DumpComplete = true;
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                    }                
                }
            }
            else
            {
                //sample string
                //2025 - 12 - 16T13: 15:00.445 + 0530    48046 document(s) restored successfully. 2 document(s) failed to restore.

                var (restoredCount, failedCount, restorePercent) = ExtractRestoreCounts(data);
                if (restoredCount > 0 || failedCount > 0)
                {
                    chunk.RestoredSuccessDocCount = restoredCount;
                    chunk.RestoredFailedDocCount = failedCount;
                }
                if (restoredCount == 0 && failedCount == 0 && restorePercent == 100)
                {
                    chunk.IsUploaded = true;
                }               


                // Check if this is a restore progress line with byte size (e.g., "sampledb.MultiIdMixed30gb 1.22GB")
                bool isRestoreProgressWithBytes = processType == "MongoRestore" &&
                                Regex.IsMatch(data, @"[\d.]+\s*(GB|MB|TB|KB)\s*$", RegexOptions.IgnoreCase);

                if (isRestoreProgressWithBytes)
                {
                    _log.ShowInMonitor($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] : {data}");
                }
                else
                {
                    if (!data.Contains("continuing through error: Duplicate key violation on the requested collection"))
                    {
                        MigrationJobContext.AddVerboseLog($"{processType} Response for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}]: {Helper.RedactPii(data)}");
                    }
                    else
                    {
                        MigrationJobContext.AddVerboseLog($"{processType} for {mu.DatabaseName}.{mu.CollectionName} Chunk[{chunkIndex}] : Duplicate key violation encountered, skipping duplicate documents.: {Helper.RedactPii(data)}");
                    }
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

        private string ExtractDocCount(string input)
        {

            var match = Regex.Match(input, @"\s+(\d+)$");
            if (match.Success)
            {
                return match.Groups[1].Value; // Extract the doc count value 
            }
            else
            {
                return ExtractDumpedDocumentCount(input).ToString();
            }

        }


        public (int RestoredCount, int FailedCount, double percentage) ExtractRestoreCounts(string input)
        {
            // Old formats
            var restoredMatch = Regex.Match(input, @"(\d+)\s+document\(s\)\s+restored\s+successfully");
            var failedMatch = Regex.Match(input, @"(\d+)\s+document\(s\)\s+failed\s+to\s+restore");

            // Extract counts with default value of 0
            int restoredCount = restoredMatch.Success ? int.Parse(restoredMatch.Groups[1].Value) : 0;
            int failedCount = failedMatch.Success ? int.Parse(failedMatch.Groups[1].Value) : 0;

            // 🔁 Fallback to NEW format only if old format gave nothing
            if (restoredCount == 0 && failedCount == 0)
            {
                var newFormatMatch = Regex.Match(
                    input,
                    @"\((\d+)\s+documents,\s+(\d+)\s+failures\)"
                );

                if (newFormatMatch.Success)
                {
                    restoredCount = int.Parse(newFormatMatch.Groups[1].Value);
                    failedCount = int.Parse(newFormatMatch.Groups[2].Value);
                }
            }

            double percentage = 0;

            // Keep your existing percentage logic
            if (restoredCount == 0 && failedCount == 0)
            {
                var match = Regex.Match(input, @"\(([\d.]+)%\)");

                if (match.Success)
                {
                    percentage = double.Parse(match.Groups[1].Value);
                }
            }

            return (restoredCount, failedCount, percentage);
        }

        public int ExtractDumpedDocumentCount(string input)
        {
            // Define the regex pattern to match "done" followed by document count
            string pattern = @"\bdone dumping.*\((\d+)\s+documents\)";
            //string pattern = @"\bdone dumping .*?\((\d+)\s+documents\)";

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

        
    }
}

