//using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;

namespace OnlineMongoMigrationProcessor
{

    public class LogBucket
    {
        public List<LogObject>? Logs { get; set; } = new List<LogObject>();
    }

    public class Log
    {
        private LogBucket _logBucket = new LogBucket();
        private List<LogObject> _verboseMessages = new List<LogObject>();
        private string _currentId = string.Empty;

        private static readonly object _verboseLock = new object();
        private static readonly object _readLock = new object();
        private static readonly object _writeLock = new object();
        private static readonly object _initLock = new object();

        public  bool IsInitialized { get; set; } = false;

        public void AddVerboseMessage(string message, LogType LogType = LogType.Message)
        {
            lock (_verboseLock)
            {
                if (_verboseMessages.Count == 5)
                {
                    _verboseMessages.RemoveAt(0); // Remove the oldest mu
                }
                _verboseMessages.Add(new LogObject(LogType, message)); // Add the new mu
            }
        }


        
        public List<LogObject> GetVerboseMessages()
        {

            try
            {
                if (_verboseMessages.Count == 0)
                {
                    return new List<LogObject>();
                }

                var reversedList = new List<LogObject>(_verboseMessages); // Create a copy to avoid modifying the original list
                reversedList.Reverse(); // Reverse the copy

                // If the reversed list has fewer than 5 elements, add empty message LogObjects
                while (reversedList.Count < 5)
                {
                    reversedList.Add(new LogObject(LogType.Message, ""));
                }
                return reversedList;
            }
            catch
            {
                var blankList = new List<LogObject>();
                for (int i = 0; i < 5; i++)
                {
                    blankList.Add(new LogObject(LogType.Message, ""));
                }
                return blankList;
            }
        }

        private static readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNamingPolicy = null,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            Converters = { new JsonStringEnumConverter() }
        };


        public string Init(string id)
        {
            lock (_initLock)
            {
                string logBackupFile = string.Empty;
                _currentId = id;

                Directory.CreateDirectory(Path.Combine(Helper.GetWorkingFolder(), "migrationlogs"));

                _logBucket = ReadLogFile(_currentId, out logBackupFile);
                _verboseMessages.Clear();

                IsInitialized=true;
                return logBackupFile;
            }
        }


        public void WriteLine(string message, LogType LogType = LogType.Message)
        {
            try
            {

                lock (_writeLock)
                {

                    // _logBucket ??= new LogBucket();
                    //_logBucket.Logs ??= new List<LogObject>();

                    if (_logBucket == null)
                    {
                        string logBackupFile = string.Empty;
                        _logBucket = ReadLogFile(_currentId, out logBackupFile);
                        Console.WriteLine($"LogBucket was null, re-initialized from file during WriteLine.");
                    }

                    var logObj = new LogObject(LogType, message);

                    // Add new log
                    _logBucket.Logs ??= new List<LogObject>();
                    _logBucket.Logs.Add(logObj);

                    // If more than 300 logs, remove the 21st mu (index 20), keep it small
                    if (_logBucket.Logs.Count > 300 && _logBucket.Logs.Count > 20)
                    {
                        _logBucket.Logs.RemoveAt(20);
                    }

                    //persits to file
                    AppendBinaryLog(logObj);
                 }
                
            }
            catch
            {
                // Optionally log or ignore
            }
        }


        public void Dispose()
        {
            _currentId = string.Empty;
            _verboseMessages.Clear();
        }

        private void WriteBinaryLog(string id, List<LogObject> logs)
        {
            if (logs == null || logs.Count == 0)
                return;

            var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");

            try
            {
                Directory.CreateDirectory(folder);

                using var fs = new FileStream(binPath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, FileOptions.WriteThrough);
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
                        Console.WriteLine($"Failed to write log entry: {ex.Message}");
                        // Continue writing other logs
                    }
                }

                bw.Flush();
                fs.Flush(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error writing binary log: {ex.Message}");
                throw;
            }
        }


        private void AppendBinaryLog(LogObject log)
        {
            try
            {
                var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
                var binPath = Path.Combine(folder, $"{_currentId}.bin");

                Directory.CreateDirectory(folder);

                using var fs = new FileStream(binPath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, FileOptions.WriteThrough);
                using var bw = new BinaryWriter(fs);

                var messageBytes = Encoding.UTF8.GetBytes(log.Message);
                bw.Write(messageBytes.Length);
                bw.Write(messageBytes);
                bw.Write((byte)log.Type);
                bw.Write(log.Datetime.ToBinary());
            }
            catch { }
        }
        

        private string CreateFileCopyWithTimestamp(string sourceFilePath)
        {
            if (string.IsNullOrEmpty(sourceFilePath))
                throw new ArgumentException("Source file path cannot be null or empty.", nameof(sourceFilePath));

            if (!File.Exists(sourceFilePath))
                throw new FileNotFoundException("Source file not found.", sourceFilePath);

            string directory = Path.GetDirectoryName(sourceFilePath) ?? string.Empty;
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(sourceFilePath);
            string extension = Path.GetExtension(sourceFilePath);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
            string newFilePath = Path.Combine(directory, newFileName);

            if (!File.Exists(newFilePath))
            {
                File.Copy(sourceFilePath, newFilePath);
            }

            return newFileName;
        }

        public LogBucket GetCurentLogBucket(string id)
        {
            if (_currentId == id && _logBucket != null)
            {
                return _logBucket;
            }

            return new LogBucket();
        }

        public LogBucket ReadLogFile(string id, out string fileName)
        {
            fileName = id;

            try
            {
                lock (_readLock)
                {
                    Console.WriteLine($"Reading log file for ID: {id}");
                    var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
                    var txtPath = Path.Combine(folder, $"{id}.txt");
                    var binPath = Path.Combine(folder, $"{id}.bin");

                    // 1. Try Binary first
                    if (File.Exists(binPath))
                    {
                        var logBucket=ParseLogBinFile(binPath);
                        if(logBucket.Logs == null || logBucket.Logs.Count == 0)
                        {
                            return HandleError(id, binPath, binPath, out fileName);
                        }
                        return logBucket;
                    }

                    // 2. Fallback to JSON if .bin is missing (backward compatibility)
                    if (File.Exists(txtPath))
                    {
                        string json = File.ReadAllText(txtPath);
                        try
                        {
                            //old format with LogBucket
                            LogBucket? logBucket = JsonSerializer.Deserialize<LogBucket>(json, _jsonOptions);
                            if (logBucket == null || logBucket.Logs == null || logBucket.Logs.Count == 0)
                            {
                                return new LogBucket(); // empty log
                            }
                            WriteBinaryLog(id, logBucket.Logs);
                            return ParseLogBinFile(binPath);
                        }
                        catch
                        {
                            try
                            {   //new format with List<LogObject>
                                List<LogObject>? logs = JsonSerializer.Deserialize<List<LogObject>>(json, _jsonOptions);
                                if (logs == null || logs.Count == 0)
                                {
                                    return new LogBucket(); // empty log
                                }

                                WriteBinaryLog(id, logs);
                                return ParseLogBinFile(binPath);
                            }
                            catch
                            {
                                return HandleError(id, binPath, txtPath, out fileName);

                            }
                        }
                    }

                    return new LogBucket();
                }
            }
            catch
            {
                throw new Exception("Log Init failed");
            }
        }

        private LogBucket HandleError(string jobId,string binPath,string currentLogFilePath, out string backupFileName)
        {
            backupFileName = CreateFileCopyWithTimestamp(currentLogFilePath);

            File.Delete(currentLogFilePath);

            var logBucket = new LogBucket();
            logBucket.Logs ??= new List<LogObject>();
            logBucket.Logs.Add(new LogObject(LogType.Error, $"Unable to load the log file; original file backed up as {backupFileName}"));
            WriteBinaryLog(jobId, logBucket.Logs);
            return ParseLogBinFile(binPath);
        }

        public byte[] DownloadLogsAsJsonBytes(string binPath, int topEntries = 20, int bottomEntries = 230)
        {
            //var logs = new List<LogObject>();
            //var offsets = new List<long>();

            //try
            //{
            //    using var fs = new FileStream(binPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            //    using var br = new BinaryReader(fs);

            //    // Pass 1: Index all log entry positions
            //    while (fs.Position < fs.Length)
            //    {
            //        long offset = fs.Position;

            //        try
            //        {
            //            int msgLen = br.ReadInt32();
            //            fs.Position += msgLen + 1 + 8; // Skip remainder: message, enum, datetime
            //            offsets.Add(offset);
            //        }
            //        catch
            //        {
            //            break; // Stop on malformed log
            //        }
            //    }

            //    List<long> selectedOffsets;

            //    if (topEntries > 0 || bottomEntries > 0)
            //    {
            //        selectedOffsets = offsets
            //            .Take(topEntries)
            //            .Concat(offsets.Skip(Math.Max(0, offsets.Count - bottomEntries)))
            //            .Distinct()
            //            .OrderBy(i => i)
            //            .ToList();
            //    }
            //    else
            //    {
            //        // Return full list without filtering
            //        selectedOffsets = offsets
            //            .Distinct()
            //            .OrderBy(i => i)
            //            .ToList();
            //    }


            //    // Pass 2: Read selected logs
            //    foreach (var offset in selectedOffsets)
            //    {
            //        fs.Position = offset;
            //        var log = TryReadLogEntry(br);
            //        if (log != null)
            //            logs.Add(log);
            //    }
            //}
            //catch
            //{
            //    // Optionally log or handle error
            //}

            var logs=ParseLogBinFile(binPath, topEntries, bottomEntries);
            // Serialize selected logs to JSON
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                Converters = { new JsonStringEnumConverter() }
            };

            return JsonSerializer.SerializeToUtf8Bytes(logs, options);
        }


        private LogBucket ParseLogBinFile(string binPath, int topCount = 20, int bottomCount = 280)
        {
            var logBucket = new LogBucket { Logs = new List<LogObject>() };
            var offsets = new List<long>();

            if (!File.Exists(binPath))
                return logBucket;

            try
            {
                using var fs = new FileStream(binPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
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
                Console.WriteLine($"Error parsing binary log file: {ex.Message}");
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
                    Console.WriteLine($"Invalid message length: {len}");
                    return null;
                }

                long requiredBytes = len + 1 + 8;
                if (br.BaseStream.Position + requiredBytes > br.BaseStream.Length)
                {
                    Console.WriteLine($"Incomplete log entry. Message length={len}, remaining={br.BaseStream.Length - br.BaseStream.Position}");
                    return null;
                }

                byte[] bytes = br.ReadBytes(len);
                if (bytes.Length != len)
                {
                    Console.WriteLine($"ReadBytes returned {bytes.Length}, expected {len}");
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
                Console.WriteLine($"Exception while reading log entry: {ex.Message}");
                return null;
            }
        }




    }

}

