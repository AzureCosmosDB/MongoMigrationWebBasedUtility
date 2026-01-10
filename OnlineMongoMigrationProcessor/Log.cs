using OnlineMongoMigrationProcessor.Context;
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
        private MigrationJob? CurrentlyActiveJob;

        private static readonly object _verboseLock = new object();
        private static readonly object _writeLock = new object();
        private static readonly object _initLock = new object();

        public  bool IsInitialized { get; set; } = false;

        /// <summary>
        /// Set the migration job reference for log level filtering
        /// </summary>
        public void SetJob(MigrationJob? job)
        {
            CurrentlyActiveJob = job;
        }

        public void ShowInMonitor(string message, LogType LogType = LogType.Info)
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
        
        public List<LogObject> GetMonitorMessages()
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
                    reversedList.Add(new LogObject(LogType.Info, ""));
                }
                return reversedList;
            }
            catch
            {
                var blankList = new List<LogObject>();
                for (int i = 0; i < 5; i++)
                {
                    blankList.Add(new LogObject(LogType.Info, ""));
                }
                return blankList;
            }
        }

        public string Init(string id)
        {
            lock (_initLock)
            {
                string logBackupFile = string.Empty;
                _currentId = id;

                _logBucket = ReadLogFile(_currentId, out logBackupFile);
                _verboseMessages.Clear();

                IsInitialized=true;
                return logBackupFile;
            }
        }

        public LogBucket ReadLogFile(string id, out string logBackupFile)
        {
            return MigrationJobContext.Store.ReadLogs(id, out logBackupFile);
        }

        public void WriteLine(string message, LogType logType = LogType.Info)
        {
            try
            {
                // Filter based on minimum log level - only log if the message type is at or below the minimum level
                // Lower numeric values = more severe (Error=0, Message=1, Warning=2, Info=3, Debug=4, Verbose=5)
                if (_currentId==string.Empty ||( CurrentlyActiveJob != null && (int)logType > (int)CurrentlyActiveJob.LogLevel))
                {
                    return; // Skip this log entry
                }

                lock (_writeLock)
                {

                    if (_logBucket == null)
                    {
                        string logBackupFile = string.Empty;
                        //_logBucket = ReadLogFile(_currentId, out logBackupFile);
                        _logBucket= ReadLogFile(_currentId, out logBackupFile);
                        Console.WriteLine($"LogBucket was null, re-initialized from file during WriteLine.");
                    }

                    var logObj = new LogObject(logType, message);

                    // Add new log
                    _logBucket.Logs ??= new List<LogObject>();
                    _logBucket.Logs.Add(logObj);

                    // If more than 300 logs, remove the 21st mu (index 20), keep it small
                    if (_logBucket.Logs.Count > 300 && _logBucket.Logs.Count > 20)
                    {
                        _logBucket.Logs.RemoveAt(20);
                    }

                    //persits to file
                    MigrationJobContext.Store.PushLogEntry(_currentId, logObj);                    
                }


            }
            catch(Exception ex) 
            {
                Console.WriteLine($"[CRITICAL] Log write failed: {message} | Error: {ex}");
            }
        }
        public void Dispose()
        {
            _currentId = string.Empty;
            _verboseMessages.Clear();
        }                

        public LogBucket GetCurentLogBucket(string id)
        {
            if (_currentId == id && _logBucket != null)
            {
                return _logBucket;
            }
            return new LogBucket();
        }

        public byte[] DownloadLogsAsJsonBytes(string id, int topEntries = 20, int bottomEntries = 230)
        {
            return MigrationJobContext.Store.DownloadLogsAsJsonBytes(id, topEntries, bottomEntries);
        }

        public int GetLogCount(string id)
        {
            return MigrationJobContext.Store.GetLogCount(id);
        }

        public byte[] DownloadLogsPaginated(string id, int skip, int take)
        {
            return MigrationJobContext.Store.DownloadLogsPaginated(id, skip, take);
        }

    }

}

