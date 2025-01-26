using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8602

    public class LogBucket
    {
        public List<LogObject>? Logs { get; set; }
        private List<LogObject>? _verboseMessages;
        private readonly object _lock = new object();

        public void AddVerboseMessage(string message, LogType logType = LogType.Message)
        {
            lock (_lock)
            {
                _verboseMessages ??= new List<LogObject>();

                if (_verboseMessages.Count == 5)
                {
                    _verboseMessages.RemoveAt(0); // Remove the oldest item
                }
                _verboseMessages.Add(new LogObject(logType, message)); // Add the new item
            }
        }

        public List<LogObject> GetVerboseMessages()
        {
            try
            {
                _verboseMessages ??= new List<LogObject>();
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
    }

    public static class Log
    {
        private static LogBucket? _logBucket;
        private static string _currentId = string.Empty;

        public static void Init(string id)
        {
            _currentId = id;
            Directory.CreateDirectory($"{Path.GetTempPath()}migrationlogs");

            _logBucket = GetLogBucket(_currentId);
        }

        public static void AddVerboseMessage(string message, LogType logType = LogType.Message)
        {
            _logBucket?.AddVerboseMessage(message, logType);
        }

        public static void WriteLine(string message, LogType logType = LogType.Message)
        {
            try
            {
                _logBucket ??= new LogBucket();
                _logBucket.Logs ??= new List<LogObject>();

                _logBucket.Logs.Add(new LogObject(logType, message));
            }
            catch { }
        }

        public static void Dispose()
        {
            _currentId = string.Empty;
            _logBucket = null;
        }

        public static void Save()
        {
            try
            {
                string json = JsonConvert.SerializeObject(_logBucket);
                var path = $"{Path.GetTempPath()}migrationlogs\\{_currentId}.txt";
                File.WriteAllText(path, json);
            }
            catch { }
        }

        public static LogBucket GetLogBucket(string id)
        {
            try
            {
                if (id == _currentId && _logBucket != null)
                    return _logBucket;

                var path = $"{Path.GetTempPath()}migrationlogs\\{id}.txt";
                if (File.Exists(path))
                {
                    string json = File.ReadAllText(path);
                    var loadedObject = JsonConvert.DeserializeObject<LogBucket>(json);
                    return loadedObject ?? new LogBucket();
                }
                else
                {
                    return new LogBucket();
                }
            }
            catch
            {
                throw new Exception("Log Init failed");
            }
        }
    }
}

