using System;
using System.Collections.Generic;
using OnlineMongoMigrationProcessor;

namespace MongoMigrationWebApp.Service
{

    public class JobManager
    {
    private JobList? _jobList;
        private MigrationWorker? MigrationWorker { get; set; }

        private DateTime _lastJobHeartBeat = DateTime.MinValue;
        private string _lastJobID = string.Empty;

        #region _configuration Management

        public bool UpdateConfig(OnlineMongoMigrationProcessor.MigrationSettings updated_config, out string errorMessage)
        {
            if (updated_config == null)
            {
                errorMessage = "Migration settings cannot be null.";
                return false;
            }
            // Save the updated config
            return updated_config.Save(out errorMessage);
        }

        public OnlineMongoMigrationProcessor.MigrationSettings GetConfig()
        {
            MigrationSettings config = new MigrationSettings();
            config.Load();
            return config;
        }

        #endregion 
        #region Job Management

        private JobList EnsureJobList()
        {
            return _jobList ??= new JobList();
        }


        public DateTime GetJobBackupDate()
        {
            return EnsureJobList().GetBackupDate();
        }


        public bool RestoreJobsFromBackup(out string errorMessage)
        {
            _jobList = null;
            _jobList = new JobList();

            var success = _jobList.LoadJobs(out errorMessage, true);
            if (!success)
            {
                return false;
            }

            if (MigrationWorker != null)
            {
                MigrationWorker.StopMigration();
                MigrationWorker = null;
            }

            MigrationWorker = new MigrationWorker(_jobList);

            errorMessage = string.Empty;
            return success;
        }

        public bool SaveJobs(out string errorMessage)
        {
            return EnsureJobList().Save(out errorMessage);
        }



        public List<MigrationJob> GetMigrations(out string errorMessage, bool force = false)
        {
            errorMessage = string.Empty;
            bool isSucess = true;
            if (_jobList == null)
            {
                _jobList = new JobList();
                isSucess = _jobList.LoadJobs(out errorMessage, false);
            }
            else
            {
                errorMessage = string.Empty;
                return _jobList.MigrationJobs ?? new List<MigrationJob>();
            }
            // Ensure we always return a non-null list
            if (_jobList.MigrationJobs == null)
            {
                _jobList.MigrationJobs = new List<MigrationJob>();
                if (isSucess || force)
                {
                    SaveJobs(out errorMessage);
                }
            }
            return _jobList.MigrationJobs;
        }

        public void ClearJobFiles(string jobId)
        {
            try
            {
                System.IO.Directory.Delete($"{Helper.GetWorkingFolder()}mongodump\\{jobId}", true);
            }
            catch
            {
            }
        }

        #endregion 
        #region Log Management

        public List<LogObject> GetVerboseMessages(string id)
        {
            //verbose messages  are only  there for active jobList so fetech from migration worker.
            if (MigrationWorker != null && MigrationWorker.IsProcessRunning(id))
                return MigrationWorker.GetVerboseMessages(id) ?? new List<LogObject>();
            else
                return new List<LogObject>();
        }

        public bool DidMigrationJobExitRecently(string jobId)
        {
            if (jobId != _lastJobID) return false;

            if (System.DateTime.UtcNow.AddSeconds(-10) > _lastJobHeartBeat)
            {
                _lastJobID = string.Empty;
                return false; ///hear beat can be max 10 seconds old
            }

            return true;
        }

        public LogBucket GetLogBucket(string id, out string fileName, out bool isLiveLog)
        {
            //Check if migration workewr is initialized and active. Return migration workers log bucket if it is.
            LogBucket? bucket = null;
            if (MigrationWorker != null && MigrationWorker.IsProcessRunning(id)) //only if worker's current job Id matches param
            {
                //Console.WriteLine($"Migration worker is running for job ID: {id}");
                bucket = MigrationWorker.GetLogBucket(id);
                _lastJobHeartBeat = DateTime.UtcNow;
                _lastJobID = id;
                isLiveLog = true;
                fileName = string.Empty;
                return bucket ?? new LogBucket { Logs = new List<LogObject>() };
            }

            //If migration worker is not running, get the log bucket from the file.Its static  
            isLiveLog = false;
            Log log = new Log();
            return log.ReadLogFile(id, out fileName) ?? new LogBucket { Logs = new List<LogObject>() };
        }

        #endregion

        #region Migration Worker Management

        public void StopMigration()
        {
            MigrationWorker?.StopMigration();
        }

        public Task CancelMigration(string id)
        {
            var list = EnsureJobList().MigrationJobs;
            if (list != null)
            {
                var migration = list.Find(m => m.Id == id);
                if (migration != null)
                {
                    migration.IsCancelled = true;
                    migration.IsStarted = false;
                }
            }
            return Task.CompletedTask;
        }

        public Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, OnlineMongoMigrationProcessor.Models.JobType jobType,bool trackChangeStreams)
        {
            MigrationWorker = new MigrationWorker(EnsureJobList());
            // Fire-and-forget: UI should not block on long-running migration
            _ = MigrationWorker?.StartMigrationAsync(job, sourceConnectionString, targetConnectionString, namespacesToMigrate, jobType, trackChangeStreams);
            return Task.CompletedTask;
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            MigrationWorker = new MigrationWorker(EnsureJobList());
            MigrationWorker?.SyncBackToSource(sourceConnectionString, targetConnectionString, job);
        }


        public string GetRunningJobId()
        {
            return MigrationWorker?.GetRunningJobId() ?? string.Empty;
        }

        public bool IsProcessRunning(string id)
        {
            return MigrationWorker?.IsProcessRunning(id) ?? false;
        }

        #endregion
                
    }
}

