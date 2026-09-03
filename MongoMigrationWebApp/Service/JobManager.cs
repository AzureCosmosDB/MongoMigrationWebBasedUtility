using System;
using System.Collections.Generic;
using System.IO;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using OnlineMongoMigrationProcessor.Context;

namespace MongoMigrationWebApp.Service
{

    public class JobManager
    {
        private MigrationWorker? MigrationWorker { get; set; }

        private DateTime _lastJobHeartBeat = DateTime.MinValue;
        private string _lastJobID = string.Empty;
        private readonly IConfiguration _configuration;
        private System.Threading.Timer? _resumeTimer;
        private bool _resumeExecuted = false;
        private string? _webAppBaseUrl = null;
        private readonly SemaphoreSlim _syncBackLock = new SemaphoreSlim(1, 1);
        private readonly ConnectionStringVault? _connectionVault;

        public JobManager(IConfiguration configuration, ConnectionStringVault? connectionVault = null)
        {
            _configuration = configuration;
            _connectionVault = connectionVault;

            MigrationJobContext.Initialize(_configuration);

            Helper.LogToFile("Invoking Timer");

            if (!Helper.IsWindows())
                return;

            // Start a timer that fires once after 1 minute
            _resumeTimer = new System.Threading.Timer(
                ResumeTimerCallback,
                null,
                TimeSpan.FromMinutes(1),
                System.Threading.Timeout.InfiniteTimeSpan
            );
        }

        private void ResumeTimerCallback(object? state)
        {
            // Ensure this only executes once
            if (_resumeExecuted)
                return;

            _resumeExecuted = true;

            try
            {
                Helper.LogToFile("Resuming migration job after application restart...");
                var migrationJobIds = GetMigrationIds();

                Helper.LogToFile("Step 0");

                // ActiveMigrationJobId is in-memory only, so after a recycle it is empty and the
                // original single-job gate meant a box with more than one job never auto-resumed.
                // Fall back to persisted state: at most one job can be mid-flight at a time.
                var resumeId = MigrationJobContext.ActiveMigrationJobId;
                if (string.IsNullOrEmpty(resumeId))
                {
                    var inFlight = (migrationJobIds ?? new List<string>())
                        .Select(id => GetMigrationJobById(id))
                        .Where(j => j != null && j.IsStarted && !j.IsCompleted && !j.IsCancelled)
                        .ToList();

                    if (inFlight.Count == 1)
                        resumeId = inFlight[0].Id;
                    else if (inFlight.Count > 1)
                        Helper.LogToFile($"Skipping auto-resume: {inFlight.Count} jobs are mid-flight, cannot pick one.");
                }

                if (!string.IsNullOrEmpty(resumeId))
                {
                    var mj = GetMigrationJobById(resumeId);
                    if (mj == null)
                        return;

                    Helper.LogToFile($"Step1 : {mj.IsStarted}-{mj.IsCompleted} -{string.IsNullOrEmpty(MigrationJobContext.SourceConnectionString[mj.Id])} - {string.IsNullOrEmpty(MigrationJobContext.TargetConnectionString[mj.Id])} - pending={mj.PendingAction}");

                    // Cutover finalization does not need connection strings or a running worker;
                    // if the app died after the user confirmed Cut Over but before the job was
                    // marked terminal, complete it now.
                    if (mj.IsStarted && !mj.IsCompleted && mj.PendingAction == OnlineMongoMigrationProcessor.Models.PendingChangeStreamAction.Cutover)
                    {
                        Helper.LogToFile("Resuming Cutover after restart: marking job complete.");
                        mj.IsCancelled = true;
                        mj.IsCompleted = true;
                        mj.PendingAction = OnlineMongoMigrationProcessor.Models.PendingChangeStreamAction.None;
                        MigrationJobContext.SaveMigrationJob(mj);
                        return;
                    }

                    if (mj.IsStarted && !mj.IsCompleted && string.IsNullOrEmpty(MigrationJobContext.SourceConnectionString[mj.Id]) && string.IsNullOrEmpty(MigrationJobContext.TargetConnectionString[mj.Id]))
                    {
                        try
                        {
                            Helper.LogToFile("Step2 : before reading config");

                            // What the job was started with, kept across recycles. Falls back to
                            // configuration, which is the only source the app had before.
                            string? sourceConnectionString = null;
                            string? targetConnectionString = null;

                            if (_connectionVault != null
                                && _connectionVault.TryLoad(mj.Id, out var vaultSource, out var vaultTarget))
                            {
                                sourceConnectionString = vaultSource;
                                targetConnectionString = vaultTarget;
                                Helper.LogToFile("Step2a : restored connection strings for this job");
                            }
                            else
                            {
                                sourceConnectionString = _configuration.GetConnectionString("SourceConnectionString");
                                targetConnectionString = _configuration.GetConnectionString("TargetConnectionString");
                            }

                            if (sourceConnectionString != null && targetConnectionString != null)
                            {
                                Helper.LogToFile($"Step 3 :Cluster found" + targetConnectionString.Contains("cluster"));

                                var tmpSrcEndpoint = Helper.ExtractHost(sourceConnectionString);
                                var tmpTgtEndpoint = Helper.ExtractHost(targetConnectionString);
                                if (mj.SourceEndpoint == tmpSrcEndpoint && mj.TargetEndpoint == tmpTgtEndpoint)
                                {
                                    MigrationJobContext.SourceConnectionString[mj.Id] = sourceConnectionString;
                                    MigrationJobContext.TargetConnectionString[mj.Id] = targetConnectionString;

                                    // Route based on persisted PendingAction so a flip queued
                                    // before the restart resumes in the right direction with
                                    // a fresh switch-over timestamp. The bootstrap warning
                                    // emitted by the worker's change-stream processor (via
                                    // PendingAction) provides the user-visible "resumed"
                                    // message; no extra log call is needed here.
                                    switch (mj.PendingAction)
                                    {
                                        case OnlineMongoMigrationProcessor.Models.PendingChangeStreamAction.SyncBackEnabled:
                                            OnlineMongoMigrationProcessor.Helpers.ChangeStreamTransitionHelper.ResetSyncBackChangeStreamForReEnable(null, mj, DateTime.UtcNow);
                                            SyncBackToSource(sourceConnectionString, targetConnectionString, mj);
                                            break;
                                        case OnlineMongoMigrationProcessor.Models.PendingChangeStreamAction.ForwardSyncEnabled:
                                            OnlineMongoMigrationProcessor.Helpers.ChangeStreamTransitionHelper.ResetForwardChangeStreamForReEnable(null, mj, DateTime.UtcNow);
                                            mj.ProcessingSyncBack = false;
                                            MigrationJobContext.SaveMigrationJob(mj);
                                            StartMigration(mj, sourceConnectionString, targetConnectionString, mj.NameSpaces ?? string.Empty, mj.JobType, Helper.IsOnline(mj));
                                            break;
                                        default:
                                            Helper.LogToFile("Job Starting");
                                            StartMigration(mj, sourceConnectionString, targetConnectionString, mj.NameSpaces ?? string.Empty, mj.JobType, Helper.IsOnline(mj));
                                            break;
                                    }

                                    Helper.LogToFile("Job Started");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Helper.LogToFile($"Exception : {ex}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Exception : {ex}");
            }
            finally
            {
                // Dispose the timer after execution
                _resumeTimer?.Dispose();
                _resumeTimer = null;
            }
        }

        
        #region _configuration Management

        /// <summary>
        /// Updates the WebAppBaseUrl from browser context. Called from Index.razor on first load.
        /// </summary>
        public void UpdateWebAppBaseUrlFromBrowser(string baseUri)
        {
            try
            {
                if (string.IsNullOrEmpty(baseUri))
                    return;

                // Remove trailing slash if present
                _webAppBaseUrl = baseUri.TrimEnd('/');
                
                // Update existing MigrationWorker if one exists
                if (MigrationWorker != null)
                {
                    MigrationWorker.SetWebAppBaseUrl(_webAppBaseUrl);
                }
                
                Helper.LogToFile($"WebAppBaseUrl updated from browser: {_webAppBaseUrl}");
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Error updating WebAppBaseUrl from browser. Details: {ex}");
            }
        }

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

        public List<MigrationUnit> GetMigrationUnits(MigrationJob mj)
        {            
            return Helper.GetMigrationUnitsToMigrate(mj);
        }

        
        public MigrationJob? GetMigrationJobById(string id, bool active =true)
        {
            var job = MigrationJobContext.GetMigrationJob(id);
            return job;
        }




        public List<string> GetMigrationIds()
        {  

            return MigrationJobContext.JobList.MigrationJobIds;
        }

        public void ClearJobFiles(string jobId)
        {
            MigrationJobContext.JobList.MigrationJobIds?.Remove(jobId);
            MigrationJobContext.SaveJobList();

            // Credentials must not outlive the job they belong to.
            _connectionVault?.Remove(jobId);

            try
            {
                MigrationJobContext.Store.DeleteDocument($"{Path.Combine("migrationjobs", jobId)}");
                MigrationJobContext.Store.DeleteLogs(jobId);
                string dumpPath = Path.Combine(Helper.GetWorkingFolder(), "mongodump", jobId);
                StorageStreamFactory.DeleteDirectory(dumpPath, true);
            }
            catch
            {
            }
        }

        public int ClearAllJobFiles()
        {
            var jobIds = MigrationJobContext.JobList.MigrationJobIds?.ToList() ?? new List<string>();
            foreach (var jobId in jobIds)
                ClearJobFiles(jobId);

            // Sweeps entries orphaned by a job whose files failed to delete.
            _connectionVault?.RemoveAll();

            // CurrentlyActiveJob reloads from this id, so leaving it set resurrects a cleared job
            // from disk and every later import is rejected as a cross-job write.
            MigrationJobContext.ActiveMigrationJobId = string.Empty;
            MigrationJobContext.ClearCurrentlyActiveJobCache();

            return jobIds.Count;
        }

        #endregion 
        #region Log Management

        public List<LogObject> GetMonitorMessages(string id)
        {
            //verbose messages  are only  there for active jobList so fetech from migration worker.
            if (MigrationWorker != null && MigrationWorker.IsProcessRunning(id))
                return MigrationWorker.GetMonitorMessages(id) ?? new List<LogObject>();
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

        public int GetLogCount(string jobId)
        {
            Log log = new Log();
            return log.GetLogCount(jobId);
        }

        public byte[] DownloadLogPage(string jobId, int pageNumber, int pageSize)
        {
            if (string.IsNullOrWhiteSpace(jobId) || pageNumber < 1 || pageSize < 1)
                return Array.Empty<byte>();

            int skip = (pageNumber - 1) * pageSize;
            Log log = new Log();
            return log.DownloadLogsPaginated(jobId, skip, pageSize);
        }

        #endregion

        #region Migration Worker Management

        public void StopMigration()
        {
            MigrationWorker?.StopMigration();
        }



        /// <summary>
        /// Checks if controlled pause is applicable for the given job type and current job state
        /// Controlled pause is only applicable during bulk copy phase, not during change stream processing
        /// </summary>
        public bool IsControlledPauseApplicable(JobType jobType, OnlineMongoMigrationProcessor.MigrationJob? job = null)
        {
            // Controlled pause only applies to chunk-based job types
            if (jobType != JobType.DumpAndRestore &&
                jobType != JobType.MongoDriver)
            {
                return false;
            }

            // If job is provided, check if bulk copy (offline phase) is still ongoing
            if (job != null)
            {
                // If offline job is completed, controlled pause is not applicable
                if (Helper.IsOfflineJobCompleted(job))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Gets whether controlled pause is currently requested
        /// </summary>
        public bool IsControlledPauseRequested()
        {
            return MigrationJobContext.ControlledPauseRequested;
        }

        /// <summary>
        /// Gets whether graceful change-stream auto-close is currently requested
        /// (set by UI flip handlers — Cutover / SyncBack / RestartForwardCS).
        /// </summary>
        public bool IsChangeStreamAutoCloseRequested()
        {
            return MigrationJobContext.ChangeStreamAutoCloseRequested;
        }

        public Task CancelMigration(string id)
        {

            var migration = MigrationJobContext.GetMigrationJob(id);
            if (migration != null)
            {
                migration.IsCancelled = true;
                migration.IsStarted = false;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Connection strings for a job: in-memory first, then the values it was started with.
        /// Rehydrates the in-memory cache on a hit so the rest of the app sees them as normal.
        /// </summary>
        public bool TryResolveConnectionStrings(string jobId, out string sourceConnectionString, out string targetConnectionString)
        {
            sourceConnectionString = MigrationJobContext.SourceConnectionString[jobId] ?? string.Empty;
            targetConnectionString = MigrationJobContext.TargetConnectionString[jobId] ?? string.Empty;

            if (!string.IsNullOrWhiteSpace(sourceConnectionString) && !string.IsNullOrWhiteSpace(targetConnectionString))
                return true;

            if (_connectionVault != null && _connectionVault.TryLoad(jobId, out var storedSource, out var storedTarget))
            {
                sourceConnectionString = storedSource;
                targetConnectionString = storedTarget;
                MigrationJobContext.SourceConnectionString[jobId] = storedSource;
                MigrationJobContext.TargetConnectionString[jobId] = storedTarget;
                return true;
            }

            return false;
        }

        public void RememberConnectionStrings(string jobId, string sourceConnectionString, string targetConnectionString)
        {
            MigrationJobContext.SourceConnectionString[jobId] = sourceConnectionString;
            MigrationJobContext.TargetConnectionString[jobId] = targetConnectionString;
            _connectionVault?.TrySave(jobId, sourceConnectionString, targetConnectionString);
        }

        public Task StartMigration(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, OnlineMongoMigrationProcessor.Models.JobType jobType,bool trackChangeStreams)
        {
            

            MigrationWorker = new MigrationWorker();
            
            // Set WebAppBaseUrl if available
            if (!string.IsNullOrEmpty(_webAppBaseUrl))
            {
                MigrationWorker.SetWebAppBaseUrl(_webAppBaseUrl);
            }
            
            MigrationJobContext.SourceConnectionString[job.Id] = sourceConnectionString;
            MigrationJobContext.TargetConnectionString[job.Id] = targetConnectionString;

            // So a recycle does not force the operator to re-enter them to resume.
            _connectionVault?.TrySave(job.Id, sourceConnectionString, targetConnectionString);

            MigrationJobContext.ActiveMigrationJobId = job.Id;

            // Fire-and-forget: UI should not block on long-running migration
            _ = MigrationWorker?.StartMigrationAsync(namespacesToMigrate, jobType, trackChangeStreams);
            
            Console.WriteLine($"Started migration for Job ID: {job.Id}");

            return Task.CompletedTask;
        }

        public async Task SyncBackToSourceAsync()
        {
            // Try to acquire lock with zero timeout - if already locked, skip this operation
            //if (!await _syncBackLock.WaitAsync(0))
            //{
            //    Helper.LogToFile($"SyncBackToSource already running for job, skipping duplicate call");
            //    return;
            //}
            MigrationWorker?.SyncBackToSource();
           
            //_syncBackLock.Release();            
        }

        /// Only one SyncBack operation can run at a time to prevent race conditions.
        /// </summary>
        public Task SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            //// Try to acquire lock with zero timeout - if already locked, skip this operation
            //if (!_syncBackLock.Wait(0))
            //{
            //    Helper.LogToFile($"SyncBackToSource already running , skipping duplicate call");
            //    return Task.CompletedTask;
            //}

            try
            {

                job.ProcessingSyncBack = true;
                MigrationJobContext.SaveMigrationJob(job);

                // Stop the forward change stream on the existing worker before creating a new one,
                // to prevent echo/feedback loops where changes bounce between directions.
                MigrationWorker?.StopMigration();

                MigrationWorker = new MigrationWorker();
                
                // Set WebAppBaseUrl if available
                if (!string.IsNullOrEmpty(_webAppBaseUrl))
                {
                    MigrationWorker.SetWebAppBaseUrl(_webAppBaseUrl);
                }

                MigrationJobContext.SourceConnectionString[job.Id] = sourceConnectionString;
                MigrationJobContext.TargetConnectionString[job.Id] = targetConnectionString;

                // So a recycle does not force the operator to re-enter them to resume.
                _connectionVault?.TrySave(job.Id, sourceConnectionString, targetConnectionString);

                MigrationJobContext.ActiveMigrationJobId = job.Id;

                // Call synchronous method since it starts async work internally
                _ = MigrationWorker?.SyncBackToSourceAsync(sourceConnectionString, targetConnectionString);
            }
            finally
            {
                //_syncBackLock.Release();
            }

            return Task.CompletedTask;
        }
        public string GetRunningJobId()
        {
            return MigrationWorker?.GetRunningJobId() ?? string.Empty;
        }
               

        public bool IsProcessRunning(string id)
        {
            return MigrationWorker?.IsProcessRunning(id) ?? false;
        }

        public void AdjustDumpWorkers(int newCount, MigrationJob? job = null)
        {
            if (TryAdjustOnLiveWorker(job, () => MigrationWorker!.AdjustDumpWorkers(newCount))) return;
            if (job != null)
            {
                job.CurrentDumpWorkers = newCount;
                MigrationJobContext.SaveMigrationJob(job);
            }
        }

        public void AdjustRestoreWorkers(int newCount, MigrationJob? job = null)
        {
            if (TryAdjustOnLiveWorker(job, () => MigrationWorker!.AdjustRestoreWorkers(newCount))) return;
            if (job != null)
            {
                job.CurrentRestoreWorkers = newCount;
                MigrationJobContext.SaveMigrationJob(job);
            }
        }

        public void AdjustInsertionWorkers(int newCount, MigrationJob? job = null)
        {
            if (TryAdjustOnLiveWorker(job, () => MigrationWorker!.AdjustInsertionWorkers(newCount))) return;
            if (job != null)
            {
                job.CurrentInsertionWorkers = newCount;
                MigrationJobContext.SaveMigrationJob(job);
            }
        }

        private bool TryAdjustOnLiveWorker(MigrationJob? job, Action adjust)
        {
            if (MigrationWorker == null) return false;
            if (job != null && !MigrationWorker.IsProcessRunning(job.Id)) return false;
            adjust();
            return true;
        }

        #endregion
                
    }
}

