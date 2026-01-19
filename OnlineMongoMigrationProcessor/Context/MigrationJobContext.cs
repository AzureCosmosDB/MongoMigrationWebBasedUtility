using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.JobList;

namespace OnlineMongoMigrationProcessor.Context
{
    public static class MigrationJobContext
    {
        private static readonly object _writeMULock = new object();
        private static readonly object _writeJobLock = new object();
        private static readonly object _writeJobListLock = new object();

        private static readonly Dictionary<string, string> _sourceConnectionStrings = new();
        private static readonly Dictionary<string, string> _targetConnectionStrings = new();

        // In-memory cache of migration jobs to ensure consistency
        private static Dictionary<string, MigrationJob> MigrationJobs { get; set; } = new Dictionary<string, MigrationJob>();

        // Cached instance of the currently active migration job for consistency across the application
        private static MigrationJob? _cachedCurrentlyActiveJob = null;

        private static Log _log;

        public static ActiveMigrationUnitsCache MigrationUnitsCache { get; set; }

        // Track OS process IDs for mongodump and mongorestore to enable cleanup
        public static List<int> ActiveDumpProcessIds { get; set; } = new List<int>();
        public static List<int> ActiveRestoreProcessIds { get; set; } = new List<int>();

        public static string ActiveMigrationJobId { get; set; }

        public static bool ControlledPauseRequested { get; private set; } = false;

        public static ConnectionAccessor SourceConnectionString => new(_sourceConnectionStrings);
        public static ConnectionAccessor TargetConnectionString => new(_targetConnectionStrings);

        public static JobList JobList { get; private set; }

        public static void ResetControlledPause()
        {
            AddVerboseLog($"Resetting controlled pause request.");
            ControlledPauseRequested = false;
        }

        public static void RequestControlledPause(string location)
        {
            if (_log == null)
                throw new Exception("Log not initialized.");

            _log.WriteLine($"{location} caused controlled pause - processing will stop after at logical point.", LogType.Warning);

            ControlledPauseRequested = true;
        }

        public static void UpdateLogLevel(LogType level, MigrationJob job)
        {
            if (CurrentlyActiveJob == null || CurrentlyActiveJob.IsCancelled || CurrentlyActiveJob.IsCompleted)
            {
                job.LogLevel = level;
                SaveMigrationJob(job);
            }
            else
            {
                CurrentlyActiveJob.LogLevel = level;
                SaveMigrationJob(CurrentlyActiveJob);
            }
        }

        public static void AddVerboseLog(string message) 
        {
            if (_log == null || CurrentlyActiveJob==null || CurrentlyActiveJob.IsCancelled || CurrentlyActiveJob.IsCompleted)
                return;

             _log?.WriteLine(message, LogType.Verbose);
        }

        /// <summary>
        /// Resets static state for a new job. Call this when starting a new migration job
        /// to prevent state from previous jobs from interfering.
        /// Kills any leftover mongodump/mongorestore processes from previous jobs.
        /// </summary>
        public static void ResetJobState()
        {
            // Kill any leftover processes from previous job
            KillAllMigrationProcesses();
            
            // Clear the lists
            ActiveDumpProcessIds.Clear();
            ActiveRestoreProcessIds.Clear();
            ControlledPauseRequested = false;
            MigrationUnitsCache = new ActiveMigrationUnitsCache();
            _log = null;
        }
        
        /// <summary>
        /// Kills all tracked mongodump and mongorestore processes, plus any orphaned processes by name.
        /// </summary>
        public static void KillAllMigrationProcesses()
        {
            int killedTracked = 0;
            int killedOrphaned = 0;
            
            // First, kill tracked processes by PID
            foreach (int pid in ActiveDumpProcessIds.Concat(ActiveRestoreProcessIds))
            {
                try
                {
                    var process = System.Diagnostics.Process.GetProcessById(pid);
                    if (!process.HasExited)
                    {
                        process.Kill(entireProcessTree: true);
                        killedTracked++;
                    }
                }
                catch (ArgumentException)
                {
                    // Process doesn't exist anymore - that's fine
                }
                catch (Exception ex)
                {
                    // Log error but continue
                }
            }
            
            // Second, kill any orphaned mongodump/mongorestore processes by name
            try
            {
                string[] processNames = { "mongodump", "mongorestore" };
                foreach (string processName in processNames)
                {
                    var processes = System.Diagnostics.Process.GetProcessesByName(processName);
                    foreach (var process in processes)
                    {
                        try
                        {
                            if (!process.HasExited)
                            {
                                process.Kill(entireProcessTree: true);
                                killedOrphaned++;
                            }
                        }
                        catch (Exception ex)
                        {
                            // Continue killing other processes even if one fails
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Log error but don't throw - this is cleanup code
            }            
            
        }

        
        public static void InitializeLog(Log log)
        {
            if (_log == null)
            {
                _log = log;
            }
            AddVerboseLog("Initialized MigrationJobContext log.");
        }


        /// <summary>
        /// Gets the currently active migration job with intelligent caching
        /// </summary>
        public static MigrationJob? CurrentlyActiveJob
        {
            get
            {
                // If we have a cached instance and it matches the active job ID, use it
                if (_cachedCurrentlyActiveJob != null && 
                    !string.IsNullOrEmpty(ActiveMigrationJobId) && 
                    _cachedCurrentlyActiveJob.Id == ActiveMigrationJobId)
                {
                    return _cachedCurrentlyActiveJob;
                }

                // Otherwise, fetch from LoadMigrationJob and cache it
                if (!string.IsNullOrEmpty(ActiveMigrationJobId))
                {
                    _cachedCurrentlyActiveJob = LoadMigrationJob(ActiveMigrationJobId);
                    MigrationUnitsCache=new ActiveMigrationUnitsCache();
                    return _cachedCurrentlyActiveJob;
                }
                
                return null;
            }
        }
        

        public static PersistenceStorage? Store  {get; private set; }

        public static string? AppId { get; set; }
        public static void Initialize(IConfiguration configuration)
        {
            var initStopwatch = System.Diagnostics.Stopwatch.StartNew();
            Helper.LogToFile($"MigrationJobContext.Initialize started");
            
            bool isLocal = true;
            var stateStoreCSorPath = string.Empty;
            var appId = string.Empty;
            try
            {
                bool.TryParse(configuration["StateStore:UseLocalDisk"], out isLocal);
                stateStoreCSorPath = configuration["StateStore:ConnectionStringOrPath"];             
                appId = configuration["StateStore:AppID"];
                AppId = appId;
                Helper.LogToFile($"Configuration loaded - isLocal: {isLocal}, appId: {appId}, elapsed: {initStopwatch.ElapsedMilliseconds}ms");
            }
            catch
            {
                //do nothing and fallback to defaults
            }

            //For local and WebApp deployments, use local disk. For others use DocumentDB
            if (Helper.IsWindows())
            {
                if (isLocal)
                {
                    Store = new DiskPersistence();
                    var localPath = string.IsNullOrEmpty(stateStoreCSorPath) ? Helper.GetWorkingFolder() : stateStoreCSorPath;
                    Helper.LogToFile($"Initializing DiskPersistence at {localPath}, elapsed: {initStopwatch.ElapsedMilliseconds}ms");
                    Store.Initialize(localPath, string.Empty);
                    Helper.LogToFile($"DiskPersistence initialized, elapsed: {initStopwatch.ElapsedMilliseconds}ms");
                }
                else
                {                  
                    if (string.IsNullOrEmpty(stateStoreCSorPath))
                    {
                        throw new InvalidOperationException("Please configure 'StateStore:ConnectionString' in appsettings.json or 'StateStoreConnectionStringOrPath'  environment variable.");
                    }
                    if (string.IsNullOrEmpty(appId))
                    {
                        throw new InvalidOperationException("Please configure 'StateStore:AppID' in appsettings.json or 'StateStoreAppID' environment variable.");
                    }

                    AppId=appId;
                    Store = new DocumentDBPersistence();
                    Helper.LogToFile($"Initializing DocumentDBPersistence (Windows), elapsed: {initStopwatch.ElapsedMilliseconds}ms");
                    Store.Initialize(stateStoreCSorPath, appId);
                    Helper.LogToFile($"DocumentDBPersistence initialized (Windows), elapsed: {initStopwatch.ElapsedMilliseconds}ms");
                } 
            }
            else
            {

                if (string.IsNullOrEmpty(stateStoreCSorPath))
                {
                    throw new InvalidOperationException("Please configure 'StateStoreConnectionStringOrPath'  environment variable.");
                }
                if (string.IsNullOrEmpty(appId))
                {
                    throw new InvalidOperationException("Please configure 'StateStoreAppID'  environment variable.");
                }
                AppId = appId;
                Store = new DocumentDBPersistence();
                Helper.LogToFile($"Initializing DocumentDBPersistence (non-Windows), elapsed: {initStopwatch.ElapsedMilliseconds}ms");
                Store.Initialize(stateStoreCSorPath, appId);
                Helper.LogToFile($"DocumentDBPersistence initialized (non-Windows), elapsed: {initStopwatch.ElapsedMilliseconds}ms");
            }

            Helper.LogToFile($"Loading JobList, elapsed: {initStopwatch.ElapsedMilliseconds}ms");
            JobList= LoadJobList(out bool notFound,out string errorMessage);
            Helper.LogToFile($"JobList loaded (notFound: {notFound}), elapsed: {initStopwatch.ElapsedMilliseconds}ms");
            if(notFound && JobList == null)
            {
                JobList=new JobList();
                JobList.MigrationJobIds=new List<string>();                
            }
            else if(JobList == null && !string.IsNullOrEmpty(errorMessage))
            {
                throw new InvalidOperationException($"Error initializing Job List: {errorMessage}");
            }
            Helper.LogToFile($"Persisting JobList, elapsed: {initStopwatch.ElapsedMilliseconds}ms");
            JobList.Persist();
            
            initStopwatch.Stop();
            Helper.LogToFile($"MigrationJobContext.Initialize completed in {initStopwatch.ElapsedMilliseconds}ms");
        }

        private static MigrationJob? LoadMigrationJob(string jobId)
        {

            if (MigrationJobs.ContainsKey(jobId))
            {
                return MigrationJobs[jobId];
            }
            else
            {
                try
                {
                    var filePath = $"{Path.Combine("migrationjobs", jobId, "jobdefinition.json")}";

                    var json = Store.ReadDocument(filePath);
                    var loadedObject = JsonConvert.DeserializeObject<MigrationJob>(json);
                    if (loadedObject == null)
                        return null;
                    MigrationJobs[jobId] = loadedObject;
                    return loadedObject;
                }
                catch
                {
                    return null;
                }
            }
        }

        public static MigrationJob? GetMigrationJob(string jobId)
        {

            if(jobId==ActiveMigrationJobId && CurrentlyActiveJob!=null)
                return CurrentlyActiveJob;

            return LoadMigrationJob(jobId);
        }

        public static List<MigrationJob>  PopulateMigrationJobs(List<string> ids)
        {

            List<MigrationJob> jobs = new List<MigrationJob>();
            foreach (var id in ids)
            {
                var job=GetMigrationJob(id);
                if(job != null)
                    jobs.Add(job);
            }
            return jobs;
        }

        public static bool SaveMigrationUnit(MigrationUnit mu, bool updateParent)
        {

            try
            {
                if (mu == null)
                    return false;               
  
                if (CurrentlyActiveJob != null)
                    mu.ParentJob = CurrentlyActiveJob;

                if(mu.ParentJob != null && updateParent)
                    mu.UpdateParentJob();      

                lock (_writeMULock)
                {
                    mu.Persist();
                }

                if (CurrentlyActiveJob != null && updateParent)
                {
                    lock (_writeJobLock)
                    {
                        CurrentlyActiveJob.Persist();                        
                    }
                }

                if(MigrationUnitsCache != null)
                {
                    MigrationUnitsCache.UpdateMigrationUnit(mu);
                }
                return true;
            }
            catch
            {
                return false;
            }
        }


        private static JobList LoadJobList(out bool notFound,out string errorMessage)
        {
            errorMessage = string.Empty;
            notFound = false;

            string newFormatPath = $"migrationjobs\\joblist.json";

            try
            {
                int max = 5;

                for (int i = 0; i < max; i++)
                {
                    try
                    {
                        if (!MigrationJobContext.Store.DocumentExists(newFormatPath))
                        {
                            notFound = true;
                            errorMessage = "No suitable file in new format found.";
                        }
                        else
                        {
                            string json = MigrationJobContext.Store.ReadDocument(newFormatPath);
                            var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                            List<string> MigrationJobIds = null;
                            if (loadedObject != null)
                            {
                                if (loadedObject.MigrationJobIds == null)
                                {
                                    errorMessage = $"Job List is corrupted, missing MigrationJobIds.";
                                    return null;
                                }
                                else
                                {
                                    JobList = loadedObject;
                                    return JobList;
                                }
                            }                            
                        }
                    }
                    catch (JsonException ex)
                    {
                        errorMessage = $"Error deserializing Job List. Details: {ex}";
                    }
                    finally
                    {
                        // Small delay before retrying
                        Task.Delay(200).Wait();
                    }
                }
                errorMessage = $"Error loading migration jobs.";
                return null;

            }
            catch (Exception ex)
            {
                errorMessage = $"Error loading migration jobs: {ex}";
                return null;
            }
            
        }

        public static bool SaveMigrationJob(MigrationJob job)
        {
            try
            {
                if (job != null)
                {
                    lock (_writeJobLock)
                    {
                        job.Persist();
                        
                        // ALWAYS update the in-memory cache for this job ID
                        MigrationJobs[job.Id] = job;
                        
                        // Update the cached currently active job if this is the active job
                        if (!string.IsNullOrEmpty(ActiveMigrationJobId) && job.Id == ActiveMigrationJobId)
                        {
                            _cachedCurrentlyActiveJob = job;
                        }
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool SaveJobList()
        {

            try
            {
                if (JobList != null)
                {
                    lock (_writeJobListLock)
                    {
                        JobList.Persist();
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }


        public static MigrationUnit GetMigrationUnit(string key, string jobId=null)
        {
            if(string.IsNullOrEmpty(jobId) && CurrentlyActiveJob != null)
            {
                jobId = CurrentlyActiveJob.Id;
            }

            if (MigrationJobContext.MigrationUnitsCache == null)
                return MigrationJobContext.GetMigrationUnitFromStorage(jobId, key);
            else
                return MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(key, jobId);
        }

        public static MigrationUnit GetMigrationUnitFromStorage(string jobId, string unitId)
        {
            AddVerboseLog($"MigrationJobContext.GetMigrationUnit: jobId={jobId}, unitId={unitId}");
            try
            {
                var filePath = $"{Path.Combine("migrationjobs", jobId, $"{unitId}.json")}";
                string json =Store.ReadDocument(filePath);
                var loadedObject = JsonConvert.DeserializeObject<MigrationUnit>(json);
                return loadedObject;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Clears the cached currently active job. Use this when switching active jobs or when you need to force a reload.
        /// </summary>
        public static void ClearCurrentlyActiveJobCache()
        {
            _cachedCurrentlyActiveJob = null;
        }

    }
}
