using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;
using System.Text.Json;

namespace MongoMigrationWebApp.Controller
{
    [ApiController]
    [Route("api/migration-jobs")]
    public class MigrationJobsController : ControllerBase
    {
        // Callers authenticate with the same password that gates the UI.
        public const string AppPasswordHeader = "X-Migration-App-Password";

        private readonly Service.JobManager _jobManager;
        private readonly Service.PasswordManager _passwordManager;

        public MigrationJobsController(Service.JobManager jobManager, Service.PasswordManager passwordManager)
        {
            _jobManager = jobManager;
            _passwordManager = passwordManager;
        }

        [HttpPost("reset")]
        public async Task<IActionResult> Reset()
        {
            var denied = await AuthorizeAsync();
            if (denied != null)
                return denied;

            if (_jobManager.GetMigrationIds().Any(_jobManager.IsProcessRunning))
                return Conflict("Cannot reset while a migration job is running.");

            return Ok(new { deletedJobs = _jobManager.ClearAllJobFiles() });
        }

        [HttpGet]
        public async Task<IActionResult> List()
        {
            var denied = await AuthorizeAsync();
            if (denied != null)
                return denied;

            var runningJobId = _jobManager.GetRunningJobId();
            var jobs = new List<object>();

            foreach (var id in _jobManager.GetMigrationIds() ?? new List<string>())
            {
                var job = _jobManager.GetMigrationJobById(id);
                if (job == null)
                    continue;

                jobs.Add(Summarize(job, _jobManager.GetMigrationUnits(job), runningJobId));
            }

            return Ok(new { runningJobId, count = jobs.Count, jobs });
        }

        [HttpGet("{jobId}")]
        public async Task<IActionResult> Get(string jobId)
        {
            var denied = await AuthorizeAsync();
            if (denied != null)
                return denied;

            if (string.IsNullOrWhiteSpace(jobId))
                return BadRequest("jobId is required.");

            var job = _jobManager.GetMigrationJobById(jobId);
            if (job == null)
                return NotFound($"No migration job with id '{jobId}'.");

            var units = _jobManager.GetMigrationUnits(job) ?? new List<MigrationUnit>();

            return Ok(new
            {
                job = Summarize(job, units, _jobManager.GetRunningJobId()),
                units = units.Select(u => new
                {
                    u.Id,
                    u.DatabaseName,
                    u.CollectionName,
                    targetDatabaseName = u.GetEffectiveTargetDatabaseName(),
                    targetCollectionName = u.GetEffectiveTargetCollectionName(),
                    u.DumpPercent,
                    u.RestorePercent,
                    u.IndexPercent,
                    u.DumpComplete,
                    u.RestoreComplete,
                    u.EstimatedDocCount,
                    u.ActualDocCount,
                    u.AvgDocSizeBytes,
                    sourceStatus = u.SourceStatus.ToString(),
                    u.FailedOperation,
                    u.SkippedDueToMaxRetries,
                    opLogError = u.OpLogError.ToString(),
                    u.BulkCopyStartedOn,
                    u.BulkCopyEndedOn,
                    u.ChangeStreamStartedOn,
                    u.CSLastChecked,
                    u.CSLastChangeUTCTime
                })
            });
        }

        /// <summary>
        /// Shapes a job for the read APIs. MigrationJob holds endpoints rather than connection
        /// strings, so nothing here needs redacting.
        /// </summary>
        private object Summarize(MigrationJob job, List<MigrationUnit>? units, string? runningJobId)
        {
            units ??= new List<MigrationUnit>();

            return new
            {
                job.Id,
                job.Name,
                jobType = job.JobType.ToString(),
                cdcMode = job.CDCMode.ToString(),
                changeStreamLevel = job.ChangeStreamLevel.ToString(),
                changeStreamMode = job.ChangeStreamMode.ToString(),
                job.SourceEndpoint,
                job.TargetEndpoint,
                job.SourceServerVersion,
                job.IsStarted,
                job.IsCompleted,
                job.IsCancelled,
                job.IsSimulatedRun,
                job.SyncBackEnabled,
                isRunning = job.Id == runningJobId,
                job.StartedOn,
                job.CSLastChecked,
                units = new
                {
                    total = units.Count,
                    dumpComplete = units.Count(u => u.DumpComplete),
                    restoreComplete = units.Count(u => u.RestoreComplete),
                    failed = units.Count(u => !string.IsNullOrEmpty(u.FailedOperation)),
                    estimatedDocs = units.Sum(u => u.EstimatedDocCount),
                    actualDocs = units.Sum(u => u.ActualDocCount)
                }
            };
        }

        [HttpGet("{jobId}/logs")]
        public async Task<IActionResult> GetLogs(string jobId)
        {
            var denied = await AuthorizeAsync();
            if (denied != null)
                return denied;

            if (string.IsNullOrWhiteSpace(jobId))
                return BadRequest("jobId is required.");

            var logBucket = _jobManager.GetLogBucket(jobId, out string fileName, out bool isLiveLog);
            return Ok(new
            {
                jobId,
                isLiveLog,
                fileName,
                logs = logBucket.Logs ?? new List<LogObject>()
            });
        }

        [HttpPost("{jobId}/start")]
        public async Task<IActionResult> Start(string jobId, [FromBody] MigrationJobStartRequest? request)
        {
            var denied = await AuthorizeAsync();
            if (denied != null)
                return denied;

            if (string.IsNullOrWhiteSpace(jobId))
                return BadRequest("jobId is required.");

            var job = _jobManager.GetMigrationJobById(jobId);
            if (job == null)
                return NotFound($"No migration job with id '{jobId}'.");

            if (job.IsCompleted)
                return Conflict("Job is already completed.");

            var runningJobId = _jobManager.GetRunningJobId();
            if (!string.IsNullOrEmpty(runningJobId))
            {
                return runningJobId == jobId
                    ? Conflict("Job is already running.")
                    : Conflict($"Another migration job is running ({runningJobId}).");
            }

            // Body wins so a rotated credential can be supplied; otherwise reuse what the job was
            // started with, which is the whole point of not having to re-enter them after a recycle.
            string source, target;
            if (!string.IsNullOrWhiteSpace(request?.SourceConnectionString)
                && !string.IsNullOrWhiteSpace(request?.TargetConnectionString))
            {
                source = request!.SourceConnectionString!;
                target = request!.TargetConnectionString!;
                _jobManager.RememberConnectionStrings(job.Id, source, target);
            }
            else if (!_jobManager.TryResolveConnectionStrings(job.Id, out source, out target))
            {
                return BadRequest(
                    "No stored connection strings for this job. Supply sourceConnectionString and targetConnectionString.");
            }

            // Endpoints are what the job records, so a mismatch means these credentials point
            // somewhere else entirely -- refuse rather than migrate into the wrong cluster.
            var sourceEndpoint = Helper.ExtractHost(source);
            var targetEndpoint = Helper.ExtractHost(target);
            if (!string.IsNullOrEmpty(job.SourceEndpoint) && job.SourceEndpoint != sourceEndpoint)
                return BadRequest($"Source endpoint '{sourceEndpoint}' does not match the job's '{job.SourceEndpoint}'.");
            if (!string.IsNullOrEmpty(job.TargetEndpoint) && job.TargetEndpoint != targetEndpoint)
                return BadRequest($"Target endpoint '{targetEndpoint}' does not match the job's '{job.TargetEndpoint}'.");

            MigrationJobContext.SaveMigrationJob(job);
            MigrationJobContext.SaveJobList();

            // Both flags are mutated by the calls below, so report the pre-start values.
            var resumed = job.IsStarted;
            var syncBack = job.ProcessingSyncBack && !job.IsSimulatedRun;

            if (syncBack)
            {
                await _jobManager.SyncBackToSource(source, target, job);
            }
            else
            {
                await _jobManager.StartMigration(
                    job, source, target, job.NameSpaces ?? string.Empty, job.JobType, Helper.IsOnline(job));
            }

            return Accepted(new
            {
                jobId = job.Id,
                name = job.Name,
                started = true,
                resumed,
                syncBack,
                sourceEndpoint = job.SourceEndpoint,
                targetEndpoint = job.TargetEndpoint
            });
        }

        [HttpPost("import")]
        public async Task<IActionResult> Import([FromBody] MigrationJobImportRequest request)
        {
            try
            {
                var denied = await AuthorizeAsync();
                if (denied != null)
                    return denied;

                if (request?.Job == null
                    || string.IsNullOrWhiteSpace(request.SourceConnectionString)
                    || string.IsNullOrWhiteSpace(request.TargetConnectionString))
                {
                    return BadRequest("Job, sourceConnectionString, and targetConnectionString are required.");
                }

                var importedJob = JsonConvert.DeserializeObject<MigrationJob>(request.Job.Value.GetRawText());
                if (importedJob == null || string.IsNullOrWhiteSpace(importedJob.Name))
                    return BadRequest("Job.Name is required.");

                importedJob.SourceEndpoint = Helper.ExtractHost(request.SourceConnectionString);
                importedJob.TargetEndpoint = Helper.ExtractHost(request.TargetConnectionString);

                var existingJob = MigrationJobContext.JobList?.MigrationJobIds?
                    .Select(MigrationJobContext.GetMigrationJob)
                    .FirstOrDefault(job => string.Equals(job?.Name, importedJob.Name, StringComparison.Ordinal));

                importedJob.Id = existingJob?.Id ?? Guid.NewGuid().ToString();
                importedJob.IsStarted = false;
                importedJob.IsCompleted = false;
                importedJob.IsCancelled = false;

                // Through the manager so they also reach the ConnectionStringVault. Assigning the
                // in-memory dictionaries directly left an imported job with nothing on disk, so a
                // recycle before it was ever started lost the strings and the viewer could then
                // only offer a resume with updated ones.
                _jobManager.RememberConnectionStrings(importedJob.Id, request.SourceConnectionString, request.TargetConnectionString);

                if (request.Settings.HasValue)
                {
                    var appSettings = new MigrationSettings();
                    appSettings.Load();
                    // PopulateObject merges only the properties present in the payload, so callers
                    // can override one setting without restating the rest.
                    JsonConvert.PopulateObject(request.Settings.Value.GetRawText(), appSettings);
                    if (!appSettings.Save(out var appSettingsError))
                        return StatusCode(StatusCodes.Status500InternalServerError, appSettingsError);
                }

                if (request.SourceCaCertificatePem != null)
                {
                    var settings = new MigrationSettings();
                    settings.Load();
                    settings.CACertContentsForSourceServer = request.SourceCaCertificatePem;
                    if (!settings.Save(out var settingsError))
                        return StatusCode(StatusCodes.Status500InternalServerError, settingsError);
                }

                if (MigrationJobContext.JobList == null)
                    return StatusCode(StatusCodes.Status503ServiceUnavailable, "Migration job storage is not initialized.");

                MigrationJobContext.JobList.MigrationJobIds ??= new List<string>();
                if (!MigrationJobContext.JobList.MigrationJobIds.Contains(importedJob.Id))
                    MigrationJobContext.JobList.MigrationJobIds.Add(importedJob.Id);

                importedJob.MigrationUnitBasics ??= new List<MigrationUnitBasic>();
                if (request.Collections != null && request.Collections.Count > 0)
                {
                    // SaveMigrationUnit rejects any unit whose JobId differs from the active job, so
                    // importing while a different job is active discards every unit and still returns 200.
                    var activeJob = MigrationJobContext.CurrentlyActiveJob;
                    if (activeJob != null
                        && !string.Equals(activeJob.Id, importedJob.Id, StringComparison.Ordinal)
                        && activeJob.IsStarted && !activeJob.IsCompleted && !activeJob.IsCancelled)
                    {
                        return Conflict($"Migration job '{activeJob.Name}' is still running. Stop it before importing.");
                    }

                    MigrationJobContext.ActiveMigrationJobId = importedJob.Id;

                    var collectionJson = JsonConvert.SerializeObject(request.Collections);
                    var units = await Helper.PopulateJobCollectionsAsync(importedJob, collectionJson, request.SourceConnectionString);

                    if (!Helper.AddMigrationUnits(units, importedJob, MigrationJobContext.Logger))
                    {
                        return StatusCode(StatusCodes.Status500InternalServerError,
                            $"Resolved {units.Count} collection(s) for '{importedJob.Name}' but persisted only "
                            + $"{importedJob.MigrationUnitBasics.Count}. See the job log for the failing namespace.");
                    }

                    if (importedJob.MigrationUnitBasics.Count == 0)
                    {
                        return StatusCode(StatusCodes.Status500InternalServerError,
                            $"Resolved {units.Count} collection(s) for '{importedJob.Name}' but persisted none.");
                    }
                }

                if (!MigrationJobContext.SaveMigrationJob(importedJob) || !MigrationJobContext.SaveJobList())
                    return StatusCode(StatusCodes.Status500InternalServerError, "Failed to persist imported job.");

                return Ok(new { importedJob.Id, importedJob.Name, importedJob.MigrationUnitBasics?.Count });
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Migration job import failed: {ex}", "ImportMigrationJobs.txt");
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        /// <summary>
        /// Returns the response to send when the caller is not allowed, or null when it is.
        /// Behind an out-of-process reverse proxy (IIS/ANCM, kubectl port-forward) every request
        /// looks loopback, so the source address alone is not treated as an authentication factor.
        /// </summary>
        private async Task<IActionResult?> AuthorizeAsync()
        {
            if (!IsLoopbackRequest())
            {
                return StatusCode(StatusCodes.Status403Forbidden,
                    "This endpoint is only available from the machine hosting the app.");
            }

            if (!await _passwordManager.IsPasswordSetAsync())
            {
                return StatusCode(StatusCodes.Status503ServiceUnavailable,
                    "Set the application password in the web UI before using the migration job API.");
            }

            if (!await _passwordManager.ValidatePasswordAsync(Request.Headers[AppPasswordHeader].ToString()))
                return StatusCode(StatusCodes.Status401Unauthorized, $"A valid {AppPasswordHeader} header is required.");

            return null;
        }

        private bool IsLoopbackRequest()
        {
            var address = HttpContext.Connection.RemoteIpAddress;
            return address != null && System.Net.IPAddress.IsLoopback(address);
        }
    }

    public sealed class MigrationJobImportRequest
    {
        public JsonElement? Job { get; set; }
        public List<CollectionInfo>? Collections { get; set; }
        public string? SourceConnectionString { get; set; }
        public string? TargetConnectionString { get; set; }
        public string? SourceCaCertificatePem { get; set; }
        public JsonElement? Settings { get; set; }
    }

    public sealed class MigrationJobStartRequest
    {
        // Both optional: omit them to reuse what the job was started with.
        public string? SourceConnectionString { get; set; }
        public string? TargetConnectionString { get; set; }
    }
}
