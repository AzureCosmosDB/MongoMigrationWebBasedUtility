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

                MigrationJobContext.SourceConnectionString[importedJob.Id] = request.SourceConnectionString;
                MigrationJobContext.TargetConnectionString[importedJob.Id] = request.TargetConnectionString;

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
    }
}
