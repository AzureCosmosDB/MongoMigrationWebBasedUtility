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
        private readonly Service.JobManager _jobManager;

        public MigrationJobsController(Service.JobManager jobManager)
        {
            _jobManager = jobManager;
        }

        [HttpPost("reset")]
        public IActionResult Reset()
        {
            if (!IsLoopbackRequest())
                return Forbid();

            if (_jobManager.GetMigrationIds().Any(_jobManager.IsProcessRunning))
                return Conflict("Cannot reset while a migration job is running.");

            return Ok(new { deletedJobs = _jobManager.ClearAllJobFiles() });
        }

        [HttpGet("{jobId}/logs")]
        public IActionResult GetLogs(string jobId)
        {
            if (!IsLoopbackRequest())
                return Forbid();

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
                if (!IsLoopbackRequest())
                    return Forbid();

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
                    Helper.AddMigrationUnits(units, importedJob, MigrationJobContext.Logger);

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

        private bool IsLoopbackRequest()
        {
            var address = HttpContext.Connection.RemoteIpAddress;
            return address != null && (System.Net.IPAddress.IsLoopback(address)
                || address.Equals(HttpContext.Connection.LocalIpAddress));
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