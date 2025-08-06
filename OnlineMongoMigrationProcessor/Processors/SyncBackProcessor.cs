using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Reflection.Metadata.BlobBuilder;

namespace OnlineMongoMigrationProcessor.Processors
{
    internal class SyncBackProcessor : IMigrationProcessor
    {
        public bool ProcessRunning { get; set; }
        private MongoChangeStreamProcessor? _syncBackToSource;
        private JobList? _jobList;
        private MigrationJob? _job;
        private bool _executionCancelled = false;

        private MigrationSettings? _config;
        private CancellationTokenSource _cts;
        private Log _log;

        public SyncBackProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config, string toolsLaunchFolder)
        {
            _log = log;
			_jobList = jobList ?? throw new ArgumentNullException(nameof(jobList), "JobList cannot be null.");
            _job = job ?? throw new ArgumentNullException(nameof(job), "MigrationJob cannot be null.");
            _config = config ?? throw new ArgumentNullException(nameof(config), "MigrationSettings cannot be null.");
        }

        public void StopProcessing(bool updateStatus = true)
        {           

            if (_job != null)
                _job.IsStarted = false;
            _jobList?.Save();

            if(updateStatus) 
                ProcessRunning = false;

            _executionCancelled = true;
            _cts?.Cancel();
            if (_syncBackToSource != null)
                _syncBackToSource.ExecutionCancelled = true;

            _syncBackToSource = null;   
        }

        public async Task StartProcessAsync(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            ProcessRunning = true;

            if (_job != null)
                _job.IsStarted = true;

            int maxRetries = 10;
            int attempts = 0;
            TimeSpan backoff = TimeSpan.FromSeconds(2);

            var sourceClient = MongoClientFactory.Create(_log, sourceConnectionString, false, _config.CACertContentsForSourceServer);
            var targetClient = MongoClientFactory.Create(_log, targetConnectionString);

            _syncBackToSource = null;
            _syncBackToSource = new MongoChangeStreamProcessor(_log, sourceClient, targetClient, _jobList, _job,_config,true);

            _cts=new CancellationTokenSource();

            bool continueProcessing = true;

            
            while (attempts < maxRetries && !_executionCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    _log.WriteLine($"Sync back to source starting.");


                    foreach(MigrationUnit unit in _job.MigrationUnits)
                    {
                        if (!unit.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            unit.SyncBackChangeStreamStartedOn = DateTime.UtcNow;
						}
					}

					var result = _syncBackToSource.RunCSPostProcessingAsync(_cts);

                    //
                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    _log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Attempt {attempts} failed: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    _log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);

                }
            }
            if (attempts == maxRetries)
            {
                _log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                
                continueProcessing = false;
                StopProcessing();

                _syncBackToSource.ExecutionCancelled = true;
                _syncBackToSource = null;
            }
        }
    }
}
