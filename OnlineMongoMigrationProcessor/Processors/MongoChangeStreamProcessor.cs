using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;
using OnlineMongoMigrationProcessor.Context;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

#if !LEGACY_MONGODB_DRIVER
namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// Factory class that creates and manages the appropriate change stream processor based on configuration
    /// </summary>
    public class MongoChangeStreamProcessor
    {
        private ChangeStreamProcessor _processor;
        private readonly Log _log;
        private readonly MigrationWorker? _migrationWorker;
      

        public bool ExecutionCancelled
        {
            get => _processor?.ExecutionCancelled ?? false;
            set
            {
                if (_processor != null)
                    _processor.ExecutionCancelled = value;
            }
        }

        public Func<string, Task>? WaitForResumeTokenTaskDelegate
        {
            get => _processor?.WaitForResumeTokenTaskDelegate;
            set
            {
                if (_processor != null)
                    _processor.WaitForResumeTokenTaskDelegate = value;
            }
        }

        public MongoChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient,  ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
        {
            MigrationJobContext.AddVerboseLog($"MongoChangeStreamProcessor: Constructor called, syncBack={syncBack}");
            _log = log;
            _migrationWorker = migrationWorker;

            // Create the appropriate processor based on configuration
            _processor = CreateProcessor(log, sourceClient, targetClient,muCache, config, syncBack, migrationWorker);
        }

        private ChangeStreamProcessor CreateProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache,  MigrationSettings config, bool syncBack, MigrationWorker? migrationWorker)
        {
            MigrationJobContext.AddVerboseLog($"MongoChangeStreamProcessor.CreateProcessor: syncBack={syncBack}, ChangeStreamLevel={MigrationJobContext.CurrentlyActiveJob?.ChangeStreamLevel}");
            // Determine which processor to use
            bool useServerLevel = MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy;

            if (useServerLevel)
            {
                _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}Using server-level change stream processor.");
                return new ServerLevelChangeStreamProcessor(log, sourceClient, targetClient,  muCache ,config, syncBack, migrationWorker);
            }
            else
            {
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                {
                    _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}RUOptimizedCopy jobs do not support server-level change streams. Using collection-level processor.");
                }
                else
                {
                    _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}Using collection-level change stream processor.");
                }
                return new CollectionLevelChangeStreamProcessor(log, sourceClient, targetClient, muCache, config, syncBack, migrationWorker);
            }
        }

        // Delegate methods to the underlying processor
        public bool AddCollectionsToProcess(string  migrationUnitId, CancellationTokenSource cts)
        {
            MigrationJobContext.AddVerboseLog($"MongoChangeStreamProcessor.AddCollectionsToProcess: migrationUnitId={migrationUnitId}");
            return _processor.AddCollectionsToProcess(migrationUnitId, cts);
        }

        public async Task RunChangeStreamProcessorForAllCollections(CancellationTokenSource cts)
        {
            MigrationJobContext.AddVerboseLog("MongoChangeStreamProcessor.RunChangeStreamProcessorForAllCollections: starting post-processing");
            await _processor.RunChangeStreamProcessorForAllCollections(cts);
        }
    }
}
#endif // !LEGACY_MONGODB_DRIVER