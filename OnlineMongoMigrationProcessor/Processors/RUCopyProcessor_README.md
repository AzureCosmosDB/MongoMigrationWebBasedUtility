# RU Copy Processor

## Overview
The `RUCopyProcessor` is a specialized migration processor designed for Cosmos DB MongoDB API that implements incremental Change Feed processing via extension commands. This processor is optimized for handling large collections with partition-based processing to efficiently manage Request Units (RU) consumption.

## Key Features

### Partition-Based Processing
- Uses the `GetChangeStreamTokens` custom command to retrieve partition-specific tokens
- Processes multiple partitions concurrently (max 5 by default)
- Maintains individual state for each partition to enable resumable operations

### Change Stream Integration
- Implements historical change stream processing from the beginning of time to a specific point
- Processes Insert, Update, Delete, and Replace operations
- Supports both simulated and live migration modes

### RU Optimization
- Batch processing with configurable duration (5 minutes by default)
- Concurrent partition processing to maximize throughput
- Built-in retry mechanism with exponential backoff

### State Management
- Tracks partition completion state using `PartitionWatchState` model
- Persists progress to enable resumable migrations
- Integrates with existing `JobList` persistence mechanism

## Usage

The `RUCopyProcessor` implements the `IMigrationProcessor` interface and can be used similarly to other processors in the migration framework:

```csharp
var processor = new RUCopyProcessor(log, jobList, job, sourceClient, config);
await processor.StartProcessAsync(migrationUnit, sourceConnectionString, targetConnectionString);
```

## Configuration

The processor uses the following constants that can be adjusted:
- `MaxConcurrentPartitions`: Maximum number of partitions processed simultaneously (default: 5)
- `BatchDuration`: Time duration for each processing batch (default: 5 minutes)

## Integration Points

- **CopyProcessContext**: Reuses existing context initialization
- **MongoChangeStreamProcessor**: Integrates with post-copy change stream processing
- **RetryHelper**: Uses the framework's retry mechanism for fault tolerance
- **JobList**: Persists state using the existing job management system

## Error Handling

The processor includes comprehensive error handling for:
- Operation cancellation
- MongoDB execution timeouts
- Change stream token errors
- General exceptions with retry logic

## Partition State Tracking

Each partition maintains its own `PartitionWatchState` with:
- Current resume token
- Stop feed item (target completion point)
- Completion status
- Processed document count
- Last processed timestamp

This design enables fine-grained progress tracking and resumable operations across multiple partitions.