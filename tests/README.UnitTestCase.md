# Unit Tests for MongoMigrationWebBasedUtility

This test suite covers core helpers and retry orchestration. It includes unit tests runnable locally and a list of scenarios that are best validated manually (integration/system tests).

## Projects
- tests/OnlineMongoMigrationProcessor.Tests (xUnit, .NET 9)

## How to run

From repository root:

```powershell
# Restore and run all tests
dotnet test

# Run only this test project
dotnet test .\tests\OnlineMongoMigrationProcessor.Tests\OnlineMongoMigrationProcessor.Tests.csproj

# Filter by a test name or trait
dotnet test .\tests\OnlineMongoMigrationProcessor.Tests\OnlineMongoMigrationProcessor.Tests.csproj --filter FullyQualifiedName~RetryHelperTests
```

## What’s covered by unit tests

- RetryHelper
  - Success immediately
  - Retries on `TaskResult.Retry` and eventually succeeds
  - Aborts when exception handler decides so
  - Fails after max retries

- SafeDictionary
  - AddOrUpdate semantics
  - Remove and ContainsKey behavior
  - TryGetFirst returns an arbitrary mu when available

- Helper
  - EncodeMongoPasswordInConnectionString
  - ValidateNamespaceFormat
  - RedactPii
  - SafeFileName
  - UpdateAppName (when URI is parsable)
  - ExtractHost

> Note: We avoid integration points that touch the file system heavily or spawn external processes (mongodump/mongorestore) in unit tests.

## Manual test scenarios (recommended)

These require a MongoDB environment and (optionally) the MongoDB Database Tools installed.

1. Dump/Restore end-to-end (single collection)
   - Configure a job with a collection of known size.
   - Verify `DumpComplete`, `RestoreComplete`, and document counts match.

2. Chunked restore and count verification
   - Use a large collection split into multiple chunks.
   - Intentionally create a mismatch to trigger re-processing of a chunk.

3. Low disk space backpressure
   - Place a size cap or fill the drive to simulate low free space.
   - Confirm uploads are queued and resume after space frees.

4. Change Stream per-mu mode
   - Set `IsOnline=true` and `CSStartsAfterAllUploads=false`.
   - Verify the change stream begins after an mu finishes restoring.

5. Change Stream after-all-uploads mode
   - Set `IsOnline=true` and `CSStartsAfterAllUploads=true`.
   - Verify post-upload processor starts only after all eligible items complete.

6. Simulated run mode
   - Set `IsSimulatedRun=true` to skip external processes.
   - Ensure code paths that would start processes are not executed and flags finalize appropriately.

7. Resume job with existing partial dumps
   - Restart the app with an incomplete job.
   - Ensure the uploader picks up pending chunks and completes them.

8. Index restore options
   - Validate `--noIndexRestore` and `--drop` behavior based on `AppendMode` and `SkipIndexes` across first vs subsequent chunks.

9. Abort and cancellation
   - Force an `OperationCanceledException` via Stop.
   - Ensure tasks abort promptly and state is saved.

10. Error logging and rotation
   - Tail `migrationlogs/*.bin` and confirm new logs append, rotation works for in-memory buffer, and backup file creation on read failure.

## Troubleshooting
- If tests fail to compile due to missing SDKs, install .NET 9 SDK.
- If file-path assertions fail, ensure you’re running on Windows (paths in production code assume `\\` separators in some places).

