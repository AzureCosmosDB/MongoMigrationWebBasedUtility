# Migration Tool for Azure DocumentDB

Streamline your migration to Azure DocumentDB with a reliable, easy‑to‑use web app. Choose your migration tool and migration mode (offline or online). The app orchestrates bulk copy and, for online jobs, change‑stream catch‑up. You don’t need to learn or use these command-line tools yourself.

### Migration tool

- **MongoDump and MongoRestore**: Uses mongo-tools. Best for moving from self-hosted or Atlas.
- **MongoDB Driver**: Uses the driver for bulk copy and change stream catch-up.
- **MongoDB Driver (Cosmos DB RU read optimized)**: Optimized for Cosmos DB Mongo RU as source. Uses RU-friendly read patterns.

### Migration mode

- **Offline**: Snapshot-only copy. Job completes automatically when copy finishes.
- **Online**: Copies bulk data, then processes change streams to catch up. Requires manual Cut Over to finish.


## Table of Contents

- [Key Features](#key-features)
- [Azure Deployment Options](#azure-deployment-options)
  - [Option 1: Azure Web App (App Service)](#option-1-azure-web-app-app-service)
  - [Option 2: Azure Container Apps (ACA)](#option-2-azure-container-apps-aca)
  - [Choosing the Right Deployment](#choosing-the-right-deployment)
- [On-Premises Deployment](#on-premises-deployment)
- [How to Use](#how-to-use)
  - [Schema Migration Tool](#schema-migration-tool)
  - [Add a New Job](#add-a-new-job)
  - [Migration modes](#migration-modes)
  - [Get List of Collections](#get-list-of-collections)
  - [View a Job](#view-a-job)
  - [Update Web App Settings](#update-web-app-settings)
  - [Remove a Job](#remove-a-job)
  - [Download Job Details](#download-job-details)
  - [Dynamic Scaling of MongoDump/Restore Workers](#dynamic-scaling-of-mongodumprestore-workers)
  - [Difference Between Immediate Pause and Controlled Pause](#difference-between-immediate-pause-and-controlled-pause)
  - [Multiple Partitioners for ObjectId and Benefits of Selecting DataType for _id](#multiple-partitioners-for-objectid-and-benefits-of-selecting-datatype-for-_id)
- [Job options and behaviors](#job-options-and-behaviors)
- [Collections input formats](#collections-input-formats)
  - [CollectionInfoFormat JSON Format](#collectioninfoformat-json-format)
- [Job lifecycle controls in Job Viewer](#job-lifecycle-controls-in-job-viewer)
- [RU-optimized copy (Cosmos DB source)](#ru-optimized-copy-cosmos-db-source)
- [Security and data handling](#security-and-data-handling)
- [Logs, backup, and recovery](#logs-backup-and-recovery)
- [Performance tips](#performance-tips)
- [Troubleshooting](#troubleshooting)

## Key Features

- **Flexible Migration Options**  
  Supports both online and offline migrations to suit your business requirements. It supports multiple tools to read data from the source and write it to the target. If you are migrating from on-premises MongoDB VM, consider using the [On-Premises Deployment](#on-premises-deployment) to install the app locally and transfer data to Azure DocumentDB. This eliminates the need to set up an Azure VPN.

- **User-Friendly Interface**  
  No steep learning curve—simply provide your connection strings and specify the collections to migrate.

- **Automatic Resume**  
  Migration resumes automatically in case of connection loss, ensuring uninterrupted reliability.

- **Private Deployment**  
  Deploy the tool within your private virtual network (VNet) for enhanced security. (Update `WebApp/main.bicep` for VNet configuration.)

- **Standalone Solution**  
  Operates independently, with no dependencies on other Azure resources.

- **Scalable Performance**  
  Select your Azure Web App pricing plan based on performance requirements:  
  - Default: **B1**  
  - Recommended for large workloads: **Premium v3 P2V3** (Update `WebApp/main.bicep` accordingly.)

- **Customizable**  
  Modify the provided C# code to suit your specific use cases.

---

Effortlessly migrate your MongoDB collections while maintaining control, security, and scalability. Begin your migration today and unlock the full potential of Azure Cosmos DB!

## Azure Deployment Options

The MongoDB Migration Web-Based Utility can be deployed to Azure using two different options, each optimized for different workload requirements:

### Option 1: Azure Web App (App Service)

**Best for**: Small to medium workloads and short-running migrations

- ✅ Quick and simple deployment
- ✅ Lower cost for smaller workloads
- ✅ Standard web application hosting
- ✅ Ideal for migrations under 24 hours

**[📖 Deploy to Azure Web App Guide](WebApp/README.md)**

### Option 2: Azure Container Apps (ACA)

**Best for**: Medium to large workloads and long-running migrations

- ✅ Configurable high-performance compute (up to 32 vCores, 64GB RAM)
- ✅ Persistent storage that survives deployments (100GB Azure File Share)
- ✅ Dedicated resources for intensive migrations
- ✅ Enterprise networking with VNet integration
- ✅ Ideal for migrations over 24 hours

**[📖 Deploy to Azure Container Apps Guide](ACA/README.md)**

### Choosing the Right Deployment

| Feature | Azure Web App | Azure Container Apps |
|---------|---------------|----------------------|
| **Workload Size** | Small to Medium | Medium to Large |
| **Migration Duration** | < 24 hours | 24+ hours |
| **CPU** | Shared/Basic tier | Up to 32 dedicated vCores |
| **Memory** | Up to 14GB (P3V3) | Up to 64GB |
| **Persistent Storage** | App Service storage | 100GB Azure File Share |
| **Deployment Time** | < 10 minutes | ~ 15-20 minutes |
| **Cost** | Lower for small workloads | Optimized for large workloads |
| **Best Use Case** | Quick migrations, dev/test | Production, large-scale migrations |

### Need Help Deciding?

- **Start small?** Use [Azure Web App](WebApp/DeployToWebApp_README.md) for quick setup and testing
- **Production migration?** Use [Azure Container Apps](ACA/DeployToACA_README.md) for reliability and performance
- **Not sure?** Start with Web App and migrate to Container Apps if needed

## On-Premises Deployment

**Best for**: Migrating from on-premises MongoDB VMs directly to Azure DocumentDB

- ✅ Eliminates need for Azure VPN setup
- ✅ Direct local access to source MongoDB
- ✅ Deploy on existing Windows Server infrastructure
- ✅ Full IIS-based hosting with .NET 9

**[📖 Deploy to On-Premises Windows Server Guide](OnPremise/README.md)**

## How to Use

### Schema Migration Tool

Before migrating your data, it's essential to prepare your target Azure DocumentDB environment with the proper schema structure. The **Schema Migration Tool** (located in the `/SchemaMigration` folder) automates this critical preparation step.

#### What It Does

The Schema Migration Tool analyzes your source MongoDB collections and intelligently transforms them for optimal performance on Azure DocumentDB. It handles:

- **Collection Creation**: Automatically creates target collections with appropriate configurations
- **Index Migration**: Transfers and optimizes indexes from source to target, including:
  - Simple and compound indexes
  - Unique indexes
  - TTL (Time-To-Live) indexes
  - Partial filter indexes
- **Shard Key Migration**: Optionally migrates sharding configurations for distributed collections
- **Index Optimization**: Identifies and removes redundant compound indexes to improve performance
- **Collection Colocation**: Groups related collections together for better query performance

#### Why Schema Migration Must Be Performed First

Running the Schema Migration Tool **before** data migration is crucial for several reasons:

1. **Performance Optimization**: Creating indexes after data is loaded can be extremely time-consuming and resource-intensive. Pre-creating indexes ensures optimal write performance during bulk data migration.

2. **Schema Compatibility**: Azure DocumentDB may have different schema requirements or optimizations compared to your source MongoDB. The tool ensures your target schema is properly configured before data arrives.

3. **Sharding Configuration**: If your collections are sharded, the shard key must be defined before any data is written to the collection. You cannot add or modify shard keys after data insertion.

#### Quick Start

```bash
cd SchemaMigration
python main.py --config-file <config.json> --source-uri <source_connection_string> --dest-uri <target_connection_string>
```

**[📖 Complete Schema Migration Guide](SchemaMigration/README.md)**

#### Recommended Workflow

1. ✅ **Schema Migration**: Run the Schema Migration Tool to prepare your Azure DocumentDB environment
2. ✅ **Verify Schema**: Confirm all collections, indexes, and shard keys are correctly created
3. ✅ **Data Migration**: Use the main Migration Web App to copy your data
4. ✅ **Validate**: Verify data integrity and application compatibility

> **Important**: When running data migration after schema migration, ensure you enable **Append Mode** in the Migration Web App. This preserves the collections, indexes, and shard keys created by the Schema Migration Tool. Without Append Mode, the data migration tool may drop and recreate collections, losing your optimized schema configuration.

### Add a New Job

1. Go to the home page. Click **New Job**.  
2. In the **New Job Details** pop-up, enter all required information.  
3. Refer to [Collections input formats](#collections-input-formats) to learn multiple ways to input collection list for migration. If necessary, use the [list collection steps](#get-list-of-collections) to create a comma-separated list of collection names.  
4. Choose the migration tool: either **Mongo Dump/Restore** or **Mongo Driver**.  
5. Select the desired [migration mode](#migration-modes).
1. Select **Append Mode** to preserve existing collection(s) on the target without deleting them.  
1. Select **Skip Indexes** to prevent the tool from copying indexes from the source.
1. Once all fields are filled, select **OK**.  
1. The job will automatically start if no other jobs are running.  


**Note for Azure Web App deployments:** For the Mongo Dump/Restore option, the Web App will download the mongo-tools from the URL specified in the Web App settings. Ensure that the Web App has access to this URL. If the Web App does not have internet access, you can download the mongo-tools zip file to your development machine, then copy it to the wwwroot folder inside the published folder before compressing it. Afterward, update the URL in the Web App settings to point to the Web App's URL (e.g., https://<WebAppName>.azurewebsites.net/<zipfilename.zip>). This note does not apply to Azure Container Apps (ACA) deployments, where mongo-tools are pre-installed in the container image.


### Migration modes

Migrations can be done in two ways:

- **Offline Migration**: A snapshot based bulk copy from source to target. New data added/updated/deleted on the source after the snapshot isn't copied to the target. The application downtime required depends on the time taken for the bulk copy activity to complete.

- **Online Migration**: Apart from the bulk data copy activity done in the offline migration, a change stream monitors all additions/updates/deletes. After the bulk data copy is completed, the data in the change stream is copied to the target to ensure that all updates made during the migration process are also transferred to the target. The application downtime required is minimal.


#### Oplog retention size

For online jobs, ensure that the oplog retention size of the source MongoDB is large enough to store operations for at least the duration of both the download and upload activities. If the oplog retention size is too small and there is a high volume of write operations, the online migration may fail or be unable to read all documents from the change stream in time.

### Get List of Collections

Run  the below script in mongo shell  to lists all authorized databases and collections in the format `dbname.collectionname`.

Set `currentOnly=true` to list collections from the current database. To list collections across all databases (excluding system collections and system databases), set `currentOnly=false`.

```javascript
// Optional boolean to restrict to current DB only
const currentOnly = true;

print("-------------------------------- ");

function isSystemCollection(name) {
    return name.startsWith("system.");
}

function isSystemDatabase(name) {
    return (
        name === "admin" ||
        name === "config" ||
        name === "local" ||
        name.startsWith("system")
    );
}

const result = [];

function listCollectionsSafely(dbName) {
    if (isSystemDatabase(dbName)) {
        print(`⚠️ Skipping system database: ${dbName}`);
        return;
    }
    try {
        const currentDb = db.getSiblingDB(dbName);
        const collections = currentDb.getCollectionNames().filter(c => !isSystemCollection(c));
        collections.forEach(c => result.push(`${dbName}.${c}`));
    } catch (err) {
        print(`⚠️ Skipping ${dbName}: ${err.message}`);
    }
}

if (currentOnly) {
    // Use only the current database (skip if it’s system)
    listCollectionsSafely(db.getName());
} else {
    // Enumerate all databases
    const dbs = db.adminCommand({ listDatabases: 1 }).databases;
    dbs.forEach(d => listCollectionsSafely(d.name));
}

print(" ");
print("******OUTPUT****************");
// Print result as CSV (db.coll,db.coll,...)
print(result.join(","));
print("-------------------------------- ");


```

#### Sequencing your collections

The job processes collections in the order they are added. Since larger collections take more time to migrate, it’s best to arrange the collections in descending order of their size or document count.

### View a Job

1. From the home page  
2. Select the **eye icon** corresponding to the job you want to view.  
3. On the **Job Details** page, you will see the collections to be migrated and their status in a tabular format.
 
1. Depending on the job's status, one or more of the following buttons will be visible:  
   - **Resume Job**: Visible if the migration is paused. Select this to resume the current job. The app may prompt you to provide the connection strings if the cache has expired.
   - **Pause Job**: Visible if the current job is running. Select this to pause the current job. You can resume it later.  
   - **Update Collections**: Select this to add/remove collections in the current job. You can only update collections for a paused job. Removing a collection that is partially or fully migrated will lead to the loss of its migration details, and you will need to remigrate it from the start.  
   - **Cut Over**: Select this to cut over an online job when the source and target are completely synced. Before cutting over, ensure to stop write traffic to the source and wait for the Time Since Last Change to become zero for all collections. Once cut over is performed, there is no rollback.  
1. The **Monitor** section lists the current actions being performed.  
1. The **Logs** section displays system-generated logs for debugging. You can download the logs by selecting the **download icon** next to the header.  

**Note**: An offline job will automatically terminate once the data is copied. However, an online job requires a manual cut over to complete.

#### Time Since Last Change

Time Since Last Change refers to the time difference between the timestamp of the last processed change and the current time. During an online migration, the lag will be high immediately after the upload completes, but it should decrease as change stream processing starts, eventually reaching zero. If the lag does not reduce, consider the following:

- Ensure the job is not paused and is processing requests. Resume the job if necessary.
- Monitor for new write operations on the source. If no new changes are detected, the lag will increase. However, this is not an issue since all changes have already been processed.
- Check if the transactions per second on the source are very high; in this case, you may need a larger app service plan or a dedicated web app for the collection.

#### Time Since Sync Back

Time Since Sync Back indicates the time elapsed between the most recent Sync Back operation and the current time. This metric becomes relevant after the application cutover, once the target account begins receiving traffic.

Initially, you may not see updates here until the application cutover is complete. Once active, the value should update regularly. If it doesn't, consider the following:

- Confirm that the target account is live and actively receiving traffic.
- Ensure the Sync Back process is running and not paused.
- Monitor for incoming changes on the target. If no new writes are occurring, the lag may grow, which is expected in the absence of new data.

#### Time Since Last Batch (CS Last Checked)

Time Since Last Batch displays the time elapsed since the change stream was last checked for updates. This metric is shown in the Job Viewer UI as **CS Last Checked** and indicates how recently the change stream processor examined the collection for new changes.

This value helps monitor the health and activity of the change stream processing:

- A small value (e.g., seconds or a few minutes) indicates active monitoring and regular checks of the change stream.
- A large or increasing value may suggest the change stream processor is not running, the job is paused, or there's an issue preventing regular checks.
- If this value is "NA", it means the change stream has not yet been initialized or checked for this collection.

This is different from "Time Since Last Change" which shows when the last actual change was processed, whereas "Time Since Last Batch" shows when the change stream was last polled for updates.


### Update Web App Settings

These settings are persisted per app instance and affect all jobs:

- Mongo tools download URL
    - HTTPS ZIP URL to mongo-tools used by Dump/Restore. If your app has no internet egress, upload the ZIP alongside your app content and point this URL to your app’s public URL of the file.
    - Must start with https:// and end with .zip

- Binary format utilized for the _id
    - Use when your source uses binary GUIDs for _id.

- Chunk size (MB) for mongodump
    - Range: 2–5120. Affects download-and-upload batching for Dump/Restore.

- Mongo driver page size
    - Range: 50–40000. Controls batch size for driver-based bulk reads/writes.

- Change stream max docs per batch
    - Range: 100–10000. Larger batches reduce overhead but increase memory/latency.

- Change stream batch duration (max/min, seconds)
    - Max range: 20–3600; Min range: 10–600; Min must be less than Max. Controls how long change processors run per batch window.

- Max collections per change stream batch
    - Range: 1–30. Concurrency limit when processing multiple collections.

- Sample size for hash comparison
    - Range: 5–2000. Used by the “Run Hash Check” feature to spot-check document parity.

- ObjectId Partitioner
    - Choose the partitioning method for ObjectId-based collections:
      - **Use Sample Command**: Uses MongoDB's $sample command (recommended for most workloads)
      - **Use Time Boundaries**: Time-based boundaries using ObjectId timestamps (recommended for well distributed objectId workloads)
      - **Use Adjusted Time Boundaries**: Time-based boundaries adjusted by actual record counts (best for balanced chunks)
      - **Use Pagination**: Pagination-based boundaries for equal-sized chunks (deterministic)
    - See [Multiple Partitioners for ObjectId](#multiple-partitioners-for-objectid-and-benefits-of-selecting-datatype-for-_id) for detailed explanations.

- CA certificate file for source server (.pem)
    - Paste/upload the PEM (CA chain) if your source requires a custom CA to establish TLS.

Advanced notes:
- App setting `AllowMongoDump` (see `MongoMigrationWebApp/appsettings.json`) toggles whether the “MongoDump and MongoRestore” option is available in the UI.
- The app’s working folder defaults to the system temp path, or to `%ResourceDrive%\home\` when present (e.g., on Azure App Service). It stores job state under `migrationjobs` and logs under `migrationlogs`.

## Job lifecycle controls in Job Viewer

- **Resume Job**: Resume with updated or existing connection strings.
- **Pause Job**: Safely pause the running job.
- **Cut Over**: For online jobs, enabled when change stream lag reaches zero for all collections. You can choose Cut Over with or without Sync Back.
- **Update Collections**: Add/remove collections on a paused job. Removing a collection discards its migration and change stream state.
- **Reset Change Stream**: For selected collections, reset the checkpoint to reprocess from the beginning. Useful if you suspect missed events.
- **Run Hash Check**: Randomly samples documents and compares hashes between source and target to detect mismatches. Controlled by the Settings sample size.

### Remove a Job
1. From the home page  
2. Select the **bin icon** corresponding to the job you want to remove.  

### Download Job Details
1. From the home page  
2. Select the **download icon** next to the job title to download the job details as JSON. This may be used for debugging purposes. 

### Dynamic Scaling of MongoDump/Restore Workers

The migration tool supports **runtime adjustment** of parallel worker counts for both dump and restore operations, allowing you to optimize performance based on system load and resource availability.

#### Understanding Worker Pools

The tool uses three types of workers during MongoDump/Restore migration:

1. **Dump Workers**: Control how many `mongodump` processes run in parallel to extract data from the source
2. **Restore Workers**: Control how many `mongorestore` processes run in parallel to load data into the target
3. **Insertion Workers**: Control the `--numInsertionWorkersPerCollection` parameter for each `mongorestore` process

#### How to Adjust Workers During Migration

1. **Navigate to Job Viewer**: Open the job that is currently running
2. **Locate the Worker Controls**: Look for the worker adjustment section showing current worker counts
3. **Adjust Workers**:
   - **Dump Workers**: Use the +/- buttons to increase or decrease parallel dump processes (range: 1-16)
   - **Restore Workers**: Use the +/- buttons to increase or decrease parallel restore processes (range: 1-16)
   - **Insertion Workers**: Adjust the number of insertion threads per restore process (range: 1-16)

#### How Dynamic Scaling Works

**Increasing Workers (Scale Up)**:
- When you increase dump/restore workers during active processing, new worker tasks are **spawned immediately**
- New workers join the existing workers to process remaining chunks from the queue
- You'll see log messages like: `"Spawning 2 additional dump workers to process queue"`
- The semaphore capacity increases, allowing more concurrent operations

**Decreasing Workers (Scale Down)**:
- When you decrease worker count, the system reduces available capacity using "blocker tasks"
- Active workers continue processing their current chunks
- No new chunks are assigned beyond the reduced capacity
- Workers complete gracefully without interruption
- You'll see log messages showing the capacity change: `"Dump: Decreased from 5 to 3 (-2)"`

#### Best Practices

**When to Increase Workers**:
- System CPU/memory utilization is low (< 60%)
- Network bandwidth is underutilized
- You have many small collections or chunks remaining
- Database throughput on source/target can handle higher load

**When to Decrease Workers**:
- System resources are constrained (high CPU/memory)
- Database is experiencing throttling or high latency
- You want to reduce impact on production systems
- Other applications on the same server need resources

**Optimal Settings**:
- **Dump Workers**: Start with 2-4 for most workloads; increase to 6-8 for high-throughput systems
- **Restore Workers**: Match or slightly exceed dump workers if target can handle the load
- **Insertion Workers**: Default is ProcessorCount/2; increase to 8-12 for large collections with high write capacity

**Note**: All worker pools always use the parallel infrastructure, even when set to 1. This ensures dynamic scaling works at any time during migration.

---

### Difference Between Immediate Pause and Controlled Pause

The migration tool offers two pause mechanisms with different behaviors:

#### Immediate Pause (Hard Stop)

**How to Trigger**: Click the **Pause Job** button in Job Viewer

**Behavior**:
- Cancellation token is immediately triggered
- All active workers check the cancellation token and exit as soon as possible
- Currently executing processes are **killed immediately**
- In-progress chunks may be left incomplete
- Job state is saved with partial progress

**When to Use**:
- Emergency stop required (system issues, unexpected errors)
- Need to free resources immediately
- Discovered configuration errors that need correction
- Critical production issue requires stopping migration

**Resume Behavior**:
- Job resumes from the last saved state
- Incomplete chunks are retried from the beginning (Note: Processing incomplete chunks is time-consuming as duplicate records prevent batch transactions and make the process extremely slow)
- Change streams resume from last saved token

#### Controlled Pause (Graceful Stop)

**How to Trigger**: Click the **Controlled Pause** button in Job Viewer

**Behavior**:
- Sets `_controlledPauseRequested` flag to true
- Active workers **complete their current chunks** before exiting
- No new tasks are queued
- Existing processes finish naturally
- All progress is saved cleanly
- When dump completes during controlled pause, **restore does NOT auto-start**


**When to Use**:
- Planned maintenance window approaching
- Want to avoid re-processing in-flight chunks
- Need clean checkpoint for later resume
- System resources need temporary reallocation
- End of business day, want to pause cleanly overnight

**Resume Behavior**:
- Job resumes exactly where it left off
- No chunks need to be re-processed
- State is consistent and complete
- For collections where dump completed during pause, resume will trigger restore automatically

#### Comparison Table

| Feature | Immediate Pause | Controlled Pause |
|---------|----------------|------------------|
| **Stop Speed** | Instant | 30 seconds - 2 minutes |
| **Process Handling** | Kill immediately | Complete gracefully |
| **Chunk State** | May be incomplete | Always complete |
| **Data Safety** | Safe, but some rework | Safest, no rework |
| **Resource Cleanup** | Immediate | Gradual |
| **Resume Efficiency** | Some chunks retry | Maximum efficiency |
| **Use Case** | Emergency | Planned pause |


### Multiple Partitioners for ObjectId and Benefits of Selecting DataType for _id

#### Understanding Data Partitioning

When migrating large collections, the tool **splits the collection into chunks** based on the `_id` field range. This enables:
- Parallel processing of different chunks simultaneously
- Better memory management (smaller working sets)
- Faster overall migration through concurrency
- Ability to retry failed chunks without re-processing entire collection

#### Partitioner Types

The tool offers **four partitioning strategies** specifically optimized for ObjectId data types:

##### 1. **Use Time Boundaries** (Recommended for most workloads)

**How it Works**:
- Extracts the embedded timestamp from ObjectId values
- Calculates equidistant time-based boundaries using BigInteger arithmetic
- Partitions based on time ranges distributed evenly across the collection's time span
- Creates chunks like boundaries at specific ObjectId values representing time intervals

**Benefits**:
- **Chronologically aligned**: Chunks correspond to time periods
- **Predictable distribution**: Recent data tends to be in later chunks
- **Efficient for time-series data**: Natural ordering matches data insertion patterns
- **Fast boundary calculation**: Uses mathematical distribution without sampling
- **Optimal for append-heavy workloads**: Newer documents cluster together

**Best For**:
- Collections where documents are primarily inserted chronologically
- Time-series data (logs, events, transactions)
- Collections with steady insert rate over time
- Large collections (millions of documents) where sampling would be expensive

##### 2. **Use Adjusted Time Boundaries** (Best for balanced chunks)

**How it Works**:
- First generates time-based boundaries (same as #1 above)
- Then validates actual record counts in each range
- Adjusts boundaries to ensure each chunk has 1K-1M records
- Merges small ranges and splits large ranges dynamically

**Benefits**:
- **Balanced chunk sizes**: Each chunk has similar document count (1K-1M records)
- **Handles uneven distribution**: Adapts when writes are clustered in time
- **Prevents tiny/huge chunks**: Automatic validation and adjustment
- **Optimal parallelism**: Even workload distribution across workers

**Best For**:
- Collections with non-uniform write patterns (bursts of activity)
- Variable document sizes where time ranges don't correlate with record counts
- When you need predictable chunk processing times
- Production workloads requiring stable performance

##### 3. **Use Pagination** (Deterministic equal-sized chunks)

**How it Works**:
- Calculates total document count and desired records per range
- Uses progressive `$gt` filters with skip/limit to sample ObjectIds at regular intervals
- Creates boundaries by paginating through the collection (e.g., every 100K records)
- Avoids loading all documents into memory

**Benefits**:
- **Equal chunk sizes**: Each chunk has the same number of records
- **Memory efficient**: Progressive filtering instead of full collection scan
- **Deterministic**: Consistent chunk boundaries based on record position
- **Works with any _id distribution**: Doesn't depend on time properties

**Best For**:
- Collections with random or unpredictable ObjectId patterns
- When you need exact control over chunk size
- Collections where time-based approaches produce imbalanced chunks
- Debugging scenarios requiring predictable record counts per chunk

##### 4. **Use Sample Command** (Fallback for small collections)

**How it Works**:
- Uses MongoDB's `$sample` aggregation command to randomly sample ObjectIds
- Sorts sampled values and creates boundaries
- Quick and simple approach for smaller datasets

**Benefits**:
- **Simple implementation**: Leverages MongoDB's built-in sampling
- **Low overhead**: Single aggregation query
- **Works universally**: No assumptions about data distribution

**Best For**:
- Small to medium collections (< 1M documents)
- Quick migrations where optimization isn't critical
- Collections where other methods fail or timeout
- Development/testing scenarios

#### Specifying DataType for _id: Why It Matters

When you specify `"DataTypeFor_Id": "ObjectId"` in the collection configuration, you unlock significant performance benefits:

##### Without DataType Specified (Generic Partitioning)

```json
{ "CollectionName": "Orders", "DatabaseName": "SalesDB" }
```

**Behavior**:
- Tool must scan collection to determine `_id` data type
- Uses generic partitioning suitable for mixed or unknown types
- Performs additional type checks during processing
- Cannot leverage ObjectId-specific optimizations
- Slower chunk boundary calculation

##### With DataType Specified (ObjectId)

```json
{ "CollectionName": "Orders", "DatabaseName": "SalesDB", "DataTypeFor_Id": "ObjectId" }
```

**Behavior**:
- Tool **immediately selects the optimal partitioner** for ObjectId
- **Skips type detection overhead** (saves 1-2 queries per collection)
- Enables **timestamp-based partitioning** by default
- Optimizes query generation with ObjectId-specific operators
- **Faster chunk boundary calculation** using ObjectId properties

#### Performance Impact

**Collection with 10M documents**:
- **Without DataType**: ~30 seconds to analyze and partition
- **With DataType (ObjectId)**: ~5 seconds to partition
- **Speedup**: 6x faster startup

**Chunk Processing**:
- **Without DataType**: Generic queries like `{ "_id": { "$gte": <value>, "$lt": <value> } }`
- **With DataType (ObjectId)**: Optimized queries leveraging timestamp component
- **Database efficiency**: Better index utilization, fewer type coercion operations

#### Supported DataType Values

You can specify these values for `DataTypeFor_Id`:

| DataType | Use Case | Partitioning Strategy |
|----------|----------|----------------------|
| **ObjectId** | Standard MongoDB collections | Timestamp-based or random sampling |
| **Int** | Integer `_id` fields | Numeric range partitioning |
| **Int64** | Long integer `_id` | Large numeric range partitioning |
| **Decimal128** | High-precision numeric `_id` | Decimal range partitioning |
| **Date** | DateTime `_id` values | Chronological partitioning |
| **BinData** | Binary GUID/UUID `_id` | Binary range partitioning |
| **String** | String-based `_id` | Lexicographic partitioning |
| **Object** | Compound `_id` objects | Hash-based partitioning |

#### How to Configure

**Using CollectionInfoFormat JSON**:

```json
[
    {
        "CollectionName": "Users",
        "DatabaseName": "AppDB",
        "DataTypeFor_Id": "ObjectId"
    },
    {
        "CollectionName": "Sessions",
        "DatabaseName": "AppDB",
        "DataTypeFor_Id": "String"
    },
    {
        "CollectionName": "Analytics",
        "DatabaseName": "AppDB",
        "DataTypeFor_Id": "Date"
    }
]
```

#### Best Practices

**Always Specify DataType When**:
- Collection has more than 1 million documents
- You know the `_id` field type is consistent
- Performance is critical (large-scale migrations)
- Collection uses ObjectId for `_id` (most common case)

**Let Tool Auto-Detect When**:
- Collection has mixed `_id` types (rare, but possible)
- Small collections (< 100K documents) where overhead is negligible
- Unsure about `_id` type consistency
- Testing/development scenarios

**Verification**:
1. Before migration, run this query to check `_id` type consistency:
```javascript
db.collection.aggregate([
    {
        $group: {
            _id: { $type: "$_id" },
            count: { $sum: 1 }
        }
    }
])
```

2. If result shows single type (e.g., "objectId"), specify that type for optimal performance

#### Advanced: Chunk Size Tuning

When using ObjectId with specified DataType, you can optimize chunk count:

**For Time-Based Partitioning**:
- Collections with ~1M docs/month: Use monthly chunks (12 chunks/year)
- Collections with ~1M docs/week: Use weekly chunks (52 chunks/year)
- Collections with ~1M docs/day: Use daily chunks (365 chunks/year)

The tool automatically calculates optimal chunk count based on:
- Total document count estimate
- Target chunk size (configurable in Settings)
- ObjectId timestamp distribution

**Result**: Balanced parallelism without creating too many tiny chunks or too few large chunks.



## Job options and behaviors

When creating or resuming a job, you can tailor behavior via these options:

- Migration tool
    - MongoDump and MongoRestore: Uses mongo-tools. Best for moving from self-hosted or Atlas to Cosmos DB. Requires access to Mongo Tools ZIP URL configured in Settings.
    - MongoDB Driver: Uses the driver for bulk copy and change stream catch-up.
    - MongoDB Driver (Cosmos DB RU read optimized): Optimized for Cosmos DB Mongo vCore as source. Skips index creation and uses RU-friendly read patterns. Filters on collections are not supported for this mode.

- Migration mode
    - Offline: Snapshot-only copy. Job completes automatically when copy finishes.
    - Online: Copies bulk data, then processes change streams to catch up. Requires manual Cut Over to finish.

- Append Mode
    - If ON: Keeps existing target data and appends new documents. No collection drop. Good for incremental top-ups.
    - If OFF: Target collections are overwritten. A confirmation checkbox is required to proceed.

- Skip Indexes
    - If ON: Skips index creation on target (data only). Forced ON for RU-optimized copy. You can create indexes separately before migration.

- Change Stream Modes
    - Delayed: Start change stream processing after all collections are completed. This mode is ideal when migrating a large number of collections.
    - Immediate: Start change stream processing immediately as each collection is processed.
    - Aggressive: Use aggressive change stream processing when the oplog is small or the write rate is very high. Avoid this mode if a large number of collections need to be migrated.

- Post Migration Sync Back
    - For online jobs, after Cut Over you can enable syncing from target back to source. This reduces rollback risk. UI shows Time Since Sync Back once active.

- All collections use ObjectId for the _id field
    - If ON: Automatically applies `"DataTypeFor_Id": "ObjectId"` to all collections in the job, enabling ObjectId-specific optimizations.
    - Benefits: Faster partitioning (6x speedup), optimized chunk boundaries, leverages timestamp-based partitioning strategies.
    - If OFF: Each collection's _id type is auto-detected, or you can specify DataTypeFor_Id individually via JSON format.
    - See [Multiple Partitioners for ObjectId](#multiple-partitioners-for-objectid-and-benefits-of-selecting-datatype-for-_id) for performance details.

- Simulation Mode (No Writes to Target)
    - Runs a dry-run where reads occur but no writes are performed on target. Useful for validation and sizing.

## Collections input formats

You can specify collections in two ways:

1) CSV list
- Example: `db1.col1,db1.col2,db2.colA`
- Order matters. Larger collections should appear first to reduce overall time.
- 
2) CSV list with wildcards
- Example: `db1.*,*.users,*.*`
- If the collection count is large consider splitting it into multiple jobs.

3) JSON list with optional filters
- Use the [CollectionInfoFormat JSON Format](#collectioninfoformat-json-format):

Notes:
- Filters must be valid MongoDB query JSON (as a string). Only supports basic operators (`eq`,`lt`,`lte`,`gt`,`gte`,`in`) on root fields. They apply to both bulk copy and change stream.
- Specify DataTypeFor_Id if the collection contains only a single data type for the _id field. Supported values are: ObjectId, Int, Int64, Decimal128, Date, BinData, String, and Object.
- RU-optimized copy does not support filters or DataTypeFor_Id ; provide only DatabaseName and CollectionName.
- System collections are not supported.

### CollectionInfoFormat JSON Format

 ```JSON
[
    { "CollectionName": "Customers", "DatabaseName": "SalesDB", "Filter": "{ \"status\": \"active\"}" },
    { "CollectionName": "Orders", "DatabaseName": "SalesDB", "Filter": "{ \"orderDate\": { \"$gte\": { \"$date\": \"2024-01-01T00:00:00Z\" } } }" },
    { "CollectionName": "Products", "DatabaseName": "InventoryDB", "DataTypeFor_Id": "ObjectId" }
]
```

## RU-optimized copy (Cosmos DB source)

- Reduce RU consumption when reading from Cosmos DB for MongoDB (RU) by using change-feed-like incremental reads and partition batching. This method skips index creation during the copy; use the schema migration script at https://aka.ms/mongoruschemamigrationscript to create the indexes on the target collections. It focuses on efficient incremental ingestion. Collection filters are not supported.

## Security and data handling

- Connection strings are not persisted on disk (they’re excluded from persistence). Endpoints are stored for display and validation.
- Provide a PEM CA certificate if your source uses a private CA for TLS.
- Consider deploying behind VNet integration and/or Private Endpoint (see earlier sections) to restrict access.
- **Entra ID Authentication (Optional)**: You can configure Entra ID-based authentication for your Azure Web App to ensure only valid users can access the migration tool. This adds an additional layer of security by requiring users to authenticate with their organizational credentials. For configuration details, see [Azure App Service Authentication](https://learn.microsoft.com/en-us/azure/static-web-apps/authentication-authorization).


## Performance tips

- Choose an appropriate App Service plan (P2v3 recommended for large or high-TPS workloads). You can dedicate a web app per large collection.
- For driver copy, tune “Mongo driver page size” upward for higher throughput, but watch memory and target write capacity.
- For online jobs, ensure oplog retention on the source is large enough to cover full bulk copy duration plus catch-up.
- For Dump/Restore, set chunk size to balance disk IO and memory; ensure sufficient disk space on the working folder drive.

## Troubleshooting

- Change stream lag not decreasing: Confirm the job is running, source writes exist, and consider increasing plan size or reducing concurrent collections.
- RU-optimized copy stalls: Validate source is Cosmos DB Mongo vCore, ensure no partition split warnings, and verify target write capacity.
- **ACA deployment - App not loading**: If the application is not loading in Azure Container Apps deployment, open the browser console using F12 (Developer Tools). If you see an error stating "statestore connection is invalid", update the connection string following the steps provided in the [ACA README](ACA/README.md).