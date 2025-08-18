# Migration Tool for Azure Cosmos DB for MongoDB (vCore-Based)

Streamline your migration to Azure Cosmos DB for MongoDB (vCore‑based) with a reliable, easy‑to‑use web app. Choose your migration tool and migration mode (offline or online). The app orchestrates bulk copy and, for online jobs, change‑stream catch‑up. You don’t need to learn or use these command-line tools yourself.

### Migration tool

- **MongoDump and MongoRestore**: Uses mongo-tools. Best for moving from self-hosted or Atlas.
- **MongoDB Driver**: Uses the driver for bulk copy and change stream catch-up.
- **MongoDB Driver (Cosmos DB RU read optimized)**: Optimized for Cosmos DB Mongo RU as source. Uses RU-friendly read patterns.

### Migration mode

- **Offline**: Snapshot-only copy. Job completes automatically when copy finishes.
- **Online**: Copies bulk data, then processes change streams to catch up. Requires manual Cut Over to finish.


## Table of Contents

- [Key Features](#key-features)
- [Azure Deployment](#azure-deployment)
  - [Prerequisites](#prerequisites)
  - [Deploy on Azure using Source Files (option 1)](#deploy-on-azure-using-source-files-option-1)
  - [Deploy on Azure using precompiled binaries (option 2)](#deploy-on-azure-using-precompiled-binaries-option-2)
  - [VNet Integration for MongoDB servers within a private Virtual Network (VNet)](#vnet-integration-for-mongodb-servers-within-a-private-virtual-network-vnet)
  - [Integrating Azure Web App with a VNet to Use a Single Public IP (Optional)](#integrating-azure-web-app-with-a-vnet-to-use-a-single-public-ip-optional)
  - [Steps to Enable Private Endpoint on the Azure Web App (Optional)](#steps-to-enable-private-endpoint-on-the-azure-web-app-optional)
- [On-Premises Deployment](#on-premises-deployment)
  - [Steps to Deploy on a Windows Server](#steps-to-deploy-on-a-windows-server)
- [How to Use](#how-to-use)
  - [Add a New Job](#add-a-new-job)
  - [Migration modes](#migration-modes)
  - [Create Comma Separated List of Collections](#create-comma-separated-list-of-collections)
  - [Sequencing your collections](#sequencing-your-collections)
  - [View a Job](#view-a-job)
  - [Update Web App Settings](#update-web-app-settings)
  - [Remove a Job](#remove-a-job)
  - [Download Job Details](#download-job-details)
- [Job options and behaviors](#job-options-and-behaviors)
- [Collections input formats](#collections-input-formats)
  - [CollectionInfoFormat Format](#collectioninfoformat-format)
- [Settings (gear icon)](#settings-gear-icon)
- [Job lifecycle controls in Job Viewer](#job-lifecycle-controls-in-job-viewer)
- [RU-optimized copy (Cosmos DB source)](#ru-optimized-copy-cosmos-db-source)
- [Security and data handling](#security-and-data-handling)
- [Logs, backup, and recovery](#logs-backup-and-recovery)
- [Performance tips](#performance-tips)
- [Troubleshooting](#troubleshooting)

## Key Features

- **Flexible Migration Options**  
  Supports both online and offline migrations to suit your business requirements. It supports multiple tools to read data from the source and write it to the target. If you are migrating from on-premises MongoDB VM, consider using the [On-Premises Deployment](#on-premises-deployment) to install the app locally and transfer data to Azure Cosmos DB for MongoDB (vCore‑based). This eliminates the need to set up an Azure VPN.

- **User-Friendly Interface**  
  No steep learning curve—simply provide your connection strings and specify the collections to migrate.

- **Automatic Resume**  
  Migration resumes automatically in case of connection loss, ensuring uninterrupted reliability.

- **Private Deployment**  
  Deploy the tool within your private virtual network (VNet) for enhanced security. (Update `main.bicep` for VNet configuration.)

- **Standalone Solution**  
  Operates independently, with no dependencies on other Azure resources.

- **Scalable Performance**  
  Select your Azure Web App pricing plan based on performance requirements:  
  - Default: **B1**  
  - Recommended for large workloads: **Premium v3 P2V3** (Update `main.bicep` accordingly.)

- **Customizable**  
  Modify the provided C# code to suit your specific use cases.

---

Effortlessly migrate your MongoDB collections while maintaining control, security, and scalability. Begin your migration today and unlock the full potential of Azure Cosmos DB!

## Azure Deployment

Follow these steps to migrate data from a cloud-based MongoDB VM or MongoDB Atlas. You can deploy the utility either by building it from the source files or using the precompiled binaries.

### Prerequisites

1. Azure Subscription
1. Azure CLI Installed
1. PowerShell


### Deploy on Azure using Source Files (option 1)
This option involves cloning the repository and building the C# project source files locally on a Windows machine. If you’re not comfortable working with code, consider using Option 2 below.


1. Install [.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
2. Clone/Download the repository: `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility`
2. Open PowerShell.
2. Navigate to the cloned project folder.
3. Run the following commands in PowerShell:

   ```powershell
   # Variables to be updated
   $resourceGroupName = <Replace with Existing Resource Group Name>
   $webAppName = <Replace with Web App Name>
   $projectFolderPath = <Replace with path to cloned repo on local>


   # Paths - No changes required
   $projectFilePath = "$projectFolderPath\MongoMigrationWebApp\MongoMigrationWebApp.csproj"
   $publishFolder = "$projectFolderPath\publish"
   $zipPath = "$publishFolder\app.zip"

   # Login to Azure
   az login

   # Set subscription (optional)
   # az account set --subscription "your-subscription-id"

   # Deploy Azure Web App
   Write-Host "Deploying Azure Web App..."
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=WestUs3 webAppName=$webAppName


    # Configure NuGet path (execute only once on a machine)
   dotnet nuget add source https://api.nuget.org/v3/index.json -n nuget.org

    # Delete the existing publish folder (if it exists)
   if (Test-Path $publishFolder) {
		Remove-Item -Path $publishFolder -Recurse -Force -Confirm:$false
   }
	
	
   # Build the Blazor app
   Write-Host "Building Blazor app..."
   dotnet publish $projectFilePath -c Release -o $publishFolder -warnaserror:none --nologo
	
	# Delete the existing zip file if it exists
	if (Test-Path $zipPath) {
		Remove-Item $zipPath -Force
	}

		
   # Archive published files
   Compress-Archive -Path "$publishFolder\*" -DestinationPath $zipPath -Update

   # Deploy files to Azure Web App
   Write-Host "Deploying to Azure Web App..."
   az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

   Write-Host "Deployment completed successfully!"
   ```

4. Open `https://<WebAppName>.azurewebsites.net` to access the tool.
5. [Enable the use of a single public IP for consistent firewall rules](#integrating-azure-web-app-with-a-vnet-to-use-a-single-public-ip-optional) or [Enable Private Endpoint](#steps-to-enable-private-endpoint-on-the-azure-web-app-optional) if required.

### Deploy on Azure using precompiled binaries (option 2)

1. Clone/Download the repository: `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility`
1. Download the .zip file (excluding source code.zip and source code.tar.gz) from the latest release available at `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility/releases`.
1. Open PowerShell.
1. Navigate to the cloned project folder.
1. Run the following commands in PowerShell:

   ```powershell
   # Variables to be updated
   $resourceGroupName = <Replace with Existing Resource Group Name>
   $webAppName = <Replace with Web App Name>
   $zipPath = <Replace with full path of downloaded latest release zip file on local>

   # Login to Azure
   az login

   # Set subscription (optional)
   # az account set --subscription "your-subscription-id"

   # Deploy Azure Web App 
   Write-Host "Deploying Azure Web App..."
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=WestUs3 webAppName=$webAppName

   # Deploy files to Azure Web App
   Write-Host "Deploying to Azure Web App..."
   az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

   Write-Host "Deployment completed successfully!"
   ```

4. Open `https://<WebAppName>.azurewebsites.net` to access the tool.
5. [Enable the use of a single public IP for consistent firewall rules](#integrating-azure-web-app-with-a-vnet-to-use-a-single-public-ip-optional) or [Enable Private Endpoint](#steps-to-enable-private-endpoint-on-the-azure-web-app-optional) if required.

### VNet Integration for MongoDB servers within a private Virtual Network (VNet)

Accessing MongoDB servers within a VNet requires VNet injection. To enable connectivity to MongoDB servers located within a private VNet, ensure that [VNet integration](#1-enable-vnet-integration-for-the-web-app) is configured for your application.


## Integrating Azure Web App with a VNet to Use a Single Public IP (Optional)

### Steps

#### 1. Create a VNet (If Not Already Existing)

1. **Create a Virtual Network**:
   - Go to **Create a resource** in the Azure Portal.
   - Search for **Virtual Network** and click **Create**.
   - Provide a **Name** for the VNet.
   - Choose the desired **Region** (ensure it matches the Web App's region for integration).
   - Define the **Address Space** (e.g., `10.0.0.0/16`).

2. **Add a Subnet**:
   - In the **Subnet** section, create a new subnet.
   - Provide a **Name** (e.g., `WebAppSubnet`).
   - Define the **Subnet Address Range** (e.g., `10.0.1.0/24`).
   - Set the **Subnet Delegation** to `Microsoft.Web` for VNet integration.

3. **Create the VNet**:
   - Click **Review + Create** and then **Create**.
#### 1. Enable VNet Integration for the Web App

1. **Go to the Web App**:
   - Navigate to your Azure Web App in the Azure Portal.

2. **Enable VNet Integration**:
   - In the left-hand menu, select **Networking**.
   - Under **Outbound Traffic**, click **VNet Integration**.
   - Click **Add VNet** and choose an existing VNet and subnet.
   - Ensure the subnet is delegated to **Microsoft.Web**.
3. **Save** the configuration.

#### 2. Configure a NAT Gateway for the VNet
1. **Create a Public IP Address**:
   - Go to **Create a resource** in the Azure Portal.
   - Search for **Public IP Address** and click **Create**.
   - Assign a name and ensure it's set to **Static**.
   - Complete the setup.

2. **Create a NAT Gateway**:
   - Go to **Create a resource** in the Azure Portal.
   - Search for **NAT Gateway** and click **Create**.
   - Assign a name and link it to the **Public IP Address** created earlier.
   - Attach the NAT Gateway to the same subnet used for the Web App.

3. **Save** the configuration.

#### 3. Update Firewall Rules

  - In your firewall (e.g., Azure Firewall, third-party), allow traffic from the single public IP address used by the NAT Gateway.
   - Test access to MongoDB Source and Destination that have been configured with the new IP in their firewall rules.



## Steps to Enable Private Endpoint on the Azure Web App (Optional)

### Prerequisites

Ensure you have:

- An existing **Azure Virtual Network (VNet)**.
- A subnet dedicated to private endpoints (e.g., `PrivateEndpointSubnet`).
- Permissions to configure networking and private endpoints in Azure.

### Enable Private Endpoint

#### 1. Navigate to the Web App

- Open the **Azure Portal**.
- In the left-hand menu, select **App Services**.
- Click the desired web app.

#### 2. Access Networking Settings

- In the web app's blade, select **Networking**.
- Under the **Private Endpoint** section, click **+ Private Endpoint**.

#### 3. Create the Private Endpoint

Follow these steps in the **Add Private Endpoint** Advanced wizard:

##### a. Basics

- **Name**: Enter a name for the private endpoint (e.g., `WebAppPrivateEndpoint`).
- **Region**: Ensure it matches your VNet’s region.

##### b. Resource

- **Resource Type**: Select `Microsoft.Web/sites` for the web app.
- **Resource**: Choose your web app.
- **Target Sub-resource**: Select `sites`.

##### c. Configuration

- **Virtual Network**: Select your VNet.
- **Subnet**: Choose the `PrivateEndpointSubnet`.
- **Integrate with private DNS zone**: Enable this to link the private endpoint with an Azure Private DNS zone (recommended).

#### 4. Review and Create

- Click **Next** to review the configuration.
- Click **Create** to finalize.

#### 5. Verify Private Endpoint Connection

- Return to the **Networking** tab of your web app.
- Verify the private endpoint status under **Private Endpoint Connections** as `Approved`.

### Configure DNS (Optional but Recommended)

If using a private DNS zone:

1. **Validate DNS Resolution**:
   Run the following command to ensure DNS resolves to the private IP:

   ```bash
   nslookup <WebAppName>.azurewebsites.net
   ```

2. **Update Custom DNS Servers (if applicable)**:
   Configure custom DNS servers to resolve the private endpoint using Azure Private DNS.

### Test Connectivity

1. Deploy a **VM in the same VNet**.
2. From the VM, access the web app via its URL (e.g., `https://<WebAppName>.azurewebsites.net`).
3. Confirm that the web app is accessible only within the VNet.


## On-Premises Deployment

Follow these steps to migrate data from an on-premises MongoDB VM. You can deploy the utility by either compiling the source files or using the precompiled binaries.

### Steps to Deploy on a Windows Server

1. Prepare the Windows Server
    - Log in to the Windows Server using Remote Desktop or a similar method.
    - Ensure the server has internet access to download required components.
2. Install .NET 9 Runtime
    - Download the [.NET 9 Hosting Bundle](https://dotnet.microsoft.com/en-us/download/dotnet/thank-you/runtime-aspnetcore-9.0.6-windows-hosting-bundle-installer):
    - Visit the .NET Download Page.
    - Download the Hosting Bundle under the Runtime section.
    - Install the Hosting Bundle:
        - Run the installer and follow the instructions.
        - This will install the ASP.NET Core Runtime and configure IIS to work with .NET applications.
3. Install and Configure IIS (Internet Information Services)
    - Open Server Manager.
    - Click Add Roles and Features and follow these steps:
        - Select Role-based or feature-based installation.
        - Choose the current server.
        - Under Server Roles, select Web Server (IIS).
        - In the Role Services section, ensure the following are selected:
            - Web Server > Common HTTP Features > Static Content, Default Document.
            - Web Server > Application Development > .NET Extensibility 4.8, ASP.NET 4.8.
            - Management Tools > IIS Management Console.
        - Click Install to complete the setup.
        - 
4. Enable Required Windows Features by running the following PowerShell command

    ```powershell

    Enable-WindowsOptionalFeature -Online -FeatureName IIS-ASPNET45, IIS-NetFxExtensibility45

    ```
5. Create the App Directory
    - On the server, create an empty folder to store the MongoMigrationWebApp files. This folder will be referred to as the app directory.
    - Recommended path: C:\inetpub\wwwroot\MongoMigrationWeb
    - Configure File Permissions
        - Right-click the app directory, select Properties > Security.
        - Ensure that the **IIS_IUSRS** group has **Read** & **Execute** permissions.
6. Configure IIS for WebApp
    - Open IIS Manager (search IIS in the Start menu).
    - Right-click Sites in the left-hand pane and select Add Website.
        - Site Name: Enter a name for your site (e.g., MongoMigrationWeb).
        - Physical Path: Point to the app directory.
        - Binding: Configure the site to use the desired port (e.g., 8080 for HTTPS).
7. Set Up Application Pool
    - In IIS Manager, select Application Pools.
    - Create a new Application Pool:
        - Right-click and select Add Application Pool.
        - Name it (e.g., MongoMigrationWebPool).
        - Set the .NET CLR version to No Managed Code.
    - Select the site, click Basic Settings, and set the application pool to the one created.
8. Deploy the binaries to the app directory
    - Use precompiled binaries
        - Download the .zip file (excluding source code.zip and source code.tar.gz) from the latest release available at `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility/releases`.
        - Unzip the files to the app directory.
        - Ensure the web.config file is present in the root of your app directory. This file is critical for configuring the IIS hosting.
      or
    - Use Source Files
        1. Install [.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet/9.0)
        2. Clone/Download the repository: `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility`
        3. Open PowerShell.
        4. Navigate to the cloned project folder.
        5. Run the following commands in PowerShell:
    
               ```powershell
               # Variables to be updated
    
               $webAppName = <Replace with Web App Name>
               $projectFolderPath = <Replace with path to cloned repo on local>
            
            
               # Paths - No changes required
               $projectFilePath = "$projectFolderPath\MongoMigrationWebApp\MongoMigrationWebApp.csproj"
               $publishFolder = "$projectFolderPath\publish"
               $zipPath = "$publishFolder\app.zip"
                 
            
               # Configure Nuget Path. Execute only once on a machine
               dotnet nuget add source https://api.nuget.org/v3/index.json -n nuget.org
            
            
               # Build the Blazor app
               Write-Host "Building Blazor app..."
               dotnet publish $projectFilePath -c Release -o $publishFolder -warnaserror:none --nologo        	
            
           
               Write-Host "Published to "$publishFolder
               ```

        6. Copy the contents of the published folder to  the app directory.

10. Restart IIS from the right-hand pane.

## How to Use

### Add a New Job

1. Go to the home page: `https://<WebAppName>.azurewebsites.net` and click **New Job**.  
2. In the **New Job Details** pop-up, enter all required information.  
3. If necessary, use the [list collection steps](#create-comma-separated-list-of-collections) to create a comma-separated list of collection names.  
4. Choose the migration tool: either **Mongo Dump/Restore** or **Mongo Driver**.  
5. Select the desired [migration mode](#migration-modes).
1. Select **Post Migration Sync Back** to enable syncing back to the source after migration, this helps reduce risk by allowing a rollback to the original server if needed.
1. Select **Append Mode** to preserve existing collection(s) on the target without deleting them.  
1. Select **Skip Indexes** to prevent the tool from copying indexes from the source.
1. Once all fields are filled, select **OK**.  
1. The job will automatically start if no other jobs are running.  


**Note:** For the Mongo Dump/Restore option, the Web App will download the mongo-tools from the URL specified in the Web App settings. Ensure that the Web App has access to this URL. If the Web App does not have internet access, you can download the mongo-tools zip file to your development machine, then copy it to the wwwroot folder inside the published folder before compressing it. Afterward, update the URL in the Web App settings to point to the Web App’s URL (e.g., https://<WebAppName>.azurewebsites.net/<zipfilename.zip>).


### Migration modes

Migrations can be done in two ways:

- **Offline Migration**: A snapshot based bulk copy from source to target. New data added/updated/deleted on the source after the snapshot isn't copied to the target. The application downtime required depends on the time taken for the bulk copy activity to complete.

- **Online Migration**: Apart from the bulk data copy activity done in the offline migration, a change stream monitors all additions/updates/deletes. After the bulk data copy is completed, the data in the change stream is copied to the target to ensure that all updates made during the migration process are also transferred to the target. The application downtime required is minimal.


#### Oplog retention size

For online jobs, ensure that the oplog retention size of the source MongoDB is large enough to store operations for at least the duration of both the download and upload activities. If the oplog retention size is too small and there is a high volume of write operations, the online migration may fail or be unable to read all documents from the change stream in time.

### Create Comma Separated List of Collections

The below script lists all authorized databases and collections in the format `dbname.collectionname`, excluding system collections and databases you don’t have access to.

```javascript
// Use global `input` if it exists
const userInput = typeof input !== "undefined" ? input : null;

print("-------------------------------- ")

function isSystemCollection(name) {
    return name.startsWith("system.");
}

const result = [];

if (!input) {
    print("❌ Input required. Use '*.*' or 'dbname.*'");
    quit(1);
}

function listCollectionsSafely(dbName) {
    try {
        const currentDb = db.getSiblingDB(dbName);
        const collections = currentDb.getCollectionNames().filter(c => !isSystemCollection(c));
        collections.forEach(c => result.push(`${dbName}.${c}`));
    } catch (err) {
        console.error(`⚠️ Skipping ${dbName}: ${err.message}`);
    }
}



if (input === "*.*") {
    const dbs = db.adminCommand({ listDatabases: 1 }).databases;
    dbs.forEach(d => listCollectionsSafely(d.name));
} else if (input.endsWith(".*")) {
    const dbName = input.slice(0, -2);
    listCollectionsSafely(dbName);
} else {
    print("❌ Invalid input. Use '*.*' or 'dbname.*'");
    quit(1);
}

// Print the result as a single comma-separated string without line breaks
print(" ")
print("******OUTPUT****************")
print(result.join(","));
print("-------------------------------- ")

```

1. Save the script as `listCollections.js` in the same folder where you run `mongosh` or your current working directory
2. Run the script with `mongosh`

```bash
# List all collections in all accessible databases
mongosh "mongodb://localhost:27017"
input = "*.*";  // or "mydb.*"
load("listCollections.js");

# List collections in a specific database 'mydb'
mongosh "mongodb://localhost:27017"
input = "mydb.*"
load("listCollections.js");

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


### Update Web App Settings
1. From the home page  
2. Select the **gear icon** to open the settings page.  

### Remove a Job
1. From the home page  
2. Select the **bin icon** corresponding to the job you want to remove.  

### Download Job Details
1. From the home page  
2. Select the **download icon** next to the job title to download the job details as JSON. This may be used for debugging purposes. 

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

- Delay Change Stream (CS starts after uploads)
    - If ON: For online jobs, change stream processing starts only after all collections finish bulk upload.
    - If OFF: Change streams start per-collection after that collection’s bulk upload is done.

- Post Migration Sync Back
    - For online jobs, after Cut Over you can enable syncing from target back to source. This reduces rollback risk. UI shows Time Since Sync Back once active.

- Simulation Mode (No Writes to Target)
    - Runs a dry-run where reads occur but no writes are performed on target. Useful for validation and sizing.

## Collections input formats

You can specify collections in two ways:

1) CSV list
- Example: `db1.col1,db1.col2,db2.colA`
- Order matters. Larger collections should appear first to reduce overall time.

2) JSON list with optional filters
- Use the structure below (see `CollectionInfoFormat.JSON` in the repo):

Notes:
- Filters must be valid MongoDB query JSON (as a string). Only supports basic operators (`eq`,`lt`,`lte`,`gt`,`gte`,`in`) on root fields. They apply to both bulk copy and change stream.
- RU-optimized copy does not support filters; provide only DatabaseName and CollectionName.
- System collections are not supported.

### CollectionInfoFormat Format

 ```JSON
[
    { "CollectionName": "Customers", "DatabaseName": "SalesDB", "Filter": "{ \"status\": \"active\"}" },
    { "CollectionName": "Orders", "DatabaseName": "SalesDB", "Filter": "{ \"orderDate\": { \"$gte\": { \"$date\": \"2024-01-01T00:00:00Z\" } } }" },
    { "CollectionName": "Products", "DatabaseName": "InventoryDB", "Filter": null }
]
```

## Settings (gear icon) 

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


## RU-optimized copy (Cosmos DB source)

- Reduce RU consumption when reading from Cosmos DB for MongoDB (RU) by using change-feed-like incremental reads and partition batching. This method skips index creation during the copy; use the schema migration script at https://aka.ms/mongoruschemamigrationscript to create the indexes on the target collections. It focuses on efficient incremental ingestion. Collection filters are not supported.

## Security and data handling

- Connection strings are not persisted on disk (they’re excluded from persistence). Endpoints are stored for display and validation.
- Provide a PEM CA certificate if your source uses a private CA for TLS.
- Consider deploying behind VNet integration and/or Private Endpoint (see earlier sections) to restrict access.

## Logs, backup, and recovery

- Logs: Each job writes under `migrationlogs`. From Job Viewer you can download the current log or a backup if corruption is detected.
- Job state: Persisted under `migrationjobs\list.json` with rotating backups every few minutes. Use the “Recover Jobs” button on Home to restore from the best available backup snapshot. This stops any running jobs and replaces the in-memory list with the recovered state.

## Performance tips

- Choose an appropriate App Service plan (P2v3 recommended for large or high-TPS workloads). You can dedicate a web app per large collection.
- For driver copy, tune “Mongo driver page size” upward for higher throughput, but watch memory and target write capacity.
- For online jobs, ensure oplog retention on the source is large enough to cover full bulk copy duration plus catch-up.
- For Dump/Restore, set chunk size to balance disk IO and memory; ensure sufficient disk space on the working folder drive.

## Troubleshooting

- “Unable to load job details” on Home: Use “Recover Jobs” to restore the latest healthy snapshot.
- Change stream lag not decreasing: Confirm the job is running, source writes exist, and consider increasing plan size or reducing concurrent collections.
- RU-optimized copy stalls: Validate source is Cosmos DB Mongo vCore, ensure no partition split warnings, and verify target write capacity.