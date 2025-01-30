# Migration Tool for Azure Cosmos DB for MongoDB (vCore-Based)

Streamline your migration to Azure Cosmos DB for MongoDB (vCore-based) with a tool built for efficiency, reliability, and ease of use. Whether you're migrating data online or offline, this tool delivers a seamless experience tailored to your requirements. It can either use `mongodump` and `mongorestore` for data movement or employ the MongoDB driver to read data from the source and write it to the target. You don’t need to learn or use these command-line tools yourself.

## Key Features

- **Flexible Migration Options**  
  Supports both online and offline migrations to suit your business requirements. It can either use `mongodump` and `mongorestore` for data movement or employ the MongoDB driver to read data from the source and write it to the target. If you are migrating an on-premises MongoDB VM, consider using the [On-Premise Deployment](#deploy-mongomigrationwebapp-on-a-onpremise-windows-server) to install the app locally and transfer data to Azure. This eliminates the need to set up an Azure VPN.

- **User-Friendly Interface**  
  No steep learning curve—simply provide your connection strings and specify the collections to migrate.

- **Automatic Resume**  
  Migration resumes automatically in case of connection loss, ensuring uninterrupted reliability.

- **Private Deployment**  
  Deploy the tool within your private virtual network (vNet) for enhanced security. (Update `main.bicep` for vNet configuration.)

- **Standalone Solution**  
  Operates independently, with no dependencies on other Azure resources.

- **Scalable Performance**  
  Select your Azure Web App pricing plan based on performance requirements:  
  - Default: **B1**  
  - Recommended for large workloads: **Premium v3 P1V3** (Update `main.bicep` accordingly.)

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


   # Configure Nuget Path. Execute only once on a machine
   dotnet nuget add source https://api.nuget.org/v3/index.json -n nuget.org


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

### Deploy on Azure using using precompiled binaries (option 2)

1. Download the .zip file (excluding source code.zip and source code.tar.gz) from the latest release available at `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility/releases`.
2. Open PowerShell.
3. Run the following commands in PowerShell:

   ```powershell
   # Variables to be updated
   $resourceGroupName = <Replace with Existing Resource Group Name>
   $webAppName = <Replace with Web App Name>
   $zipPath = <Replace with full path of downloaded zip file on local>

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
6. Keep in mind that [vNet injection](#1-enable-vnet-integration-for-the-web-app) is required if the source or target MongoDB servers are within a private vNet.


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


## On-Premise Deployment

Follow these steps to migrate data from an on-premises MongoDB VM. You can deploy the utility by either compiling the source files or using the precompiled binaries.

### Steps to Deploy on a Windows Server

1. Prepare the Windows Server
    - Log in to the Windows Server using Remote Desktop or a similar method.
    - Ensure the server has internet access to download required components.
2. Install .NET 6 Runtime
    - Download the .NET 6 Hosting Bundle:
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
        1. Install [.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
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
1. From the home page (https://<WebAppName>.azurewebsites.net), select **New Job**.
2. In the "New Job Details" pop-up, provide the necessary details and select OK.
3. Choose the migration tool: either "Mongo Dump/Restore" or "Mongo Driver".
4. The job will automatically start if no other jobs are running.

#### Migrations modes

Migrations can be done in two ways:

- Offline Migration: A snapshot based bulk copy from source to target. New data added/updated/deleted on the source after the snapshot isn't copied to the target. The application downtime required depends on the time taken for the bulk copy activity to complete.

- Online Migration: Apart from the bulk data copy activity done in the offline migration, a change stream monitors all additions/updates/deletes. After the bulk data copy is completed, the data in the change stream is copied to the target to ensure that all updates made during the migration process are also transferred to the target. The application downtime required is minimal.

#### Oplog retention size

For online jobs, ensure that the oplog retention size of the source MongoDB is large enough to store operations for at least the duration of both the download and upload activities. If the oplog retention size is too small and there is a high volume of write operations, the online migration may fail or be unable to read all documents from the change stream in time.

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
   - **Cut Over**: Select this to cut over an online job when the source and target are completely synced. Before cutting over, ensure to stop write traffic to the source and wait for the Change Stream Lag to become zero for all collections. Once cut over is performed, there is no rollback.  
1. The **Monitor** section lists the current actions being performed.  
1. The **Logs** section displays system-generated logs for debugging. You can download the logs by selecting the **download icon** next to the header.  

**Note**: An offline job will automatically terminate once the data is copied. However, an online job requires a manual cut over to complete.

#### Change Stream Lag

Change Stream Lag refers to the time difference between the timestamp of the last processed change and the current time. During an online migration, the lag will be high immediately after the upload completes, but it should decrease as change stream processing starts, eventually reaching zero. If the lag does not reduce, consider the following:

- Ensure the job is not paused and is processing requests. Resume the job if necessary.
- Monitor for new write operations on the source. If no new changes are detected, the lag will increase. However, this is not an issue since all changes have already been processed.
- Check if the transactions per second on the source are very high; in this case, you may need a larger app service plan or a dedicated web app for the collection.



### Remove a Job
1. From the home page  
2. Select the **bin icon** corresponding to the job you want to remove.  

### Download Job Details
1. From the home page  
2. Select the **download icon** next to the job title to download the job details as JSON. This may be used for debugging purposes. 