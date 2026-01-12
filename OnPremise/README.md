# Deploy to On-Premises Windows Server

This guide explains how to deploy the MongoDB Migration Web-Based Utility to an on-premises Windows Server. This deployment option is ideal for migrating data from an on-premises MongoDB VM directly to Azure DocumentDB without needing to set up an Azure VPN.

## When to Use On-Premises Deployment

**Best for:**
- ✅ Migrating from on-premises MongoDB VMs
- ✅ Eliminating need for Azure VPN setup
- ✅ Direct local access to source MongoDB
- ✅ Organizations with existing Windows Server infrastructure
- ✅ Data locality requirements during migration

## Prerequisites

- Windows Server with Remote Desktop access
- Internet access to download required components
- Administrative privileges on the server
- Existing MongoDB instance (source)
- Azure Cosmos DB account (target)

## Deployment Options

You can deploy the utility either by compiling the source files or using precompiled binaries.

## Steps to Deploy on a Windows Server

### 1. Prepare the Windows Server

- Log in to the Windows Server using Remote Desktop or a similar method
- Ensure the server has internet access to download required components

### 2. Install .NET 9 Runtime

Download the [.NET 9 Hosting Bundle](https://dotnet.microsoft.com/en-us/download/dotnet/thank-you/runtime-aspnetcore-9.0.6-windows-hosting-bundle-installer):

1. Visit the .NET Download Page
2. Download the Hosting Bundle under the Runtime section
3. Install the Hosting Bundle:
   - Run the installer and follow the instructions
   - This will install the ASP.NET Core Runtime and configure IIS to work with .NET applications

### 3. Install and Configure IIS (Internet Information Services)

1. Open Server Manager
2. Click **Add Roles and Features** and follow these steps:
   - Select **Role-based or feature-based installation**
   - Choose the current server
   - Under **Server Roles**, select **Web Server (IIS)**
   - In the **Role Services** section, ensure the following are selected:
     - Web Server > Common HTTP Features > Static Content, Default Document
     - Web Server > Application Development > .NET Extensibility 4.8, ASP.NET 4.8
     - Management Tools > IIS Management Console
   - Click **Install** to complete the setup

### 4. Enable Required Windows Features

Run the following PowerShell command:

```powershell
Enable-WindowsOptionalFeature -Online -FeatureName IIS-ASPNET45, IIS-NetFxExtensibility45
```

### 5. Create the App Directory

1. On the server, create an empty folder to store the MongoMigrationWebApp files. This folder will be referred to as the **app directory**
2. **Recommended path**: `C:\inetpub\wwwroot\MongoMigrationWeb`
3. **Configure File Permissions**:
   - Right-click the app directory, select **Properties > Security**
   - Ensure that the **IIS_IUSRS** group has **Read** & **Execute** permissions

### 6. Configure IIS for WebApp

1. Open IIS Manager (search IIS in the Start menu)
2. Right-click **Sites** in the left-hand pane and select **Add Website**
   - **Site Name**: Enter a name for your site (e.g., `MongoMigrationWeb`)
   - **Physical Path**: Point to the app directory
   - **Binding**: Configure the site to use the desired port (e.g., 8080 for HTTP)

### 7. Set Up Application Pool

1. In IIS Manager, select **Application Pools**
2. Create a new Application Pool:
   - Right-click and select **Add Application Pool**
   - Name it (e.g., `MongoMigrationWebPool`)
   - Set the **.NET CLR version** to **No Managed Code**
3. Select the site, click **Basic Settings**, and set the application pool to the one created

### 8. Deploy the Binaries to the App Directory

Choose one of the following options:

#### Option 1: Use Precompiled Binaries (Recommended)

1. Download the `.zip` file (excluding source code.zip and source code.tar.gz) from the [latest release](https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility/releases)
2. Unzip the files to the app directory
3. Ensure the `web.config` file is present in the root of your app directory. This file is critical for configuring the IIS hosting

#### Option 2: Build from Source Files

1. **Install [.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet/9.0)**

2. **Clone/Download the repository**: `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility`

3. **Open PowerShell**

4. **Navigate to the cloned project folder**

5. **Run the following commands in PowerShell**:

   ```powershell
   # Variables to be updated
   $webAppName = <Replace with Web App Name>
   $projectFolderPath = <Replace with path to cloned repo on local>

   # Paths - No changes required
   $projectFilePath = "$projectFolderPath\MongoMigrationWebApp\MongoMigrationWebApp.csproj"
   $publishFolder = "$projectFolderPath\publish"
   $zipPath = "$publishFolder\app.zip"

   # Configure NuGet Path. Execute only once on a machine
   dotnet nuget add source https://api.nuget.org/v3/index.json -n nuget.org

   # Build the Blazor app
   Write-Host "Building Blazor app..."
   dotnet publish $projectFilePath -c Release -o $publishFolder -warnaserror:none --nologo

   Write-Host "Published to $publishFolder"
   ```

6. **Copy the contents of the published folder to the app directory**

### 9. Restart IIS

Restart IIS from the right-hand pane in IIS Manager.

### 10. Access the Application

Open your browser and navigate to:
- `http://localhost:8080` (or the port you configured)
- Or from another machine: `http://<ServerIP>:8080`

## Post-Deployment Configuration

### Configure Firewall Rules

If accessing the application from other machines:

1. Open Windows Firewall with Advanced Security
2. Create an inbound rule for the port (e.g., 8080)
3. Allow TCP connections on that port

### Security Recommendations

1. **Enable HTTPS**: Configure SSL certificate in IIS for production use
2. **Authentication**: Consider enabling Windows Authentication or other authentication methods in IIS
3. **Network Security**: Restrict access to the application using firewall rules or network segmentation

## Troubleshooting

### Application Not Starting

1. Check IIS logs: `C:\inetpub\logs\LogFiles`
2. Check Application Event Viewer for .NET runtime errors
3. Verify .NET 9 Hosting Bundle is installed correctly
4. Ensure the Application Pool is running

### Permission Issues

1. Verify **IIS_IUSRS** has Read & Execute permissions on the app directory
2. Check that the Application Pool identity has necessary permissions

### Port Already in Use

1. Change the binding port in IIS to an available port
2. Update firewall rules accordingly

## Additional Resources

- [IIS Configuration Reference](https://learn.microsoft.com/en-us/iis/configuration/)
- [ASP.NET Core Hosting on Windows](https://learn.microsoft.com/en-us/aspnet/core/host-and-deploy/iis/)
- [.NET Download](https://dotnet.microsoft.com/download)

## Migrating to Azure Later?

If you need to scale up or move to Azure later:
- [Deploy to Azure Web App](../WebApp/DeployToWebApp_README.md) - For small to medium workloads
- [Deploy to Azure Container Apps](../ACA/DeployToACA_README.md) - For large, long-running migrations
