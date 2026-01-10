# Deploy to Azure Web App

This guide explains how to deploy the MongoDB Migration Web-Based Utility to Azure App Service (Web App). This deployment option is ideal for **small to medium workloads** and **short-running migrations**.

## When to Use Azure Web App Deployment

**Best for:**
- ✅ Small to medium-sized databases
- ✅ Short-running migration jobs (less than 24 hours)
- ✅ Quick setup and deployment
- ✅ Lower cost for smaller workloads
- ✅ Standard web application hosting needs

**Consider [Azure Container Apps](../ACA/DeployToACA_README.md) instead if:**
- ❌ Large databases requiring extended processing
- ❌ Long-running migration jobs (24+ hours)
- ❌ Need for dedicated high-performance compute (8-32 vCores, up to 64GB RAM)
- ❌ Requirement for persistent storage across deployments

## Prerequisites

1. Azure Subscription
2. Azure CLI Installed
3. PowerShell
4. Existing Azure Resource Group

## Deployment Options

You can deploy the utility either by building it from source files or using precompiled binaries.

### Option 1: Deploy Using Source Files

This option involves cloning the repository and building the C# project source files locally on a Windows machine.

#### Prerequisites for Source Build
- [.NET SDK 9.0](https://dotnet.microsoft.com/en-us/download/dotnet/9.0) installed

#### Deployment Steps

1. **Clone the Repository**
   ```powershell
   git clone https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility
   cd MongoMigrationWebBasedUtility/WebApp
   ```

2. **Run Deployment Script**
   ```powershell
   # Variables to be updated
   $resourceGroupName = "<Replace with Existing Resource Group Name>"
   $webAppName = "<Replace with Web App Name>"
   $location = "<Replace with Azure Region (e.g., WestUs3, EastUS, WestEurope)>"
   $projectFolderPath = Split-Path (Get-Location) -Parent

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
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=$location webAppName=$webAppName

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

3. **Access the Application**
   - Open `https://<WebAppName>.azurewebsites.net` in your browser

### Option 2: Deploy Using Precompiled Binaries

This option uses pre-built binaries from the latest release, eliminating the need for local compilation.

#### Deployment Steps

1. **Download Release**
   - Clone/Download the repository: `https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility`
   - Download the `.zip` file (excluding source code files) from the [latest release](https://github.com/AzureCosmosDB/MongoMigrationWebBasedUtility/releases)

2. **Run Deployment Script**
   ```powershell
   # Variables to be updated
   $resourceGroupName = "<Replace with Existing Resource Group Name>"
   $webAppName = "<Replace with Web App Name>"
   $location = "<Replace with Azure Region (e.g., WestUs3, EastUS, WestEurope)>"
   $zipPath = "<Replace with full path of downloaded latest release zip file on local>"

   # Login to Azure
   az login

   # Set subscription (optional)
   # az account set --subscription "your-subscription-id"

   # Deploy Azure Web App 
   Write-Host "Deploying Azure Web App..."
   az deployment group create --resource-group $resourceGroupName --template-file main.bicep --parameters location=$location webAppName=$webAppName

   # Deploy files to Azure Web App
   Write-Host "Deploying to Azure Web App..."
   az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

   Write-Host "Deployment completed successfully!"
   ```

3. **Access the Application**
   - Open `https://<WebAppName>.azurewebsites.net` in your browser

## Security Configuration (Recommended)

### Enable App Authentication

The Web App deployed using the above steps will be publicly accessible to anyone with the URL. To secure the application, enable App Service Authentication.

**Documentation**: [Add app authentication to your web app](https://learn.microsoft.com/en-us/azure/app-service/scenario-secure-app-authentication-app-service?tabs=workforce-configuration)

### VNet Integration and Single Public IP (Optional)

If you need to access MongoDB servers within a private Virtual Network (VNet) or require a consistent public IP for firewall rules, configure VNet integration.

#### 1. Create a VNet (If Not Already Existing)

1. **Create a Virtual Network**:
   - Go to **Create a resource** in the Azure Portal
   - Search for **Virtual Network** and click **Create**
   - Provide a **Name** for the VNet
   - Choose the desired **Region** (must match the Web App's region)
   - Define the **Address Space** (e.g., `10.0.0.0/16`)

2. **Add a Subnet**:
   - In the **Subnet** section, create a new subnet
   - Provide a **Name** (e.g., `WebAppSubnet`)
   - Define the **Subnet Address Range** (e.g., `10.0.1.0/24`)
   - Set the **Subnet Delegation** to `Microsoft.Web` for VNet integration

3. **Create the VNet**:
   - Click **Review + Create** and then **Create**

#### 2. Enable VNet Integration for the Web App

1. **Go to the Web App**:
   - Navigate to your Azure Web App in the Azure Portal

2. **Enable VNet Integration**:
   - In the left-hand menu, select **Networking**
   - Under **Outbound Traffic**, click **VNet Integration**
   - Click **Add VNet** and choose an existing VNet and subnet
   - Ensure the subnet is delegated to **Microsoft.Web**

3. **Save** the configuration

#### 3. Configure a NAT Gateway for Consistent Public IP

1. **Create a Public IP Address**:
   - Go to **Create a resource** in the Azure Portal
   - Search for **Public IP Address** and click **Create**
   - Assign a name and ensure it's set to **Static**
   - Complete the setup

2. **Create a NAT Gateway**:
   - Go to **Create a resource** in the Azure Portal
   - Search for **NAT Gateway** and click **Create**
   - Assign a name and link it to the **Public IP Address** created earlier
   - Attach the NAT Gateway to the same subnet used for the Web App

3. **Save** the configuration

#### 4. Update Firewall Rules

- In your firewall (e.g., Azure Firewall, third-party), allow traffic from the single public IP address used by the NAT Gateway
- Test access to MongoDB Source and Destination that have been configured with the new IP in their firewall rules

### Enable Private Endpoint (Optional)

For enhanced security, you can restrict access to the Web App using private endpoints.

#### Prerequisites

- An existing **Azure Virtual Network (VNet)**
- A subnet dedicated to private endpoints (e.g., `PrivateEndpointSubnet`)
- Permissions to configure networking and private endpoints in Azure

#### Steps to Enable Private Endpoint

1. **Navigate to the Web App**
   - Open the **Azure Portal**
   - In the left-hand menu, select **App Services**
   - Click the desired web app

2. **Access Networking Settings**
   - In the web app's blade, select **Networking**
   - Under the **Private Endpoint** section, click **+ Private Endpoint**

3. **Create the Private Endpoint**
   
   **a. Basics**
   - **Name**: Enter a name for the private endpoint (e.g., `WebAppPrivateEndpoint`)
   - **Region**: Ensure it matches your VNet's region

   **b. Resource**
   - **Resource Type**: Select `Microsoft.Web/sites` for the web app
   - **Resource**: Choose your web app
   - **Target Sub-resource**: Select `sites`

   **c. Configuration**
   - **Virtual Network**: Select your VNet
   - **Subnet**: Choose the `PrivateEndpointSubnet`
   - **Integrate with private DNS zone**: Enable this to link the private endpoint with an Azure Private DNS zone (recommended)

4. **Review and Create**
   - Click **Next** to review the configuration
   - Click **Create** to finalize

5. **Verify Private Endpoint Connection**
   - Return to the **Networking** tab of your web app
   - Verify the private endpoint status under **Private Endpoint Connections** as `Approved`

#### Configure DNS (Recommended)

If using a private DNS zone:

1. **Validate DNS Resolution**:
   ```bash
   nslookup <WebAppName>.azurewebsites.net
   ```

2. **Update Custom DNS Servers (if applicable)**:
   - Configure custom DNS servers to resolve the private endpoint using Azure Private DNS

#### Test Connectivity

1. Deploy a **VM in the same VNet**
2. From the VM, access the web app via its URL (e.g., `https://<WebAppName>.azurewebsites.net`)
3. Confirm that the web app is accessible only within the VNet

## Resource Scaling

Azure App Service provides various pricing tiers to match your workload requirements:

- **B1 (Basic)**: Default tier, suitable for development and small workloads
- **P1V3 (Premium v3)**: Recommended for production workloads
- **P2V3 (Premium v3)**: Recommended for larger migrations

To change the pricing tier, update the `main.bicep` file or modify the App Service Plan in the Azure Portal.

## Monitoring and Troubleshooting

### View Application Logs
```powershell
az webapp log tail --resource-group <resource-group-name> --name <web-app-name>
```

### Check Web App Status
```powershell
az webapp show --resource-group <resource-group-name> --name <web-app-name> --query state
```

### Common Issues

1. **Application not starting**: Check application logs for errors
2. **Connection timeout**: Verify firewall rules and VNet configuration
3. **Performance issues**: Consider upgrading to a higher pricing tier

## Additional Resources

- [Azure App Service Documentation](https://learn.microsoft.com/en-us/azure/app-service/)
- [App Service Pricing](https://azure.microsoft.com/pricing/details/app-service/windows/)
- [VNet Integration Documentation](https://learn.microsoft.com/en-us/azure/app-service/overview-vnet-integration)
- [Private Endpoint Documentation](https://learn.microsoft.com/en-us/azure/private-link/private-endpoint-overview)

## Need More Power?

If your migration workload requires:
- Extended processing time (24+ hours)
- High-performance compute resources
- Persistent storage across deployments
- Dedicated resources (8-32 vCores, up to 64GB RAM)

Consider deploying to **[Azure Container Apps](../ACA/DeployToACA_README.md)** instead for better performance and scalability.
