# Deploy to Azure Container Apps (ACA)

This guide explains how to deploy the MongoDB Migration Web-Based Utility to Azure Container Apps using the `aca_main.bicep` template. ACA provides enterprise-grade dedicated compute resources and persistent storage for high-performance migrations.

## Deployment Scripts Overview

Two PowerShell scripts are provided for different deployment scenarios:

1. **`deploy-to-aca.ps1`** - Full initial deployment
   - Deploys complete infrastructure (ACR, Container Apps Environment, Storage)
   - Prompts for DocumentDB connection string
   - Configures all environment variables and secrets
   - Use for first-time deployment or when changing infrastructure

2. **`update-aca-app.ps1`** - Application updates only
   - Updates only the Docker image
   - Preserves existing environment variables and secrets
   - Faster deployment without infrastructure changes
   - Use for deploying code changes after initial setup

## Prerequisites

**Note**: This guide assumes you have cloned the repository to `c:\Work\GitHub\repos\MongoMigrationWebBasedUtility`.

- Azure CLI installed and logged in (`az login`)
- An Azure subscription with appropriate permissions
- Resource group created (`az group create -n <rg-name> -l <location>`)
- **Azure DocumentDB account** created for state storage (you'll need the connection string)
- **Supported Azure region** - Container Apps with dedicated workload profiles are not available in all regions

**Note**: Docker is not required locally. Images are built directly in Azure Container Registry (ACR) using `az acr build`.

### Supported Regions for Container Apps with Dedicated Plans

To get the latest list of regions that support Azure Container Apps, run:

```powershell
# Check all regions where Container Apps managed environments are available
az provider show --namespace Microsoft.App --query "resourceTypes[?resourceType=='managedEnvironments'].locations" -o table

# Check regions that support dedicated workload profiles
az containerapp env workload-profile list-supported -l <your-location>
```

**Commonly supported regions include**: `eastus`, `eastus2`, `westus2`, `westus3`, `northeurope`, `westeurope`, `australiaeast`, `southeastasia`, `canadacentral`

**Note**: If you encounter deployment errors, verify region support using the commands above or try switching to a well-supported region like `eastus`.

## Quick Start (Automated Deployment)

For a streamlined deployment experience, use the provided PowerShell script:

```powershell
# Navigate to the repository directory
cd c:\Work\GitHub\repos\MongoMigrationWebBasedUtility

# Minimal deployment (auto-generates ACR, Storage, StateStoreAppID, and AcrRepository)
# NOTE: ContainerAppName must use lowercase letters, numbers, and hyphens only (no underscores)
.\deploy-to-aca.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -Location "eastus" `
  -OwnerTag "yourname@company.com"

# Full deployment with custom names and existing resources
.\deploy-to-aca.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -AcrName "mongomigrationacr" `
  -AcrRepository "myapp" `
  -StorageAccountName "mongomigstg" `
  -StateStoreAppID "aca_server1" `
  -Location "eastus" `
  -ImageTag "latest" `
  -OwnerTag "yourname@company.com"

# Deployment with VNet integration (Environment must be created with VNet from day one)
.\deploy-to-aca.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -Location "eastus" `
  -OwnerTag "yourname@company.com" `
  -InfrastructureSubnetResourceId "/subscriptions/{sub-id}/resourceGroups/{rg-name}/providers/Microsoft.Network/virtualNetworks/{vnet-name}/subnets/{subnet-name}"
```

**Using Existing Azure Resources:**
- If ACR exists, the script uses it; otherwise creates a new one
- If Storage Account exists, the script uses it; otherwise creates a new one
- If the specified image:tag exists in ACR, the script skips building and uses it directly
- Auto-generated names use the format: `<ContainerAppName><suffix>` (e.g., `mongomigrationacr`, `mongomigrationstg`)

The script will:
1. Deploy infrastructure using Bicep (ACR, Container Apps Environment, Storage Account)
2. Check if image exists in ACR; if not, build and push Docker image
3. Securely prompt for DocumentDB connection string
4. Deploy Container App with final configuration and environment variables
5. Display the application URL with a clickable launch link

### Subsequent Application Updates

After initial deployment, use the update script for faster deployments:

```powershell
# Minimal update (uses same ACR and repository as initial deployment)
.\update-aca-app.ps1 `
  -ResourceGroupName "MongoMigrationRGProd" `
  -ContainerAppName "mongomigration-prod" `
  -AcrName "mongomigprod1234"

# Update with custom repository and tag
.\update-aca-app.ps1 `
  -ResourceGroupName "MongoMigrationRGProd" `
  -ContainerAppName "mongomigration-prod" `
  -AcrName "mongomigprod1234" `
  -AcrRepository "customrepo" `
  -ImageTag "v1.1"
```

**Using Existing Images:**
- If the specified image:tag exists in ACR, the script uses it directly without rebuilding
- This saves time when deploying the same image across multiple environments
- AcrRepository defaults to ContainerAppName if not specified

The update script:
1. Checks if image exists in ACR; if not, builds and pushes new Docker image
2. Updates Container App with new image
3. Preserves all environment variables and secrets
4. Displays the application URL with a clickable launch link

### Configurable Resource Sizing

The script supports flexible resource allocation to match your workload requirements:

```powershell
# Small workload (development/testing)
-VCores 4 -MemoryGB 16

# Default production workload  
# (uses defaults: 8 vCores, 32GB - no parameters needed)

# Large workload (high-performance migration)
-VCores 16 -MemoryGB 64

# Maximum workload (enterprise-scale migration)
-VCores 32 -MemoryGB 64
```

---

## Manual Deployment Steps

If you prefer to run each step manually, follow the detailed steps below.

### Step 1: Deploy Infrastructure with Bicep

Deploy the Bicep template to create all required resources:

```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> acrName=<acr-name> ownerTag="yourname@company.com"
```

This will create:
- Managed Identity
- Azure Container Registry (ACR)
- Container Apps Environment with Configurable Dedicated Plan (1-32 vCores, 2-64GB)
- Storage Account with 100GB Azure File Share

### Step 2: Build and Push Docker Image to ACR

Build your Docker image directly in Azure Container Registry:

```powershell
# Navigate to the repository root
cd c:\Work\GitHub\repos\MongoMigrationWebBasedUtility

# Build and push the image to ACR
az acr build `
  --registry <acrName> `
  --resource-group <resource-group-name> `
  --image <containerAppName>:latest `
  --file MongoMigrationWebApp/Dockerfile `
  .
```

### Step 3: Deploy Container App with Environment Variables

Deploy the final Container App configuration with environment variables:

```powershell
# Securely get connection string
$secureConnString = Read-Host -Prompt "Enter DocumentDB Connection String" -AsSecureString
$connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString))

# Deploy final configuration
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> acrName=<acr-name> ownerTag="yourname@company.com" stateStoreAppID=<your-app-id> stateStoreConnectionString="`"$connString`"" aspNetCoreEnvironment=Development imageTag=latest

# Clear the variable from memory
Remove-Variable connString, secureConnString
```

## Parameters Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `ResourceGroupName` | Name of the Azure resource group | `MongoMigrationRG` |
| `ContainerAppName` | Name of the Container App | `mongomigration` |
| `Location` | Azure region (must support Container Apps) | `eastus` |
| `OwnerTag` | Owner tag required by Azure Policy | `yourname@company.com` |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `AcrName` | string | `<ContainerAppName>acr` | Name of the Azure Container Registry (must be globally unique, alphanumeric only). Uses existing ACR if found, otherwise creates new. |
| `AcrRepository` | string | `<ContainerAppName>` | Repository name within ACR for storing images. Allows multiple apps to share the same ACR. |
| `StorageAccountName` | string | `<ContainerAppName>stg` | Name of the Storage Account. Uses existing account if found, otherwise creates new. |
| `StateStoreAppID` | string | `<ContainerAppName>` | Application identifier for state storage in DocumentDB. |
| `ImageTag` | string | `latest` | Docker image tag to deploy. Script checks if image exists before building. |
| `VCores` | int | `8` (range: 1-32) | Number of vCores for the container. |
| `MemoryGB` | int | `32` (range: 2-64) | Memory in GB for the container. |
| `InfrastructureSubnetResourceId` | string | `""` (empty) | Resource ID of the subnet for VNet integration. **Must be provided at environment creation time**. See [VNet Integration](#vnet-integration) section. |

## Naming Constraints

**Important**: Azure resources have specific naming requirements:

- **Container App Name** (`ContainerAppName`):
  - Must use lowercase letters, numbers, and hyphens only
  - ❌ **No underscores allowed** (e.g., `my_app` is invalid)
  - ✅ Use hyphens instead (e.g., `my-app` is valid)
  - Used to generate environment name: `{ContainerAppName}-env-{profile}`

- **Storage Account Name** (auto-generated or explicit):
  - Must be 3-24 characters
  - Lowercase letters and numbers only
  - No special characters (hyphens, underscores, etc.)
  - Auto-generation removes hyphens from ContainerAppName

- **ACR Name** (auto-generated or explicit):
  - Alphanumeric characters only
  - Must be globally unique

**Example Valid Names:**
```powershell
# ✅ Valid
-ContainerAppName "mongomigration-prod"        # Generates env: mongomigration-prod-env-D8
-ContainerAppName "mongo-mig-app-01"           # Generates env: mongo-mig-app-01-env-D16

# ❌ Invalid
-ContainerAppName "mongo_migration"            # Underscore not allowed
-ContainerAppName "MongoMigration"             # Uppercase not recommended
```

## Resource Configurations

Azure Container Apps with Dedicated Plan provides configurable high-performance compute:

### Configurable Resources
- **CPU**: 1-32 vCores (default: 8 vCores, dedicated)
- **Memory**: 2-64GB RAM (default: 32GB)
- **Storage**: 100GB persistent Azure File Share
- **Replicas**: Fixed at 1 replica (auto-scaling not supported)
- **Networking**: Private VNet integration with optional public ingress

### Resource Configuration Examples

**Development/Testing (4 vCores, 16GB):**
```powershell
-VCores 4 -MemoryGB 16
```

**Default Production (8 vCores, 32GB):**
```powershell
# Uses defaults, no additional parameters needed
```

**High-Performance (16 vCores, 64GB):**
```powershell
-VCores 16 -MemoryGB 64
```

**Maximum Performance (32 vCores, 64GB):**
```powershell
-VCores 32 -MemoryGB 64
```

**Key Features:**
- ✅ Configurable high CPU and memory (up to 32 vCores, 64GB)
- ✅ Persistent storage that survives deployments
- ✅ Enterprise networking with VNet integration
- ✅ Built-in ingress controller with automatic HTTPS/TLS (HTTP also supported)
- ✅ Managed identity for secure resource access

**Note**: Auto-scaling is not supported. The application runs with a fixed single replica to ensure data consistency during migration operations.

## Complete Deployment Examples

### Development Setup (Small Workload)

```powershell
# Minimal deployment - auto-generates ACR, Storage, and StateStore names
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-dev" `
  -ContainerAppName "mongomigration-dev" `
  -Location "eastus" `
  -VCores 4 `
  -MemoryGB 16 `
  -ImageTag "dev"

# Or with custom names to use existing resources
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-dev" `
  -ContainerAppName "mongomigration-dev" `
  -AcrName "mongomigdevacr" `
  -StorageAccountName "mongomigdevstg" `
  -StateStoreAppID "dev-migration-01" `
  -Location "eastus" `
  -VCores 4 `
  -MemoryGB 16 `
  -ImageTag "dev"
```

### High-Performance Migration Setup (Large Workload)

```powershell
# Deploy with maximum resources - reuse existing ACR and storage
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-perf" `
  -ContainerAppName "mongomigration-perf" `
  -AcrName "sharedproductionacr" `
  -AcrRepository "mongomig-perf" `
  -StorageAccountName "sharedprodstg" `
  -StateStoreAppID "perf-migration-01" `
  -Location "eastus" `
  -VCores 32 `
  -MemoryGB 64 `
  -ImageTag "v1.0"

# With VNet integration for enhanced security
.\deploy-to-aca.ps1 `
  -ResourceGroupName "rg-mongomig-perf" `
  -ContainerAppName "mongomigration-perf" `
  -AcrName "sharedproductionacr" `
  -AcrRepository "mongomig-perf" `
  -StorageAccountName "sharedprodstg" `
  -StateStoreAppID "perf-migration-01" `
  -Location "eastus" `
  -VCores 32 `
  -MemoryGB 64 `
  -ImageTag "v1.0" `
  -InfrastructureSubnetResourceId "/subscriptions/{sub-id}/resourceGroups/{rg-name}/providers/Microsoft.Network/virtualNetworks/{vnet-name}/subnets/{subnet-name}"

# Script will:
# - Use existing ACR "sharedproductionacr" (no creation needed)
# - Store image in "mongomig-perf" repository within the ACR
# - Use existing storage account "sharedprodstg" (no creation needed)
# - Check if image v1.0 exists; if yes, skip build and deploy directly
# - Create environment with VNet integration (if subnet provided)
```

## VNet Integration

### Overview

Azure Container Apps supports VNet integration for enhanced security and network isolation. **Critical limitations:**

⚠️ **VNet integration must be configured at environment creation time**  
⚠️ **You CANNOT add VNet to an existing Container Apps Environment**  
⚠️ **VNet integration is at the Environment level, not per Container App**

If you need VNet integration, the environment must be created with the VNet from day one. To change VNet settings later, you must delete and recreate the entire environment.

### Prerequisites for VNet Integration

1. **Existing Azure Virtual Network (VNet)** with available address space
2. **Dedicated Subnet for Container Apps**:
   - Recommended size: **/23 or larger** (512+ addresses)
   - Must be delegated to `Microsoft.App/environments`
   - Should not be used by other services
   - Must have sufficient address space for the environment and all apps
3. **Subnet Resource ID** in the format:
   ```
   /subscriptions/{subscription-id}/resourceGroups/{rg-name}/providers/Microsoft.Network/virtualNetworks/{vnet-name}/subnets/{subnet-name}
   ```

### Deploying with VNet Integration

Use the `-InfrastructureSubnetResourceId` parameter during initial deployment:

```powershell
# Deploy with VNet integration
.\deploy-to-aca.ps1 `
  -ResourceGroupName "MongoMigrationRG" `
  -ContainerAppName "mongomigration-prod" `
  -Location "eastus" `
  -VCores 16 `
  -MemoryGB 64 `
  -InfrastructureSubnetResourceId "/subscriptions/220fc532-6091-423c-8ba0-66c2397d591b/resourceGroups/MyRG/providers/Microsoft.Network/virtualNetworks/my-vnet/subnets/aca-subnet"
```

**What happens:**
- Container Apps Environment is created with VNet integration enabled
- The infrastructure subnet is used for environment networking
- Apps can communicate privately within the VNet
- Public ingress is still enabled by default (can be restricted separately)

### Getting Subnet Resource ID

```powershell
# List all subnets in a VNet
az network vnet subnet list `
  --resource-group "MyRG" `
  --vnet-name "my-vnet" `
  --query "[].{Name:name, Id:id}" `
  --output table

# Get specific subnet ID
az network vnet subnet show `
  --resource-group "MyRG" `
  --vnet-name "my-vnet" `
  --name "aca-subnet" `
  --query "id" `
  --output tsv
```

### Important Notes

- **Cannot modify VNet after creation**: If you need to change VNet settings, you must delete and recreate the environment
- **All apps share the VNet**: Every Container App in the environment uses the same VNet integration
- **Ingress control**: VNet integration alone doesn't restrict access. Configure ingress settings separately:
  - **External**: Accessible from the internet (default)
  - **Internal**: Only accessible within the VNet
- **Private endpoints**: You can still configure private endpoints for ACR, Storage, and DocumentDB separately (see below)

## Network Security (Optional)

### Setting Up Private Endpoints

For enhanced security, configure private endpoints for your Azure resources to keep traffic within the Azure backbone network. This is separate from VNet integration and can be done after deployment.

#### Prerequisites for Private Endpoints
- An existing Azure Virtual Network (VNet)
- Subnet for private endpoints (recommended: /28 or larger)
- Container Apps Environment with VNet integration (recommended but not required)

#### Step 2: Set Up Private Endpoint for Azure Container Registry (ACR)

1. **Navigate to your ACR** in Azure Portal
   - Go to your Azure Container Registry resource
   - Select **Networking** from the left menu

2. **Create Private Endpoint**
   - Click on **Private endpoint connections** tab
   - Click **+ Private endpoint**
   
3. **Configure Private Endpoint (Basics)**
   - **Subscription**: Select your subscription
   - **Resource group**: Same as your Container App
   - **Name**: `pe-<acr-name>`
   - **Region**: Same as your ACR
   - Click **Next: Resource**

4. **Configure Resource**
   - **Connection method**: Connect to an Azure resource in my directory
   - **Resource type**: Microsoft.ContainerRegistry/registries
   - **Resource**: Select your ACR
   - **Target sub-resource**: registry
   - Click **Next: Virtual Network**

5. **Configure Virtual Network**
   - **Virtual network**: Select your VNet
   - **Subnet**: Select a subnet for private endpoints
   - **Private IP configuration**: Dynamic
   - **Application security group**: (Optional)
   - Click **Next: DNS**

6. **Configure DNS**
   - **Integrate with private DNS zone**: Yes
   - **Private DNS zone**: (Auto-created) privatelink.azurecr.io
   - Click **Next: Tags**, then **Review + create**

7. **Disable Public Access** (Optional)
   - Return to ACR **Networking**
   - Under **Public network access** tab
   - Select **Disabled**
   - Click **Save**

#### Step 3: Set Up Private Endpoint for Storage Account

1. **Navigate to your Storage Account** in Azure Portal
   - Go to your Storage Account resource
   - Select **Networking** from the left menu

2. **Create Private Endpoint**
   - Click on **Private endpoint connections**
   - Click **+ Private endpoint**

3. **Configure Private Endpoint (Basics)**
   - **Name**: `pe-<storage-account-name>-file`
   - **Region**: Same as your storage account
   - Click **Next: Resource**

4. **Configure Resource**
   - **Resource type**: Microsoft.Storage/storageAccounts
   - **Resource**: Select your storage account
   - **Target sub-resource**: **file** (for Azure Files)
   - Click **Next: Virtual Network**

5. **Configure Virtual Network**
   - **Virtual network**: Select your VNet
   - **Subnet**: Select a subnet for private endpoints
   - Click **Next: DNS**

6. **Configure DNS**
   - **Integrate with private DNS zone**: Yes
   - **Private DNS zone**: (Auto-created) privatelink.file.core.windows.net
   - Click **Review + create**

7. **Disable Public Access** (Optional)
   - Return to Storage Account **Networking**
   - Under **Firewalls and virtual networks**
   - Select **Disabled** for public network access
   - Or select **Enabled from selected virtual networks and IP addresses**
     - Add your Container Apps subnet
   - Click **Save**

#### Step 4: Set Up Private Endpoint for Azure DocumentDB (StateStore)

1. **Navigate to your DocumentDB Account** in Azure Portal
   - Go to your Azure DocumentDB (Cosmos DB) account
   - Select **Networking** from the left menu

2. **Create Private Endpoint**
   - Click on **Private endpoint connections** tab
   - Click **+ Private endpoint**

3. **Configure Private Endpoint (Basics)**
   - **Name**: `pe-<documentdb-name>`
   - **Region**: Same as your DocumentDB
   - Click **Next: Resource**

4. **Configure Resource**
   - **Resource type**: Microsoft.DocumentDB/databaseAccounts
   - **Resource**: Select your DocumentDB account
   - **Target sub-resource**: **MongoDB** (or **Sql** depending on your API)
   - Click **Next: Virtual Network**

5. **Configure Virtual Network**
   - **Virtual network**: Select your VNet
   - **Subnet**: Select a subnet for private endpoints
   - Click **Next: DNS**

6. **Configure DNS**
   - **Integrate with private DNS zone**: Yes
   - **Private DNS zone**: (Auto-created) privatelink.mongo.cosmos.azure.com
   - Click **Review + create**

7. **Configure Firewall** (Optional)
   - Return to DocumentDB **Networking**
   - Under **Public network access**
   - Select **Disabled** for full private access
   - Or select **Selected networks** and add your Container Apps subnet
   - Click **Save**

#### Step 5: Verify Private Endpoint Connections

1. **Check DNS Resolution**
   ```powershell
   # From a VM in the same VNet, test DNS resolution
   nslookup <acr-name>.azurecr.io
   nslookup <storage-account-name>.file.core.windows.net
   nslookup <documentdb-name>.mongo.cosmos.azure.com
   
   # Should resolve to private IP addresses (10.x.x.x or 172.x.x.x or 192.168.x.x)
   ```

2. **Test Connectivity**
   - Deploy or restart your Container App
   - Check logs to ensure it can access ACR, Storage, and DocumentDB
   - Verify no public internet access is being used

#### Benefits of Private Endpoints

- ✅ **Enhanced Security**: All traffic stays within Azure backbone network
- ✅ **No Public Exposure**: Resources not accessible from the internet
- ✅ **Compliance**: Meets regulatory requirements for private data access
- ✅ **Network Isolation**: Reduces attack surface
- ✅ **Data Exfiltration Protection**: Prevents unauthorized data transfer

#### Important Considerations

- **Cost**: Private endpoints incur additional costs (~$0.01/hour per endpoint)
- **DNS**: Ensure proper DNS configuration for private endpoint resolution
- **Subnet Planning**: Plan subnet sizes carefully to accommodate growth
- **Managed Identity**: Container Apps use managed identity to access resources, which works seamlessly with private endpoints
- **Troubleshooting**: If connection issues occur, verify NSG rules and DNS resolution

## Persistent Storage

### Migration Data Storage
- **Path**: `/app/migration-data` (accessible via `ResourceDrive` environment variable)
- **Capacity**: 100GB Azure File Share
- **Persistence**: Data survives container restarts, deployments, and scaling events
- **Performance**: Standard_LRS storage for consistent I/O performance

### Using Persistent Storage in Application
Your application can access the persistent storage using the environment variable:

```csharp
var resourceDrive = Environment.GetEnvironmentVariable("ResourceDrive"); // Returns: "/app/migration-data"
// Store migration files in this directory - they persist across deployments
```

## Deployment Outputs

After successful deployment, the template provides these outputs:

| Output | Description |
|--------|-------------|
| `containerAppEnvironmentId` | Container Apps Environment resource ID |
| `containerAppFQDN` | Direct Container App FQDN |
| `containerAppUrl` | Application URL (HTTP or HTTPS based on setup) |
| `managedIdentityId` | Resource ID of the managed identity |
| `managedIdentityClientId` | Client ID for the managed identity |
| `storageAccountName` | Storage account name for migration data |
| `fileShareName` | File share name (migration-data) |
| `resourceDrivePath` | Mount path for persistent storage |

View outputs:
```powershell
az deployment group show `
  --resource-group <resource-group-name> `
  --name <deployment-name> `
  --query properties.outputs
```

## Updating the Application

### Quick Update (Recommended)

For updating only the application image without resetting environment variables or secrets, use the dedicated update script:

```powershell
# Update using existing image (no rebuild if image exists)
.\update-aca-app.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -AcrName "mongomigrationacr" `
  -ImageTag "v1.1"

# Update with custom repository
.\update-aca-app.ps1 `
  -ResourceGroupName "MongoMigrationRGTest" `
  -ContainerAppName "mongomigration" `
  -AcrName "sharedacr" `
  -AcrRepository "myapp" `
  -ImageTag "v1.1"
```

The update script will:
1. Check if image exists in ACR; if yes, skip build and use existing image
2. If image doesn't exist, build and push the new Docker image to ACR
3. Update the Container App with the new image
4. Preserve all existing environment variables and secrets
5. Display the application URL with a clickable launch link

**Advantages of using the update script:**
- ✅ Faster deployment (skips infrastructure provisioning)
- ✅ No need to re-enter connection strings or secrets
- ✅ Environment variables remain unchanged
- ✅ Intelligent image reuse - skips rebuild if image already exists
- ✅ Simpler command with fewer parameters required

### Updating Secrets (Connection Strings)

If you need to update the StateStore connection string after initial deployment:

Update via Azure Portal

1. Navigate to your Container App in the Azure Portal
2. Under **Settings**, click **Secrets**
3. Find and edit the secret (e.g., `statestore-connection`)
4. Paste your new MongoDB connection string
5. Click **Save**
6. Run the update script to restart the app with new secrets:


Restart the app to pick up new secrets

```
.\update-aca-app.ps1 `
  -ResourceGroupName "<resource-group-name>" `
  -ContainerAppName "<container-app-name>" `
  -AcrName "<acr-name>"
```

### Update with Full Redeployment

Deploy updated configuration with all parameters (use this when you need to change environment variables or infrastructure):
```powershell
az deployment group create `
  --resource-group <resource-group-name> `
  --template-file aca_main.bicep `
  --parameters containerAppName=<container-app-name> acrName=<acr-name> imageTag=v2.0 # ... other parameters
```

### Manual Image Update

If you prefer manual control over each step:

1. Build and push new image:
```powershell
az acr build --registry <acrName> `
  --resource-group <resource-group-name> `
  --image <containerAppName>:v2.0 `
  --file MongoMigrationWebApp/Dockerfile `
  .
```

2. Update Container App:
```powershell
az containerapp update `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --image <acrName>.azurecr.io/<containerAppName>:v2.0
```

**Note**: For most scenarios, using `update-aca-app.ps1` is recommended over manual updates as it handles both steps and provides better error handling.

## Monitoring and Troubleshooting

### Common Deployment Issues

### Troubleshooting Script

Before deploying, you can run the troubleshooting script to check for common issues:

```powershell
./troubleshoot-aca.ps1 -ResourceGroupName "my-rg" -Location "eastus"
```

This script will:
- Verify Azure login and CLI extensions
- Check region support for Container Apps
- Validate workload profile availability  
- Check ACR name availability
- Validate Bicep template syntax
- Display subscription quotas

#### 1. Template Deployment Validation Error
**Error**: `The template deployment 'aca_main' is not valid according to the validation procedure`

**Possible Causes & Solutions**:
- **Region not supported**: Use a supported region like `eastus`, `westus2`, or `northeurope`
- **ACR name conflict**: ACR names must be globally unique - try adding random numbers
- **Resource limits**: Check if your subscription has quotas for Container Apps

```powershell
# Check Container Apps availability in region
az provider show --namespace Microsoft.App --query "resourceTypes[?resourceType=='managedEnvironments'].locations" -o table

# Generate unique ACR name
$uniqueAcrName = "mongomigacr$(Get-Random -Minimum 1000 -Maximum 9999)"
```

#### 2. Workload Profile Not Available
**Error**: Related to D4 workload profile

**Solution**: The Bicep template now includes both Consumption and Dedicated profiles. If D4 is not available, it will fall back to Consumption profile with lower resource limits.

#### 3. Network Configuration Issues
**Error**: VNet or subnet configuration problems

**Solution**: Ensure the region supports Container Apps networking features.

#### 4. Slow Performance with VNet Integration
**Symptom**: Application runs slowly after enabling VNet integration, especially when connecting to MongoDB/Cosmos DB with Private Endpoint

**Common Causes & Solutions**:

1. **Private DNS Zone Not Linked to VNet**
   - **Problem**: Container App cannot resolve MongoDB private endpoint hostname
   - **Check**: `az network private-dns link vnet list --resource-group <rg> --zone-name privatelink.mongocluster.cosmos.azure.com --output table`
   - **Fix**: Link the DNS zone to your VNet:
   ```powershell
   az network private-dns link vnet create `
     --resource-group <rg> `
     --zone-name privatelink.mongocluster.cosmos.azure.com `
     --name <vnet-name>-link `
     --virtual-network <vnet-name> `
     --registration-enabled false
   ```

2. **DNS Resolution Delays**
   - **Problem**: Azure DNS through VNet can be slower than public DNS
   - **Check**: Monitor application logs for connection timeouts
   - **Solution**: Increase connection timeouts in your MongoDB connection string
   - **Alternative**: Consider using Azure Private DNS Resolver for better performance

3. **NSG Rules Blocking Traffic**
   - **Problem**: Network Security Group rules may be blocking or slowing down traffic
   - **Check Outbound Rules**: `az network nsg show --ids <nsg-id> --query "securityRules[?direction=='Outbound']"`
   - **Solution**: Ensure NSG allows outbound traffic to MongoDB (port 27017/10255) and doesn't have excessive deny rules

4. **Missing Service Endpoints**
   - **Problem**: Traffic routing through public internet instead of Azure backbone
   - **Check**: `az network vnet subnet show --ids <subnet-id> --query "serviceEndpoints"`
   - **Consider**: Adding Microsoft.AzureCosmosDB service endpoint to the subnet (though Private Endpoint is preferred)

5. **Connection String Issues**
   - **Problem**: Using public endpoint URL instead of private endpoint URL
   - **Check**: Verify your StateStore connection string uses the private endpoint hostname
   - **Fix**: Update the connection string to use `*.mongocluster.cosmos.azure.com` (private) not `*.mongo.cosmos.azure.com` (public)

**Verification Commands**:
```powershell
# Check VNet integration
az containerapp env show --name <env-name> --resource-group <rg> --query "properties.vnetConfiguration"

# Check DNS zone links
az network private-dns link vnet list --resource-group <rg> --zone-name privatelink.mongocluster.cosmos.azure.com --output table

# Restart the app after DNS fixes
az containerapp revision restart --name <app-name> --resource-group <rg> --revision <revision-name>

# Compare performance with non-VNet deployment
# Deploy without VNet first, test performance, then add VNet to isolate the issue
```

### DNS Resolution Issues with VNet Integration

When using VNet integration with Container Apps, DNS resolution is critical for connecting to both private endpoints and public endpoints. Incorrect DNS configuration can cause connection failures.

#### Problem: Private DNS Zone Intercepting Public Endpoint Queries

**Symptom**: Application fails to connect to public MongoDB endpoints (e.g., `fc-xxxxx.mongocluster.cosmos.azure.com`) with DNS resolution errors or timeouts.

**Root Cause**: When a Private DNS zone for `privatelink.mongocluster.cosmos.azure.com` is linked to your VNet, it intercepts **ALL** queries for `*.mongocluster.cosmos.azure.com` domains, including public endpoints. If the public endpoint hostname isn't in the Private DNS zone, resolution fails.

**Example Error**:
```
System.Net.Dns.GetHostAddresses failed
A timeout occurred after 30000ms selecting a server
State: "Disconnected", Type: "Unknown", HeartbeatException
```

#### Scenarios and Solutions

**Scenario 1: Using Private Endpoint MongoDB Only**
- **Solution**: Link the Private DNS zone to your VNet (this is the correct setup)
```powershell
# Link Private DNS zone to VNet
az network private-dns link vnet create `
  --resource-group <dns-zone-rg> `
  --zone-name privatelink.mongocluster.cosmos.azure.com `
  --name <vnet-name>-link `
  --virtual-network /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/virtualNetworks/{vnet} `
  --registration-enabled false
```
**Scenario 2: Using Both Private and Public MongoDB Endpoints**
- **Challenge**: Private DNS zone intercepts all `*.mongocluster.cosmos.azure.com` queries
- **Solution Option A**: Keep Private DNS linked and add A records for public endpoints
```powershell
# First, resolve the public endpoint's IP from outside the VNet
nslookup fc-xxxxx.mongocluster.cosmos.azure.com

# Add A record to Private DNS zone pointing to public IP
az network private-dns record-set a add-record `
  --resource-group <dns-zone-rg> `
  --zone-name privatelink.mongocluster.cosmos.azure.com `
  --record-set-name fc-xxxxx-000 `
  --ipv4-address <public-ip>
```
- **Solution Option B**: Use Custom DNS Servers with conditional forwarding
- **Solution Option C** (Recommended): Migrate all MongoDB instances to private endpoints for consistency

#### Diagnostic Commands

**Check if Private DNS zone is linked to your VNet:**
```powershell
# List all Private DNS zones
az network private-dns zone list `
  --query "[?contains(name, 'mongocluster')].{name:name, resourceGroup:resourceGroup}" `
  --output table

# Check which VNets are linked to the DNS zone
az network private-dns link vnet list `
  --resource-group <dns-zone-rg> `
  --zone-name privatelink.mongocluster.cosmos.azure.com `
  --query "[].{name:name, vnetName:virtualNetwork.id}" `
  --output table
```

**Check DNS records in the Private DNS zone:**
```powershell
# List A records
az network private-dns record-set a list `
  --resource-group <dns-zone-rg> `
  --zone-name privatelink.mongocluster.cosmos.azure.com `
  --output table
```

**Test DNS resolution from Container App:**
```powershell
# View application logs for DNS errors
az containerapp logs show `
  --name <app-name> `
  --resource-group <rg> `
  --tail 50 `
  --follow false `
  | Select-String -Pattern "DNS|GetHostAddresses|timeout"

# Note: You cannot directly nslookup from Container Apps, but logs will show DNS resolution failures
```

**Check NSG rules that might affect DNS (port 53):**
```powershell
# List NSG rules
az network nsg rule list `
  --resource-group <rg> `
  --nsg-name <nsg-name> `
  --query "[?direction=='Outbound'].{priority:priority, name:name, access:access, protocol:protocol, destPorts:destinationPortRange}" `
  --output table
```

#### Best Practices

1. **Plan DNS Strategy Before Deployment**
   - Decide: Private endpoints only, public only, or mixed
   - Configure Private DNS zones accordingly before deploying Container Apps

2. **Use Consistent Endpoint Types**
   - **Recommended**: All MongoDB instances use private endpoints
   - **Avoid**: Mixing private and public endpoints (adds DNS complexity)

3. **Document Your DNS Configuration**
   - Record which Private DNS zones are linked to which VNets
   - Track which MongoDB instances use private vs. public endpoints

4. **Test Connectivity After VNet Changes**
   - After linking/unlinking DNS zones, restart Container App revisions
   - Monitor logs for connection errors immediately after changes

5. **Monitor DNS Resolution**
   - Set up alerts for connection timeouts
   - Review Container App logs regularly for DNS-related errors

6. **Handle Multiple Private DNS Zones**
   - If you have multiple resource groups with the same Private DNS zone name, ensure only the correct one is linked to your VNet
   - Use descriptive link names: `<vnet-name>-<purpose>-link`

#### Recovery Steps

If your application suddenly stops connecting after VNet or DNS changes:

1. **Identify the issue:**
   ```powershell
   # Check recent logs for errors
   az containerapp logs show --name <app-name> --resource-group <rg> --tail 100
   ```

2. **Verify DNS zone links:**
   ```powershell
   # Check all linked VNets
   az network private-dns link vnet list `
     --resource-group <dns-zone-rg> `
     --zone-name privatelink.mongocluster.cosmos.azure.com
   ```

3. **Apply the fix** (link or unlink based on your scenario)

4. **Restart the application:**
   ```powershell
   # Get current revision
   $revision = az containerapp revision list `
     --name <app-name> `
     --resource-group <rg> `
     --query '[0].name' `
     -o tsv

   # Restart
   az containerapp revision restart `
     --name <app-name> `
     --resource-group <rg> `
     --revision $revision
   ```

5. **Verify connectivity:**
   ```powershell
   # Monitor logs for successful connections
   az containerapp logs show --name <app-name> --resource-group <rg> --follow
   ```

### View Container App Logs
```powershell
az containerapp logs show `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --follow
```

### Check Container App Status
```powershell
az containerapp show `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --query "properties.provisioningState"
```

### Access Container App
```powershell
# Get the URL
az containerapp show `
  --name <containerAppName> `
  --resource-group <resource-group-name> `
  --query "properties.configuration.ingress.fqdn" `
  --output tsv
```

### Access Persistent Storage
```powershell
# List files in Azure File Share
az storage file list `
  --account-name <storageAccountName> `
  --share-name migration-data `
  --output table
```

## Cost Optimization Tips

1. **Right-size resources** - Start with smaller vCores/memory configurations and increase only if needed
2. **Monitor resource usage** - Use Azure Monitor to track CPU and memory utilization
3. **Right-size storage** - 100GB File Share, expand only if needed
4. **Choose appropriate regions** - Some regions have lower Container Apps pricing
5. **Use dedicated plan efficiently** - Dedicated plan provides consistent performance for predictable costs
6. **Clean up resources** - Remove Container Apps and storage when migrations are complete

## Security Features

### Container Apps Security
- ✅ **Managed Identity** - No passwords or keys needed for ACR and storage access
- ✅ **Secure Environment Variables** - Connection strings stored as secrets
- ✅ **VNet Integration** - Private networking with optional public ingress
- ✅ **ACR Integration** - Secure image pull using managed identity
- ✅ **HTTPS Support** - Automatic TLS certificate provisioning (both HTTP and HTTPS enabled by default)

## Additional Resources

- [Azure Container Apps Documentation](https://learn.microsoft.com/azure/container-apps/)
- [Container Apps Pricing](https://azure.microsoft.com/pricing/details/container-apps/)
- [Azure Container Registry Documentation](https://learn.microsoft.com/azure/container-registry/)
- [Azure File Share Documentation](https://learn.microsoft.com/azure/storage/files/)
- [Managed Identity Documentation](https://learn.microsoft.com/azure/active-directory/managed-identities-azure-resources/)