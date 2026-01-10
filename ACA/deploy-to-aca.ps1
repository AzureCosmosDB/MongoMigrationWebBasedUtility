# Azure Container Apps Deployment Script
# Deploys the MongoDB Migration Web-Based Utility to Azure Container Apps

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$ContainerAppName,
    
    [Parameter(Mandatory=$false)]
    [string]$AcrName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$StateStoreAppID = "",
    
    [Parameter(Mandatory=$true)]
    [string]$Location,
    
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",
    
    [Parameter(Mandatory=$false)]
    [string]$AcrRepository = "",
    
    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 32)]
    [int]$VCores = 8,
    
    [Parameter(Mandatory=$false)]
    [ValidateRange(2, 64)]
    [int]$MemoryGB = 32,
    
    [Parameter(Mandatory=$false)]
    [string]$InfrastructureSubnetResourceId = "",
    
    [Parameter(Mandatory=$true)]
    [string]$OwnerTag
)

$ErrorActionPreference = "Stop"

# Generate ACR name if not provided
if ([string]::IsNullOrEmpty($AcrName)) {
    $AcrName = ($ContainerAppName -replace '-', '').ToLower() + 'acr'
    if ($AcrName.Length -gt 50) {
        $AcrName = $AcrName.Substring(0, 50)
    }
    Write-Host "Using generated ACR name: $AcrName" -ForegroundColor Cyan
}

# Generate StateStoreAppID if not provided
if ([string]::IsNullOrEmpty($StateStoreAppID)) {
    $StateStoreAppID = $ContainerAppName
    Write-Host "Using ContainerAppName as StateStoreAppID: $StateStoreAppID" -ForegroundColor Cyan
}

# Generate ACR repository name if not provided
if ([string]::IsNullOrEmpty($AcrRepository)) {
    $AcrRepository = $ContainerAppName
    Write-Host "Using ContainerAppName as ACR repository: $AcrRepository" -ForegroundColor Cyan
}

# Generate storage account name if not provided
if ([string]::IsNullOrEmpty($StorageAccountName)) {
    $StorageAccountName = ($ContainerAppName -replace '-', '').ToLower() + 'stor'
    if ($StorageAccountName.Length -gt 24) {
        $StorageAccountName = $StorageAccountName.Substring(0, 24)
    }
    Write-Host "Using generated storage account name: $StorageAccountName" -ForegroundColor Cyan
}

Write-Host "Using location: $Location" -ForegroundColor Cyan

Write-Host "`nStep 1: Deploying infrastructure (ACR, Storage Account, Managed Identity, Container Apps Environment)..." -ForegroundColor Yellow
Write-Host "Note: This may take 3-5 minutes..." -ForegroundColor Gray

$bicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "acrRepository=$AcrRepository",
        "location=$Location",
        "storageAccountName=$StorageAccountName",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "ownerTag=$OwnerTag"
)

# Add VNet configuration if provided
if (-not [string]::IsNullOrEmpty($InfrastructureSubnetResourceId)) {
    Write-Host "VNet integration enabled with subnet: $InfrastructureSubnetResourceId" -ForegroundColor Cyan
    $bicepParams += "infrastructureSubnetResourceId=$InfrastructureSubnetResourceId"
}

Write-Host "Running: az deployment group create..." -ForegroundColor Gray
az @bicepParams

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Infrastructure deployment failed" -ForegroundColor Red
    exit 1
}

Write-Host "Infrastructure deployment completed successfully" -ForegroundColor Green

Write-Host "`nStep 2: Checking if Docker image exists in ACR..." -ForegroundColor Yellow

# Check if the image exists in ACR
$ErrorActionPreference = 'Continue'
$imageExists = 'false'
try {
    $tags = az acr repository show-tags `
        --name $AcrName `
        --repository $AcrRepository `
        --output json `
        2>&1 | Where-Object { $_ -notmatch 'WARNING' -and $_ -notmatch 'not found' }
    
    if ($LASTEXITCODE -eq 0 -and $tags) {
        $tagsList = $tags | ConvertFrom-Json
        if ($tagsList -contains $ImageTag) {
            $imageExists = 'true'
        }
    }
}
catch {
    Write-Host "Repository not found or error checking tags. Will build image." -ForegroundColor Gray
}
$ErrorActionPreference = 'Stop'

if ($imageExists -eq 'true') {
    Write-Host "Image '${AcrRepository}:${ImageTag}' found in ACR. Skipping build." -ForegroundColor Green
} else {
    Write-Host "Image '${AcrRepository}:${ImageTag}' not found in ACR. Building and pushing..." -ForegroundColor Yellow
    Write-Host "Note: Warnings about packing source code and excluding .git files are normal and expected." -ForegroundColor Gray
    
    $ErrorActionPreference = 'Continue'
    az acr build `
        --registry $AcrName `
        --resource-group $ResourceGroupName `
        --image "$($AcrRepository):$($ImageTag)" `
        --file ../MongoMigrationWebApp/Dockerfile `
        ..
    $ErrorActionPreference = 'Stop'
    
    Write-Host "Docker image built and pushed successfully." -ForegroundColor Green
}

Write-Host "`nStep 3: Prompting for StateStore connection string..." -ForegroundColor Yellow
$secureConnString = Read-Host -Prompt "The StateStore keeps track of migration job details in a DocumentDB. You may use the same database as the Target DocumentDB or a separate one. Enter the connection string for the StateStore." -AsSecureString
$connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString)
)

Write-Host "`nStep 4: Deploying Container App with application image..." -ForegroundColor Yellow

$finalBicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "acrRepository=$AcrRepository",
        "location=$Location",
        "storageAccountName=$StorageAccountName",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "stateStoreAppID=$StateStoreAppID",
        "stateStoreConnectionString=`"$connString`"",
        "aspNetCoreEnvironment=Development",
        "imageTag=$ImageTag",
        "ownerTag=$OwnerTag"
)

# Add VNet configuration if provided
if (-not [string]::IsNullOrEmpty($InfrastructureSubnetResourceId)) {
    $finalBicepParams += "infrastructureSubnetResourceId=$InfrastructureSubnetResourceId"
}

az @finalBicepParams

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Container App deployment failed" -ForegroundColor Red
    Remove-Variable connString, secureConnString -ErrorAction Ignore
    exit 1
}

Remove-Variable connString, secureConnString -ErrorAction Ignore

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan

# Deactivate old revisions to free up resources
Write-Host "`nCleaning up old revisions..." -ForegroundColor Yellow
$ErrorActionPreference = 'Continue'

$latestRevision = az containerapp show `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --query "properties.latestRevisionName" `
    --output tsv `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' }

if ($latestRevision) {
    Write-Host "Latest revision: $latestRevision" -ForegroundColor Cyan
    
    # Get all active revisions
    $allRevisions = az containerapp revision list `
        --name $ContainerAppName `
        --resource-group $ResourceGroupName `
        --query "[?properties.active==``true``].name" `
        --output tsv `
        2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' }
    
    if ($allRevisions) {
        $revisionList = $allRevisions -split "`n" | Where-Object { $_ -and $_ -ne $latestRevision }
        
        foreach ($oldRevision in $revisionList) {
            if ($oldRevision.Trim()) {
                Write-Host "  Deactivating old revision: $oldRevision" -ForegroundColor Gray
                az containerapp revision deactivate `
                    --name $ContainerAppName `
                    --resource-group $ResourceGroupName `
                    --revision $oldRevision `
                    2>&1 | Out-Null
            }
        }
        Write-Host "Old revisions deactivated successfully" -ForegroundColor Green
    }
}

$ErrorActionPreference = 'Stop'

# Step 5: Verify the new image becomes active
Write-Host "`nStep 5: Verifying new image deployment..." -ForegroundColor Yellow
$ErrorActionPreference = 'Continue'

# Get the expected replica count from scaling configuration
$scaleConfig = az containerapp show `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --query "properties.template.scale" `
    --output json `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' } | ConvertFrom-Json

$expectedReplicaCount = 1
if ($scaleConfig.minReplicas) {
    $expectedReplicaCount = $scaleConfig.minReplicas
}

Write-Host "Expected replica count: $expectedReplicaCount (minReplicas: $($scaleConfig.minReplicas), maxReplicas: $($scaleConfig.maxReplicas))" -ForegroundColor Cyan

# Get the deployed image name
$imageName = "$AcrName.azurecr.io/$($AcrRepository):$($ImageTag)"

# Wait for the new container to become ready
Write-Host "`nWaiting for container to become active and healthy..." -ForegroundColor Yellow
$maxAttempts = 60  # 10 minutes (60 * 10 seconds)
$attemptCount = 0
$isReady = $false

while ($attemptCount -lt $maxAttempts -and -not $isReady) {
    $attemptCount++
    Write-Host "Checking deployment status (attempt $attemptCount/$maxAttempts)..." -ForegroundColor Gray
    
    # Get the active revision
    $activeRevision = az containerapp revision list `
        --name $ContainerAppName `
        --resource-group $ResourceGroupName `
        --query "[?properties.active==``true``].name" `
        --output tsv `
        2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' }
    
    if ($activeRevision -and $LASTEXITCODE -eq 0) {
        # Get comprehensive revision details
        $revisionOutput = az containerapp revision show `
            --name $ContainerAppName `
            --resource-group $ResourceGroupName `
            --revision $activeRevision `
            --output json `
            2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' -and $_ -notmatch 'ERROR' }
        
        if ($LASTEXITCODE -eq 0 -and $revisionOutput) {
            try {
                $revisionInfo = $revisionOutput | ConvertFrom-Json
                
                $runningState = $revisionInfo.properties.runningState
                $provisioningState = $revisionInfo.properties.provisioningState
                $healthState = $revisionInfo.properties.healthState
                $activeReplicaCount = $revisionInfo.properties.replicas
                
                # Check if the new image is actually running
                $currentImage = $revisionInfo.properties.template.containers[0].image
                
                Write-Host "  Running State: $runningState | Provisioning: $provisioningState | Health: $healthState | Replicas: $activeReplicaCount" -ForegroundColor Gray
                Write-Host "  Current Image: $currentImage" -ForegroundColor Gray
                
                # Verify all conditions are met
                $imageMatches = $currentImage -eq $imageName
                $statesOk = ($runningState -eq "RunningAtMaxScale" -or $runningState -eq "Running") -and ($provisioningState -eq "Provisioned") -and ($healthState -eq "Healthy")
                $correctReplicaCount = $activeReplicaCount -eq $expectedReplicaCount
                
                if ($imageMatches -and $statesOk -and $correctReplicaCount) {
                    $isReady = $true
                    Write-Host "`nContainer is fully active and healthy!" -ForegroundColor Green
                    Write-Host "  Running state: $runningState" -ForegroundColor Green
                    Write-Host "  Provisioning state: $provisioningState" -ForegroundColor Green
                    Write-Host "  Health state: $healthState" -ForegroundColor Green
                    Write-Host "  Active replicas: $activeReplicaCount (expected: $expectedReplicaCount)" -ForegroundColor Green
                    Write-Host "  Image verified: $currentImage" -ForegroundColor Green
                    break
                } else {
                    if (-not $imageMatches) {
                        Write-Host "  Waiting for image to be deployed..." -ForegroundColor Yellow
                    }
                    if (-not $statesOk) {
                        Write-Host "  Waiting for container to reach healthy state..." -ForegroundColor Yellow
                    }
                    if (-not $correctReplicaCount) {
                        if ($activeReplicaCount -gt $expectedReplicaCount) {
                            Write-Host "  Waiting for replicas to stabilize ($activeReplicaCount -> $expectedReplicaCount)..." -ForegroundColor Yellow
                        } else {
                            Write-Host "  Waiting for replicas to start ($activeReplicaCount -> $expectedReplicaCount)..." -ForegroundColor Yellow
                        }
                    }
                    Write-Host "  Checking again in 10 seconds..." -ForegroundColor Gray
                    Start-Sleep -Seconds 10
                }
            }
            catch {
                Write-Host "  Error parsing revision info. Retrying in 10 seconds..." -ForegroundColor Yellow
                Start-Sleep -Seconds 10
            }
        } else {
            Write-Host "  Revision info not available yet. Waiting..." -ForegroundColor Yellow
            Start-Sleep -Seconds 10
        }
    } else {
        Write-Host "  Waiting for active revision..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
    }
}

if (-not $isReady) {
    Write-Host "`nWarning: Container did not become fully active within expected time." -ForegroundColor Yellow
    Write-Host "The deployment may still be in progress. Please check the Azure Portal for more details." -ForegroundColor Yellow
}

$ErrorActionPreference = 'Stop'
Write-Host ""

# Retrieve and display the application URL
Write-Host "Retrieving application URL..." -ForegroundColor Yellow
$ErrorActionPreference = 'Continue'
$appUrl = az containerapp show `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --query "properties.configuration.ingress.fqdn" `
    --output tsv `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' -and $_ -notmatch 'WARNING:' }
$ErrorActionPreference = 'Stop'

if ($appUrl) {
    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Green
    Write-Host "  Application deployed successfully!" -ForegroundColor Green
    Write-Host "==========================================" -ForegroundColor Green
    Write-Host "  Launch URL: https://$appUrl" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Unable to retrieve application URL. Please check the Azure Portal." -ForegroundColor Yellow
}
