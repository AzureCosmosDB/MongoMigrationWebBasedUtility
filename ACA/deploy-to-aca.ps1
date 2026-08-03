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
    [string]$AcrLocation = "",
    
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
    
    [Parameter(Mandatory=$false)]
    [switch]$UseEntraIdForAzureStorage,

    [Parameter(Mandatory=$false)]
    [string]$StorageAccountResourceId = "",

    [Parameter(Mandatory=$false)]
    [ValidateRange(100, 102400)]
    [int]$FileShareSizeGB = 100,

    [Parameter(Mandatory=$false)]
    [switch]$UsePrivateAcr,

    [Parameter(Mandatory=$false)]
    [string]$AcrPrivateEndpointSubnetResourceId = "",

    [Parameter(Mandatory=$false)]
    [string]$AcrPrivateDnsVnetResourceId = "",

    [Parameter(Mandatory=$false)]
    [string]$AcrPrivateDnsZoneResourceGroup = "",

    [Parameter(Mandatory=$false)]
    [switch]$DisableAcrPublicAccess,

    [Parameter(Mandatory=$true)]
    [string]$OwnerTag
)

$ErrorActionPreference = "Stop"

function Invoke-AzChecked {
    param([Parameter(Mandatory=$true)][string[]]$Arguments)

    $result = az @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI command failed: az $($Arguments -join ' ')"
    }
    return $result
}

function Get-VnetResourceIdFromSubnet {
    param([Parameter(Mandatory=$true)][string]$SubnetResourceId)

    if ($SubnetResourceId -notmatch '^(.*?/virtualNetworks/[^/]+)/subnets/[^/]+$') {
        throw "Invalid subnet resource ID: '$SubnetResourceId'."
    }
    return $Matches[1]
}

function Set-AcrPrivateLink {
    param(
        [Parameter(Mandatory=$true)][string]$RegistryName,
        [Parameter(Mandatory=$true)][string]$RegistryResourceGroup,
        [Parameter(Mandatory=$true)][string]$PrivateEndpointSubnetId,
        [Parameter(Mandatory=$true)][string]$DnsVnetId,
        [Parameter(Mandatory=$true)][string]$DnsZoneResourceGroup
    )

    $privateDnsZoneName = "privatelink.azurecr.io"
    $privateEndpointName = "$RegistryName-pe"
    $dnsVnetName = $DnsVnetId.Split('/')[-1]
    $dnsLinkName = "$dnsVnetName-acr-link"
    $acrId = Invoke-AzChecked @("acr", "show", "--name", $RegistryName, "--resource-group", $RegistryResourceGroup, "--query", "id", "--output", "tsv")

    Write-Host "`nConfiguring ACR Private Link..." -ForegroundColor Yellow
    Invoke-AzChecked @("network", "vnet", "subnet", "update", "--ids", $PrivateEndpointSubnetId, "--private-endpoint-network-policies", "Disabled", "--output", "none") | Out-Null

    $privateDnsZoneId = az network private-dns zone show --resource-group $DnsZoneResourceGroup --name $privateDnsZoneName --query id --output tsv 2>$null
    if ($LASTEXITCODE -ne 0) {
        $privateDnsZoneId = Invoke-AzChecked @("network", "private-dns", "zone", "create", "--resource-group", $DnsZoneResourceGroup, "--name", $privateDnsZoneName, "--query", "id", "--output", "tsv")
    }

    az network private-dns link vnet show --resource-group $DnsZoneResourceGroup --zone-name $privateDnsZoneName --name $dnsLinkName --output none 2>$null
    if ($LASTEXITCODE -ne 0) {
        Invoke-AzChecked @("network", "private-dns", "link", "vnet", "create", "--resource-group", $DnsZoneResourceGroup, "--zone-name", $privateDnsZoneName, "--name", $dnsLinkName, "--virtual-network", $DnsVnetId, "--registration-enabled", "false", "--output", "none") | Out-Null
    }

    $linkedVnetId = Invoke-AzChecked @("network", "private-dns", "link", "vnet", "show", "--resource-group", $DnsZoneResourceGroup, "--zone-name", $privateDnsZoneName, "--name", $dnsLinkName, "--query", "virtualNetwork.id", "--output", "tsv")
    if ($linkedVnetId -ine $DnsVnetId) {
        throw "Private DNS link '$dnsLinkName' targets '$linkedVnetId' instead of workload VNet '$DnsVnetId'."
    }

    az network private-endpoint show --name $privateEndpointName --resource-group $RegistryResourceGroup --output none 2>$null
    if ($LASTEXITCODE -ne 0) {
        Invoke-AzChecked @("network", "private-endpoint", "create", "--name", $privateEndpointName, "--resource-group", $RegistryResourceGroup, "--subnet", $PrivateEndpointSubnetId, "--private-connection-resource-id", $acrId, "--group-ids", "registry", "--connection-name", "$RegistryName-connection", "--output", "none") | Out-Null
    }

    $dnsZoneGroup = Invoke-AzChecked @("network", "private-endpoint", "dns-zone-group", "create", "--resource-group", $RegistryResourceGroup, "--endpoint-name", $privateEndpointName, "--name", "default", "--private-dns-zone", $privateDnsZoneId, "--zone-name", "privatelink-azurecr-io", "--output", "json") | ConvertFrom-Json
    $dnsRecordSets = @($dnsZoneGroup.privateDnsZoneConfigs | ForEach-Object { $_.recordSets })
    if ($dnsRecordSets.Count -lt 2) {
        throw "ACR private DNS records were not created for private endpoint '$privateEndpointName'."
    }

    $connectionStatus = Invoke-AzChecked @("network", "private-endpoint", "show", "--name", $privateEndpointName, "--resource-group", $RegistryResourceGroup, "--query", "privateLinkServiceConnections[0].privateLinkServiceConnectionState.status", "--output", "tsv")
    if ($connectionStatus -ne "Approved") {
        Write-Warning "ACR private endpoint status is '$connectionStatus'. An ACR owner must approve it."
    }

    Write-Host "ACR Private Link configured. Connection status: $connectionStatus" -ForegroundColor Green
    return $connectionStatus
}

function Test-AcaPrivateAcrPull {
    param(
        [Parameter(Mandatory=$true)][string]$AppName,
        [Parameter(Mandatory=$true)][string]$AppResourceGroup
    )

    $revisionName = Invoke-AzChecked @("containerapp", "show", "--name", $AppName, "--resource-group", $AppResourceGroup, "--query", "properties.latestRevisionName", "--output", "tsv")
    Write-Host "Validating the private ACR path by restarting revision '$revisionName'..." -ForegroundColor Yellow
    Invoke-AzChecked @("containerapp", "revision", "restart", "--name", $AppName, "--resource-group", $AppResourceGroup, "--revision", $revisionName, "--output", "none") | Out-Null

    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $revision = Invoke-AzChecked @("containerapp", "revision", "show", "--name", $AppName, "--resource-group", $AppResourceGroup, "--revision", $revisionName, "--output", "json") | ConvertFrom-Json
        $runningState = $revision.properties.runningState
        $healthState = $revision.properties.healthState
        if (($runningState -eq "Running" -or $runningState -eq "RunningAtMaxScale") -and $healthState -eq "Healthy") {
            Write-Host "Private ACR image pull validation succeeded." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 10
    }

    throw "Private ACR image pull validation failed: revision '$revisionName' did not return to a healthy running state."
}

if ($DisableAcrPublicAccess -and -not $UsePrivateAcr) {
    throw "-DisableAcrPublicAccess requires -UsePrivateAcr."
}
if ($UsePrivateAcr -and [string]::IsNullOrWhiteSpace($AcrPrivateEndpointSubnetResourceId)) {
    throw "-AcrPrivateEndpointSubnetResourceId is required when -UsePrivateAcr is specified."
}
if ($UsePrivateAcr -and [string]::IsNullOrWhiteSpace($InfrastructureSubnetResourceId)) {
    throw "-InfrastructureSubnetResourceId is required when -UsePrivateAcr is specified because Container Apps must be VNet-integrated to reach the private endpoint."
}
if ($UsePrivateAcr) {
    if ([string]::IsNullOrWhiteSpace($AcrPrivateDnsVnetResourceId)) {
        $dnsSourceSubnetId = if ([string]::IsNullOrWhiteSpace($InfrastructureSubnetResourceId)) { $AcrPrivateEndpointSubnetResourceId } else { $InfrastructureSubnetResourceId }
        $AcrPrivateDnsVnetResourceId = Get-VnetResourceIdFromSubnet $dnsSourceSubnetId
    }
    if ([string]::IsNullOrWhiteSpace($AcrPrivateDnsZoneResourceGroup)) {
        $AcrPrivateDnsZoneResourceGroup = $ResourceGroupName
    }
}

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

# Validate StorageAccountResourceId and StorageAccountName are mutually exclusive
if (-not [string]::IsNullOrEmpty($StorageAccountResourceId) -and $PSBoundParameters.ContainsKey('StorageAccountName')) {
    Write-Host "`nError: -StorageAccountResourceId and -StorageAccountName cannot be used together" -ForegroundColor Red
    exit 1
}

# Handle pre-configured storage account via resource ID, or generate/use name
if (-not [string]::IsNullOrEmpty($StorageAccountResourceId)) {
    if (-not $UseEntraIdForAzureStorage) {
        Write-Host "`nError: -UseEntraIdForAzureStorage is required when using -StorageAccountResourceId (PE-enabled storage requires Blob SDK access, not Azure Files mount)" -ForegroundColor Red
        exit 1
    }
    $StorageAccountName = $StorageAccountResourceId.Split('/')[-1]
    Write-Host "Using pre-configured storage account: $StorageAccountName (Entra ID + Blob SDK)" -ForegroundColor Cyan
} elseif ([string]::IsNullOrEmpty($StorageAccountName)) {
    $StorageAccountName = ($ContainerAppName -replace '-', '').ToLower() + 'stor'
    if ($StorageAccountName.Length -gt 24) {
        $StorageAccountName = $StorageAccountName.Substring(0, 24)
    }
    Write-Host "Using generated storage account name: $StorageAccountName" -ForegroundColor Cyan
}

# Resolve AcrLocation - defaults to main Location if not specified
if ([string]::IsNullOrEmpty($AcrLocation)) {
    $AcrLocation = $Location
}

Write-Host "Using location: $Location" -ForegroundColor Cyan
if ($AcrLocation -ne $Location) {
    Write-Host "Using ACR location: $AcrLocation (different from main location)" -ForegroundColor Cyan
}
if ($UseEntraIdForAzureStorage) {
    Write-Host "Using Entra ID (Managed Identity) for Azure Storage instead of mounted disk" -ForegroundColor Cyan
}

Write-Host "`nStep 1: Deploying infrastructure (ACR, Storage Account, Managed Identity, Container Apps Environment)..." -ForegroundColor Yellow
Write-Host "Note: This may take 3-5 minutes..." -ForegroundColor Gray

$bicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "acrSku=$(if ($UsePrivateAcr) { 'Premium' } else { 'Basic' })",
        "acrRepository=$AcrRepository",
        "acrLocation=$AcrLocation",
        "location=$Location",
        "storageAccountName=$StorageAccountName",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "ownerTag=$OwnerTag",
        "useEntraIdForStorage=$($UseEntraIdForAzureStorage.ToString().ToLower())",
        "storageAccountResourceId=$StorageAccountResourceId",
        "fileShareSizeGB=$FileShareSizeGB"
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

if ($UsePrivateAcr) {
    $privateLinkStatus = Set-AcrPrivateLink `
        -RegistryName $AcrName `
        -RegistryResourceGroup $ResourceGroupName `
        -PrivateEndpointSubnetId $AcrPrivateEndpointSubnetResourceId `
        -DnsVnetId $AcrPrivateDnsVnetResourceId `
        -DnsZoneResourceGroup $AcrPrivateDnsZoneResourceGroup

    if ($privateLinkStatus -ne "Approved") {
        throw "The ACR private endpoint must be approved before the Container App image is deployed."
    }
    Write-Host "ACR public access was not changed; Container Apps pulls resolve through Private Link." -ForegroundColor Green
}

Write-Host "`nStep 3: Prompting for StateStore connection string..." -ForegroundColor Yellow
$secureConnString = Read-Host -Prompt "The StateStore keeps track of migration job details in a DocumentDB. You may use the same database as the Target DocumentDB or a separate one. Enter the connection string for the StateStore." -AsSecureString
$isWindowsPlatform = ($env:OS -eq 'Windows_NT') -or ((Get-Variable IsWindows -ErrorAction SilentlyContinue) -and $IsWindows)

if ($isWindowsPlatform) {
    # Keep the previous Windows behavior to avoid deployment issues observed with PtrToStringBSTR.
    $connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString)
    )
    $stateStoreConnectionStringParam = "stateStoreConnectionString=`"$connString`""
} else {
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString)
    try {
        $connString = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        $stateStoreConnectionStringParam = "stateStoreConnectionString=$connString"
    } catch {
        Write-Host "`nError: Failed to read the StateStore connection string: $_" -ForegroundColor Red
        exit 1
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

Write-Host "`nStep 4: Deploying Container App with application image..." -ForegroundColor Yellow

$finalBicepParams = @(
    "deployment", "group", "create",
    "--resource-group", $ResourceGroupName,
    "--template-file", "aca_main.bicep",
    "--parameters",
        "containerAppName=$ContainerAppName",
        "acrName=$AcrName",
        "acrSku=$(if ($UsePrivateAcr) { 'Premium' } else { 'Basic' })",
        "acrRepository=$AcrRepository",
        "acrLocation=$AcrLocation",
        "location=$Location",
        "storageAccountName=$StorageAccountName",
        "vCores=$VCores",
        "memoryGB=$MemoryGB",
        "stateStoreAppID=$StateStoreAppID",
        $stateStoreConnectionStringParam,
        "aspNetCoreEnvironment=Development",
        "imageTag=$ImageTag",
        "ownerTag=$OwnerTag",
        "useEntraIdForStorage=$($UseEntraIdForAzureStorage.ToString().ToLower())",
        "storageAccountResourceId=$StorageAccountResourceId",
        "fileShareSizeGB=$FileShareSizeGB"
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

if ($UsePrivateAcr) {
    Test-AcaPrivateAcrPull -AppName $ContainerAppName -AppResourceGroup $ResourceGroupName
    if ($DisableAcrPublicAccess) {
        Invoke-AzChecked @("acr", "update", "--name", $AcrName, "--resource-group", $ResourceGroupName, "--public-network-enabled", "false", "--output", "none") | Out-Null
        Write-Host "ACR public network access disabled." -ForegroundColor Green
    }
}
