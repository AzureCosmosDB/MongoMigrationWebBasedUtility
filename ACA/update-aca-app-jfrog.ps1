# Azure Container Apps - Application Update Script (JFrog Registry Edition)
# Updates only the application image without resetting environment variables and secrets
# Uses JFrog Artifactory as the container registry

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$ContainerAppName,
    
    [Parameter(Mandatory=$true)]
    [string]$JFrogRegistryServer,
    
    [Parameter(Mandatory=$true)]
    [string]$JFrogUsername,
    
    [Parameter(Mandatory=$true)]
    [string]$JFrogRepository,
    
    [Parameter(Mandatory=$false)]
    [string]$JFrogBaseImageRegistry = "",
    
    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipDockerBuild
)

$ErrorActionPreference = "Stop"

# Use the same registry for base images if not specified
if ([string]::IsNullOrEmpty($JFrogBaseImageRegistry)) {
    $JFrogBaseImageRegistry = $JFrogRegistryServer
}

# Build full image path
$FullImagePath = "$JFrogRegistryServer/$JFrogRepository"
$FullImageWithTag = "${FullImagePath}:${ImageTag}"

Write-Host "`n=== Azure Container App - Image Update (JFrog Registry) ===" -ForegroundColor Cyan
Write-Host "Resource Group: $ResourceGroupName" -ForegroundColor White
Write-Host "Container App: $ContainerAppName" -ForegroundColor White
Write-Host "JFrog Registry: $JFrogRegistryServer" -ForegroundColor White
Write-Host "JFrog Repository: $JFrogRepository" -ForegroundColor White
Write-Host "Image Tag: $ImageTag" -ForegroundColor White
Write-Host "Full Image: $FullImageWithTag" -ForegroundColor White
Write-Host ""

# Prompt for JFrog password/API key
Write-Host "JFrog Authentication" -ForegroundColor Yellow
$secureJFrogPassword = Read-Host -Prompt "Enter JFrog password or API key" -AsSecureString
$jfrogPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureJFrogPassword)
)

if (-not $SkipDockerBuild) {
    Write-Host "`nStep 1: Building and pushing new image to JFrog..." -ForegroundColor Yellow
    
    # Check if Docker is running
    Write-Host "Checking Docker availability..." -ForegroundColor Gray
    $ErrorActionPreference = 'Continue'
    docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Docker is not running. Please start Docker Desktop or Docker service." -ForegroundColor Red
        Remove-Variable jfrogPassword, secureJFrogPassword -ErrorAction Ignore
        exit 1
    }
    $ErrorActionPreference = 'Stop'
    Write-Host "Docker is available." -ForegroundColor Green
    
    # Login to JFrog
    Write-Host "Logging in to JFrog registry..." -ForegroundColor Gray
    $ErrorActionPreference = 'Continue'
    $jfrogPassword | docker login $JFrogRegistryServer -u $JFrogUsername --password-stdin
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to login to JFrog registry" -ForegroundColor Red
        Remove-Variable jfrogPassword, secureJFrogPassword -ErrorAction Ignore
        exit 1
    }
    $ErrorActionPreference = 'Stop'
    Write-Host "Successfully logged in to JFrog." -ForegroundColor Green
    
    # Build the Docker image using JFrog base images
    Write-Host "Building Docker image with JFrog base images..." -ForegroundColor Gray
    Write-Host "  Base image registry: $JFrogBaseImageRegistry" -ForegroundColor Gray
    Write-Host "  Target image: $FullImageWithTag" -ForegroundColor Gray
    
    $ErrorActionPreference = 'Continue'
    Push-Location ..
    try {
        docker build `
            -f MongoMigrationWebApp/Dockerfile.jfrog `
            --build-arg JFROG_REGISTRY=$JFrogBaseImageRegistry `
            -t $FullImageWithTag `
            .
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Error: Docker build failed" -ForegroundColor Red
            Remove-Variable jfrogPassword, secureJFrogPassword -ErrorAction Ignore
            exit 1
        }
    }
    finally {
        Pop-Location
    }
    $ErrorActionPreference = 'Stop'
    Write-Host "Docker image built successfully." -ForegroundColor Green
    
    # Push the image to JFrog
    Write-Host "Pushing image to JFrog..." -ForegroundColor Gray
    $ErrorActionPreference = 'Continue'
    docker push $FullImageWithTag
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to push image to JFrog" -ForegroundColor Red
        Remove-Variable jfrogPassword, secureJFrogPassword -ErrorAction Ignore
        exit 1
    }
    $ErrorActionPreference = 'Stop'
    Write-Host "Docker image pushed successfully." -ForegroundColor Green
    
    # Logout from JFrog (cleanup)
    docker logout $JFrogRegistryServer 2>&1 | Out-Null
} else {
    Write-Host "`nStep 1: Skipping Docker build (SkipDockerBuild flag set)" -ForegroundColor Yellow
    Write-Host "Assuming image already exists at: $FullImageWithTag" -ForegroundColor Gray
}

# Step 2: Update Container App with new image
Write-Host "`nStep 2: Updating Container App with new image..." -ForegroundColor Yellow
Write-Host "Note: Warnings about cryptography or UserWarnings are normal and can be ignored." -ForegroundColor Gray

# First, update the registry secret with the new password
Write-Host "Updating JFrog registry secret..." -ForegroundColor Gray
$ErrorActionPreference = 'Continue'
az containerapp secret set `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --secrets "jfrog-password=$jfrogPassword" `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' }

if ($LASTEXITCODE -ne 0) {
    Write-Host "Warning: Could not update JFrog secret. Continuing with existing secret..." -ForegroundColor Yellow
}

# Update the container app with new image
az containerapp update `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --min-replicas 1 `
    --max-replicas 1 `
    --set template.scale.rules=[] `
    --image $FullImageWithTag `
    2>&1 | Where-Object { $_ -notmatch 'cryptography' -and $_ -notmatch 'UserWarning' }

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to update Container App" -ForegroundColor Red
    Remove-Variable jfrogPassword, secureJFrogPassword -ErrorAction Ignore
    exit 1
}
$ErrorActionPreference = 'Stop'

Remove-Variable jfrogPassword, secureJFrogPassword -ErrorAction Ignore

Write-Host "`n=== Update Complete ===" -ForegroundColor Cyan
Write-Host "The Container App '$ContainerAppName' has been updated with image: $FullImageWithTag" -ForegroundColor Green
Write-Host "Environment variables and secrets remain unchanged." -ForegroundColor Green
Write-Host ""

# Deactivate old revisions to free up resources
Write-Host "Cleaning up old revisions..." -ForegroundColor Yellow
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

# Step 3: Verify the new image becomes active
Write-Host "Step 3: Verifying new image deployment..." -ForegroundColor Yellow
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

# Wait for the new container to become ready
Write-Host "`nWaiting for new image to become active and healthy..." -ForegroundColor Yellow
$maxAttempts = 60  # 10 minutes (60 * 10 seconds)
$attemptCount = 0
$isReady = $false
$imageName = $FullImageWithTag

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
                $statesOk = ($runningState -eq "RunningAtMaxScale") -and ($provisioningState -eq "Provisioned") -and ($healthState -eq "Healthy")
                $correctReplicaCount = $activeReplicaCount -eq $expectedReplicaCount
                
                if ($imageMatches -and $statesOk -and $correctReplicaCount) {
                    $isReady = $true
                    Write-Host "`nNew image is fully active and healthy!" -ForegroundColor Green
                    Write-Host "  Running state: $runningState" -ForegroundColor Green
                    Write-Host "  Provisioning state: $provisioningState" -ForegroundColor Green
                    Write-Host "  Health state: $healthState" -ForegroundColor Green
                    Write-Host "  Active replicas: $activeReplicaCount (expected: $expectedReplicaCount)" -ForegroundColor Green
                    Write-Host "  Image verified: $currentImage" -ForegroundColor Green
                    break
                } else {
                    if (-not $imageMatches) {
                        Write-Host "  Waiting for new image to be deployed..." -ForegroundColor Yellow
                    }
                    if (-not $statesOk) {
                        Write-Host "  Waiting for container to reach healthy state..." -ForegroundColor Yellow
                    }
                    if (-not $correctReplicaCount) {
                        if ($activeReplicaCount -gt $expectedReplicaCount) {
                            Write-Host "  Waiting for old replica to terminate ($activeReplicaCount -> $expectedReplicaCount)..." -ForegroundColor Yellow
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
    Write-Host "`nWarning: New image did not become fully active within expected time." -ForegroundColor Yellow
    Write-Host "Current state: Running=$runningState | Provisioning=$provisioningState | Health=$healthState | Replicas=$activeReplicaCount" -ForegroundColor Yellow
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
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "  Application updated successfully!" -ForegroundColor Green
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host "  Launch URL: https://$appUrl" -ForegroundColor Cyan
    Write-Host "  Registry: JFrog ($JFrogRegistryServer)" -ForegroundColor Cyan
    Write-Host "===========================================" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Unable to retrieve application URL. Please check the Azure Portal." -ForegroundColor Yellow
}
