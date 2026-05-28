# Azure Container Registry Publish Script
# Builds the MongoDB Migration Web-Based Utility image and pushes it to a provided ACR.
# This script does not touch AKS workloads.

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [string]$AcrName,

    [Parameter(Mandatory=$false)]
    [string]$AcrRepository = "",

    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",

    [Parameter(Mandatory=$false)]
    [string]$DockerfilePath = "../MongoMigrationWebApp/Dockerfile",

    [Parameter(Mandatory=$false)]
    [string]$BuildContextPath = ".."
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($AcrRepository)) {
    $AcrRepository = "mongomigration"
    Write-Host "Using default ACR repository: $AcrRepository" -ForegroundColor Cyan
}

if (-not (Test-Path $DockerfilePath)) {
    Write-Host "Error: Dockerfile not found at '$DockerfilePath'." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $BuildContextPath)) {
    Write-Host "Error: Build context not found at '$BuildContextPath'." -ForegroundColor Red
    exit 1
}

Write-Host "`n=== ACR Publish ===" -ForegroundColor Cyan
Write-Host "Resource Group : $ResourceGroupName" -ForegroundColor White
Write-Host "ACR            : $AcrName" -ForegroundColor White
Write-Host "Repository     : $AcrRepository" -ForegroundColor White
Write-Host "Image Tag      : $ImageTag" -ForegroundColor White
Write-Host "Dockerfile     : $DockerfilePath" -ForegroundColor White
Write-Host "Context        : $BuildContextPath" -ForegroundColor White
Write-Host ""

Write-Host "Step 1: Checking if the image already exists in ACR..." -ForegroundColor Yellow

$ErrorActionPreference = 'Continue'
$imageExists = $false
try {
    $tags = az acr repository show-tags `
        --name $AcrName `
        --repository $AcrRepository `
        --output json `
        2>&1 | Where-Object { $_ -notmatch 'WARNING' -and $_ -notmatch 'not found' }

    if ($LASTEXITCODE -eq 0 -and $tags) {
        $tagsList = $tags | ConvertFrom-Json
        if ($tagsList -contains $ImageTag) {
            $imageExists = $true
        }
    }
} catch {
    Write-Host "  Repository not found or error checking tags. Will build." -ForegroundColor Gray
}
$ErrorActionPreference = 'Stop'

if ($imageExists) {
    Write-Host "  Image '${AcrRepository}:${ImageTag}' already exists. Skipping build." -ForegroundColor Green
    exit 0
}

Write-Host "  Image not found. Building and pushing via ACR Tasks..." -ForegroundColor Yellow

$ErrorActionPreference = 'Continue'
az acr build `
    --registry $AcrName `
    --resource-group $ResourceGroupName `
    --image "$($AcrRepository):$($ImageTag)" `
    --file $DockerfilePath `
    $BuildContextPath
$ErrorActionPreference = 'Stop'

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to build and push Docker image." -ForegroundColor Red
    exit 1
}

Write-Host "  Docker image built and pushed successfully." -ForegroundColor Green
Write-Host "  Image: $($AcrName).azurecr.io/$($AcrRepository):$($ImageTag)" -ForegroundColor Green