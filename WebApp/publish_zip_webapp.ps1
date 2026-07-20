param (
    [string]$resourceGroupName = "<Replace with Existing Resource Group Name>",
    [string]$webAppName = "<Replace with Web App Name>",
    [string]$zipFileName = "app.zip",
    [bool]$SupportMongoDump = $true
)

# Calculate full zip path based on parent folder
$workingFolder = Split-Path (Get-Location) -Parent
$zipPath = Join-Path $workingFolder $zipFileName

# Login to Azure
#az login

# Set subscription (optional)
# az account set --subscription "your-subscription-id"

# If MongoDump is not supported, extract and modify the zip contents
if (-not $SupportMongoDump) {
    $tempFolder = Join-Path $workingFolder "TempAppDeploy"

    if (Test-Path $tempFolder) {
        Remove-Item -Path $tempFolder -Recurse -Force
    }
    New-Item -ItemType Directory -Path $tempFolder | Out-Null

    # Extract the zip
    Expand-Archive -Path $zipPath -DestinationPath $tempFolder -Force

    # Modify appsettings.json
    $appSettingsPath = Join-Path $tempFolder "appsettings.json"
    if (Test-Path $appSettingsPath) {
        $json = Get-Content $appSettingsPath -Raw | ConvertFrom-Json
        $json.AllowMongoDump = $false
        $json | ConvertTo-Json -Depth 10 | Set-Content -Path $appSettingsPath -Encoding UTF8
        Write-Host "Set AllowMongoDump to false in appsettings.json"
    } else {
        Write-Warning "appsettings.json not found at $appSettingsPath"
    }

    # Recreate the zip
    Remove-Item $zipPath -Force
    Compress-Archive -Path "$tempFolder\*" -DestinationPath $zipPath

    # Optionally clean up temp folder
    Remove-Item -Path $tempFolder -Recurse -Force
}

# Deploy zip contents to Azure Web App
Write-Host "Deploying to Azure Web App..."
az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip

Write-Host "Deployment completed successfully!"
