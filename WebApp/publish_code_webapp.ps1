param (
    [bool]$SupportMongoDump = $true
)

# Variables to be replaced
$resourceGroupName = "<Replace with Existing Resource Group Name>"
$webAppName = "<Replace with WebApp Name>"
$projectFolderPath = Split-Path (Get-Location) -Parent


# Paths - No changes required
$projectFilePath = Join-Path $projectFolderPath "MongoMigrationWebApp\MongoMigrationWebApp.csproj"
$publishFolder = Join-Path $projectFolderPath "publish"
$zipPath = Join-Path $publishFolder "app.zip"

# Delete the existing publish folder (if exist)
if (Test-Path $publishFolder) {
    Remove-Item -Path $publishFolder -Recurse -Force -Confirm:$false
}

# Build the Blazor app
Write-Host "Building Blazor app..."
dotnet publish $projectFilePath -c Release -o $publishFolder -warnaserror:none --nologo

# Modify appsettings.json if SupportMongoDump is false
if (-not $SupportMongoDump) {
    $appSettingsPath = Join-Path $publishFolder "appsettings.json"
    if (Test-Path $appSettingsPath) {
        $json = Get-Content $appSettingsPath -Raw | ConvertFrom-Json
        $json.AllowMongoDump = $false
        $json | ConvertTo-Json -Depth 10 | Set-Content -Path $appSettingsPath -Encoding UTF8
        Write-Host "Set AllowMongoDump to false in appsettings.json"
    } else {
        Write-Warning "appsettings.json not found at $appSettingsPath"
    }
}

# Delete the existing zip file if it exists
if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

# Archive published files
Compress-Archive -Path "$publishFolder\*" -DestinationPath $zipPath -Update

# Deploy files to Azure Web App
Write-Host "Deploying to Azure Web App..."
az webapp deploy --resource-group $resourceGroupName --name $webAppName --src-path $zipPath --type zip
