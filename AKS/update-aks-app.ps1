# Azure Kubernetes Service (AKS) - Application Update Script
# Updates only the Docker image across all instances without changing storage or infrastructure.
#
# Usage:
#   Update a single named instance:  -InstanceName "<instance-name>"
#   Update instances 1..N:           -InstanceCount 3
#   Both can be combined; InstanceName takes precedence when provided.

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [string]$ClusterName,

    [Parameter(Mandatory=$true)]
    [string]$AcrName,

    [Parameter(Mandatory=$false)]
    [string]$AcrRepository = "",

    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",

    # Update a specific instance by its full name.
    # When set, only this instance is updated and InstanceCount is ignored.
    [Parameter(Mandatory=$false)]
    [string]$InstanceName = "",

    # Update instances 1..N (ignored when InstanceName is provided).
    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 10)]
    [int]$InstanceCount = 1,

    [Parameter(Mandatory=$false)]
    [string]$Namespace = "mongomigration",

    [Parameter(Mandatory=$false)]
    [switch]$UsePrivateAcr,

    [Parameter(Mandatory=$false)]
    [string]$AcrPrivateDnsVnetResourceId = "",

    [Parameter(Mandatory=$false)]
    [string]$AcrPrivateDnsZoneResourceGroup = "",

    [Parameter(Mandatory=$false)]
    [switch]$DisableAcrPublicAccess
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

function Get-AksVnetResourceId {
    param(
        [Parameter(Mandatory=$true)][string]$AksName,
        [Parameter(Mandatory=$true)][string]$AksResourceGroup
    )

    $subnetId = Invoke-AzChecked @("aks", "show", "--name", $AksName, "--resource-group", $AksResourceGroup, "--query", "agentPoolProfiles[0].vnetSubnetId", "--output", "tsv")
    if (-not [string]::IsNullOrWhiteSpace($subnetId)) {
        return Get-VnetResourceIdFromSubnet $subnetId
    }

    $nodeResourceGroup = Invoke-AzChecked @("aks", "show", "--name", $AksName, "--resource-group", $AksResourceGroup, "--query", "nodeResourceGroup", "--output", "tsv")
    $nodeVnets = @(Invoke-AzChecked @("network", "vnet", "list", "--resource-group", $nodeResourceGroup, "--query", "[].id", "--output", "tsv")) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($nodeVnets.Count -ne 1) {
        throw "Could not uniquely determine the AKS node VNet. Pass -AcrPrivateDnsVnetResourceId explicitly."
    }
    return $nodeVnets[0]
}

function Assert-AcrPrivateLinkApproved {
    param(
        [Parameter(Mandatory=$true)][string]$RegistryName,
        [Parameter(Mandatory=$true)][string]$RegistryResourceGroup,
        [Parameter(Mandatory=$true)][string]$DnsVnetId,
        [Parameter(Mandatory=$true)][string]$DnsZoneResourceGroup
    )

    $acr = Invoke-AzChecked @("acr", "show", "--name", $RegistryName, "--resource-group", $RegistryResourceGroup, "--output", "json") | ConvertFrom-Json
    if ($acr.sku.name -ne "Premium") {
        throw "ACR '$RegistryName' must use the Premium SKU for Private Link."
    }

    $approvedConnection = Invoke-AzChecked @("acr", "private-endpoint-connection", "list", "--registry-name", $RegistryName, "--resource-group", $RegistryResourceGroup, "--query", "[?privateLinkServiceConnectionState.status=='Approved'] | [0].name", "--output", "tsv")
    if ([string]::IsNullOrWhiteSpace($approvedConnection)) {
        throw "ACR '$RegistryName' does not have an approved private endpoint connection."
    }

    $dnsLinks = Invoke-AzChecked @("network", "private-dns", "link", "vnet", "list", "--resource-group", $DnsZoneResourceGroup, "--zone-name", "privatelink.azurecr.io", "--output", "json") | ConvertFrom-Json
    $matchingLink = $dnsLinks | Where-Object { $_.virtualNetwork.id -ieq $DnsVnetId } | Select-Object -First 1
    if (-not $matchingLink) {
        throw "Private DNS zone 'privatelink.azurecr.io' is not linked to the AKS node VNet '$DnsVnetId'."
    }
    Write-Host "Approved ACR private endpoint and AKS node VNet DNS link verified." -ForegroundColor Green
}

if ($DisableAcrPublicAccess -and -not $UsePrivateAcr) {
    throw "-DisableAcrPublicAccess requires -UsePrivateAcr."
}
if ($UsePrivateAcr) {
    if ([string]::IsNullOrWhiteSpace($AcrPrivateDnsVnetResourceId)) {
        $AcrPrivateDnsVnetResourceId = Get-AksVnetResourceId -AksName $ClusterName -AksResourceGroup $ResourceGroupName
    }
    if ([string]::IsNullOrWhiteSpace($AcrPrivateDnsZoneResourceGroup)) {
        $AcrPrivateDnsZoneResourceGroup = $ResourceGroupName
    }
    Assert-AcrPrivateLinkApproved -RegistryName $AcrName -RegistryResourceGroup $ResourceGroupName -DnsVnetId $AcrPrivateDnsVnetResourceId -DnsZoneResourceGroup $AcrPrivateDnsZoneResourceGroup
    Write-Host "ACR public access remains available for publishing; AKS pulls resolve through Private Link." -ForegroundColor Green
}

# Generate ACR repository name if not provided
if ([string]::IsNullOrEmpty($AcrRepository)) {
    $AcrRepository = $ClusterName
    Write-Host "Using ClusterName as ACR repository: $AcrRepository" -ForegroundColor Cyan
}

# Build the list of deployment names to update
if (-not [string]::IsNullOrEmpty($InstanceName)) {
    $instancesToUpdate = @($InstanceName)
} else {
    $instancesToUpdate = 1..$InstanceCount | ForEach-Object { "$ClusterName-$_" }
}

Write-Host "`n=== AKS Application Update ===" -ForegroundColor Cyan
Write-Host "Resource Group : $ResourceGroupName" -ForegroundColor White
Write-Host "Cluster        : $ClusterName" -ForegroundColor White
Write-Host "ACR            : $AcrName" -ForegroundColor White
Write-Host "ACR Repository : $AcrRepository" -ForegroundColor White
Write-Host "Image Tag      : $ImageTag" -ForegroundColor White
Write-Host "Instances      : $($instancesToUpdate -join ', ')" -ForegroundColor White
Write-Host "Namespace      : $Namespace" -ForegroundColor White
Write-Host ""

# ── Step 1: Build / push Docker image ─────────────────────────────────────────
Write-Host "Step 1: Checking if Docker image exists in ACR..." -ForegroundColor Yellow

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
        if ($tagsList -contains $ImageTag) { $imageExists = $true }
    }
} catch {
    Write-Host "  Repository not found or error checking tags. Will build." -ForegroundColor Gray
}
$ErrorActionPreference = 'Stop'

if ($imageExists) {
    Write-Host "  Image '${AcrRepository}:${ImageTag}' found in ACR. Skipping build." -ForegroundColor Green
} else {
    Write-Host "  Image not found. Building and pushing via ACR Tasks..." -ForegroundColor Yellow
    Write-Host "  Note: Warnings about packing source code are normal." -ForegroundColor Gray

    $ErrorActionPreference = 'Continue'
    az acr build `
        --registry $AcrName `
        --resource-group $ResourceGroupName `
        --image "$($AcrRepository):$($ImageTag)" `
        --file ../MongoMigrationWebApp/Dockerfile `
        ..
    $ErrorActionPreference = 'Stop'

    if ($LASTEXITCODE -ne 0) { Write-Host "Error: Failed to build Docker image" -ForegroundColor Red; exit 1 }
    Write-Host "  Docker image built and pushed successfully." -ForegroundColor Green
}

# ── Step 2: Fetch kubectl credentials ─────────────────────────────────────────
Write-Host "`nStep 2: Fetching kubectl credentials..." -ForegroundColor Yellow
az aks get-credentials --name $ClusterName --resource-group $ResourceGroupName --overwrite-existing | Out-Null

if (-not (Get-Command kubelogin -ErrorAction SilentlyContinue)) {
    $kubeloginDefaultPath = Join-Path $env:USERPROFILE ".azure-kubelogin"
    if (Test-Path (Join-Path $kubeloginDefaultPath "kubelogin.exe")) {
        $env:PATH = "$kubeloginDefaultPath;$env:PATH"
    }
}

if (Get-Command kubelogin -ErrorAction SilentlyContinue) {
    kubelogin convert-kubeconfig -l azurecli | Out-Null
}

$ErrorActionPreference = 'Continue'
kubectl auth can-i get deployments --namespace $Namespace 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    az aks get-credentials --name $ClusterName --resource-group $ResourceGroupName --overwrite-existing --admin | Out-Null
}
$ErrorActionPreference = 'Stop'
Write-Host "  kubectl context updated." -ForegroundColor Green

if ($UsePrivateAcr) {
    Invoke-AzChecked @("aks", "check-acr", "--name", $ClusterName, "--resource-group", $ResourceGroupName, "--acr", "$AcrName.azurecr.io", "--output", "none") | Out-Null
    Write-Host "  AKS connectivity to the private ACR verified." -ForegroundColor Green
}

# ── Step 3: Update image on each instance ─────────────────────────────────────
Write-Host "`nStep 3: Updating image on $($instancesToUpdate.Count) instance(s)..." -ForegroundColor Yellow

$imageName = "$AcrName.azurecr.io/$($AcrRepository):$($ImageTag)"

foreach ($instanceName in $instancesToUpdate) {
    Write-Host "  Updating '$instanceName' -> $imageName" -ForegroundColor Cyan

    kubectl set image deployment/$instanceName `
        mongomigration=$imageName `
        --namespace $Namespace

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Warning: Failed to update '$instanceName'. It may not exist yet." -ForegroundColor Yellow
        continue
    }

    # Force a rollout restart so the new image is always pulled (handles same-tag re-push)
    kubectl rollout restart deployment/$instanceName --namespace $Namespace | Out-Null
    Write-Host "  Rollout restarted for '$instanceName'." -ForegroundColor Green
}

# ── Step 4: Wait for rollouts to complete ─────────────────────────────────────
Write-Host "`nStep 4: Waiting for rollouts to complete..." -ForegroundColor Yellow
$allRolloutsReady = $true

foreach ($instanceName in $instancesToUpdate) {
    Write-Host "  Waiting for '$instanceName' rollout..." -ForegroundColor Gray

    $ErrorActionPreference = 'Continue'
    kubectl rollout status deployment/$instanceName --namespace $Namespace --timeout=5m
    $ErrorActionPreference = 'Stop'

    if ($LASTEXITCODE -eq 0) {
        Write-Host "  '$instanceName' is ready." -ForegroundColor Green
    } else {
        $allRolloutsReady = $false
        Write-Host "  '$instanceName' rollout timed out. Check with: kubectl rollout status deployment/$instanceName -n $Namespace" -ForegroundColor Yellow
    }
}

if ($UsePrivateAcr -and -not $allRolloutsReady) {
    throw "Private ACR image pull validation failed because one or more deployments did not become ready."
}

if ($DisableAcrPublicAccess) {
    Invoke-AzChecked @("acr", "update", "--name", $AcrName, "--resource-group", $ResourceGroupName, "--public-network-enabled", "false", "--output", "none") | Out-Null
    Write-Host "ACR public network access disabled after successful image-pull validation." -ForegroundColor Green
}

# ── Step 5: Print URLs ─────────────────────────────────────────────────────────
Write-Host "`nStep 5: Fetching ingress/service URLs..." -ForegroundColor Yellow

$ingressName = "$ClusterName-ingress"
$ErrorActionPreference = 'Continue'
$ingressHost = kubectl get ingress $ingressName `
    --namespace $Namespace `
    --output jsonpath='{.spec.rules[0].host}' 2>$null
$ErrorActionPreference = 'Stop'

if ($ingressHost) {
    Write-Host "  Ingress host: https://$ingressHost" -ForegroundColor Green
    foreach ($instanceName in $instancesToUpdate) {
        $instanceSuffix = "1"
        if ($instanceName -match "^$([Regex]::Escape($ClusterName))-(\d+)$") {
            $instanceSuffix = $Matches[1]
        }
        $url = "https://$ingressHost/$instanceSuffix"
        Write-Host "  $instanceName  ->  $url" -ForegroundColor Green
        Write-Host "  Launch: `e]8;;$url`e\$url`e]8;;`e\" -ForegroundColor Blue
    }
} else {
    foreach ($instanceName in $instancesToUpdate) {
        $ErrorActionPreference = 'Continue'
        $ip = kubectl get service $instanceName `
            --namespace $Namespace `
            --output jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>$null
        $ErrorActionPreference = 'Stop'

        if ($ip -and $ip -ne '<pending>') {
            $url = "http://$ip"
            Write-Host "  $instanceName  ->  $url" -ForegroundColor Green
            Write-Host "  Launch: `e]8;;$url`e\$url`e]8;;`e\" -ForegroundColor Blue
        } else {
            Write-Host "  $instanceName  ->  (no ingress host and no service IP found)" -ForegroundColor Yellow
        }
    }
}

Write-Host "`n=== Update Complete ===" -ForegroundColor Cyan
