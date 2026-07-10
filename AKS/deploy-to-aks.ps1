# Azure Kubernetes Service (AKS) Deployment Script
# Deploys the MongoDB Migration Web-Based Utility to AKS.
# Storage is accessed by the app through Azure Blob Storage using Workload Identity.

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [string]$ClusterName,

    [Parameter(Mandatory=$true)]
    [string]$Location,

    [Parameter(Mandatory=$true)]
    [string]$OwnerTag,

    [Parameter(Mandatory=$false)]
    [string]$AcrName = "",

    [Parameter(Mandatory=$false)]
    [string]$AcrRepository = "",

    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "",

    [Parameter(Mandatory=$false)]
    [string]$StorageAccountResourceId = "",

    [Parameter(Mandatory=$false)]
    [string]$WorkloadIdentityResourceId = "",

    [Parameter(Mandatory=$false)]
    [string]$StateStoreAppID = "",

    [Parameter(Mandatory=$false)]
    [string]$InfrastructureSubnetResourceId = "",

    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",

    [Parameter(Mandatory=$false)]
    [string]$NodeVmSize = "Standard_D16s_v3",

    [Parameter(Mandatory=$false)]
    [string]$ServiceCidr = "10.250.0.0/16",

    [Parameter(Mandatory=$false)]
    [string]$DnsServiceIp = "10.250.0.10",

    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 10)]
    [int]$InstanceCount = 1,

    [Parameter(Mandatory=$false)]
    [string]$Namespace = "mongomigration",

    [Parameter(Mandatory=$false)]
    [string]$KubernetesVersion = ""
)

$ErrorActionPreference = "Stop"

$useProvidedSubnet = -not [string]::IsNullOrEmpty($InfrastructureSubnetResourceId)
$useProvidedStorage = -not [string]::IsNullOrEmpty($StorageAccountResourceId)
$useProvidedWorkloadIdentity = -not [string]::IsNullOrEmpty($WorkloadIdentityResourceId)
$serviceIsPublic = -not $useProvidedSubnet

if ($useProvidedStorage -and -not [string]::IsNullOrEmpty($StorageAccountName)) {
    Write-Host "Error: -StorageAccountResourceId and -StorageAccountName cannot be used together." -ForegroundColor Red
    exit 1
}

# Auto-generate names
$ClusterBase = ($ClusterName -replace '-', '').ToLower()

if ([string]::IsNullOrEmpty($AcrName)) {
    $AcrName = $ClusterBase + 'acr'
    if ($AcrName.Length -gt 50) { $AcrName = $AcrName.Substring(0, 50) }
    Write-Host "Using generated ACR name: $AcrName" -ForegroundColor Cyan
}

if ([string]::IsNullOrEmpty($AcrRepository)) {
    $AcrRepository = $ClusterName
    Write-Host "Using ClusterName as ACR repository: $AcrRepository" -ForegroundColor Cyan
}

if ([string]::IsNullOrEmpty($StateStoreAppID)) {
    $StateStoreAppID = $ClusterName
    Write-Host "Using ClusterName as StateStoreAppID base: $StateStoreAppID" -ForegroundColor Cyan
}

if ($useProvidedStorage) {
    $StorageAccountName = $StorageAccountResourceId.Split('/')[-1]
    Write-Host "Using pre-configured storage account: $StorageAccountName" -ForegroundColor Cyan
} elseif ([string]::IsNullOrEmpty($StorageAccountName)) {
    $StorageAccountName = $ClusterBase + 'stor'
    if ($StorageAccountName.Length -gt 24) { $StorageAccountName = $StorageAccountName.Substring(0, 24) }
    Write-Host "Using generated storage account name: $StorageAccountName" -ForegroundColor Cyan
}

$ManagedIdentityName = "$ClusterName-workload-id"
if ($useProvidedWorkloadIdentity) {
  $ManagedIdentityName = $WorkloadIdentityResourceId.Split('/')[-1]
  Write-Host "Using pre-configured workload identity: $ManagedIdentityName" -ForegroundColor Cyan
}

Write-Host "`n=== AKS Deployment ===" -ForegroundColor Cyan
Write-Host "Resource Group           : $ResourceGroupName" -ForegroundColor White
Write-Host "Cluster                  : $ClusterName" -ForegroundColor White
Write-Host "Location                 : $Location" -ForegroundColor White
Write-Host "ACR                      : $AcrName" -ForegroundColor White
Write-Host "ACR Repository           : $AcrRepository" -ForegroundColor White
Write-Host "Storage Account          : $StorageAccountName" -ForegroundColor White
Write-Host "Storage Source           : $(if ($useProvidedStorage) {'Provided resource ID'} else {'Script-created'})" -ForegroundColor White
Write-Host "Workload Identity        : $ManagedIdentityName" -ForegroundColor White
Write-Host "Workload Identity Source : $(if ($useProvidedWorkloadIdentity) {'Provided resource ID'} else {'Script-created'})" -ForegroundColor White
Write-Host "Subnet Mode              : $(if ($useProvidedSubnet) {'BYO subnet'} else {'AKS default networking'})" -ForegroundColor White
Write-Host "Service Exposure         : $(if ($serviceIsPublic) {'Public LoadBalancer'} else {'Internal LoadBalancer'})" -ForegroundColor White
Write-Host "Node VM Size             : $NodeVmSize" -ForegroundColor White
Write-Host "AKS Service CIDR         : $ServiceCidr" -ForegroundColor White
Write-Host "AKS DNS Service IP       : $DnsServiceIp" -ForegroundColor White
Write-Host "Instance Count           : $InstanceCount" -ForegroundColor White
Write-Host ""

# Step 1: Azure infra via Bicep
$infraTemplatePath = Join-Path $PSScriptRoot "infra/main.bicep"
if (-not (Test-Path $infraTemplatePath)) {
    Write-Host "Error: Bicep template not found at '$infraTemplatePath'." -ForegroundColor Red
    exit 1
}

Write-Host "Step 1: Deploying Azure infra via Bicep..." -ForegroundColor Yellow

$infraDeploymentName = "$ClusterName-infra"
$infraCreateArgs = @(
    "deployment", "group", "create",
    "--name", $infraDeploymentName,
    "--resource-group", $ResourceGroupName,
    "--template-file", $infraTemplatePath,
    "--parameters",
        "location=$Location",
        "ownerTag=$OwnerTag",
        "clusterName=$ClusterName",
        "acrName=$AcrName",
        "storageAccountName=$StorageAccountName",
        "storageAccountResourceId=$StorageAccountResourceId",
        "workloadIdentityName=$ManagedIdentityName",
        "workloadIdentityResourceId=$WorkloadIdentityResourceId"
)

$infraJson = az @infraCreateArgs --query "properties.outputs" --output json
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($infraJson)) {
    Write-Host "Error: Failed to deploy Azure infra via Bicep" -ForegroundColor Red
    exit 1
}

$infraOutputs = $infraJson | ConvertFrom-Json
$AcrName = $infraOutputs.acrNameResolved.value
$acrLoginServer = $infraOutputs.acrLoginServer.value
$StorageAccountName = $infraOutputs.storageAccountNameResolved.value
$storageResourceId = $infraOutputs.storageAccountResourceIdResolved.value
$uamiClientId = $infraOutputs.workloadIdentityClientId.value
$uamiPrincipalId = $infraOutputs.workloadIdentityPrincipalId.value
$managedIdentityResourceId = $infraOutputs.workloadIdentityResourceIdResolved.value
$ManagedIdentityName = $infraOutputs.workloadIdentityNameResolved.value

Write-Host "  ACR               : $AcrName" -ForegroundColor Green
Write-Host "  ACR Login Server  : $acrLoginServer" -ForegroundColor Green
Write-Host "  Storage Account   : $StorageAccountName" -ForegroundColor Green
Write-Host "  Managed Identity  : $ManagedIdentityName" -ForegroundColor Green

# Step 2: Build / push image
Write-Host "`nStep 2: Checking if Docker image exists in ACR..." -ForegroundColor Yellow

$ErrorActionPreference = 'Continue'
$imageExists = $false
try {
    $tags = az acr repository show-tags --name $AcrName --repository $AcrRepository --output json 2>&1 | Where-Object { $_ -notmatch 'WARNING' -and $_ -notmatch 'not found' }
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
    az acr build --registry $AcrName --resource-group $ResourceGroupName --image "$($AcrRepository):$($ImageTag)" --file ../MongoMigrationWebApp/Dockerfile ..
    if ($LASTEXITCODE -ne 0) { Write-Host "Error: Failed to build Docker image" -ForegroundColor Red; exit 1 }
    Write-Host "  Docker image built and pushed successfully." -ForegroundColor Green
}

# Step 3: Networking mode
Write-Host "`nStep 3: Resolving AKS networking mode..." -ForegroundColor Yellow

$aksSubnetId = ""
$internalLbSubnetName = ""
if ($useProvidedSubnet) {
  $ErrorActionPreference = 'Continue'
  $aksSubnetId = az network vnet subnet show --ids $InfrastructureSubnetResourceId --query "id" --output tsv 2>$null
  $internalLbSubnetName = az network vnet subnet show --ids $InfrastructureSubnetResourceId --query "name" --output tsv 2>$null
  $ErrorActionPreference = 'Stop'

  if (-not $aksSubnetId) {
    Write-Host "Error: InfrastructureSubnetResourceId is invalid or not accessible." -ForegroundColor Red
    exit 1
  }
  Write-Host "  Using provided subnet: $aksSubnetId" -ForegroundColor Green
} else {
  Write-Host "  No subnet provided. AKS will use default networking (no custom VNet/subnet)." -ForegroundColor Green
}

# Step 4: Storage account
Write-Host "`nStep 4: Using storage account from Bicep outputs..." -ForegroundColor Yellow
Write-Host "  Storage account '$StorageAccountName' is ready." -ForegroundColor Green

# Step 5: AKS cluster
Write-Host "`nStep 5: Ensuring AKS cluster exists..." -ForegroundColor Yellow

$ErrorActionPreference = 'Continue'
$aksExists = az aks show --name $ClusterName --resource-group $ResourceGroupName --query "name" --output tsv 2>$null
$ErrorActionPreference = 'Stop'

if ($aksExists) {
    Write-Host "  AKS cluster '$ClusterName' already exists. Skipping creation." -ForegroundColor Green
} else {
    Write-Host "  Creating AKS cluster '$ClusterName' (this may take 5-10 minutes)..." -ForegroundColor Gray

    $aksCreateArgs = @(
        "aks", "create",
        "--name", $ClusterName,
        "--resource-group", $ResourceGroupName,
        "--location", $Location,
        "--node-vm-size", $NodeVmSize,
        "--node-count", "1",
        "--enable-managed-identity",
        "--generate-ssh-keys",
        "--enable-aad",
        "--enable-oidc-issuer",
        "--enable-workload-identity",
        "--enable-azure-rbac",
        "--network-plugin", "azure",
        "--service-cidr", $ServiceCidr,
        "--dns-service-ip", $DnsServiceIp,
        "--tags", "owner=$OwnerTag"
    )

    if (-not [string]::IsNullOrEmpty($KubernetesVersion)) {
        $aksCreateArgs += "--kubernetes-version"
        $aksCreateArgs += $KubernetesVersion
    }

    if ($useProvidedSubnet) {
      $aksCreateArgs += "--vnet-subnet-id"
      $aksCreateArgs += $aksSubnetId
    }

    az @aksCreateArgs
    if ($LASTEXITCODE -ne 0) { Write-Host "Error: Failed to create AKS cluster" -ForegroundColor Red; exit 1 }
    Write-Host "  AKS cluster created." -ForegroundColor Green
}

# Attach ACR to AKS if needed
$kubeletObjectId = az aks show --name $ClusterName --resource-group $ResourceGroupName --query "identityProfile.kubeletidentity.objectId" --output tsv
$acrResourceId = az acr show --name $AcrName --resource-group $ResourceGroupName --query "id" --output tsv

$ErrorActionPreference = 'Continue'
$acrPullAssignment = az role assignment list --assignee-object-id $kubeletObjectId --scope $acrResourceId --query "[?roleDefinitionName=='AcrPull'] | [0].id" --output tsv 2>$null
$ErrorActionPreference = 'Stop'

if ($acrPullAssignment) {
    Write-Host "  ACR already attached to AKS (AcrPull assignment exists)." -ForegroundColor Green
} else {
    Write-Host "  Attaching ACR to AKS cluster..." -ForegroundColor Gray
    az aks update --name $ClusterName --resource-group $ResourceGroupName --attach-acr $AcrName | Out-Null
    Write-Host "  ACR attached to AKS." -ForegroundColor Green
}

# Step 6: Workload identity
Write-Host "`nStep 6: Setting up Workload Identity for Azure Blob access..." -ForegroundColor Yellow

$oidcIssuer = az aks show --name $ClusterName --resource-group $ResourceGroupName --query "oidcIssuerProfile.issuerUrl" --output tsv
$federatedCredName = "$ClusterName-fedcred"
$k8sSaName = "$ClusterName-workload-sa"
$identityResourceGroupName = $ResourceGroupName

if (-not [string]::IsNullOrEmpty($managedIdentityResourceId)) {
  $miParts = $managedIdentityResourceId.Split('/')
  if ($miParts.Length -ge 5) {
    $identityResourceGroupName = $miParts[4]
  }
}

$ErrorActionPreference = 'Continue'
$fedExists = az identity federated-credential list --identity-name $ManagedIdentityName --resource-group $identityResourceGroupName --query "[?name=='$federatedCredName'] | [0].name" --output tsv 2>$null
$ErrorActionPreference = 'Stop'

if (-not $fedExists) {
  az identity federated-credential create --name $federatedCredName --identity-name $ManagedIdentityName --resource-group $identityResourceGroupName --issuer $oidcIssuer --subject "system:serviceaccount:${Namespace}:${k8sSaName}" --audience "api://AzureADTokenExchange" | Out-Null
}
Write-Host "  Federated credential ensured for workload identity '$ManagedIdentityName'." -ForegroundColor Green

# Step 7: kubectl context
Write-Host "`nStep 7: Fetching kubectl credentials..." -ForegroundColor Yellow
az aks get-credentials --name $ClusterName --resource-group $ResourceGroupName --overwrite-existing | Out-Null

if (-not (Get-Command kubelogin -ErrorAction SilentlyContinue)) {
    $kubeloginDefaultPath = Join-Path $env:USERPROFILE ".azure-kubelogin"
    if (Test-Path (Join-Path $kubeloginDefaultPath "kubelogin.exe")) {
        $env:PATH = "$kubeloginDefaultPath;$env:PATH"
    }
}

if (Get-Command kubelogin -ErrorAction SilentlyContinue) {
    kubelogin convert-kubeconfig -l azurecli | Out-Null
} else {
    Write-Host "Error: kubelogin is required for this AAD-enabled AKS cluster. Install it with 'az aks install-cli' and rerun." -ForegroundColor Red
    exit 1
}

$ErrorActionPreference = 'Continue'
kubectl auth can-i get namespaces 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    az aks get-credentials --name $ClusterName --resource-group $ResourceGroupName --overwrite-existing --admin | Out-Null
}
$ErrorActionPreference = 'Stop'
Write-Host "  kubectl context updated." -ForegroundColor Green

# Step 8: Namespace
Write-Host "`nStep 8: Ensuring namespace '$Namespace'..." -ForegroundColor Yellow
$ErrorActionPreference = 'Continue'
kubectl get namespace $Namespace 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    kubectl create namespace $Namespace | Out-Null
}
$ErrorActionPreference = 'Stop'

# Step 9: Service account
Write-Host "`nStep 9: Ensuring ServiceAccount for Workload Identity..." -ForegroundColor Yellow
$saYaml = @"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $k8sSaName
  namespace: $Namespace
  annotations:
    azure.workload.identity/client-id: "$uamiClientId"
  labels:
    azure.workload.identity/use: "true"
"@
$saYaml | kubectl apply -f - | Out-Null

# Step 10: StateStore connection string
Write-Host "`nStep 10: Prompting for StateStore connection string..." -ForegroundColor Yellow
$secureConnString = Read-Host -Prompt "The StateStore keeps track of migration job details in a DocumentDB. You may use the same database as the Target DocumentDB or a separate one. Enter the connection string for the StateStore." -AsSecureString
$connString = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureConnString))

# Step 10b: Encryption key seed (optional, entered securely so it is not shown on screen)
Write-Host "`nStep 10b: Prompting for encryption key seed (optional)..." -ForegroundColor Yellow
$secureSeed = Read-Host -Prompt "Enter the encryption key for the application. Press Enter to keep the default or existing." -AsSecureString
$encryptionKeySeed = ""
if ($secureSeed -and $secureSeed.Length -gt 0) {
    $encryptionKeySeed = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureSeed))
}

# Step 11: Deploy instances
Write-Host "`nStep 11: Deploying $InstanceCount instance(s)..." -ForegroundColor Yellow
$imageName = "$AcrName.azurecr.io/$($AcrRepository):$($ImageTag)"

for ($i = 1; $i -le $InstanceCount; $i++) {
    $instanceName = "$ClusterName-$i"
    $containerName = "$ClusterName-instance-$i"
    $containerName = $containerName.ToLower() -replace '[^a-z0-9-]', '-'
    if ($containerName.Length -gt 63) { $containerName = $containerName.Substring(0, 63) }
    $instanceSsId = "$StateStoreAppID-$i"
    $connSecretName = "$instanceName-connstr"

    Write-Host "  Deploying instance $instanceName" -ForegroundColor Cyan

    $ErrorActionPreference = 'Continue'
    az storage container create --name $containerName --account-name $StorageAccountName --auth-mode login --public-access off 2>$null | Out-Null
    $ErrorActionPreference = 'Stop'

    kubectl create secret generic $connSecretName --namespace $Namespace --from-literal=connectionstring="$connString" --dry-run=client -o yaml | kubectl apply -f - | Out-Null

    # Add the per-install encryption key seed to the instance secret when supplied.
    # When omitted, the app keeps using its legacy built-in seed for backward compatibility.
    $encryptionKeySeedEnv = ""
    if (-not [string]::IsNullOrEmpty($encryptionKeySeed)) {
        $seedSecretName = "$instanceName-keyseed"
        kubectl create secret generic $seedSecretName --namespace $Namespace --from-literal=encryptionkeyseed="$encryptionKeySeed" --dry-run=client -o yaml | kubectl apply -f - | Out-Null
        $encryptionKeySeedEnv = @"
        - name: EncryptionKeySeed
          valueFrom:
            secretKeyRef:
              name: $seedSecretName
              key: encryptionkeyseed
"@
    }

  $serviceAnnotations = ""
  if (-not $serviceIsPublic) {
    $serviceAnnotations = @"
  annotations:
    service.beta.kubernetes.io/azure-load-balancer-internal: "true"
"@
    if (-not [string]::IsNullOrEmpty($internalLbSubnetName)) {
      $serviceAnnotations += "`n    service.beta.kubernetes.io/azure-load-balancer-internal-subnet: `"$internalLbSubnetName`""
    }
  }

    $deployYaml = @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $instanceName
  namespace: $Namespace
  labels:
    app: $instanceName
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $instanceName
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: $instanceName
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: $k8sSaName
      containers:
      - name: mongomigration
        image: $imageName
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
          protocol: TCP
        env:
        - name: ASPNETCORE_ENVIRONMENT
          value: "Development"
        - name: ASPNETCORE_URLS
          value: "http://+:8080"
        - name: ResourceDrive
          value: "/app/migration-data"
        - name: UseBlobServiceClient
          value: "true"
        - name: BlobServiceClientURI
          value: "https://$StorageAccountName.blob.core.windows.net"
        - name: BlobContainerName
          value: "$containerName"
        - name: StateStoreAppID
          value: "$instanceSsId"
        - name: StateStoreConnectionStringOrPath
          valueFrom:
            secretKeyRef:
              name: $connSecretName
              key: connectionstring
        - name: AZURE_CLIENT_ID
          value: "$uamiClientId"
$encryptionKeySeedEnv
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "16"
            memory: "64Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: $instanceName
  namespace: $Namespace
  labels:
    app: $instanceName
$serviceAnnotations
spec:
  type: LoadBalancer
  selector:
    app: $instanceName
  ports:
  - name: http
    port: 80
    targetPort: 8080
    protocol: TCP
"@

    $deployYaml | kubectl apply -f - | Out-Null
}

Remove-Variable connString, secureConnString, encryptionKeySeed, secureSeed -ErrorAction Ignore

# Step 12: Wait for endpoints
Write-Host "`nStep 12: Waiting for service endpoint IPs..." -ForegroundColor Yellow

function Test-IsPrivateIPv4 {
  param([string]$IpAddress)

  if ([string]::IsNullOrWhiteSpace($IpAddress)) { return $false }

  $bytes = $null
  if (-not [System.Net.IPAddress]::TryParse($IpAddress, [ref]$bytes)) {
    return $false
  }

  if ($bytes.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetwork) {
    return $false
  }

  $octets = $bytes.GetAddressBytes()
  if ($octets[0] -eq 10) { return $true }
  if ($octets[0] -eq 172 -and $octets[1] -ge 16 -and $octets[1] -le 31) { return $true }
  if ($octets[0] -eq 192 -and $octets[1] -eq 168) { return $true }
  return $false
}

$urls = @()
for ($i = 1; $i -le $InstanceCount; $i++) {
    $instanceName = "$ClusterName-$i"
  $serviceIp = $null
  $ingressIp = $null

  $ErrorActionPreference = 'Continue'
  $serviceIp = kubectl get service $instanceName --namespace $Namespace --output jsonpath='{.spec.clusterIP}' 2>$null
  $ErrorActionPreference = 'Stop'

  for ($attempt = 1; $attempt -le 36 -and -not $ingressIp; $attempt++) {
        Start-Sleep -Seconds 5
        $ErrorActionPreference = 'Continue'
    $ingressIp = kubectl get service $instanceName --namespace $Namespace --output jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>$null
        $ErrorActionPreference = 'Stop'
    if ($ingressIp -eq '<pending>' -or [string]::IsNullOrEmpty($ingressIp)) { $ingressIp = $null }
    }

  if ($ingressIp) {
    $url = "http://$ingressIp"
    $isPrivateIngress = Test-IsPrivateIPv4 -IpAddress $ingressIp
    $endpointType = if ($isPrivateIngress) { 'Private LoadBalancer endpoint' } else { 'Public LoadBalancer endpoint' }
    $urls += [PSCustomObject]@{
      Instance = $instanceName
      URL = $url
      ServiceIP = $serviceIp
      EndpointIP = $ingressIp
      EndpointType = $endpointType
      IsPrivateIngress = $isPrivateIngress
    }
    } else {
    $urls += [PSCustomObject]@{
      Instance = $instanceName
      URL = "(pending)"
      ServiceIP = $serviceIp
      EndpointIP = "(pending)"
      EndpointType = "(pending)"
      IsPrivateIngress = $false
    }
    }
}

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan
Write-Host "Access mode: $(if ($serviceIsPublic) {'Public'} else {'VNet/Internal only'})" -ForegroundColor White
foreach ($entry in $urls) {
    if ($entry.URL -ne "(pending)") {
    Write-Host "  $($entry.Instance) -> $($entry.URL) [$($entry.EndpointType)]" -ForegroundColor Green
    Write-Host "    Service IP: $($entry.ServiceIP) | LoadBalancer Ingress: $($entry.EndpointIP)" -ForegroundColor Gray
    if ($serviceIsPublic -and $entry.IsPrivateIngress) {
      Write-Host "    Warning: expected public endpoint but ingress IP is private. Run: kubectl get svc $($entry.Instance) -n $Namespace -o wide" -ForegroundColor Yellow
    }
    if ((-not $serviceIsPublic) -and (-not $entry.IsPrivateIngress)) {
      Write-Host "    Warning: expected internal endpoint but ingress IP appears public. Run: kubectl get svc $($entry.Instance) -n $Namespace -o wide" -ForegroundColor Yellow
    }
    } else {
    Write-Host "  $($entry.Instance) -> (pending - run: kubectl get svc -n $Namespace -o wide)" -ForegroundColor Yellow
    }
}

