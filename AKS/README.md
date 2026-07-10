# Deploy to Azure Kubernetes Service (AKS)

This guide explains how to deploy the MongoDB Migration Web-Based Utility to AKS with **direct HTTP access per instance**. Each instance runs as a separate Kubernetes Deployment with its own LoadBalancer Service.

The script supports two networking modes:

- Private access mode (recommended for enterprise/internal use):
  If you provide `InfrastructureSubnetResourceId`, the app is exposed through an internal LoadBalancer.
  This means the app is reachable only from inside your private network (VNet/corporate network), not from the public internet.

- Public access mode (recommended for quick testing):
  If you do not provide `InfrastructureSubnetResourceId`, AKS uses default networking and creates a public LoadBalancer.
  This means the app gets a public IP and can be reached from the internet (subject to your NSG/firewall rules).

Storage is accessed by the app via Azure Blob SDK + Workload Identity.

The repeatable Azure resource layer is defined in Bicep at [AKS/infra/main.bicep](AKS/infra/main.bicep). The PowerShell deployment script calls that template, so you can rerun infrastructure deployment directly with `az deployment group create` to repair or recreate Azure-side resources.

## Deployment Scripts Overview

Three PowerShell scripts are provided:

1. `deploy-to-aks.ps1` - Full initial deployment
- Deploys the Bicep infra template that creates or reuses ACR, storage, and workload identity resources
- Uses provided AKS subnet when `InfrastructureSubnetResourceId` is passed
- Uses AKS default networking when no subnet is passed
- Sets up Workload Identity (UAMI + federated credential) for Azure Blob
- Creates one Blob container, Kubernetes Deployment, and LoadBalancer Service per instance
- Supports existing storage account via `StorageAccountResourceId`
- Prompts securely for DocumentDB connection string
- Use for first-time deployment or when adding instances

2. `publish-image-to-acr.ps1` - Image publish only
- Builds and pushes the Docker image to a provided ACR
- Does not touch AKS resources, pods, secrets, or services
- Use when you only need a refreshed image in ACR

3. `update-aks-app.ps1` - In-cluster image update
- Rebuilds (or reuses) the Docker image in ACR
- Updates container image on selected instances
- Preserves secrets, volumes, and infrastructure
- Use for deploying code changes when the AKS workload exists

## Prerequisites

- Azure CLI installed and logged in (`az login`)
- `kubectl` installed
- Azure subscription permissions to create AKS, ACR, and Storage resources
- Resource group created: `az group create -n <rg-name> -l <location>`
- Azure DocumentDB account for state storage

### Register Required Providers

```powershell
az provider register --namespace Microsoft.ContainerService
az provider register --namespace Microsoft.ContainerRegistry
az provider register --namespace Microsoft.Storage
```

## Full Deployment Example (All Options)

The command below shows all currently supported parameters.
Parameters marked with `# Optional` are not required.

```powershell
.\deploy-to-aks.ps1 `
  -ResourceGroupName "<resource-group-name>" `
  -ClusterName "<cluster-name>" `
  -Location "<azure-region>" `
  -OwnerTag "<owner-alias-or-email>" `
  -AcrName "<acr-name>" ` # Optional
  -AcrRepository "<acr-repository>" ` # Optional
  -StorageAccountName "<storage-account-name>" ` # Optional (use only when StorageAccountResourceId is not provided)
  -StorageAccountResourceId "/subscriptions/<subscription-id>/resourceGroups/<resource-group-name>/providers/Microsoft.Storage/storageAccounts/<storage-account-name>" ` # Optional
  -WorkloadIdentityResourceId "/subscriptions/<subscription-id>/resourceGroups/<resource-group-name>/providers/Microsoft.ManagedIdentity/userAssignedIdentities/<uami-name>" ` # Optional
  -StateStoreAppID "<state-store-app-id-base>" ` # Optional (reuse this value across redeployments to continue viewing existing jobs/logs)
  -InfrastructureSubnetResourceId "/subscriptions/<subscription-id>/resourceGroups/<resource-group-name>/providers/Microsoft.Network/virtualNetworks/<vnet-name>/subnets/<subnet-name>" ` # Optional
  -ImageTag "latest" ` # Optional
  -NodeVmSize "<node-vm-size>" ` # Optional
  -ServiceCidr "10.250.0.0/16" ` # Optional
  -DnsServiceIp "10.250.0.10" ` # Optional
  -InstanceCount 2 ` # Optional
  -Namespace "<kubernetes-namespace>" ` # Optional
  -KubernetesVersion "<kubernetes-version>" # Optional
```

Notes:
- Do not pass both `-StorageAccountName` and `-StorageAccountResourceId` together.
- BYO subnet mode is enabled only when `-InfrastructureSubnetResourceId` is provided.
- Service exposure is based on subnet mode: BYO subnet => internal endpoint, no subnet => public endpoint.
- Endpoints are exposed through `type: LoadBalancer`. Use Service `EXTERNAL-IP` / `LoadBalancer Ingress` as the endpoint, not Service `IP`.
- Avoid CIDR overlap: `-ServiceCidr` must not overlap any VNet or subnet CIDR used by AKS (for example, if subnet is `10.0.0.0/24`, do not use `10.0.0.0/16`).
- `-DnsServiceIp` must be an IP inside `-ServiceCidr` and must not be the network/broadcast address.

The script will:
1. Create or reuse ACR
2. Build and push Docker image (skip build if the image tag exists)
3. Create or reuse storage account + one Blob container per instance
4. Create or reuse AKS cluster (OIDC + Workload Identity enabled), using either provided subnet or default networking
5. Attach ACR to AKS
6. Create UAMI + federated credential for Azure Blob access
7. Assign `Storage Blob Data Contributor` on storage account
8. Prompt for StateStore connection string
9. Deploy one Deployment + LoadBalancer Service per instance with blob env vars:
  - `UseBlobServiceClient=true`
  - `BlobServiceClientURI=https://<storage>.blob.core.windows.net`
  - `BlobContainerName=<cluster>-instance-<n>`
10. Display HTTP URLs (`http://<public-ip>`) for each instance

## Parameters Reference

### deploy-to-aks.ps1

#### Required

| Parameter | Description |
|---|---|
| `ResourceGroupName` | Azure resource group |
| `ClusterName` | AKS cluster name (base for generated names) |
| `Location` | Azure region |
| `OwnerTag` | Owner tag for created resources |

#### Optional

| Parameter | Type | Default | Description |
|---|---|---|---|
| `AcrName` | string | `<ClusterName>acr` | ACR name |
| `AcrRepository` | string | `<ClusterName>` | ACR repository |
| `StorageAccountName` | string | `<ClusterName>stor` | Storage account name (only when StorageAccountResourceId not provided) |
| `StorageAccountResourceId` | string | `""` | Existing storage account resource ID |
| `WorkloadIdentityResourceId` | string | `""` | Existing user-assigned managed identity resource ID |
| `StateStoreAppID` | string | `<ClusterName>` | App identity key used by StateStore (`-1`, `-2`, ... per instance). Reuse the same base value across redeployments to continue viewing existing jobs and logs. |
| `InfrastructureSubnetResourceId` | string | `""` | Existing subnet resource ID for BYO subnet mode |
| `ImageTag` | string | `latest` | Docker image tag |
| `NodeVmSize` | string | `Standard_D16s_v3` | AKS node size |
| `ServiceCidr` | string | `10.250.0.0/16` | AKS service CIDR. Must not overlap VNet/subnet CIDRs |
| `DnsServiceIp` | string | `10.250.0.10` | AKS DNS service IP (must be inside ServiceCidr) |
| `InstanceCount` | int | `1` | Number of instances to deploy |
| `Namespace` | string | `mongomigration` | Kubernetes namespace |
| `KubernetesVersion` | string | latest | Optional AKS version |

> **Encryption key seed:** The script securely prompts (input hidden, not shown on screen) for a per-install secret seed used to derive the AES-256 key that encrypts the stored app password. It is stored as a Kubernetes secret and passed to the container via the `EncryptionKeySeed` environment variable. Press Enter at the prompt to keep the legacy built-in seed (not recommended for production).

CIDR guidance:
- Check the subnet CIDR before deployment when using BYO subnet (`InfrastructureSubnetResourceId`).
- Pick a non-overlapping service CIDR range such as `10.250.0.0/16` or `172.31.0.0/16`.
- Keep `DnsServiceIp` within that service CIDR (for example `10.250.0.10`).

### update-aks-app.ps1

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ResourceGroupName` | string | required | Azure resource group |
| `ClusterName` | string | required | AKS cluster name |
| `AcrName` | string | required | ACR name |
| `AcrRepository` | string | `<ClusterName>` | ACR repository |
| `ImageTag` | string | `latest` | Image tag |
| `InstanceName` | string | `""` | Update one specific instance |
| `InstanceCount` | int | `1` | Update instances `1..N` |
| `Namespace` | string | `mongomigration` | Kubernetes namespace |

### publish-image-to-acr.ps1

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ResourceGroupName` | string | required | Resource group that contains the ACR |
| `AcrName` | string | required | Azure Container Registry name |
| `AcrRepository` | string | `mongomigration` | Image repository name inside ACR |
| `ImageTag` | string | `latest` | Image tag to publish |
| `DockerfilePath` | string | `../MongoMigrationWebApp/Dockerfile` | Dockerfile used by ACR build |
| `BuildContextPath` | string | `..` | Build context passed to ACR build |

## Architecture

```text
AKS Cluster
└── Namespace: mongomigration
    ├── ServiceAccount: <cluster>-workload-sa (Workload Identity)
    ├── Instance 1
    │   ├── Deployment: <cluster>-1
    │   ├── Service:    <cluster>-1 (LoadBalancer -> public IP #1)
  │   ├── Blob container: <cluster>-instance-1
    │   └── Secret:     <cluster>-1-connstr
    ├── Instance 2
    │   ├── Deployment: <cluster>-2
    │   ├── Service:    <cluster>-2 (LoadBalancer -> public IP #2)
  │   ├── Blob container: <cluster>-instance-2
    │   └── Secret:     <cluster>-2-connstr
    └── ...
```

## Storage: Azure Blob + Workload Identity

The deployment configures:
1. User-Assigned Managed Identity (UAMI)
2. Federated credential to Kubernetes ServiceAccount
3. `Storage Blob Data Contributor` role assignment on storage account
4. ServiceAccount annotation with UAMI client ID
Storage account is created with shared key access disabled (`allowSharedKeyAccess=false`) when script-created.

## Exposure Rules

- BYO subnet mode (`InfrastructureSubnetResourceId` set): internal LoadBalancer endpoint.
- No subnet provided: public LoadBalancer endpoint.
- Use `EXTERNAL-IP` (or `LoadBalancer Ingress`) as the endpoint.
- Service `IP` is internal cluster networking and is not an internet endpoint.

## Useful kubectl Commands

```powershell
az aks get-credentials --name <cluster> --resource-group <rg>
kubectl get all -n mongomigration
kubectl get svc -n mongomigration
kubectl logs -n mongomigration deployment/<cluster>-1 --follow
kubectl describe pod -n mongomigration -l app=<cluster>-1
kubectl rollout restart deployment/<cluster>-1 -n mongomigration
kubectl rollout status deployment/<cluster>-1 -n mongomigration
```

## Publish to ACR and Deploy Container to Existing AKS

Use this flow when you manage AKS and need to:
1. Publish this app image to their ACR.
2. Deploy (or update) one AKS container instance with required secrets and environment variables.

### 1) Publish code to ACR

```powershell
cd .\AKS

.\publish-image-to-acr.ps1 `
  -ResourceGroupName "<resource-group-name>" `
  -AcrName "<acr-name>" `
  -AcrRepository "<acr-repository>" `
  -ImageTag "latest"
```

### 2) Ensure AKS can pull from ACR (one-time)

```powershell
az aks update --name <cluster-name> --resource-group <resource-group-name> --attach-acr <acr-name>
```

### 3) Create/update secret and service account

```powershell
$resourceGroup = "<resource-group-name>"
$clusterName = "<cluster-name>"
$namespace = "mongomigration"
$deploymentName = "<instance-name>"            # Example: mycluster-1
$serviceAccountName = "$clusterName-workload-sa"
$uamiClientId = "<managed-identity-client-id>"

az aks get-credentials --name $clusterName --resource-group $resourceGroup --overwrite-existing
kubectl create namespace $namespace --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic "$deploymentName-connstr" `
  --namespace $namespace `
  --from-literal=connectionstring="<STATE_STORE_CONNECTION_STRING>" `
  --dry-run=client -o yaml | kubectl apply -f -

@"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $serviceAccountName
  namespace: $namespace
  annotations:
    azure.workload.identity/client-id: "$uamiClientId"
  labels:
    azure.workload.identity/use: "true"
"@ | kubectl apply -f -
```

### 4) Deploy/update the container image with required env vars

```powershell
$acrName = "<acr-name>"
$acrRepository = "<acr-repository>"
$imageTag = "latest"
$storageAccountName = "<storage-account-name>"
$blobContainerName = "<blob-container-name>"
$stateStoreAppId = "<state-store-app-id>"

@"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $deploymentName
  namespace: $namespace
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $deploymentName
  template:
    metadata:
      labels:
        app: $deploymentName
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: $serviceAccountName
      containers:
      - name: mongomigration
        image: $acrName.azurecr.io/${acrRepository}:$imageTag
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
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
          value: "https://$storageAccountName.blob.core.windows.net"
        - name: BlobContainerName
          value: "$blobContainerName"
        - name: StateStoreAppID
          value: "$stateStoreAppId"
        - name: StateStoreConnectionStringOrPath
          valueFrom:
            secretKeyRef:
              name: $deploymentName-connstr
              key: connectionstring
        - name: AZURE_CLIENT_ID
          value: "$uamiClientId"
---
apiVersion: v1
kind: Service
metadata:
  name: $deploymentName
  namespace: $namespace
spec:
  type: LoadBalancer
  selector:
    app: $deploymentName
  ports:
  - name: http
    port: 80
    targetPort: 8080
"@ | kubectl apply -f -

kubectl rollout status deployment/$deploymentName -n $namespace
kubectl get svc $deploymentName -n $namespace -o wide
```

Required runtime values:
- `StateStoreConnectionStringOrPath` from secret `$deploymentName-connstr`
- `UseBlobServiceClient=true`
- `BlobServiceClientURI=https://<storage-account>.blob.core.windows.net`
- `BlobContainerName=<blob-container-name>`
- `StateStoreAppID=<state-store-app-id>`
- `AZURE_CLIENT_ID=<managed-identity-client-id>`

`StateStoreAppID` guidance:
- `StateStoreAppID` identifies the app instance in StateStore.
- If you deploy again and want to continue viewing the same jobs and logs, use the same `StateStoreAppID` values as before.
- If you use a different `StateStoreAppID`, the deployment is treated as a different app identity in StateStore.

## Troubleshooting

### Pod stuck in Pending

```powershell
kubectl describe pod -n mongomigration -l app=<instance-name>
```

Common causes: insufficient node resources, image pull failure, blob auth issues, or network policy restrictions.

### Portal shows Forbidden (403) for namespaces

If the Azure portal Kubernetes view shows a 403 for namespace listing, grant an AKS Azure RBAC role at the managed cluster scope.

Grant read-only access for portal browsing:

```powershell
az role assignment create --assignee <user-upn> --role "Azure Kubernetes Service RBAC Reader" --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group-name>/providers/Microsoft.ContainerService/managedClusters/<cluster-name>"
```

If portal users also need to deploy/manage workloads, grant higher access:

```powershell
az role assignment create --assignee <user-upn> --role "Azure Kubernetes Service RBAC Cluster Admin" --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group-name>/providers/Microsoft.ContainerService/managedClusters/<cluster-name>"
```

After role assignment, wait a few minutes for RBAC propagation, then refresh the portal.

### Blob access errors

Verify these env vars on the deployment:

```powershell
kubectl get deploy <cluster>-1 -n mongomigration -o jsonpath='{.spec.template.spec.containers[0].env}'
```

Check UAMI role assignment:

```powershell
az role assignment list --assignee <uami-principal-id> --scope <storage-account-resource-id> -o table
```

### ImagePullBackOff

```powershell
az aks update --name <cluster> --resource-group <rg> --attach-acr <acr-name>
```

## Optional: Add HTTPS Later

Default deployment is HTTP by design. If you want HTTPS later, use one of these patterns.

### Option A: Keep current HTTP deployment, add ingress + TLS later

1. Install ingress-nginx in the cluster.
2. Change app Services from `LoadBalancer` to `ClusterIP`.
3. Create a TLS secret from your certificate.
4. Create an Ingress resource with HTTPS and path/host rules.
5. Point DNS to ingress public IP.

Example TLS secret:

```powershell
kubectl create secret tls mongomigration-ingress-tls `
  -n mongomigration `
  --cert <path-to-cert.crt> `
  --key <path-to-key.key>
```

### Option B: Keep public IP per instance and terminate TLS in-app

1. Configure app/Kestrel for HTTPS on port 443.
2. Provide cert/key per instance.
3. Expose service port 443 on each LoadBalancer.

This is possible but usually harder to operate than ingress-based TLS.

