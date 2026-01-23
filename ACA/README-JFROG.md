# Azure Container Apps Deployment with JFrog Artifactory

This guide explains how to deploy the MongoDB Migration Web-Based Utility to Azure Container Apps using JFrog Artifactory as the container registry instead of Azure Container Registry (ACR).

## Overview

The JFrog deployment differs from the standard ACR deployment in the following ways:

| Aspect | ACR Deployment | JFrog Deployment |
|--------|----------------|------------------|
| Base Images | Pulled from `mcr.microsoft.com` | Pulled from JFrog (mirrored/proxied) |
| Build Location | Azure ACR Tasks (cloud) | Local Docker (your machine) |
| Image Storage | Azure Container Registry | JFrog Artifactory |
| Authentication | Managed Identity | Username/Password or API Key |

## Prerequisites

### 1. JFrog Artifactory Setup

Ensure you have JFrog Artifactory configured with:

1. **Docker Repository** - Either:
   - A **local repository** (e.g., `docker-local`) for pushing your built images
   - A **remote repository** (e.g., `docker-remote`) that proxies `mcr.microsoft.com` for base images
   - A **virtual repository** (e.g., `docker-virtual`) that combines local and remote

2. **.NET 9.0 Base Images Available** via one of:
   - Remote repository proxying Microsoft Container Registry (MCR)
   - Manual upload of `dotnet/sdk:9.0` and `dotnet/aspnet:9.0`
   - Virtual repository that resolves from remote proxy

3. **Authentication Credentials**:
   - JFrog username
   - JFrog password or API key (preferred for automation)

### 2. Local Requirements

- **Docker Desktop** or Docker CLI installed and running
- **Azure CLI** installed and logged in (`az login`)
- Network access to your JFrog Artifactory instance

## Files

| File | Description |
|------|-------------|
| `Dockerfile.jfrog` | Dockerfile with parameterized JFrog base images |
| `aca_main_jfrog.bicep` | Bicep template for ACA with JFrog registry config |
| `deploy-to-aca-jfrog.ps1` | Full deployment script (infrastructure + app) |
| `update-aca-app-jfrog.ps1` | Update script (app only, preserves config) |

## JFrog Repository Structure

Recommended JFrog structure:

```
yourcompany.jfrog.io/
├── docker-remote/          # Remote repo proxying mcr.microsoft.com
│   └── dotnet/
│       ├── sdk:9.0
│       └── aspnet:9.0
├── docker-local/           # Local repo for your images
│   └── mongomigration:latest
└── docker-virtual/         # Virtual repo (optional, combines above)
    ├── dotnet/...
    └── mongomigration:...
```

## Deployment

### Full Deployment (First Time)

```powershell
.\deploy-to-aca-jfrog.ps1 `
    -ResourceGroupName "my-rg" `
    -ContainerAppName "mongo-migration" `
    -JFrogRegistryServer "yourcompany.jfrog.io" `
    -JFrogUsername "your-username" `
    -JFrogRepository "docker-local/mongomigration" `
    -JFrogBaseImageRegistry "yourcompany.jfrog.io/docker-virtual" `
    -Location "eastus" `
    -OwnerTag "your-email@company.com"
```

#### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `ResourceGroupName` | Yes | Azure resource group name |
| `ContainerAppName` | Yes | Name for the Container App |
| `JFrogRegistryServer` | Yes | JFrog server URL (e.g., `yourcompany.jfrog.io`) |
| `JFrogUsername` | Yes | JFrog username for authentication |
| `JFrogRepository` | Yes | Path in JFrog for the image (e.g., `docker-local/mongomigration`) |
| `JFrogBaseImageRegistry` | No | Registry for base images (defaults to `JFrogRegistryServer`) |
| `Location` | Yes | Azure region (e.g., `eastus`) |
| `OwnerTag` | Yes | Owner tag for Azure resources |
| `ImageTag` | No | Image tag (default: `latest`) |
| `VCores` | No | CPU cores 1-32 (default: 8) |
| `MemoryGB` | No | Memory in GB 2-64 (default: 32) |
| `SkipDockerBuild` | No | Skip build if image already exists |
| `UseEntraIdForAzureStorage` | No | Use Managed Identity for storage |
| `InfrastructureSubnetResourceId` | No | VNet subnet for network integration |

### Update Existing Deployment

```powershell
.\update-aca-app-jfrog.ps1 `
    -ResourceGroupName "my-rg" `
    -ContainerAppName "mongo-migration" `
    -JFrogRegistryServer "yourcompany.jfrog.io" `
    -JFrogUsername "your-username" `
    -JFrogRepository "docker-local/mongomigration" `
    -ImageTag "v1.2.3"
```

## Authentication Flow

1. **During Deployment**: Script prompts for JFrog password/API key
2. **Stored in Azure**: Password is stored as a Container App secret (`jfrog-password`)
3. **At Runtime**: ACA uses stored credentials to pull images from JFrog

## Troubleshooting

### Docker Build Fails

**Error**: `failed to solve: yourcompany.jfrog.io/dotnet/sdk:9.0: failed to resolve source metadata`

**Cause**: Base images not available in JFrog

**Fix**:
1. Ensure your JFrog remote repository is configured to proxy `mcr.microsoft.com`
2. Or upload base images manually to your JFrog local repository
3. Verify connectivity: `docker pull yourcompany.jfrog.io/docker-virtual/dotnet/sdk:9.0`

### Authentication Fails

**Error**: `unauthorized: authentication required`

**Fix**:
1. Verify username and password/API key
2. Check JFrog permissions for the repository
3. Test manually: `docker login yourcompany.jfrog.io -u your-username`

### Container App Fails to Start

**Error**: Image pull fails in Azure

**Fix**:
1. Verify the full image path is correct in Azure Portal
2. Check JFrog credentials are stored correctly as secrets
3. Ensure JFrog allows access from Azure IP ranges (if using IP restrictions)

## Security Considerations

1. **API Keys**: Use JFrog API keys instead of passwords for automation
2. **Minimal Permissions**: Create a dedicated JFrog user with only pull/push permissions for the specific repositories
3. **Credential Rotation**: Plan for periodic rotation of JFrog API keys
4. **Network Security**: Consider JFrog IP whitelisting if your organization requires it

## Comparison: ACR vs JFrog

| Feature | ACR | JFrog |
|---------|-----|-------|
| **Build Location** | Cloud (ACR Tasks) | Local (Docker) |
| **Build Speed** | Slower (network upload) | Faster (local) |
| **Cost** | ACR pricing | JFrog licensing |
| **Integration** | Native Azure | Cross-platform |
| **Authentication** | Managed Identity | Username/Password |
| **Artifact Types** | Containers only | Universal (npm, maven, etc.) |

## Migration from ACR

If you have an existing ACR deployment:

1. Build and push image to JFrog using the JFrog scripts
2. Update the Container App to use JFrog credentials
3. Deploy new revision pointing to JFrog image
4. (Optional) Delete old ACR if no longer needed

The infrastructure (Storage, Managed Identity, Container Apps Environment) remains the same—only the registry configuration changes.
