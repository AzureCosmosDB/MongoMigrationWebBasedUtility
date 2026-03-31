@description('Location for all resources')
param location string = resourceGroup().location

@description('Owner tag required by Azure Policy')
param ownerTag string

@description('Name of the Container App')
param containerAppName string

@description('Location for the Azure Container Registry. Defaults to the main location. Override if reusing an existing ACR in a different region.')
param acrLocation string = location

@description('Name of the Azure Container Registry')
param acrName string

@description('ACR repository name for the container image')
param acrRepository string = containerAppName

@description('Docker image tag to deploy')
param imageTag string = 'latest'

@description('Storage account name for persistent migration files')
param storageAccountName string = take('${replace(containerAppName, '-', '')}stor', 24)

@secure()
@description('StateStore connection string for the container')
param stateStoreConnectionString string = ''

@description('StateStore App ID for the container')
param stateStoreAppID string = ''

@description('Number of vCores for the container')
@minValue(1)
@maxValue(32)
param vCores int = 8

@description('Memory in GB for the container')
@minValue(2)
@maxValue(64)
param memoryGB int = 32

@description('ASP.NET Core environment setting')
param aspNetCoreEnvironment string = 'Development'

@description('Optional: Resource ID of the subnet for VNet integration (e.g., /subscriptions/{sub-id}/resourceGroups/{rg-name}/providers/Microsoft.Network/virtualNetworks/{vnet-name}/subnets/{subnet-name})')
param infrastructureSubnetResourceId string = ''

@description('Use Entra ID (Managed Identity) for Azure Blob Storage instead of mounting Azure Files. When true, UseBlobServiceClient env var is set and no volume is mounted.')
param useEntraIdForStorage bool = false

@description('Size of the Azure File Share in GB (only applies when useEntraIdForStorage is false)')
@minValue(100)
@maxValue(102400)
param fileShareSizeGB int = 100

@description('Use a pre-configured (existing) storage account instead of creating a new one. Requires useEntraIdForStorage=true. Provide the full resource ID of the storage account (e.g., /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{name}).')
param storageAccountResourceId string = ''

// Derived: whether an existing storage account is referenced instead of creating one
var usePreConfiguredStorageAccount = !empty(storageAccountResourceId)

// Effective storage account name: parsed from resource ID when pre-configured, or from the param
var effectiveStorageAccountName = !empty(storageAccountResourceId) ? last(split(storageAccountResourceId, '/')) : storageAccountName

// Subscription and resource group parsed from the resource ID (used to scope the existing resource reference)
var storageAccountSubscriptionId = usePreConfiguredStorageAccount ? split(storageAccountResourceId, '/')[2] : subscription().subscriptionId
var storageAccountResourceGroupName = usePreConfiguredStorageAccount ? split(storageAccountResourceId, '/')[4] : resourceGroup().name

// Variables for dynamic workload profile selection
var workloadProfileType = vCores <= 4 ? 'D4' : vCores <= 8 ? 'D8' : vCores <= 16 ? 'D16' : 'D32'
var workloadProfileName = 'Dedicated'

// Managed Identity for Container App and ACR access
resource managedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${containerAppName}-identity'
  location: location
  tags: {
    owner: ownerTag
  }
}

// Azure Container Registry
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: acrLocation
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
  }
  tags: {
    owner: ownerTag
  }
}

// ACR Pull Role Assignment to Managed Identity
resource acrPullRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, managedIdentity.id, 'acrPull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Storage Account for persistent migration files (skip creation when using pre-configured account)
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = if (!usePreConfiguredStorageAccount) {
  name: effectiveStorageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    // Disable shared key access when using Entra ID - required by some org policies
    allowSharedKeyAccess: !useEntraIdForStorage
  }
  tags: {
    owner: ownerTag
  }
}

// File Share for migration data - only needed when NOT using Entra ID
resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = if (!useEntraIdForStorage) {
  name: '${effectiveStorageAccountName}/default/migration-data'
  properties: {
    shareQuota: fileShareSizeGB
    enabledProtocols: 'SMB'
  }
}

// Container Apps Environment with Dedicated Plan
resource containerAppEnvironment 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: '${containerAppName}-env-${workloadProfileType}'
  location: location
  tags: {
    owner: ownerTag
  }
  properties: union(
    {
      workloadProfiles: [
        {
          name: 'Consumption'
          workloadProfileType: 'Consumption'
        }
        {
          name: workloadProfileName
          workloadProfileType: workloadProfileType
          minimumCount: 1
          maximumCount: 1
        }
      ]
    },
    infrastructureSubnetResourceId != '' ? {
      vnetConfiguration: {
        infrastructureSubnetId: infrastructureSubnetResourceId
        internal: false
      }
    } : {}
  )
}

// Storage configuration for Container Apps Environment (only when not using Entra ID and not using pre-configured storage)
// Safe: storageAccount is always deployed when this resource is deployed (both require !usePreConfiguredStorageAccount)
resource storageConfiguration 'Microsoft.App/managedEnvironments/storages@2023-05-01' = if (!useEntraIdForStorage && !usePreConfiguredStorageAccount) {
  parent: containerAppEnvironment
  name: 'migration-storage'
  properties: {
    azureFile: {
      accountName: effectiveStorageAccountName
      #disable-next-line BCP422
      accountKey: storageAccount.listKeys().keys[0].value
      shareName: 'migration-data'
      accessMode: 'ReadWrite'
    }
  }
}

// Role assignment for Managed Identity to access Blob Storage (new storage account)
resource storageBlobDataContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (useEntraIdForStorage && !usePreConfiguredStorageAccount) {
  name: guid(storageAccount.id, managedIdentity.id, 'storageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: managedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Role assignment for Managed Identity to access Blob Storage (pre-configured storage account, potentially cross-RG)
// Deployed as a module scoped to the storage account's resource group to support cross-RG references
module storageRoleAssignmentExisting './storageRoleAssignment.bicep' = if (useEntraIdForStorage && usePreConfiguredStorageAccount) {
  name: 'storageRoleAssignmentExisting'
  scope: resourceGroup(storageAccountSubscriptionId, storageAccountResourceGroupName)
  params: {
    storageAccountName: effectiveStorageAccountName
    managedIdentityId: managedIdentity.id
    managedIdentityPrincipalId: managedIdentity.properties.principalId
  }
}

// Container App
resource containerApp 'Microsoft.App/containerApps@2023-05-01' = {
  name: containerAppName
  location: location
  tags: {
    owner: ownerTag
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: containerAppEnvironment.id
    workloadProfileName: workloadProfileName
    configuration: {
      secrets: stateStoreConnectionString != '' ? [
        {
          name: 'statestore-connection'
          value: stateStoreConnectionString
        }
      ] : []
      registries: [
        {
          server: acr.properties.loginServer
          identity: managedIdentity.id
        }
      ]
      ingress: {
        external: true
        targetPort: 8080
        allowInsecure: true
        traffic: [
          {
            weight: 100
            latestRevision: true
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: containerAppName
          image: stateStoreAppID == '' ? 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest' : '${acr.properties.loginServer}/${acrRepository}:${imageTag}'
          resources: {
            cpu: vCores
            memory: '${memoryGB}Gi'
          }
          volumeMounts: useEntraIdForStorage ? [] : [
            {
              volumeName: 'migration-data-volume'
              mountPath: '/app/migration-data'
            }
          ]
          env: concat([
            {
              name: 'ASPNETCORE_ENVIRONMENT'
              value: aspNetCoreEnvironment
            }
            {
              name: 'ASPNETCORE_HTTP_PORTS'
              value: '8080'
            }
            {
              name: 'StateStoreAppID'
              value: stateStoreAppID
            }
            {
              name: 'ResourceDrive'
              value: '/app/migration-data'
            }
            {
              name: 'STORAGE_QUOTA_GB'
              value: string(fileShareSizeGB)
            }
          ], stateStoreConnectionString != '' ? [
            {
              name: 'StateStoreConnectionStringOrPath'
              secretRef: 'statestore-connection'
            }
          ] : [], useEntraIdForStorage ? [
            {
              name: 'UseBlobServiceClient'
              value: 'true'
            }
            {
              name: 'BlobServiceClientURI'
              value: 'https://${effectiveStorageAccountName}.blob.${environment().suffixes.storage}'
            }
            {
              name: 'BlobContainerName'
              value: 'migration-data'
            }
            {
              name: 'AZURE_CLIENT_ID'
              value: managedIdentity.properties.clientId
            }
          ] : [])
          probes: [
            {
              type: 'Startup'
              httpGet: {
                path: '/api/HealthCheck/ping'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 10
              periodSeconds: 5
              failureThreshold: 30
              successThreshold: 1
              timeoutSeconds: 3
            }
            {
              type: 'Liveness'
              httpGet: {
                path: '/api/HealthCheck/ping'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 0
              periodSeconds: 30
              failureThreshold: 3
              successThreshold: 1
              timeoutSeconds: 5
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/api/HealthCheck/ping'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 5
              periodSeconds: 10
              failureThreshold: 3
              successThreshold: 1
              timeoutSeconds: 3
            }
          ]
        }
      ]
      volumes: useEntraIdForStorage ? [] : [
        {
          name: 'migration-data-volume'
          storageType: 'AzureFile'
          storageName: 'migration-storage'
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  dependsOn: useEntraIdForStorage ? [
    storageBlobDataContributorRole
    storageRoleAssignmentExisting
  ] : [
    storageConfiguration
  ]
}

// Outputs
@description('Container Apps Environment ID')
output containerAppEnvironmentId string = containerAppEnvironment.id

@description('Container App FQDN')
output containerAppFQDN string = stateStoreAppID != '' ? containerApp.properties.configuration.ingress.fqdn : 'not-ready'

@description('Container App URL')
output containerAppUrl string = stateStoreAppID == '' ? 'not-ready' : 'https://${containerApp.properties.configuration.ingress.fqdn}'

@description('Managed Identity Resource ID')
output managedIdentityId string = managedIdentity.id

@description('Managed Identity Client ID')
output managedIdentityClientId string = managedIdentity.properties.clientId

@description('Storage Account Name for migration data')
output storageAccountName string = effectiveStorageAccountName

@description('File Share Name for migration data (only when not using Entra ID)')
output fileShareName string = 'migration-data'

@description('Resource Drive Mount Path in container')
output resourceDrivePath string = '/app/migration-data'

@description('Storage mode: MountedAzureFiles or EntraIdBlobStorage')
output storageMode string = useEntraIdForStorage ? 'EntraIdBlobStorage' : 'MountedAzureFiles'

@description('Blob Service URI (only when using Entra ID)')
output blobServiceUri string = useEntraIdForStorage ? 'https://${storageAccount.name}.blob.${environment().suffixes.storage}' : ''
