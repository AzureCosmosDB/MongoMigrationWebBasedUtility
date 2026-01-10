@description('Location for all resources')
param location string = resourceGroup().location

@description('Owner tag required by Azure Policy')
param ownerTag string

@description('Name of the Container App')
param containerAppName string

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
  location: location
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

// Storage Account for persistent migration files
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
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
  }
  tags: {
    owner: ownerTag
  }
}

// File Share for migration data (100GB)
resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = {
  name: '${storageAccount.name}/default/migration-data'
  properties: {
    shareQuota: 100
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

// Storage configuration for Container Apps Environment
resource storageConfiguration 'Microsoft.App/managedEnvironments/storages@2023-05-01' = {
  parent: containerAppEnvironment
  name: 'migration-storage'
  properties: {
    azureFile: {
      accountName: storageAccount.name
      accountKey: storageAccount.listKeys().keys[0].value
      shareName: 'migration-data'
      accessMode: 'ReadWrite'
    }
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
          image: stateStoreAppID == '' ? '${acr.properties.loginServer}/nginx:latest' : '${acr.properties.loginServer}/${acrRepository}:${imageTag}'
          resources: {
            cpu: vCores
            memory: '${memoryGB}Gi'
          }
          volumeMounts: [
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
          ], stateStoreConnectionString != '' ? [
            {
              name: 'StateStoreConnectionStringOrPath'
              secretRef: 'statestore-connection'
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
      volumes: [
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
  dependsOn: [
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
output storageAccountName string = storageAccount.name

@description('File Share Name for migration data')
output fileShareName string = 'migration-data'

@description('Resource Drive Mount Path in container')
output resourceDrivePath string = '/app/migration-data'
