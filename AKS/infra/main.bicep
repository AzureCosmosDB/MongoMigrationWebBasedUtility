targetScope = 'resourceGroup'

@description('Location for all Azure resources created by this template.')
param location string = resourceGroup().location

@description('Owner tag required by Azure Policy.')
param ownerTag string

@description('AKS cluster name. Used to generate resource names.')
param clusterName string

@description('Azure Container Registry name.')
param acrName string = take('${replace(clusterName, '-', '')}acr', 50)

@description('Storage account name used for migration artifacts when a preconfigured storage account is not supplied.')
param storageAccountName string = take('${replace(clusterName, '-', '')}stor', 24)

@description('Optional existing storage account resource ID. When supplied, the template reuses that storage account and grants Blob RBAC on it.')
param storageAccountResourceId string = ''

@description('Managed identity name used by the workload for Blob access and federated identity.')
param workloadIdentityName string = '${clusterName}-workload-id'

@description('Optional existing managed identity resource ID. When supplied, the template reuses that identity and skips creation.')
param workloadIdentityResourceId string = ''

var useExistingStorageAccount = !empty(storageAccountResourceId)
var storageAccountResourceIdParts = split(storageAccountResourceId, '/')
var storageAccountSubscriptionId = empty(storageAccountResourceId) ? subscription().subscriptionId : storageAccountResourceIdParts[2]
var storageAccountResourceGroupName = empty(storageAccountResourceId) ? resourceGroup().name : storageAccountResourceIdParts[4]
var effectiveStorageAccountName = empty(storageAccountResourceId) ? storageAccountName : last(storageAccountResourceIdParts)
var useExistingWorkloadIdentity = !empty(workloadIdentityResourceId)
var workloadIdentityResourceIdParts = split(workloadIdentityResourceId, '/')
var workloadIdentitySubscriptionId = empty(workloadIdentityResourceId) ? subscription().subscriptionId : workloadIdentityResourceIdParts[2]
var workloadIdentityResourceGroupName = empty(workloadIdentityResourceId) ? resourceGroup().name : workloadIdentityResourceIdParts[4]
var effectiveWorkloadIdentityName = empty(workloadIdentityResourceId) ? workloadIdentityName : last(workloadIdentityResourceIdParts)

resource workloadIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = if (!useExistingWorkloadIdentity) {
  name: effectiveWorkloadIdentityName
  location: location
  tags: {
    owner: ownerTag
  }
}

resource workloadIdentityExisting 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = if (useExistingWorkloadIdentity) {
  name: effectiveWorkloadIdentityName
  scope: resourceGroup(workloadIdentitySubscriptionId, workloadIdentityResourceGroupName)
}

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: {
    name: 'Standard'
  }
  properties: {
    adminUserEnabled: false
  }
  tags: {
    owner: ownerTag
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = if (!useExistingStorageAccount) {
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
    allowSharedKeyAccess: false
  }
  tags: {
    owner: ownerTag
  }
}

resource acrPullRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, useExistingWorkloadIdentity ? workloadIdentityExisting!.id : workloadIdentity!.id, 'acrPull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: useExistingWorkloadIdentity ? workloadIdentityExisting!.properties.principalId : workloadIdentity!.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource storageBlobDataContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!useExistingStorageAccount) {
  name: guid(storageAccount.id, useExistingWorkloadIdentity ? workloadIdentityExisting!.id : workloadIdentity!.id, 'storageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: useExistingWorkloadIdentity ? workloadIdentityExisting!.properties.principalId : workloadIdentity!.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

module storageRoleAssignmentExisting './storageRoleAssignment.bicep' = if (useExistingStorageAccount) {
  name: 'storageRoleAssignmentExisting'
  scope: resourceGroup(storageAccountSubscriptionId, storageAccountResourceGroupName)
  params: {
    storageAccountName: effectiveStorageAccountName
    managedIdentityId: useExistingWorkloadIdentity ? workloadIdentityExisting!.id : workloadIdentity!.id
    managedIdentityPrincipalId: useExistingWorkloadIdentity ? workloadIdentityExisting!.properties.principalId : workloadIdentity!.properties.principalId
  }
}

output acrLoginServer string = acr.properties.loginServer
output acrNameResolved string = acr.name
output acrResourceId string = acr.id
output storageAccountNameResolved string = effectiveStorageAccountName
output storageAccountResourceIdResolved string = useExistingStorageAccount ? storageAccountResourceId : storageAccount.id
output workloadIdentityNameResolved string = effectiveWorkloadIdentityName
output workloadIdentityClientId string = useExistingWorkloadIdentity ? workloadIdentityExisting!.properties.clientId : workloadIdentity!.properties.clientId
output workloadIdentityPrincipalId string = useExistingWorkloadIdentity ? workloadIdentityExisting!.properties.principalId : workloadIdentity!.properties.principalId
output workloadIdentityResourceId string = useExistingWorkloadIdentity ? workloadIdentityResourceId : workloadIdentity!.id
output workloadIdentityResourceIdResolved string = useExistingWorkloadIdentity ? workloadIdentityResourceId : workloadIdentity!.id