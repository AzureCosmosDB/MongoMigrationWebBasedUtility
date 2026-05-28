targetScope = 'resourceGroup'

param storageAccountName string
param managedIdentityId string
param managedIdentityPrincipalId string

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource storageBlobDataContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, managedIdentityId, 'storageBlobDataContributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}