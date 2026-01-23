using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Factory for creating storage streams, supporting both local file system and Azure Blob Storage.
    /// Uses environment variables to determine storage mode:
    /// - UseBlobServiceClient: "true" to use Azure Blob Storage, otherwise local file system
    /// - BlobServiceClientURI: Azure Blob Storage URI (e.g., "https://mystorageaccount.blob.core.windows.net")
    /// </summary>
    public static class StorageStreamFactory
    {
        private static readonly Lazy<BlobServiceClient?> _blobServiceClient = new Lazy<BlobServiceClient?>(() =>
        {
            if (!UseBlobStorage)
                return null;

            var uri = BlobServiceClientUri;
            if (string.IsNullOrEmpty(uri))
                throw new InvalidOperationException("BlobServiceClientURI environment variable is required when UseBlobServiceClient is true");

            // Use ManagedIdentityCredential with explicit Client ID for User-Assigned Managed Identity
            // AZURE_CLIENT_ID is set by the Container App configuration
            var clientId = Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
            if (!string.IsNullOrEmpty(clientId))
            {
                // User-Assigned Managed Identity - requires Client ID
                return new BlobServiceClient(new Uri(uri), new ManagedIdentityCredential(clientId));
            }
            else
            {
                // Fallback to DefaultAzureCredential (works for local dev, System-Assigned MI, etc.)
                return new BlobServiceClient(new Uri(uri), new DefaultAzureCredential());
            }
        });

        /// <summary>
        /// Gets whether blob storage should be used based on environment variable
        /// </summary>
        public static bool UseBlobStorage =>
            string.Equals(Environment.GetEnvironmentVariable("UseBlobServiceClient"), "true", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Gets the blob service client URI from environment variable
        /// </summary>
        public static string? BlobServiceClientUri =>
            Environment.GetEnvironmentVariable("BlobServiceClientURI");

        /// <summary>
        /// Gets the container name for migration data (defaults to "migration-data")
        /// </summary>
        public static string ContainerName =>
            Environment.GetEnvironmentVariable("BlobContainerName") ?? "migration-data";

        /// <summary>
        /// Gets the blob service client (singleton, lazily initialized)
        /// </summary>
        public static BlobServiceClient? BlobServiceClient => _blobServiceClient.Value;

        /// <summary>
        /// Opens a write stream for the specified file path.
        /// Returns either a FileStream or Azure Blob write stream based on configuration.
        /// </summary>
        /// <param name="filePath">Local file path (used as blob name when using blob storage)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A writable stream</returns>
        public static async Task<Stream> OpenWriteAsync(string filePath, CancellationToken cancellationToken = default)
        {
            if (UseBlobStorage)
            {
                return await OpenBlobWriteStreamAsync(filePath, cancellationToken);
            }
            else
            {
                return OpenFileWriteStream(filePath);
            }
        }

        /// <summary>
        /// Opens a read stream for the specified file path.
        /// Returns either a FileStream or Azure Blob read stream based on configuration.
        /// </summary>
        /// <param name="filePath">Local file path (used as blob name when using blob storage)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A readable stream</returns>
        public static async Task<Stream> OpenReadAsync(string filePath, CancellationToken cancellationToken = default)
        {
            if (UseBlobStorage)
            {
                return await OpenBlobReadStreamAsync(filePath, cancellationToken);
            }
            else
            {
                return OpenFileReadStream(filePath);
            }
        }

        /// <summary>
        /// Checks if a file/blob exists
        /// </summary>
        public static async Task<bool> ExistsAsync(string filePath, CancellationToken cancellationToken = default)
        {
            if (UseBlobStorage)
            {
                var blobClient = GetBlobClient(filePath);
                var response = await blobClient.ExistsAsync(cancellationToken);
                return response.Value;
            }
            else
            {
                return File.Exists(filePath);
            }
        }

        /// <summary>
        /// Deletes a file/blob if it exists
        /// </summary>
        public static async Task DeleteIfExistsAsync(string filePath, CancellationToken cancellationToken = default)
        {
            if (UseBlobStorage)
            {
                var blobClient = GetBlobClient(filePath);
                await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);
            }
            else
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
            }
        }

        /// <summary>
        /// Gets the file/blob size in bytes
        /// </summary>
        public static async Task<long> GetFileSizeAsync(string filePath, CancellationToken cancellationToken = default)
        {
            if (UseBlobStorage)
            {
                var blobClient = GetBlobClient(filePath);
                var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
                return properties.Value.ContentLength;
            }
            else
            {
                return new FileInfo(filePath).Length;
            }
        }

        /// <summary>
        /// Synchronous version of ExistsAsync. 
        /// WARNING: Blocks the calling thread when using blob storage. Use ExistsAsync when possible.
        /// </summary>
        public static bool Exists(string filePath)
        {
            if (UseBlobStorage)
            {
                var blobClient = GetBlobClient(filePath);
                return blobClient.Exists().Value;
            }
            else
            {
                return File.Exists(filePath);
            }
        }

        /// <summary>
        /// Synchronous version of DeleteIfExistsAsync.
        /// WARNING: Blocks the calling thread when using blob storage. Use DeleteIfExistsAsync when possible.
        /// </summary>
        public static void DeleteIfExists(string filePath)
        {
            if (UseBlobStorage)
            {
                var blobClient = GetBlobClient(filePath);
                blobClient.DeleteIfExists();
            }
            else
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
            }
        }

        /// <summary>
        /// Writes text content to a file/blob synchronously.
        /// </summary>
        public static bool WriteAllText(string filePath, string content)
        {
            if (UseBlobStorage)
            {
                try
                {
                    var blobClient = GetBlobClient(filePath);
                    var containerClient = blobClient.GetParentBlobContainerClient();
                    containerClient.CreateIfNotExists();
                    
                    using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
                    blobClient.Upload(stream, new BlobUploadOptions());
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            else
            {
                try
                {
                    var directory = Path.GetDirectoryName(filePath);
                    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }
                    File.WriteAllText(filePath, content);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Reads all text from a file/blob synchronously.
        /// </summary>
        public static string? ReadAllText(string filePath)
        {
            if (UseBlobStorage)
            {
                try
                {
                    var blobClient = GetBlobClient(filePath);
                    if (!blobClient.Exists().Value)
                        return null;
                    
                    var response = blobClient.DownloadContent();
                    return response.Value.Content.ToString();
                }
                catch
                {
                    return null;
                }
            }
            else
            {
                try
                {
                    if (!File.Exists(filePath))
                        return null;
                    return File.ReadAllText(filePath);
                }
                catch
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Appends text content to a file/blob synchronously.
        /// For blob storage, uses AppendBlobClient for efficient appends.
        /// </summary>
        public static void AppendText(string filePath, string content)
        {
            if (UseBlobStorage)
            {
                try
                {
                    if (BlobServiceClient == null)
                        return;
                    
                    var containerClient = BlobServiceClient.GetBlobContainerClient(ContainerName);
                    containerClient.CreateIfNotExists();
                    
                    var blobName = ConvertFilePathToBlobName(filePath);
                    var appendBlobClient = containerClient.GetAppendBlobClient(blobName);
                    
                    if (!appendBlobClient.Exists().Value)
                    {
                        appendBlobClient.Create();
                    }
                    
                    using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
                    appendBlobClient.AppendBlock(stream);
                }
                catch
                {
                    // Silently ignore append errors to prevent application crashes
                }
            }
            else
            {
                try
                {
                    var directory = Path.GetDirectoryName(filePath);
                    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }
                    File.AppendAllText(filePath, content);
                }
                catch
                {
                    // Silently ignore append errors to prevent application crashes
                }
            }
        }

        /// <summary>
        /// Lists all files/blobs matching a pattern in a directory/prefix.
        /// For local storage, uses Directory.GetFiles.
        /// For blob storage, lists blobs with the given prefix.
        /// </summary>
        public static List<string> ListFiles(string directoryPath, string searchPattern, bool recursive = false)
        {
            var results = new List<string>();
            
            if (UseBlobStorage)
            {
                try
                {
                    if (BlobServiceClient == null)
                        return results;
                    
                    var containerClient = BlobServiceClient.GetBlobContainerClient(ContainerName);
                    if (!containerClient.Exists().Value)
                        return results;
                    
                    // Convert directory path to blob prefix
                    var prefix = ConvertFilePathToBlobName(directoryPath);
                    if (!string.IsNullOrEmpty(prefix) && !prefix.EndsWith("/"))
                        prefix += "/";
                    
                    // Extract extension filter from search pattern (e.g., "*.json" -> ".json")
                    string? extensionFilter = null;
                    if (searchPattern.StartsWith("*"))
                        extensionFilter = searchPattern.Substring(1);
                    
                    foreach (var blobItem in containerClient.GetBlobs(prefix: prefix))
                    {
                        var blobName = blobItem.Name;
                        
                        // Apply extension filter if specified
                        if (extensionFilter != null && !blobName.EndsWith(extensionFilter, StringComparison.OrdinalIgnoreCase))
                            continue;
                        
                        // For non-recursive, skip blobs in subdirectories
                        if (!recursive)
                        {
                            var relativePath = blobName.Substring(prefix.Length);
                            if (relativePath.Contains('/'))
                                continue;
                        }
                        
                        results.Add(blobName);
                    }
                }
                catch
                {
                    // Return empty list on error
                }
            }
            else
            {
                try
                {
                    if (!Directory.Exists(directoryPath))
                        return results;
                    
                    var searchOption = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
                    results.AddRange(Directory.GetFiles(directoryPath, searchPattern, searchOption));
                }
                catch
                {
                    // Return empty list on error
                }
            }
            
            return results;
        }

        /// <summary>
        /// Deletes all files/blobs matching a prefix (directory-like delete).
        /// For local storage, deletes the directory recursively.
        /// For blob storage, deletes all blobs with the given prefix.
        /// </summary>
        public static bool DeleteDirectory(string directoryPath, bool recursive = true)
        {
            if (UseBlobStorage)
            {
                try
                {
                    if (BlobServiceClient == null)
                        return false;
                    
                    var containerClient = BlobServiceClient.GetBlobContainerClient(ContainerName);
                    if (!containerClient.Exists().Value)
                        return true; // Nothing to delete
                    
                    // Convert directory path to blob prefix
                    var prefix = ConvertFilePathToBlobName(directoryPath);
                    if (!string.IsNullOrEmpty(prefix) && !prefix.EndsWith("/"))
                        prefix += "/";
                    
                    // Delete all blobs with this prefix
                    foreach (var blobItem in containerClient.GetBlobs(prefix: prefix))
                    {
                        containerClient.DeleteBlobIfExists(blobItem.Name);
                    }
                    
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            else
            {
                try
                {
                    if (!Directory.Exists(directoryPath))
                        return true; // Nothing to delete
                    
                    Directory.Delete(directoryPath, recursive);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Ensures a directory exists. For blob storage, this is a no-op since directories are virtual.
        /// </summary>
        public static void EnsureDirectoryExists(string directoryPath)
        {
            if (!UseBlobStorage)
            {
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }
            }
            // For blob storage, directories are virtual - no action needed
        }

        /// <summary>
        /// Copies a file/blob from source to destination.
        /// For local storage, uses File.Copy.
        /// For blob storage, copies the blob to a new location.
        /// </summary>
        public static void CopyFile(string sourceFilePath, string destinationFilePath)
        {
            if (UseBlobStorage)
            {
                try
                {
                    if (BlobServiceClient == null)
                        throw new InvalidOperationException("BlobServiceClient is not initialized.");
                    
                    var containerClient = BlobServiceClient.GetBlobContainerClient(ContainerName);
                    containerClient.CreateIfNotExists();
                    
                    var sourceBlobName = ConvertFilePathToBlobName(sourceFilePath);
                    var destBlobName = ConvertFilePathToBlobName(destinationFilePath);
                    
                    var sourceBlob = containerClient.GetBlobClient(sourceBlobName);
                    var destBlob = containerClient.GetBlobClient(destBlobName);
                    
                    // Start the copy operation and wait for completion
                    destBlob.StartCopyFromUri(sourceBlob.Uri);
                }
                catch (Exception ex)
                {
                    throw new IOException($"Failed to copy blob from {sourceFilePath} to {destinationFilePath}", ex);
                }
            }
            else
            {
                File.Copy(sourceFilePath, destinationFilePath);
            }
        }

        /// <summary>
        /// Opens a file stream for reading with shared access (allows concurrent reads/writes).
        /// </summary>
        public static Stream? OpenReadShared(string filePath)
        {
            if (UseBlobStorage)
            {
                try
                {
                    var blobClient = GetBlobClient(filePath);
                    if (!blobClient.Exists().Value)
                        return null;
                    
                    return blobClient.OpenRead();
                }
                catch
                {
                    return null;
                }
            }
            else
            {
                try
                {
                    if (!File.Exists(filePath))
                        return null;
                    
                    return new FileStream(
                        filePath,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.ReadWrite | FileShare.Delete);
                }
                catch
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Opens a file stream for appending.
        /// Note: For blob storage, this reads existing content, appends, and rewrites.
        /// </summary>
        public static Stream OpenAppend(string filePath)
        {
            if (UseBlobStorage)
            {
                // For blob storage, we use AppendBlobClient for efficient appends
                if (BlobServiceClient == null)
                    throw new InvalidOperationException("BlobServiceClient is not initialized.");
                
                var containerClient = BlobServiceClient.GetBlobContainerClient(ContainerName);
                containerClient.CreateIfNotExists();
                
                var blobName = ConvertFilePathToBlobName(filePath);
                var appendBlobClient = containerClient.GetAppendBlobClient(blobName);
                
                if (!appendBlobClient.Exists().Value)
                {
                    appendBlobClient.Create();
                }
                
                return appendBlobClient.OpenWrite(overwrite: false);
            }
            else
            {
                var directory = Path.GetDirectoryName(filePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
                
                return new FileStream(
                    filePath,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.Read,
                    4096,
                    FileOptions.WriteThrough);
            }
        }

        #region Private Methods

        private static Stream OpenFileWriteStream(string filePath)
        {
            // Ensure directory exists
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            return new FileStream(
                filePath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 81920,
                useAsync: true);
        }

        private static Stream OpenFileReadStream(string filePath)
        {
            return new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 81920,
                useAsync: true);
        }

        private static async Task<Stream> OpenBlobWriteStreamAsync(string filePath, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(filePath);

            // Ensure container exists
            var containerClient = blobClient.GetParentBlobContainerClient();
            await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

            // Open write stream with optimized settings for large files
            var options = new BlockBlobOpenWriteOptions
            {
                BufferSize = 4 * 1024 * 1024, // 4MB buffer for better throughput
            };

            return await blobClient.OpenWriteAsync(overwrite: true, options, cancellationToken);
        }

        private static async Task<Stream> OpenBlobReadStreamAsync(string filePath, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(filePath);

            // Open read stream with optimized settings
            var options = new BlobOpenReadOptions(allowModifications: false)
            {
                BufferSize = 4 * 1024 * 1024, // 4MB buffer for better throughput
            };

            return await blobClient.OpenReadAsync(options, cancellationToken);
        }

        private static BlockBlobClient GetBlobClient(string filePath)
        {
            if (BlobServiceClient == null)
                throw new InvalidOperationException("BlobServiceClient is not initialized. Ensure UseBlobServiceClient and BlobServiceClientURI environment variables are set.");

            // Convert file path to blob-friendly name
            // e.g., "/app/migration-data/job123/db.col/0.bson" -> "job123/db.col/0.bson"
            var blobName = ConvertFilePathToBlobName(filePath);

            var containerClient = BlobServiceClient.GetBlobContainerClient(ContainerName);
            return containerClient.GetBlockBlobClient(blobName);
        }

        /// <summary>
        /// Converts a local file path to a blob-friendly name
        /// </summary>
        private static string ConvertFilePathToBlobName(string filePath)
        {
            // Normalize path separators
            var normalized = filePath.Replace('\\', '/');

            // Remove common prefixes that shouldn't be part of blob name
            var prefixesToRemove = new[]
            {
                "/app/migration-data/",
                "/app/dump/",
                "migration-data/",
                "dump/"
            };

            foreach (var prefix in prefixesToRemove)
            {
                if (normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    normalized = normalized.Substring(prefix.Length);
                    break;
                }
            }

            // Also handle Windows-style paths
            // e.g., "C:\Work\dump\job123\db.col\0.bson" -> "job123/db.col/0.bson"
            var dumpIndex = normalized.LastIndexOf("/dump/", StringComparison.OrdinalIgnoreCase);
            if (dumpIndex >= 0)
            {
                normalized = normalized.Substring(dumpIndex + 6); // Skip "/dump/"
            }

            // Remove leading slashes
            normalized = normalized.TrimStart('/');

            return normalized;
        }

        #endregion
    }
}
