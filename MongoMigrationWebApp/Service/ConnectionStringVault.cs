using System;
using System.IO;
using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using OnlineMongoMigrationProcessor;

namespace MongoMigrationWebApp.Service
{
    /// <summary>
    /// Persists a job's source/target connection strings so a resume does not have to ask for them
    /// again after the process recycles.
    /// </summary>
    /// <remarks>
    /// The strings were previously held only in a static dictionary on MigrationJobContext, so an
    /// app-pool recycle, an iisreset or a redeploy silently dropped them and a started job could
    /// only be resumed by re-entering the credentials. Progress survived; the credentials did not.
    ///
    /// Payloads are encrypted with ASP.NET Core Data Protection rather than written as plaintext,
    /// and both the payloads and the keyring live under the state store, which survives a redeploy
    /// -- the app directory does not.
    /// </remarks>
    public sealed class ConnectionStringVault
    {
        private const string Purpose = "MongoMigrationWebApp.ConnectionStrings.v1";

        private readonly IDataProtector _protector;
        private readonly string _root;

        public ConnectionStringVault(IDataProtectionProvider provider, IConfiguration configuration)
        {
            _protector = provider.CreateProtector(Purpose);
            _root = Path.Combine(ResolveStateRoot(configuration), "connections");
        }

        /// <summary>State store when it is a local path, otherwise the working folder (blob-backed
        /// state stores give a connection string here, which is not somewhere we can write files).</summary>
        internal static string ResolveStateRoot(IConfiguration configuration)
        {
            var configured = configuration?["StateStore:ConnectionStringOrPath"];
            if (!string.IsNullOrWhiteSpace(configured))
            {
                try
                {
                    if (Path.IsPathRooted(configured) && configured.IndexOf("://", StringComparison.Ordinal) < 0)
                    {
                        return configured;
                    }
                }
                catch (ArgumentException)
                {
                    // Not a usable path (illegal characters in a connection string) -- fall through.
                }
            }

            return Helper.GetWorkingFolder();
        }

        public bool TrySave(string jobId, string sourceConnectionString, string targetConnectionString)
        {
            if (string.IsNullOrWhiteSpace(jobId)) { return false; }
            if (string.IsNullOrWhiteSpace(sourceConnectionString) && string.IsNullOrWhiteSpace(targetConnectionString))
            {
                return false;
            }

            try
            {
                Directory.CreateDirectory(_root);
                var payload = JsonSerializer.Serialize(new StoredConnections
                {
                    Source = sourceConnectionString ?? string.Empty,
                    Target = targetConnectionString ?? string.Empty
                });

                File.WriteAllText(PathFor(jobId), _protector.Protect(payload));
                return true;
            }
            catch (Exception ex)
            {
                // Never fail a migration because the convenience cache could not be written.
                Helper.LogToFile($"ConnectionStringVault: save failed for {jobId}: {ex.Message}");
                return false;
            }
        }

        public bool TryLoad(string jobId, out string sourceConnectionString, out string targetConnectionString)
        {
            sourceConnectionString = string.Empty;
            targetConnectionString = string.Empty;
            if (string.IsNullOrWhiteSpace(jobId)) { return false; }

            var path = PathFor(jobId);
            if (!File.Exists(path)) { return false; }

            try
            {
                var stored = JsonSerializer.Deserialize<StoredConnections>(_protector.Unprotect(File.ReadAllText(path)));
                if (stored == null) { return false; }

                sourceConnectionString = stored.Source ?? string.Empty;
                targetConnectionString = stored.Target ?? string.Empty;
                return !string.IsNullOrWhiteSpace(sourceConnectionString)
                    && !string.IsNullOrWhiteSpace(targetConnectionString);
            }
            catch (Exception ex)
            {
                // A rotated or lost keyring makes the payload undecryptable; treat it as absent so
                // the caller falls back to asking for the connection strings.
                Helper.LogToFile($"ConnectionStringVault: load failed for {jobId}: {ex.Message}");
                return false;
            }
        }

        public void Remove(string jobId)
        {
            if (string.IsNullOrWhiteSpace(jobId)) { return; }
            try
            {
                var path = PathFor(jobId);
                if (File.Exists(path)) { File.Delete(path); }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"ConnectionStringVault: remove failed for {jobId}: {ex.Message}");
            }
        }

        public void RemoveAll()
        {
            try
            {
                if (Directory.Exists(_root)) { Directory.Delete(_root, recursive: true); }
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"ConnectionStringVault: clear failed: {ex.Message}");
            }
        }

        private string PathFor(string jobId)
        {
            // Job ids are GUIDs, but this is a filename built from caller input either way.
            var safe = string.Join("_", jobId.Split(Path.GetInvalidFileNameChars()));
            return Path.Combine(_root, safe + ".dat");
        }

        private sealed class StoredConnections
        {
            public string Source { get; set; } = string.Empty;
            public string Target { get; set; } = string.Empty;
        }
    }
}
