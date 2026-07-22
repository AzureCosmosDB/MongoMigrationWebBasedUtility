using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using System.Security.Cryptography;
using System.Text;

namespace MongoMigrationWebApp.Service
{
    public class PasswordManager
    {
        private const string PasswordFileName = "app.password";

        // One-way password hashing (PBKDF2 / HMAC-SHA256). Only a salted hash of the password
        // is persisted; the password itself is never stored and cannot be recovered from the
        // stored value. Validation re-derives the hash for the candidate password and compares
        // the digests in constant time.
        private const string HashPrefix = "PBKDF2";
        private const int SaltSizeBytes = 16;
        private const int HashSizeBytes = 32;

        // OWASP-recommended minimum iteration count for PBKDF2-HMAC-SHA256.
        private const int Pbkdf2Iterations = 210_000;

        private readonly string _passwordFilePath;

        public PasswordManager()
        {
            var workingFolder = Helper.GetWorkingFolder();
            
            // Only create local directory if not using Blob Storage
            if (!StorageStreamFactory.UseBlobStorage && !Directory.Exists(workingFolder))
            {
                Directory.CreateDirectory(workingFolder);
            }

            _passwordFilePath = Path.Combine(workingFolder, PasswordFileName);
        }

        public async Task<bool> ValidatePasswordAsync(string password)
        {
            if (string.IsNullOrEmpty(password))
            {
                return false;
            }

            var storedHash = await GetStoredHashAsync();
            if (string.IsNullOrEmpty(storedHash))
            {
                return false;
            }

            return VerifyPassword(password, storedHash);
        }

        public async Task<bool> IsPasswordSetAsync()
        {
            // Only report a password as "set" when a valid one-way hash is stored. A missing
            // file — or a legacy/unrecognized format (e.g. the old reversible-encryption blob) —
            // is treated as "not set" so the app forces a fresh password to be created.
            var storedHash = await GetStoredHashAsync();
            return !string.IsNullOrEmpty(storedHash) && IsValidHashFormat(storedHash);
        }

        public async Task SetPasswordAsync(string newPassword)
        {
            var encodedHash = HashPassword(newPassword);
            var bytes = Encoding.UTF8.GetBytes(encodedHash);

            if (StorageStreamFactory.UseBlobStorage)
            {
                // Write to Blob Storage
                using var stream = await StorageStreamFactory.OpenWriteAsync(_passwordFilePath);
                await stream.WriteAsync(bytes);
            }
            else
            {
                // Ensure directory exists for local file
                var directory = Path.GetDirectoryName(_passwordFilePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                await File.WriteAllBytesAsync(_passwordFilePath, bytes);
            }
        }

        private async Task<string?> GetStoredHashAsync()
        {
            bool exists = await StorageStreamFactory.ExistsAsync(_passwordFilePath);
            if (!exists)
            {
                return null;
            }

            try
            {
                byte[] bytes;
                if (StorageStreamFactory.UseBlobStorage)
                {
                    // Read from Blob Storage
                    using var stream = await StorageStreamFactory.OpenReadAsync(_passwordFilePath);
                    using var ms = new MemoryStream();
                    await stream.CopyToAsync(ms);
                    bytes = ms.ToArray();
                }
                else
                {
                    // Read from local file
                    bytes = await File.ReadAllBytesAsync(_passwordFilePath);
                }

                return Encoding.UTF8.GetString(bytes);
            }
            catch
            {
                // If the stored hash cannot be read, treat it as unset.
                return null;
            }
        }

        // Produces a self-describing, one-way hash: "PBKDF2$<iterations>$<saltB64>$<hashB64>".
        private static string HashPassword(string password)
        {
            byte[] salt = RandomNumberGenerator.GetBytes(SaltSizeBytes);
            byte[] hash = Rfc2898DeriveBytes.Pbkdf2(
                Encoding.UTF8.GetBytes(password),
                salt,
                Pbkdf2Iterations,
                HashAlgorithmName.SHA256,
                HashSizeBytes);

            return string.Join('$',
                HashPrefix,
                Pbkdf2Iterations,
                Convert.ToBase64String(salt),
                Convert.ToBase64String(hash));
        }

        // Determines whether the stored value is a recognized new-format one-way hash.
        private static bool IsValidHashFormat(string encodedHash)
        {
            var parts = encodedHash.Split('$');
            return parts.Length == 4
                && parts[0] == HashPrefix
                && int.TryParse(parts[1], out int iterations)
                && iterations > 0
                && IsBase64(parts[2])
                && IsBase64(parts[3]);
        }

        private static bool IsBase64(string value)
        {
            try
            {
                Convert.FromBase64String(value);
                return true;
            }
            catch
            {
                return false;
            }
        }

        // Re-derives the hash for the candidate password and compares it in constant time.
        private static bool VerifyPassword(string password, string encodedHash)
        {
            var parts = encodedHash.Split('$');
            if (parts.Length != 4 || parts[0] != HashPrefix)
            {
                // Unrecognized/legacy format: cannot be validated. A password reset is required.
                return false;
            }

            if (!int.TryParse(parts[1], out int iterations) || iterations <= 0)
            {
                return false;
            }

            byte[] salt;
            byte[] expectedHash;
            try
            {
                salt = Convert.FromBase64String(parts[2]);
                expectedHash = Convert.FromBase64String(parts[3]);
            }
            catch
            {
                return false;
            }

            byte[] actualHash = Rfc2898DeriveBytes.Pbkdf2(
                Encoding.UTF8.GetBytes(password),
                salt,
                iterations,
                HashAlgorithmName.SHA256,
                expectedHash.Length);

            return CryptographicOperations.FixedTimeEquals(actualHash, expectedHash);
        }
    }
}
