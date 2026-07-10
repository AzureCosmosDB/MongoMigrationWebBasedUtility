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

        // Name of the environment variable that supplies a per-install secret seed.
        // The AES-256 key is derived from this seed (SHA-256) rather than being hardcoded.
        private const string EncryptionKeySeedEnvVariable = "EncryptionKeySeed";

        // Legacy seed retained only as a fallback so existing installs (that stored the
        // password before a per-install seed was configured) can still be decrypted.
        // Supply the EncryptionKeySeed environment variable to override this in production.
        private const string LegacyEncryptionKeySeed = "MongoMigration2025SecureKey12345";

        // 32-byte AES-256 key derived once at startup from the configured seed.
        private static readonly byte[] EncryptionKey = ResolveEncryptionKey();

        // Legacy 32-byte key (raw UTF-8 bytes of the legacy seed) used to decrypt passwords
        // that were stored before a per-install seed was configured. Passwords decrypted with
        // this key are transparently re-encrypted with EncryptionKey on first use.
        private static readonly byte[] LegacyEncryptionKey = Encoding.UTF8.GetBytes(LegacyEncryptionKeySeed);

        private readonly string _passwordFilePath;

        private static byte[] ResolveEncryptionKey()
        {
            var seed = Environment.GetEnvironmentVariable(EncryptionKeySeedEnvVariable);
            if (string.IsNullOrWhiteSpace(seed))
            {
                // No seed supplied: fall back to the legacy built-in value for backward compatibility.
                return Encoding.UTF8.GetBytes(LegacyEncryptionKeySeed);
            }

            // Derive a deterministic 32-byte key from the supplied seed so any string length works.
            using var sha256 = SHA256.Create();
            return sha256.ComputeHash(Encoding.UTF8.GetBytes(seed));
        }

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
            var storedPassword = await GetStoredPasswordAsync();
            if (storedPassword == null)
            {
                return false;
            }
            return password == storedPassword;
        }

        public async Task<string?> GetStoredPasswordAsync()
        {
            bool exists = await StorageStreamFactory.ExistsAsync(_passwordFilePath);
            if (!exists)
            {
                return null;
            }

            byte[] encryptedBytes;

            try
            {
                if (StorageStreamFactory.UseBlobStorage)
                {
                    // Read from Blob Storage
                    using var stream = await StorageStreamFactory.OpenReadAsync(_passwordFilePath);
                    using var ms = new MemoryStream();
                    await stream.CopyToAsync(ms);
                    encryptedBytes = ms.ToArray();
                }
                else
                {
                    // Read from local file
                    encryptedBytes = await File.ReadAllBytesAsync(_passwordFilePath);
                }
            }
            catch
            {
                // If the stored password cannot be read, return null
                return null;
            }

            // Try the active key first.
            try
            {
                return Decrypt(encryptedBytes, EncryptionKey);
            }
            catch
            {
                // Fall through to legacy-key handling below.
            }

            // Backward compatibility: if the active key differs from the legacy key, try the
            // legacy key. If it succeeds, re-encrypt the password with the active key on first use.
            if (!EncryptionKey.AsSpan().SequenceEqual(LegacyEncryptionKey))
            {
                try
                {
                    var decryptedPassword = Decrypt(encryptedBytes, LegacyEncryptionKey);
                    try
                    {
                        // Migrate the stored password to the active key.
                        await SetPasswordAsync(decryptedPassword);
                    }
                    catch
                    {
                        // A migration failure must not block a valid login; the next
                        // successful call will retry the migration.
                    }
                    return decryptedPassword;
                }
                catch
                {
                    // If legacy decryption also fails, return null.
                    return null;
                }
            }

            return null;
        }

        public async Task<bool> IsPasswordSetAsync()
        {
            return await StorageStreamFactory.ExistsAsync(_passwordFilePath);
        }

        public async Task SetPasswordAsync(string newPassword)
        {
            var encryptedBytes = Encrypt(newPassword);
            
            if (StorageStreamFactory.UseBlobStorage)
            {
                // Write to Blob Storage
                using var stream = await StorageStreamFactory.OpenWriteAsync(_passwordFilePath);
                await stream.WriteAsync(encryptedBytes);
            }
            else
            {
                // Ensure directory exists for local file
                var directory = Path.GetDirectoryName(_passwordFilePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                await File.WriteAllBytesAsync(_passwordFilePath, encryptedBytes);
            }
        }

        private byte[] Encrypt(string plainText)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = EncryptionKey;
                aes.GenerateIV();

                using (var encryptor = aes.CreateEncryptor(aes.Key, aes.IV))
                using (var ms = new MemoryStream())
                {
                    // Write IV to the beginning of the stream
                    ms.Write(aes.IV, 0, aes.IV.Length);

                    using (var cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
                    using (var sw = new StreamWriter(cs))
                    {
                        sw.Write(plainText);
                    }

                    return ms.ToArray();
                }
            }
        }

        private string Decrypt(byte[] cipherText, byte[] key)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = key;

                // Extract IV from the beginning of the cipher text
                byte[] iv = new byte[aes.IV.Length];
                Array.Copy(cipherText, 0, iv, 0, iv.Length);
                aes.IV = iv;

                using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
                using (var ms = new MemoryStream(cipherText, iv.Length, cipherText.Length - iv.Length))
                using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                using (var sr = new StreamReader(cs))
                {
                    return sr.ReadToEnd();
                }
            }
        }
    }
}
