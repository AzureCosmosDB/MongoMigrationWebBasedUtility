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
        private static readonly byte[] EncryptionKey = Encoding.UTF8.GetBytes("MongoMigration2025SecureKey12345"); // 32 bytes for AES-256

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

            try
            {
                byte[] encryptedBytes;
                
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
                
                var decryptedPassword = Decrypt(encryptedBytes);
                return decryptedPassword;
            }
            catch
            {
                // If decryption fails, return null
                return null;
            }
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

        private string Decrypt(byte[] cipherText)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = EncryptionKey;

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
