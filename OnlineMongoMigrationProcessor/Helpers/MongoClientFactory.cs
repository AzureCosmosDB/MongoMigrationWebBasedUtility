using MongoDB.Driver;
using System;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Web;
using static System.Net.Mime.MediaTypeNames;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public static class MongoClientFactory
    {
        public static MongoClient Create(Log log,string connectionString, bool ReadConcernMajority=false, string? PEMFileContents=null)
        {
            
            var cleanConnStr = RemovePemPathFromConnectionString(connectionString);

            var settings = MongoClientSettings.FromUrl(new MongoUrl(cleanConnStr));

        if (!string.IsNullOrWhiteSpace(PEMFileContents))
            {

                settings.SslSettings = new SslSettings
                {
                    ServerCertificateValidationCallback = ValidateAmazonDocDbCertificate(log,PEMFileContents!)
                };
            }

        if(ReadConcernMajority)
        {
           settings.ReadConcern = ReadConcern.Majority;
        }

            return new MongoClient(settings);
        }

        private static string RemovePemPathFromConnectionString(string connStr)
        {
            
            string baseConnStr=string.Empty;
            string queryString = string.Empty;

            var parts = connStr.Split('?');
            if(parts.Length >= 1)
                baseConnStr= parts[0];
            if(parts.Length >=2)
                queryString = parts[1];

            if(string.IsNullOrWhiteSpace(queryString))
                return baseConnStr;

            var query = HttpUtility.ParseQueryString(queryString);
            query.Remove("pemPath");

            var keys = query.AllKeys ?? Array.Empty<string>();
            var newQuery = string.Join("&", keys
                .Where(k => !string.IsNullOrWhiteSpace(k))
                .Select(k =>
                    $"{HttpUtility.UrlEncode((k ?? string.Empty).Trim())}={HttpUtility.UrlEncode((query[k] ?? string.Empty).Trim())}"));


            return string.IsNullOrWhiteSpace(newQuery) ? baseConnStr : $"{baseConnStr}?{newQuery}";
        }

        private static RemoteCertificateValidationCallback ValidateAmazonDocDbCertificate(Log log,string pem)
        {
            return (sender, certificate, chain, sslPolicyErrors) =>
            {
                try
                {
                    var caCerts = new X509Certificate2Collection();

                    var certs = SplitPemCertificates(pem);

                    foreach (var certText in certs)
                    {
                        var raw = Convert.FromBase64String(certText);
                        #pragma warning disable SYSLIB0057
                        var cert = new X509Certificate2(raw);
                        #pragma warning restore SYSLIB0057
                        caCerts.Add(cert);
                    }

                    if (chain == null || certificate == null)
                        return false;

                    chain.ChainPolicy.ExtraStore.AddRange(caCerts);
                    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

                    var isValid = chain.Build((X509Certificate2)certificate);

                    if (!isValid)
                    {
                        Console.WriteLine("Certificate chain build failed.");
                    }

                    return isValid;
                }
                catch (Exception ex)
                {
                    log.WriteLine($"Certificate validation failed: { ex.ToString()}");
                    
                    return false;
                }
            };
        }

        private static string[] SplitPemCertificates(string pemContent)
        {
            const string header = "-----BEGIN CERTIFICATE-----";
            const string footer = "-----END CERTIFICATE-----";

            var certs = new List<string>();
            int start = 0;

            while ((start = pemContent.IndexOf(header, start)) != -1)
            {
                int end = pemContent.IndexOf(footer, start);
                if (end == -1) break;

                int certStart = start + header.Length;
                string base64 = pemContent.Substring(certStart, end - certStart)
                    .Replace("\n", "")
                    .Replace("\r", "")
                    .Trim();

                certs.Add(base64);
                start = end + footer.Length;
            }

            return certs.ToArray();
        }
    }
}
