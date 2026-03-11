using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

internal sealed class ProbeRequest
{
    public string? ConnectionString { get; set; }
    public string? DatabaseName { get; set; }
    public string? CollectionName { get; set; }
    public int TimeoutSeconds { get; set; } = 30;
    public string? StartAtUtc { get; set; }
    public string? PEMFileContents { get; set; }
}

internal sealed class ProbeResult
{
    public bool Success { get; set; }
    public string? ResumeToken { get; set; }
    public string? OperationType { get; set; }
    public string? ResumeDocumentKey { get; set; }
    public DateTime? CursorUtcTimestamp { get; set; }
    public string? Error { get; set; }
}

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        ProbeResult result;
        try
        {
            string? jsonDebug = args.Length > 1 ? args[1] : null;

            //jsonDebug="{\"ConnectionString\": \"<connstring>\",  \"DatabaseName\": \"<dbname>\",\n  \"CollectionName\": \"<collectionname>\",\n  \"TimeoutSeconds\": 30,\n  \"StartAtUtc\": \"2026-02-19T12:00:00Z\",\n  \"PEMFileContents\": \"\"\n}";
            var request = ParseRequest(args, jsonDebug);
            result = await RunProbeAsync(request);
        }
        catch (Exception ex)
        {
            result = new ProbeResult
            {
                Success = false,
                Error = ex.Message
            };
        }

        Console.WriteLine(JsonSerializer.Serialize(result));
        return result.Success ? 0 : 1;
    }

    private static ProbeRequest ParseRequest(string[] args, string? jsonDebug = null)
    {
        string requestJson;
        
        // If jsonDebug is provided, use it directly (for debugging)
        if (!string.IsNullOrWhiteSpace(jsonDebug))
        {
            requestJson = jsonDebug;
        }
        else if (args.Length < 1)
        {
            throw new InvalidOperationException("Expected base64-encoded JSON request as the first argument.");
        }
        else
        {
            // Try to parse as direct JSON first (for debugging), then try base64
            try
            {
                // Check if it looks like JSON
                var testArg = args[0].Trim();
                if (testArg.StartsWith("{"))
                {
                    requestJson = testArg;
                }
                else
                {
                    requestJson = Encoding.UTF8.GetString(Convert.FromBase64String(args[0]));
                }
            }
            catch
            {
                // If base64 decode fails, try as direct JSON
                requestJson = args[0];
            }
        }

        var request = JsonSerializer.Deserialize<ProbeRequest>(requestJson);

        if (request == null)
            throw new InvalidOperationException("Invalid request payload.");

        if (string.IsNullOrWhiteSpace(request.ConnectionString))
            throw new InvalidOperationException("ConnectionString is required.");
        if (string.IsNullOrWhiteSpace(request.DatabaseName))
            throw new InvalidOperationException("DatabaseName is required.");
        if (string.IsNullOrWhiteSpace(request.CollectionName))
            throw new InvalidOperationException("CollectionName is required.");

        if (request.TimeoutSeconds <= 0)
            request.TimeoutSeconds = 30;

        return request;
    }

    private static async Task<ProbeResult> RunProbeAsync(ProbeRequest request)
    {
        // Debug output to stderr (won't interfere with JSON output on stdout)
        Console.Error.WriteLine($"[DEBUG] Starting probe for {request.DatabaseName}.{request.CollectionName}");
        Console.Error.WriteLine($"[DEBUG] Timeout: {request.TimeoutSeconds}s");
        Console.Error.WriteLine($"[DEBUG] Connection string length: {request.ConnectionString?.Length ?? 0}");
        Console.Error.WriteLine($"[DEBUG] PEM provided: {!string.IsNullOrWhiteSpace(request.PEMFileContents)}");
        
        try
        {
            // Create MongoClient using same pattern as MongoClientFactory.Create
            MongoClient client;
            try
            {
                var cleanConnStr = RemovePemPathFromConnectionString(request.ConnectionString ?? "");
                Console.Error.WriteLine($"[DEBUG] Cleaned connection string length: {cleanConnStr.Length}");
                Console.Error.WriteLine($"[DEBUG] Creating MongoUrl...");
                
                var mongoUrl = new MongoUrl(cleanConnStr);
                Console.Error.WriteLine($"[DEBUG] MongoUrl created, creating settings...");
                
                var settings = MongoClientSettings.FromUrl(mongoUrl);
                Console.Error.WriteLine($"[DEBUG] Settings created successfully");
                
                // Apply SSL settings if PEM certificate provided
                if (!string.IsNullOrWhiteSpace(request.PEMFileContents))
                {
                    Console.Error.WriteLine($"[DEBUG] Applying SSL settings...");
                    settings.SslSettings = new SslSettings
                    {
                        ServerCertificateValidationCallback = ValidateCertificate(request.PEMFileContents!)
                    };
                }
                
                Console.Error.WriteLine($"[DEBUG] Creating MongoClient...");
                client = new MongoClient(settings);
                Console.Error.WriteLine($"[DEBUG] MongoClient created successfully");
            }
            catch (Exception ex)
            {
                // If settings creation fails (e.g., DNS issues in connection string parsing), 
                // return detailed error
                Console.Error.WriteLine($"[DEBUG] Exception during client creation: {ex.GetType().Name}: {ex.Message}");
                var innerMsg = ex.InnerException != null ? $" -> [{ex.InnerException.GetType().Name}] {ex.InnerException.Message}" : "";
                return new ProbeResult
                {
                    Success = false,
                    Error = $"Failed to parse connection string: {ex.GetType().Name}: {ex.Message}{innerMsg}"
                };
            }

            var database = client.GetDatabase(request.DatabaseName);
            var collection = database.GetCollection<BsonDocument>(request.CollectionName);
            Console.Error.WriteLine($"[DEBUG] Collection reference obtained");

            var options = new ChangeStreamOptions
            {
                BatchSize = 500,
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                MaxAwaitTime = TimeSpan.FromMilliseconds(500)
            };

            if (!string.IsNullOrWhiteSpace(request.StartAtUtc) &&
                DateTime.TryParse(request.StartAtUtc, out var startAtUtc))
            {
                var bsonTimestamp = new BsonTimestamp((int)new DateTimeOffset(startAtUtc.ToUniversalTime()).ToUnixTimeSeconds(), 0);
                options.StartAtOperationTime = bsonTimestamp;
                Console.Error.WriteLine($"[DEBUG] StartAtOperationTime set to {startAtUtc}");
            }

            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(request.TimeoutSeconds));

            try
            {
                Console.Error.WriteLine($"[DEBUG] Starting WatchAsync...");
                using var cursor = await collection.WatchAsync<ChangeStreamDocument<BsonDocument>>(
                    pipeline: Array.Empty<BsonDocument>(),
                    options: options,
                    cancellationToken: timeoutCts.Token);
                Console.Error.WriteLine($"[DEBUG] WatchAsync succeeded, cursor created");

                while (!timeoutCts.Token.IsCancellationRequested)
                {
                    var hasNext = await cursor.MoveNextAsync(timeoutCts.Token);
                    if (!hasNext)
                    {
                        continue;
                    }

                    foreach (var change in cursor.Current)
                    {
                        var timestamp = DateTime.UtcNow;
                        if (change.ClusterTime != null)
                        {
                            timestamp = DateTimeOffset.FromUnixTimeSeconds(change.ClusterTime.Timestamp).UtcDateTime;
                        }
                        else if (change.WallTime.HasValue)
                        {
                            timestamp = change.WallTime.Value.ToUniversalTime();
                        }

                        return new ProbeResult
                        {
                            Success = true,
                            ResumeToken = change.ResumeToken?.ToJson(),
                            OperationType = change.OperationType.ToString(),
                            ResumeDocumentKey = change.DocumentKey?.ToJson(),
                            CursorUtcTimestamp = timestamp
                        };
                    }
                }

                return new ProbeResult
                {
                    Success = false,
                    Error = "Timeout - no changes detected within the time window"
                };
            }
            catch (OperationCanceledException)
            {
                return new ProbeResult
                {
                    Success = false,
                    Error = "Timeout - no changes detected within the time window"
                };
            }
            catch (TimeoutException tex)
            {
                return new ProbeResult
                {
                    Success = false,
                    Error = $"Timeout: {tex.Message}"
                };
            }
        }
        catch (MongoAuthenticationException mae)
        {
            return new ProbeResult
            {
                Success = false,
                Error = $"Authentication error: {mae.Message}"
            };
        }
        catch (MongoCommandException cmdEx)
        {
            return new ProbeResult
            {
                Success = false,
                Error = $"MongoDB command error: {cmdEx.Message}"
            };
        }
        catch (MongoConnectionException mce)
        {
            var innerMsg = mce.InnerException != null ? $" Inner: {mce.InnerException.Message}" : "";
            var innerType = mce.InnerException != null ? $" ({mce.InnerException.GetType().Name})" : "";
            return new ProbeResult
            {
                Success = false,
                Error = $"Connection error: {mce.Message}{innerType}{innerMsg}"
            };
        }
        catch (Exception ex)
        {
            // Build detailed error with exception chain
            var errorMsg = new System.Text.StringBuilder();
            errorMsg.Append($"{ex.GetType().Name}: {ex.Message}");
            
            var currentEx = ex.InnerException;
            var depth = 1;
            while (currentEx != null && depth <= 3)
            {
                errorMsg.Append($" -> [{currentEx.GetType().Name}] {currentEx.Message}");
                currentEx = currentEx.InnerException;
                depth++;
            }
            
            // For DNS errors, add the connection string host (sanitized)
            if (ex.Message.Contains("DNS") || ex.Message.Contains("dns") || 
                (ex.InnerException?.Message.Contains("DNS") ?? false))
            {
                try
                {
                    var connStr = request.ConnectionString ?? "";
                    var hostPart = connStr.Contains("@") 
                        ? connStr.Substring(connStr.IndexOf("@") + 1).Split('/').FirstOrDefault() 
                        : connStr.Split('/').Skip(2).FirstOrDefault();
                    if (!string.IsNullOrEmpty(hostPart))
                    {
                        errorMsg.Append($" | Host: {hostPart}");
                    }
                }
                catch { /* ignore parse errors */ }
            }

            return new ProbeResult
            {
                Success = false,
                Error = errorMsg.ToString()
            };
        }
    }

    private static string RemovePemPathFromConnectionString(string connStr)
    {
        if (string.IsNullOrWhiteSpace(connStr))
            return connStr;

        var parts = connStr.Split('?');
        if (parts.Length < 2)
            return connStr;

        var baseConnStr = parts[0];
        var queryString = parts[1];

        // Simple query parameter removal for pemPath
        var queryParts = queryString.Split('&')
            .Where(p => !p.StartsWith("pemPath=", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (queryParts.Length == 0)
            return baseConnStr;

        return $"{baseConnStr}?{string.Join("&", queryParts)}";
    }

    private static RemoteCertificateValidationCallback ValidateCertificate(string pem)
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

                return chain.Build((X509Certificate2)certificate);
            }
            catch
            {
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
