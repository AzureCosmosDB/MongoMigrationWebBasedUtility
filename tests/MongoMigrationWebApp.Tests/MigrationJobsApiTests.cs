using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using MongoMigrationWebApp.Service;
using Xunit;

namespace MongoMigrationWebApp.Tests;

/// <summary>
/// Covers the fork-added migration job API (MongoMigrationWebApp/Controller/MigrationJobsController.cs).
/// Only paths that stop before a migration actually starts are exercised — nothing here needs, or
/// contacts, a MongoDB server.
/// </summary>
public sealed class MigrationJobsApiTests : IClassFixture<MigrationApiFactory>, IAsyncLifetime
{
    private const string SourceConnectionString = "mongodb://user:p@ss word@source.example:27017/?replicaSet=rs";
    private const string SourceEndpoint = "source.example:27017";
    private const string TargetConnectionString = "mongodb://admin:secret@target.example:10260/?tls=true";

    private readonly MigrationApiFactory _factory;

    public MigrationJobsApiTests(MigrationApiFactory factory)
    {
        _factory = factory;
    }

    public async Task InitializeAsync()
    {
        await _factory.EnsurePasswordSetAsync();

        // Jobs live in a process-wide static store, so every test starts from a known-empty one.
        using var client = _factory.CreateApiClient();
        (await client.PostAsync("/api/migration-jobs/reset", null)).EnsureSuccessStatusCode();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Rejects_a_request_from_off_the_host()
    {
        using var client = _factory.CreateApiClient(remoteIp: "203.0.113.10");

        var response = await client.GetAsync("/api/migration-jobs");

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("not-the-password")]
    public async Task Rejects_a_missing_or_wrong_password(string? password)
    {
        using var client = _factory.CreateApiClient(password);

        var response = await client.GetAsync("/api/migration-jobs");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Reports_unavailable_until_the_app_password_is_set()
    {
        var passwordFile = MigrationApiFactory.PasswordFilePath;
        var saved = await File.ReadAllBytesAsync(passwordFile);
        File.Delete(passwordFile);

        try
        {
            using var client = _factory.CreateApiClient();

            var response = await client.GetAsync("/api/migration-jobs");

            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        }
        finally
        {
            await File.WriteAllBytesAsync(passwordFile, saved);
        }
    }

    [Fact]
    public async Task Lists_no_jobs_after_a_reset()
    {
        using var client = _factory.CreateApiClient();

        using var body = await GetJsonAsync(client, "/api/migration-jobs");

        Assert.Equal(0, body.RootElement.GetProperty("count").GetInt32());
        Assert.Empty(body.RootElement.GetProperty("jobs").EnumerateArray());
    }

    [Fact]
    public async Task Reports_an_unknown_job_as_not_found()
    {
        using var client = _factory.CreateApiClient();

        var get = await client.GetAsync($"/api/migration-jobs/{Guid.NewGuid()}");
        var start = await client.PostAsJsonAsync($"/api/migration-jobs/{Guid.NewGuid()}/start", new { });

        Assert.Equal(HttpStatusCode.NotFound, get.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, start.StatusCode);
    }

    [Fact]
    public async Task Rejects_an_import_without_a_job_or_connection_strings()
    {
        using var client = _factory.CreateApiClient();

        var response = await client.PostAsJsonAsync("/api/migration-jobs/import", new { });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Imports_a_job_and_serves_it_back()
    {
        using var client = _factory.CreateApiClient();

        var jobId = await ImportAsync(client, "ci-import");

        using var list = await GetJsonAsync(client, "/api/migration-jobs");
        Assert.Equal(1, list.RootElement.GetProperty("count").GetInt32());

        using var detail = await GetJsonAsync(client, $"/api/migration-jobs/{jobId}");
        var job = detail.RootElement.GetProperty("job");
        Assert.Equal("ci-import", job.GetProperty("name").GetString());
        Assert.False(job.GetProperty("isStarted").GetBoolean());

        // ExtractHost has to strip the credentials off the URL-encoded form of the string.
        Assert.Equal(SourceEndpoint, job.GetProperty("sourceEndpoint").GetString());
        Assert.Equal("target.example:10260", job.GetProperty("targetEndpoint").GetString());

        using var logs = await GetJsonAsync(client, $"/api/migration-jobs/{jobId}/logs");
        Assert.Equal(jobId, logs.RootElement.GetProperty("jobId").GetString());
    }

    /// <summary>
    /// An imported job is never started, so no other path writes its connection strings to the
    /// vault. Without them a recycle leaves the viewer able to offer only a resume with updated
    /// strings, because the in-memory cache is all there ever was.
    /// </summary>
    [Fact]
    public async Task Persists_imported_connection_strings_to_the_vault()
    {
        using var client = _factory.CreateApiClient();

        var jobId = await ImportAsync(client, "ci-import-vault");

        var vault = _factory.Services.GetRequiredService<ConnectionStringVault>();
        Assert.True(vault.TryLoad(jobId, out var source, out var target));
        Assert.Equal(SourceConnectionString, source);
        Assert.Equal(TargetConnectionString, target);
    }

    [Fact]
    public async Task Reuses_the_existing_job_when_a_name_is_imported_twice()
    {
        using var client = _factory.CreateApiClient();

        var first = await ImportAsync(client, "ci-reimport");
        var second = await ImportAsync(client, "ci-reimport");

        Assert.Equal(first, second);

        using var list = await GetJsonAsync(client, "/api/migration-jobs");
        Assert.Equal(1, list.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Refuses_to_start_a_job_against_a_different_cluster()
    {
        using var client = _factory.CreateApiClient();
        var jobId = await ImportAsync(client, "ci-endpoint-guard");

        var response = await client.PostAsJsonAsync($"/api/migration-jobs/{jobId}/start", new
        {
            sourceConnectionString = "mongodb://user:secret@somewhere.else:27017/?replicaSet=rs",
            targetConnectionString = TargetConnectionString
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains(SourceEndpoint, await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Resets_away_every_job()
    {
        using var client = _factory.CreateApiClient();
        await ImportAsync(client, "ci-reset");

        var response = await client.PostAsync("/api/migration-jobs/reset", null);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

        response.EnsureSuccessStatusCode();
        Assert.Equal(1, body.RootElement.GetProperty("deletedJobs").GetInt32());

        using var list = await GetJsonAsync(client, "/api/migration-jobs");
        Assert.Equal(0, list.RootElement.GetProperty("count").GetInt32());
    }

    /// <summary>
    /// The key ring has to outlive a redeploy, which replaces the app directory. Asserted against
    /// the app's own Data Protection wiring in Program.cs rather than a copy of it.
    /// </summary>
    [Fact]
    public void Protects_the_connection_string_key_ring_under_the_state_store()
    {
        var vault = _factory.Services.GetRequiredService<ConnectionStringVault>();
        Assert.True(vault.TrySave(Guid.NewGuid().ToString(), SourceConnectionString, TargetConnectionString));

        var keyRing = Path.Combine(TestEnvironment.StateStore, "dataprotection-keys");
        var keyFile = Assert.Single(Directory.GetFiles(keyRing, "key-*.xml"));

        if (OperatingSystem.IsWindows())
        {
            // An unprotected key ring stores the key as a plaintext <masterKey> next to the
            // payloads it protects, so read access to the state folder would yield the credentials.
            var xml = File.ReadAllText(keyFile);
            Assert.DoesNotContain("<masterKey", xml, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("encryptedKey", xml, StringComparison.OrdinalIgnoreCase);
        }
    }

    private static async Task<string> ImportAsync(HttpClient client, string name)
    {
        var response = await client.PostAsJsonAsync("/api/migration-jobs/import", new
        {
            job = new { name },
            sourceConnectionString = SourceConnectionString,
            targetConnectionString = TargetConnectionString
        });

        response.EnsureSuccessStatusCode();

        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return body.RootElement.GetProperty("id").GetString()!;
    }

    private static async Task<JsonDocument> GetJsonAsync(HttpClient client, string requestUri)
    {
        var response = await client.GetAsync(requestUri);
        response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync());
    }
}
