using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoMigrationWebApp.Service;
using Xunit;

namespace MongoMigrationWebApp.Tests;

/// <summary>
/// The fork's fix for connection strings being lost on an app-pool recycle, iisreset or redeploy:
/// they used to live only in a static dictionary, so a started job could not be resumed without
/// re-entering them.
/// </summary>
public sealed class ConnectionStringVaultTests : IDisposable
{
    private const string Source = "mongodb://user:p@ss word@source.example:27017/?replicaSet=rs";
    private const string Target = "mongodb://admin:s3cr3t-target@target.example:10260/?tls=true";

    private readonly string _root = Path.Combine(TestEnvironment.Root, "vault", Guid.NewGuid().ToString("n"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Fact]
    public void Stored_strings_survive_a_new_process()
    {
        var jobId = Guid.NewGuid().ToString();

        Assert.True(CreateVault().TrySave(jobId, Source, Target));

        // A separate vault over a separate provider: what the app gets after a recycle.
        Assert.True(CreateVault().TryLoad(jobId, out var source, out var target));
        Assert.Equal(Source, source);
        Assert.Equal(Target, target);
    }

    [Fact]
    public void Stores_the_payload_encrypted()
    {
        var jobId = Guid.NewGuid().ToString();
        CreateVault().TrySave(jobId, Source, Target);

        var payload = File.ReadAllText(Path.Combine(_root, "connections", jobId + ".dat"));

        Assert.DoesNotContain("s3cr3t-target", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("source.example", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void Reports_a_job_it_never_stored_as_absent()
    {
        Assert.False(CreateVault().TryLoad(Guid.NewGuid().ToString(), out _, out _));
    }

    [Fact]
    public void Removes_a_single_job_without_touching_the_others()
    {
        var kept = Guid.NewGuid().ToString();
        var removed = Guid.NewGuid().ToString();
        var vault = CreateVault();
        vault.TrySave(kept, Source, Target);
        vault.TrySave(removed, Source, Target);

        vault.Remove(removed);

        Assert.False(vault.TryLoad(removed, out _, out _));
        Assert.True(vault.TryLoad(kept, out _, out _));
    }

    [Fact]
    public void Removes_everything_on_a_reset()
    {
        var jobId = Guid.NewGuid().ToString();
        var vault = CreateVault();
        vault.TrySave(jobId, Source, Target);

        vault.RemoveAll();

        Assert.False(vault.TryLoad(jobId, out _, out _));
    }

    /// <summary>Mirrors how Program.cs wires the vault up, minus the app itself.</summary>
    private ConnectionStringVault CreateVault()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["StateStore:ConnectionStringOrPath"] = _root })
            .Build();

        var services = new ServiceCollection();
        var dataProtection = services.AddDataProtection()
            .PersistKeysToFileSystem(new DirectoryInfo(Path.Combine(_root, "dataprotection-keys")))
            .SetApplicationName("MongoMigrationWebUtility");

        if (OperatingSystem.IsWindows())
        {
            dataProtection.ProtectKeysWithDpapi(protectToLocalMachine: true);
        }

        var provider = services.BuildServiceProvider();
        return new ConnectionStringVault(provider.GetRequiredService<IDataProtectionProvider>(), configuration);
    }
}
