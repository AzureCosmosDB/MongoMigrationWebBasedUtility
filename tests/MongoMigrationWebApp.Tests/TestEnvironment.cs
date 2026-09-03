using System.Runtime.CompilerServices;
using Xunit;

// MigrationJobContext, the job store and Helper's working folder are all process-wide statics.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace MongoMigrationWebApp.Tests;

/// <summary>
/// Points everything the app writes at a per-run sandbox.
/// </summary>
/// <remarks>
/// Helper.GetWorkingFolder caches its answer in a private static on first call and falls back to
/// Path.GetTempPath(), so this has to run before any app code does — hence a module initializer
/// rather than a fixture. Without it the tests would read and overwrite the app.password file and
/// job store of a locally installed copy of the utility.
/// </remarks>
internal static class TestEnvironment
{
    public static string Root { get; private set; } = string.Empty;

    public static string StateStore { get; private set; } = string.Empty;

    [ModuleInitializer]
    internal static void Initialize()
    {
        Root = Path.Combine(Path.GetTempPath(), "mmwu-tests", Guid.NewGuid().ToString("n"));
        StateStore = Path.Combine(Root, "state");
        Directory.CreateDirectory(StateStore);

        // Path.GetTempPath() reads TMP then TEMP, so redirecting both moves the working folder.
        Environment.SetEnvironmentVariable("TMP", Root);
        Environment.SetEnvironmentVariable("TEMP", Root);
        Environment.SetEnvironmentVariable("ResourceDrive", null);

        Environment.SetEnvironmentVariable("StateStoreUseLocalDisk", "true");
        Environment.SetEnvironmentVariable("StateStoreConnectionStringOrPath", StateStore);
        Environment.SetEnvironmentVariable("StateStoreAppID", "mmwu-ci");
    }
}
