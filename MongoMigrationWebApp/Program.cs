using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection;
using MongoMigrationWebApp.Service;
using OnlineMongoMigrationProcessor;
using System.Security.AccessControl;
using Microsoft.AspNetCore.Components.Authorization;
using OnlineMongoMigrationProcessor.Context;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllersWithViews();

// Register HttpClient and dynamically set the base address using NavigationManager
builder.Services.AddScoped(sp =>
{
    // Retrieve NavigationManager from the service provider
    var navigationManager = sp.GetRequiredService<NavigationManager>();

    // Create and configure HttpClient with dynamic base address
    var client = new HttpClient
    {
        BaseAddress = new Uri(navigationManager.BaseUri)  // Use NavigationManager's BaseUri
    };

    return client;
});

builder.Configuration.AddEnvironmentVariables();

//Map environment variables to configuration keys
var stateStoreCSorPath = Environment.GetEnvironmentVariable("StateStoreConnectionStringOrPath");
if (!string.IsNullOrEmpty(stateStoreCSorPath))
{
    builder.Configuration["StateStore:ConnectionStringOrPath"] = stateStoreCSorPath;
}

var appId = Environment.GetEnvironmentVariable("StateStoreAppID");
if (!string.IsNullOrEmpty(appId))
{
    builder.Configuration["StateStore:AppID"] = appId;
    MigrationJobContext.AppId = appId;
}


var useLocalDisk=Environment.GetEnvironmentVariable("StateStoreUseLocalDisk");
bool useLocal=false;
if (!string.IsNullOrEmpty(useLocalDisk))
{
    bool.TryParse(useLocalDisk, out useLocal);
    builder.Configuration["StateStore:UseLocalDisk"] = useLocal.ToString();
}

builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddSingleton(builder.Configuration);

// Keyring lives in the state store, not the app directory: a redeploy replaces the app
// directory, which would otherwise make every persisted connection string undecryptable.
var dataProtection = builder.Services.AddDataProtection()
    .PersistKeysToFileSystem(new DirectoryInfo(
        Path.Combine(MongoMigrationWebApp.Service.ConnectionStringVault.ResolveStateRoot(builder.Configuration), "dataprotection-keys")))
    .SetApplicationName("MongoMigrationWebUtility");

if (OperatingSystem.IsWindows())
{
    // Otherwise the keyring is plaintext XML next to the payloads it protects, so read access to
    // the state folder alone yields the stored credentials. Machine scope rather than user scope
    // so an app-pool identity change does not orphan the keys. Windows-only API.
    dataProtection.ProtectKeysWithDpapi(protectToLocalMachine: true);
}

builder.Services.AddSingleton<MongoMigrationWebApp.Service.ConnectionStringVault>();
builder.Services.AddSingleton<JobManager>();
builder.Services.AddScoped<FileService>();

// Add authentication services
builder.Services.AddSingleton<PasswordManager>();
builder.Services.AddScoped<AuthenticationService>();
builder.Services.AddScoped<AuthenticationStateProvider, CustomAuthenticationStateProvider>();
builder.Services.AddAuthorizationCore();

var app = builder.Build();

// Warm the job store so the first request doesn't pay for it. A bad StateStore configuration
// is reported here instead of crashing the host, which would crash-loop the container.
try
{
    app.Services.GetRequiredService<JobManager>();
}
catch (Exception ex)
{
    app.Logger.LogError(ex, "Migration job store initialization failed. Check StateStore:ConnectionStringOrPath, StateStore:AppID and the ResourceDrive environment variable.");
}

// _configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.MapBlazorHub();
app.MapFallbackToPage("/_Host");

app.MapControllers(); // Ensure controllers are mapped

app.Run();

// Top-level statements produce an internal entry point; WebApplicationFactory needs a public one.
public partial class Program { }

