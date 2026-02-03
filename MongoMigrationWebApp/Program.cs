using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
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

try
{
    //Map environment variables to configuration keys
    var stateStoreCSorPath = Environment.GetEnvironmentVariable("StateStoreConnectionStringOrPath");
    if (!string.IsNullOrEmpty(stateStoreCSorPath))
    {
        // Check if the value is a file path and read the connection string from the file
        string connectionString;
        if (File.Exists(stateStoreCSorPath))
        {
            // Read connection string from file (single line text file)
            connectionString = File.ReadAllText(stateStoreCSorPath).Trim();
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception($"The file '{stateStoreCSorPath}' is empty. Expected a connection string.");
            }
        }
        else
        {
            // Use the value directly as connection string
            connectionString = stateStoreCSorPath;
        }
        builder.Configuration["StateStore:ConnectionStringOrPath"] = connectionString;
    }
    else
    {
        throw new Exception("StateStoreConnectionStringOrPath environment variable is not set.");
    }
    var appId = Environment.GetEnvironmentVariable("StateStoreAppID");
    if (!string.IsNullOrEmpty(appId))
    {
        builder.Configuration["StateStore:AppID"] = appId;
        MigrationJobContext.AppId= appId;
    }
    bool.TryParse( Environment.GetEnvironmentVariable("StateStoreUseLocalDisk"), out var useLocal);
    builder.Configuration["StateStore:UseLocalDisk"] = useLocal.ToString();
}
catch
{
    // Ignore errors
}

builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddSingleton(builder.Configuration);
builder.Services.AddSingleton<JobManager>();
builder.Services.AddScoped<FileService>();

// Add authentication services
builder.Services.AddSingleton<PasswordManager>();
builder.Services.AddScoped<AuthenticationService>();
builder.Services.AddScoped<AuthenticationStateProvider, CustomAuthenticationStateProvider>();
builder.Services.AddAuthorizationCore();

var app = builder.Build();

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

