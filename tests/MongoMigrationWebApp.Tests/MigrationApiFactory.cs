using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoMigrationWebApp.Controller;
using MongoMigrationWebApp.Service;
using OnlineMongoMigrationProcessor;

namespace MongoMigrationWebApp.Tests;

/// <summary>
/// Boots the real app so the migration job API is exercised through its own middleware pipeline.
/// </summary>
public sealed class MigrationApiFactory : WebApplicationFactory<global::Program>
{
    public const string Password = "ci-test-password-1";

    /// <summary>Overrides the connection's remote address for a single request.</summary>
    public const string RemoteIpHeader = "X-Test-Remote-Ip";

    public static string PasswordFilePath => Path.Combine(Helper.GetWorkingFolder(), "app.password");

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment(Environments.Development);
        builder.ConfigureTestServices(services => services.AddSingleton<IStartupFilter, RemoteIpStartupFilter>());
    }

    public HttpClient CreateApiClient(string? password = Password, string? remoteIp = null)
    {
        var client = CreateClient();
        if (password != null)
        {
            client.DefaultRequestHeaders.Add(MigrationJobsController.AppPasswordHeader, password);
        }

        if (remoteIp != null)
        {
            client.DefaultRequestHeaders.Add(RemoteIpHeader, remoteIp);
        }

        return client;
    }

    public async Task EnsurePasswordSetAsync()
    {
        var passwordManager = Services.GetRequiredService<PasswordManager>();
        if (!await passwordManager.IsPasswordSetAsync())
        {
            await passwordManager.SetPasswordAsync(Password);
        }
    }

    /// <summary>
    /// TestServer never populates the connection's remote address, which the API's loopback guard
    /// reads as "not local" and rejects. Defaulting it to loopback makes the endpoints reachable
    /// while the header keeps the guard itself testable.
    /// </summary>
    private sealed class RemoteIpStartupFilter : IStartupFilter
    {
        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
        {
            app.Use(async (context, nextMiddleware) =>
            {
                var requested = context.Request.Headers[RemoteIpHeader].ToString();
                context.Connection.RemoteIpAddress = string.IsNullOrEmpty(requested)
                    ? IPAddress.Loopback
                    : IPAddress.Parse(requested);

                await nextMiddleware();
            });

            next(app);
        };
    }
}
