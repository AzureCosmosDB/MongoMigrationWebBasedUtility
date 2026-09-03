using System.Globalization;
using MongoMigrationWebApp.Helpers;
using OnlineMongoMigrationProcessor;
using Xunit;

namespace MongoMigrationWebApp.Tests;

public sealed class HelperExtractHostTests
{
    /// <summary>
    /// The original implementation measured its indices against the URL-encoded string but sliced
    /// the raw one, so any password that needed escaping shifted the result. It also indexed
    /// Split('@')[1] unconditionally, which threw for a credential-less string.
    /// </summary>
    [Theory]
    [InlineData("mongodb://user:p@ss word@host1:27017/?replicaSet=rs", "host1:27017")]
    [InlineData("mongodb://user:p%40ss@host1:27017,host2:27017/?replicaSet=rs", "host1:27017,host2:27017")]
    [InlineData("mongodb://host1:27017,host2:27017/?replicaSet=rs", "host1:27017,host2:27017")]
    [InlineData("mongodb+srv://u:p@cluster0.mongocluster.cosmos.azure.com/?tls=true", "cluster0.mongocluster.cosmos.azure.com")]
    [InlineData("mongodb://user:pass@host1:27017", "host1:27017")]
    [InlineData("host1:27017", "")]
    public void Extracts_the_host(string connectionString, string expected)
    {
        Assert.Equal(expected, Helper.ExtractHost(connectionString));
    }
}

public sealed class TimestampFormatterTests
{
    public TimestampFormatterTests()
    {
        // The formatter renders with the ambient culture, which varies by runner.
        CultureInfo.CurrentCulture = new CultureInfo("en-US");
    }

    [Theory]
    [InlineData(1, 15, "1/15/2026 11:30:00 AM")] // MST, UTC-7
    [InlineData(7, 15, "7/15/2026 12:30:00 PM")] // MDT, UTC-6
    public void Converts_utc_to_mountain_time(int month, int day, string expected)
    {
        var utc = new DateTime(2026, month, day, 18, 30, 0, DateTimeKind.Utc);

        Assert.Equal(expected, TimestampFormatter.MountainTime(utc));
    }

    [Fact]
    public void Treats_an_unspecified_kind_as_utc()
    {
        var unspecified = new DateTime(2026, 1, 15, 18, 30, 0, DateTimeKind.Unspecified);

        Assert.Equal("1/15/2026 11:30:00 AM", TimestampFormatter.MountainTime(unspecified));
    }

    [Fact]
    public void Renders_a_missing_timestamp_as_not_available()
    {
        Assert.Equal("N/A", TimestampFormatter.MountainTime(null));
        Assert.Equal("N/A", TimestampFormatter.MountainTime(DateTime.MinValue));
    }
}
