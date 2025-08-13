using System;
using System.IO;
using FluentAssertions;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Models;
using Xunit;

namespace OnlineMongoMigrationProcessor.Tests
{
    public class HelperTests
    {
        [Theory]
        [InlineData("mongodb://user:pa$$@host/db", "mongodb://user:pa%24%24@host/db")]
        [InlineData("mongodb+srv://u:p@cluster.mongodb.net/?retryWrites=true", "mongodb+srv://u:p@cluster.mongodb.net/?retryWrites=true")]
        public void EncodeMongoPasswordInConnectionString_EncodesPassword(string input, string expected)
        {
            var result = Helper.EncodeMongoPasswordInConnectionString(input);
            result.Should().Be(expected);
        }

        [Theory]
        [InlineData("db.col", true)]
        [InlineData("db1.col1,db2.col2", true)]
        [InlineData("db.col with space", false)]
        [InlineData("db. col", false)]
        public void ValidateNamespaceFormat_Validates(string input, bool valid)
        {
            var (ok, cleaned,errorMessage) = Helper.ValidateNamespaceFormat(input,JobType.DumpAndRestore);
            ok.Should().Be(valid);
            if (valid) cleaned.Should().NotBeNullOrWhiteSpace();
        }

        [Fact]
        public void RedactPii_ShouldRedactUserAndPassword()
        {
            var input = "mongodb://user:secret@host/db";
            var redacted = Helper.RedactPii(input);
            redacted.Should().Contain("[REDACTED]:[REDACTED]");
        }

        [Fact]
        public void SafeFileName_RemovesInvalidChars_And_Truncates()
        {
            var name = new string('a', 300) + ":*?\"<>|";
            var safe = Helper.SafeFileName(name);
            safe.Should().NotContainAny(new[] {":","*","?","\"","<",">","|","\\","/"});
            safe.Length.Should().BeLessOrEqualTo(255);
        }

        [Fact]
        public void UpdateAppName_AddsQueryParam_WhenParsable()
        {
            var cs = "https://example.com/?retryWrites=true";
            var updated = Helper.UpdateAppName(cs, "myapp");
            updated.Should().Contain("appName=myapp");
        }

        [Fact]
        public void ExtractHost_ReturnsHost_WhenPresent()
        {
            var cs = "mongodb://user:pwd@my.host:27017/db";
            var host = Helper.ExtractHost(cs);
            host.Should().Be("my.host:27017");
        }
    }
}
