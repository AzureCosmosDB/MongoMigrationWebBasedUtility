using System.Collections.Generic;
using FluentAssertions;
using OnlineMongoMigrationProcessor.Helpers;
using Xunit;

namespace OnlineMongoMigrationProcessor.Tests
{
    public class SafeDictionaryTests
    {
        [Fact]
        public void AddOrUpdate_InsertsAndUpdates()
        {
            var dict = new SafeDictionary<string, int>();
            dict.AddOrUpdate("a", 1);
            dict.AddOrUpdate("a", 2);

            dict.TryGet("a", out var value).Should().BeTrue();
            value.Should().Be(2);
            dict.Count.Should().Be(1);
        }

        [Fact]
        public void Remove_And_ContainsKey_Behaves()
        {
            var dict = new SafeDictionary<string, int>();
            dict.AddOrUpdate("a", 1);
            dict.ContainsKey("a").Should().BeTrue();

            dict.Remove("a").Should().BeTrue();
            dict.ContainsKey("a").Should().BeFalse();
        }

        [Fact]
        public void TryGetFirst_ReturnsAnyItem_WhenAvailable()
        {
            var dict = new SafeDictionary<string, int>();
            dict.AddOrUpdate("a", 1);
            dict.AddOrUpdate("b", 2);

            dict.TryGetFirst(out var mu).Should().BeTrue();
            new HashSet<string> { "a", "b" }.Should().Contain(mu.Key);
            new HashSet<int> { 1, 2 }.Should().Contain(mu.Value);
        }
    }
}
