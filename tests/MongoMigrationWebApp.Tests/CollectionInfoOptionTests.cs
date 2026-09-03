using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Models;
using Xunit;

namespace MongoMigrationWebApp.Tests;

/// <summary>
/// Overwrite and IndexingStrategy were previously settable only through the Manage Collections
/// UI, so a seeded import could not express them. These cover the wire shape now that they are
/// part of CollectionInfo.
/// </summary>
public sealed class CollectionInfoPerCollectionOptionTests
{
    [Fact]
    public void Omitting_the_new_options_leaves_them_null()
    {
        var json = """
        [{ "DatabaseName": "WarehouseTest", "CollectionName": "*" }]
        """;

        var parsed = JsonConvert.DeserializeObject<List<CollectionInfo>>(json)!;

        Assert.Null(parsed[0].Overwrite);
        Assert.Null(parsed[0].IndexingStrategy);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Overwrite_round_trips(string literal, bool expected)
    {
        var json = $$"""
        [{ "DatabaseName": "db", "CollectionName": "c", "Overwrite": {{literal}} }]
        """;

        var parsed = JsonConvert.DeserializeObject<List<CollectionInfo>>(json)!;

        Assert.Equal(expected, parsed[0].Overwrite);
    }

    [Theory]
    [InlineData("\"DontIndex\"", IndexingStrategy.DontIndex)]
    [InlineData("\"SameAsSource\"", IndexingStrategy.SameAsSource)]
    [InlineData("\"SameAsSourceBlocking\"", IndexingStrategy.SameAsSourceBlocking)]
    [InlineData("2", IndexingStrategy.DontIndex)]
    public void IndexingStrategy_accepts_names_and_numbers(string literal, IndexingStrategy expected)
    {
        var json = $$"""
        [{ "DatabaseName": "db", "CollectionName": "c", "IndexingStrategy": {{literal}} }]
        """;

        var parsed = JsonConvert.DeserializeObject<List<CollectionInfo>>(json)!;

        Assert.Equal(expected, parsed[0].IndexingStrategy);
    }

    [Fact]
    public void Serialising_a_unit_back_out_preserves_the_options()
    {
        var info = new CollectionInfo
        {
            DatabaseName = "db",
            CollectionName = "c",
            Overwrite = true,
            IndexingStrategy = IndexingStrategy.DontIndex
        };

        var round = JsonConvert.DeserializeObject<List<CollectionInfo>>(
            JsonConvert.SerializeObject(new[] { info }))!;

        Assert.True(round[0].Overwrite);
        Assert.Equal(IndexingStrategy.DontIndex, round[0].IndexingStrategy);
    }
}
