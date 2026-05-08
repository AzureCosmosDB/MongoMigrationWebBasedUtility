using MongoDB.Bson;
using MongoDB.Bson.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    /// <summary>
    /// Converts relaxed MongoDB queries into strict Extended JSON for mongodump.
    /// </summary>
    public static class MongoQueryConverter
    {

        public static string ConvertMondumpFilter(string query, BsonValue? gte, BsonValue? lt, BsonValue? lte, DataType dataType)
        {
            if(dataType!= DataType.Object )
            {
                return query;
            }
            else
            {
                return CreateMongoDumpFilter(gte, lt, lte);
            }

        }


        public static string CreateMongoDumpFilter(BsonValue? gte, BsonValue? lt, BsonValue? lte)
        {
            var ops = new List<string>();
            
            // Add $gte if it's not null and not BsonNull
            if (gte is not null && !(gte is BsonNull))
                ops.Add($"\"$gte\": {gte.ToJson()}");
            
            // Add $lt if it's not null and not BsonNull (prefer $lt over $lte)
            if (lt is not null && !(lt is BsonNull))
                ops.Add($"\"$lt\": {lt.ToJson()}");
            // Add $lte only if $lt is not present/null
            else if (lte is not null && !(lte is BsonNull))
                ops.Add($"\"$lte\": {lte.ToJson()}");
            
            var criteria = $"{{ {string.Join(", ", ops)} }}";
            var filter = $"{{ \"_id\": {criteria} }}";

            // Escape all double quotes for safe use in shell command strings
            return filter.Replace("\"", "\\\"");
        }






    }
}