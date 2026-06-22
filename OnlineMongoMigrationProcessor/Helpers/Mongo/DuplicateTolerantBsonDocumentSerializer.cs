using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    /// <summary>
    /// A BsonDocument serializer that tolerates duplicate field names anywhere in the
    /// document tree. Some servers (notably Azure DocumentDB) return
    /// currentOp results with duplicate field names (e.g. `createIndexes`) that the
    /// default BsonDocumentSerializer refuses to deserialize.
    /// </summary>
    internal sealed class DuplicateTolerantBsonDocumentSerializer : SerializerBase<BsonDocument>
    {
        public static readonly DuplicateTolerantBsonDocumentSerializer Instance = new();

        public override BsonDocument Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            return ReadDocument(context.Reader);
        }

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, BsonDocument value)
        {
            BsonDocumentSerializer.Instance.Serialize(context, args, value);
        }

        private static BsonDocument ReadDocument(IBsonReader reader)
        {
            var doc = new BsonDocument { AllowDuplicateNames = true };
            reader.ReadStartDocument();
            while (reader.ReadBsonType() != BsonType.EndOfDocument)
            {
                var name = reader.ReadName();
                var value = ReadValue(reader);
                doc.Add(name, value);
            }
            reader.ReadEndDocument();
            return doc;
        }

        private static BsonArray ReadArray(IBsonReader reader)
        {
            var arr = new BsonArray();
            reader.ReadStartArray();
            while (reader.ReadBsonType() != BsonType.EndOfDocument)
            {
                arr.Add(ReadValue(reader));
            }
            reader.ReadEndArray();
            return arr;
        }

        private static BsonValue ReadValue(IBsonReader reader)
        {
            switch (reader.GetCurrentBsonType())
            {
                case BsonType.Document:
                    return ReadDocument(reader);
                case BsonType.Array:
                    return ReadArray(reader);
                case BsonType.Double:
                    return new BsonDouble(reader.ReadDouble());
                case BsonType.String:
                    return new BsonString(reader.ReadString());
                case BsonType.Boolean:
                    return new BsonBoolean(reader.ReadBoolean());
                case BsonType.Int32:
                    return new BsonInt32(reader.ReadInt32());
                case BsonType.Int64:
                    return new BsonInt64(reader.ReadInt64());
                case BsonType.DateTime:
                    return new BsonDateTime(reader.ReadDateTime());
                case BsonType.Null:
                    reader.ReadNull();
                    return BsonNull.Value;
                case BsonType.Undefined:
                    reader.ReadUndefined();
                    return BsonUndefined.Value;
                case BsonType.ObjectId:
                    return new BsonObjectId(reader.ReadObjectId());
                case BsonType.Binary:
                    return new BsonBinaryData(reader.ReadBinaryData().Bytes, reader.ReadBinaryData().SubType);
                case BsonType.Symbol:
                    return BsonSymbolTable.Lookup(reader.ReadSymbol());
                case BsonType.JavaScript:
                    return new BsonJavaScript(reader.ReadJavaScript());
                case BsonType.JavaScriptWithScope:
                    var code = reader.ReadJavaScriptWithScope();
                    var scope = ReadDocument(reader);
                    return new BsonJavaScriptWithScope(code, scope);
                case BsonType.RegularExpression:
                    return reader.ReadRegularExpression();
                case BsonType.Timestamp:
                    return new BsonTimestamp(reader.ReadTimestamp());
                case BsonType.Decimal128:
                    return new BsonDecimal128(reader.ReadDecimal128());
                case BsonType.MinKey:
                    reader.ReadMinKey();
                    return BsonMinKey.Value;
                case BsonType.MaxKey:
                    reader.ReadMaxKey();
                    return BsonMaxKey.Value;
                default:
                    reader.SkipValue();
                    return BsonNull.Value;
            }
        }
    }
}
