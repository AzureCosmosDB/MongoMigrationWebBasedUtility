namespace OnlineMongoMigrationProcessor
{
    public enum DataType
    {
        ObjectId,
        Int,
        Int64,
        Decimal128,
        Date,
        BinData,
        String,
        Object,
        Other,
        // Appended after Other to preserve existing integer values for already-persisted jobs.
        Double,
        Timestamp
    }
}