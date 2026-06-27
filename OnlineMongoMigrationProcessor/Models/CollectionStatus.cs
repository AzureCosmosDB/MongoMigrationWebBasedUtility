namespace OnlineMongoMigrationProcessor
{
    public enum CollectionStatus
    {
        Unknown,
        OK,
        NotFound,
        IsView,
        IsTimeSeries
    }
}