namespace OnlineMongoMigrationProcessor
{
    public enum ChangeStreamError
    {
        None,
        ResumeTokenExpired,
        WatchFailed
    }
}
