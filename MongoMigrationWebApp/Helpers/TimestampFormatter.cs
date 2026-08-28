namespace MongoMigrationWebApp.Helpers;

public static class TimestampFormatter
{
    // "Mountain Standard Time" is the Windows ID; Linux/macOS (e.g. the app's Docker
    // image) only resolve the IANA equivalent, so fall back to it if the first lookup fails.
    private static readonly TimeZoneInfo MountainTimeZone = ResolveMountainTimeZone();

    private static TimeZoneInfo ResolveMountainTimeZone()
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById("Mountain Standard Time");
        }
        catch (TimeZoneNotFoundException)
        {
            return TimeZoneInfo.FindSystemTimeZoneById("America/Denver");
        }
    }

    public static string MountainTime(DateTime? timestamp)
    {
        if (!timestamp.HasValue || timestamp.Value == DateTime.MinValue)
        {
            return "N/A";
        }

        var utcTimestamp = DateTime.SpecifyKind(timestamp.Value, DateTimeKind.Utc);
        return TimeZoneInfo.ConvertTimeFromUtc(utcTimestamp, MountainTimeZone)
            .ToString("M/d/yyyy h:mm:ss tt");
    }
}