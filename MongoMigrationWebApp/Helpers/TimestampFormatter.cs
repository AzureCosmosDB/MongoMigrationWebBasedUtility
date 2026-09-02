namespace MongoMigrationWebApp.Helpers;

public static class TimestampFormatter
{
    // "Mountain Standard Time" is the Windows ID; Linux/macOS (e.g. the app's Docker
    // image) only resolve the IANA equivalent, so fall back to it if the first lookup fails.
    private static readonly TimeZoneInfo MountainTimeZone = ResolveMountainTimeZone();

    private static TimeZoneInfo ResolveMountainTimeZone()
    {
        foreach (var id in new[] { "Mountain Standard Time", "America/Denver" })
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(id);
            }
            catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
            {
                // Try the next id; a throw here would surface as TypeInitializationException on first render.
            }
        }

        return TimeZoneInfo.Utc;
    }

    public static string MountainTime(DateTime? timestamp)
    {
        if (!timestamp.HasValue || timestamp.Value == DateTime.MinValue)
        {
            return "N/A";
        }

        // Unspecified is the common case (values round-tripped through JSON) and is UTC here;
        // Local must be converted rather than relabelled or it shifts by the host offset.
        var value = timestamp.Value;
        var utcTimestamp = value.Kind == DateTimeKind.Local
            ? value.ToUniversalTime()
            : DateTime.SpecifyKind(value, DateTimeKind.Utc);

        return TimeZoneInfo.ConvertTimeFromUtc(utcTimestamp, MountainTimeZone)
            .ToString("M/d/yyyy h:mm:ss tt");
    }
}
