using MongoDB.Bson;
using System;
using System.Globalization;

namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// Stateless utilities for inspecting MongoDB v1 change-stream resume tokens
    /// (the BSON document <c>{ _data: &lt;hex&gt; }</c> form). Used by change-stream
    /// processors for diagnostic logging and for picking the oldest token from a
    /// set of stuck cursors.
    /// </summary>
    public static class ResumeTokenInspector
    {
        /// <summary>
        /// Returns a short stable identifier (last 12 chars) for a resume token JSON
        /// so log lines can correlate tokens across rounds without dumping full BSON.
        /// </summary>
        public static string ShortHash(string token)
        {
            if (string.IsNullOrEmpty(token)) return "<empty>";
            int len = token.Length;
            return len <= 12 ? token : token.Substring(len - 12);
        }

        /// <summary>
        /// Decode the leading 4-byte unix timestamp embedded in a v1 resume token.
        /// (The <c>_data</c> hex starts with type byte 0x82 then 4 bytes BE seconds.)
        /// Returns <c>true</c> and the UTC <see cref="DateTime"/> when parsing succeeds.
        /// </summary>
        public static bool TryDecodeUtc(string tokenJson, out DateTime ts)
        {
            ts = DateTime.MinValue;
            try
            {
                if (string.IsNullOrEmpty(tokenJson)) return false;
                var doc = BsonDocument.Parse(tokenJson);
                if (!doc.Contains("_data")) return false;
                string hex = doc["_data"].AsString;
                if (hex.Length < 10) return false;
                // v1 resume tokens start with the KeyString type byte 0x82; if absent,
                // the format is unknown and our byte offsets would be wrong.
                if (!hex.StartsWith("82", StringComparison.OrdinalIgnoreCase)) return false;
                uint seconds = Convert.ToUInt32(hex.Substring(2, 8), 16);
                ts = DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Convenience wrapper around <see cref="TryDecodeUtc"/> that returns the
        /// ISO-8601 representation or the literal <c>"?"</c> when undecodable.
        /// </summary>
        public static string DecodeUtcString(string tokenJson)
        {
            return TryDecodeUtc(tokenJson, out var ts)
                ? ts.ToString("o", CultureInfo.InvariantCulture)
                : "?";
        }

        /// <summary>
        /// Extracts the 16-hex-char (ts + ordinal) prefix that follows the leading
        /// type byte 0x82 in a v1 resume token. Both fields are BE-encoded unsigned
        /// 32-bit integers, so lexicographic comparison of this prefix is identical
        /// to numeric comparison of the (ts, ordinal) tuple. Use this when you need
        /// strict oplog ordering at sub-second resolution (e.g. backward-rollback
        /// protection when many events share the same second).
        /// </summary>
        public static bool TryGetClusterPositionKey(string tokenJson, out string key)
        {
            key = string.Empty;
            try
            {
                if (string.IsNullOrEmpty(tokenJson)) return false;
                var doc = BsonDocument.Parse(tokenJson);
                if (!doc.Contains("_data")) return false;
                string hex = doc["_data"].AsString;
                // 2 (type byte) + 8 (ts) + 8 (ordinal) = 18 hex chars minimum.
                if (hex.Length < 18) return false;
                if (!hex.StartsWith("82", StringComparison.OrdinalIgnoreCase)) return false;
                key = hex.Substring(2, 16).ToUpperInvariant();
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
