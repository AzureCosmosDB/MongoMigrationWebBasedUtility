using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;

#if !LEGACY_MONGODB_DRIVER
namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// Recovers per-collection change-stream cursors whose own watch has gone
    /// silent (postBatchResumeToken frozen for multiple consecutive idle rounds).
    /// Owners enqueue stuck migration units; <see cref="ResolveAsync"/> is invoked
    /// between rounds and opens a single cluster-wide change stream anchored at
    /// the oldest enqueued resume token. For each event whose namespace matches an
    /// enqueued unit, the unit's resume token is advanced to the PBRT captured
    /// BEFORE the surfacing <c>MoveNextAsync</c> — guaranteeing the matched event
    /// is re-emitted on the unit's own next round (no skipped data). Any units
    /// the cluster watch never sees are fast-forwarded to the cursor's final PBRT.
    /// </summary>
    internal sealed class StuckCursorUnblocker
    {
        public sealed class UnblockEntry
        {
            public MigrationUnit Mu { get; init; } = null!;
            public string ResumeToken { get; init; } = string.Empty;
            public DateTime AddedUtc { get; init; }
            public DateTime PbrtClusterTimeUtc { get; init; }
        }

        private readonly Log _log;
        private readonly MongoClient _changeStreamMongoClient;
        private readonly bool _syncBack;
        private readonly string _syncBackPrefix;
        private readonly Func<float, int> _getBatchDurationInSeconds;
        private readonly Action<MigrationUnit, bool> _trySaveMigrationUnit;
        private readonly Func<bool> _shouldSplitLargeEvents;
        private readonly ConcurrentDictionary<string, UnblockEntry> _entries =
            new ConcurrentDictionary<string, UnblockEntry>(StringComparer.Ordinal);

        public StuckCursorUnblocker(
            Log log,
            MongoClient changeStreamMongoClient,
            bool syncBack,
            string syncBackPrefix,
            Func<float, int> getBatchDurationInSeconds,
            Action<MigrationUnit, bool> trySaveMigrationUnit,
            Func<bool> shouldSplitLargeEvents)
        {
            _log = log;
            _changeStreamMongoClient = changeStreamMongoClient;
            _syncBack = syncBack;
            _syncBackPrefix = syncBackPrefix ?? string.Empty;
            _getBatchDurationInSeconds = getBatchDurationInSeconds;
            _trySaveMigrationUnit = trySaveMigrationUnit;
            _shouldSplitLargeEvents = shouldSplitLargeEvents ?? (() => false);
        }

        public int Count => _entries.Count;
        public bool IsEmpty => _entries.IsEmpty;

        /// <summary>
        /// Enqueue a stuck migration unit. Each collection is processed at most
        /// once per round and <see cref="ResolveAsync"/> drains the queue between
        /// rounds, so no duplicate key is ever observed in practice.
        /// </summary>
        public void Enqueue(MigrationUnit mu, string collectionKey, string? postBatchTokenJson)
        {
            if (mu == null || string.IsNullOrEmpty(collectionKey) || string.IsNullOrEmpty(postBatchTokenJson))
                return;

            ResumeTokenInspector.TryDecodeUtc(postBatchTokenJson!, out DateTime pbrtTs);

            _entries[collectionKey] = new UnblockEntry
            {
                Mu = mu,
                ResumeToken = postBatchTokenJson!,
                AddedUtc = DateTime.UtcNow,
                PbrtClusterTimeUtc = pbrtTs,
            };

            _log.WriteLine(
                $"{_syncBackPrefix}[PBRT Unblock] enqueued {collectionKey} tokenHash={ResumeTokenInspector.ShortHash(postBatchTokenJson!)} pbrtTs={FormatTs(pbrtTs)} listSize={_entries.Count}",
                LogType.Info);
        }

        /// <summary>Remove an enqueued entry by collection key (called when an MU is removed from the job).</summary>
        public void Remove(string collectionKey)
        {
            if (!string.IsNullOrEmpty(collectionKey))
                _entries.TryRemove(collectionKey, out _);
        }

        /// <summary>
        /// Walk a single cluster-wide change stream to advance every currently
        /// enqueued unit. Safe to call between rounds; assumes no concurrent
        /// writers to the queue while running.
        /// </summary>
        public async Task ResolveAsync(CancellationToken cancellationToken)
        {
            if (_entries.IsEmpty) return;

            // Local "still pending" view; _entries is the source of truth and is
            // mutated only when an entry is actually resolved or fast-forwarded.
            var working = _entries.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
            if (working.Count == 0) return;

            UnblockEntry oldest = PickOldest(working.Values);

            _log.WriteLine(
                $"{_syncBackPrefix}[PBRT Unblock] resolving {working.Count} stuck MU(s) via cluster-watch; oldestKey={oldest.Mu.DatabaseName}.{oldest.Mu.CollectionName} pbrtTs={FormatTs(oldest.PbrtClusterTimeUtc)} tokenHash={ResumeTokenInspector.ShortHash(oldest.ResumeToken)}",
                LogType.Info);

            int batchSeconds = _getBatchDurationInSeconds(1.0f);
            int maxAwaitSeconds = Math.Max(5, (int)(batchSeconds * 0.8));
            DateTime deadline = DateTime.UtcNow.AddSeconds(batchSeconds);

            if (!TryParseToken(oldest.ResumeToken, out BsonDocument oldestTokenDoc))
                return;

            var pipeline = BuildPipeline();
            var options = BuildOptions(oldestTokenDoc, maxAwaitSeconds);

            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? cursor = TryOpenCursor(pipeline, options, cancellationToken);
            if (cursor == null) return;

            try
            {
                var stats = await WalkClusterWatchAsync(cursor, working, oldest, deadline, cancellationToken);
                FastForwardLeftovers(cursor, working);

                _log.WriteLine(
                    $"{_syncBackPrefix}[PBRT Unblock] cluster-watch round done moveNext={stats.MoveNextCalls} withBatch={stats.MoveNextWithBatch} rawEvents={stats.RawEventsRead} matched={stats.MatchedEvents}",
                    LogType.Info);
            }
            finally
            {
                try { cursor.Dispose(); } catch { /* best-effort */ }
            }
        }

        // ---------- private modular helpers ----------

        private static UnblockEntry PickOldest(IEnumerable<UnblockEntry> entries)
        {
            return entries
                .OrderBy(e => e.PbrtClusterTimeUtc == DateTime.MinValue ? DateTime.MaxValue : e.PbrtClusterTimeUtc)
                .First();
        }

        private BsonDocument[] BuildPipeline()
        {
            // $project keeps only ns / _id (resume token) + the metadata the driver
            // needs to deserialize a ChangeStreamDocument. Namespace filtering is
            // performed client-side after MoveNextAsync.
            var stages = new List<BsonDocument>
            {
                new BsonDocument("$project", new BsonDocument
                {
                    { "operationType", 1 },
                    { "_id", 1 },
                    { "ns", 1 },
                    { "clusterTime", 1 }
                })
            };

            // When OptimizeForLargeDocs is enabled and the server supports it,
            // append $changeStreamSplitLargeEvent so a single >16 MB oplog event is
            // fragmented at the shard rather than killing the cursor with a
            // BSONObjectTooLarge. Must be the last stage in the pipeline.
            if (_shouldSplitLargeEvents())
            {
                stages.Add(new BsonDocument("$changeStreamSplitLargeEvent", new BsonDocument()));
            }

            return stages.ToArray();
        }

        private static ChangeStreamOptions BuildOptions(BsonDocument resumeAfter, int maxAwaitSeconds)
        {
            return new ChangeStreamOptions
            {
                BatchSize = 5000,
                ResumeAfter = resumeAfter,
                MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds)
            };
        }

        private bool TryParseToken(string tokenJson, out BsonDocument doc)
        {
            try
            {
                doc = BsonDocument.Parse(tokenJson);
                return true;
            }
            catch (Exception ex)
            {
                doc = new BsonDocument();
                _log.WriteLine($"{_syncBackPrefix}[PBRT Unblock] could not parse oldest resume token; skipping this round. Details: {ex.Message}", LogType.Warning);
                return false;
            }
        }

        private IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? TryOpenCursor(
            BsonDocument[] pipeline, ChangeStreamOptions options, CancellationToken cancellationToken)
        {
            try
            {
                return _changeStreamMongoClient.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options, cancellationToken);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}[PBRT Unblock] failed to open cluster-watch cursor; entries remain queued for retry next round. Details: {ex.Message}", LogType.Warning);
                return null;
            }
        }

        private struct WalkStats
        {
            public long MoveNextCalls;
            public long MoveNextWithBatch;
            public long RawEventsRead;
            public long MatchedEvents;
        }

        private async Task<WalkStats> WalkClusterWatchAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            IDictionary<string, UnblockEntry> working,
            UnblockEntry oldest,
            DateTime deadline,
            CancellationToken cancellationToken)
        {
            var stats = new WalkStats();
            string effectiveResumeToken = oldest.ResumeToken;
            DateTime effectiveTs = oldest.PbrtClusterTimeUtc;

            while (DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested && working.Count > 0)
            {
                CaptureEffectiveToken(cursor, ref effectiveResumeToken, ref effectiveTs);

                bool hasNext;
                try
                {
                    hasNext = await cursor.MoveNextAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}[PBRT Unblock] MoveNextAsync error after {stats.MoveNextCalls} calls; aborting cluster-watch. Details: {ex.Message}", LogType.Warning);
                    break;
                }
                stats.MoveNextCalls++;
                if (!hasNext) break;

                int batchCount = cursor.Current?.Count() ?? 0;
                if (batchCount > 0)
                {
                    stats.MoveNextWithBatch++;
                    stats.RawEventsRead += batchCount;
                }

                stats.MatchedEvents += ProcessBatchEvents(cursor, working, effectiveResumeToken, effectiveTs);
            }

            return stats;
        }

        // Captures the cursor's current PBRT into <paramref name="token"/>/<paramref name="ts"/>
        // BEFORE the next MoveNextAsync. Any event surfaced by that MoveNext has
        // clusterTime >= this PBRT, so resuming a collection-scoped watch from
        // <paramref name="token"/> guarantees the matched event will be re-emitted.
        private static void CaptureEffectiveToken(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            ref string token,
            ref DateTime ts)
        {
            try
            {
                var preToken = cursor.GetResumeToken();
                if (preToken == null) return;

                string preJson = preToken.ToJson();
                if (string.IsNullOrEmpty(preJson)) return;

                token = preJson;
                if (ResumeTokenInspector.TryDecodeUtc(preJson, out DateTime parsedTs))
                    ts = parsedTs;
            }
            catch
            {
                /* best-effort */
            }
        }

        private int ProcessBatchEvents(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            IDictionary<string, UnblockEntry> working,
            string preBatchToken,
            DateTime preBatchTs)
        {
            int matched = 0;

            // "Just before the current event" anchor. Starts at the pre-MoveNext
            // PBRT (== end of previous batch / cursor's ResumeAfter on iter 1).
            // After each event we advance the anchor to that event's own resume
            // token, which is exactly the "resume after this event" position --
            // i.e. the "just before the next event" position the MU should rewind
            // to in order to re-emit the next matching event without loss.
            string prevAnchorToken = preBatchToken;
            DateTime prevAnchorTs = preBatchTs;

            foreach (var change in cursor.Current ?? Enumerable.Empty<ChangeStreamDocument<BsonDocument>>())
            {
                if (change?.CollectionNamespace == null) continue;
                string ns = $"{change.CollectionNamespace.DatabaseNamespace.DatabaseName}.{change.CollectionNamespace.CollectionName}";

                if (working.TryGetValue(ns, out var entry))
                {
                    DateTime stampTs = prevAnchorTs != DateTime.MinValue ? prevAnchorTs : DateTime.UtcNow;
                    AdvanceMu(entry, stampTs, prevAnchorToken);
                    working.Remove(ns);
                    _entries.TryRemove(ns, out _);
                    matched++;

                    _log.WriteLine(
                        $"{_syncBackPrefix}[PBRT Unblock] unstuck {ns} via cluster-watch event op={change.OperationType} tokenHash={ResumeTokenInspector.ShortHash(prevAnchorToken)} ts={stampTs:o}; remaining={working.Count}",
                        LogType.Info);

                    if (working.Count == 0) break;
                }

                // Advance the anchor to this event's resume token regardless of
                // whether it matched, so a later matched event in the same batch
                // rewinds to the position immediately preceding it.
                try
                {
                    var rt = change.ResumeToken;
                    if (rt != null)
                    {
                        string rtJson = rt.ToJson();
                        if (!string.IsNullOrEmpty(rtJson))
                        {
                            prevAnchorToken = rtJson;
                            if (ResumeTokenInspector.TryDecodeUtc(rtJson, out DateTime curTs))
                                prevAnchorTs = curTs;
                        }
                    }
                }
                catch { /* best-effort */ }
            }
            return matched;
        }

        private void FastForwardLeftovers(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            IDictionary<string, UnblockEntry> working)
        {
            if (working.Count == 0) return;

            string pbrtJson = ReadCursorPbrtJson(cursor);
            if (string.IsNullOrEmpty(pbrtJson))
            {
                _log.WriteLine(
                    $"{_syncBackPrefix}[PBRT Unblock] cluster PBRT unavailable at end of batch; {working.Count} MU(s) left in queue for next-round retry.",
                    LogType.Warning);
                return;
            }

            ResumeTokenInspector.TryDecodeUtc(pbrtJson, out DateTime pbrtTs);
            DateTime stampTs = pbrtTs != DateTime.MinValue ? pbrtTs : DateTime.UtcNow;

            foreach (var kv in working.ToList())
            {
                AdvanceMu(kv.Value, stampTs, pbrtJson);
                working.Remove(kv.Key);
                _entries.TryRemove(kv.Key, out _);

                _log.WriteLine(
                    $"{_syncBackPrefix}[PBRT Unblock] fast-forwarded {kv.Key} to cluster PBRT tokenHash={ResumeTokenInspector.ShortHash(pbrtJson)} ts={stampTs:o} (no matching event observed)",
                    LogType.Warning);
            }
        }

        private static string ReadCursorPbrtJson(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor)
        {
            try
            {
                var pbrt = cursor.GetResumeToken();
                return pbrt?.ToJson() ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        private void AdvanceMu(UnblockEntry entry, DateTime ts, string tokenJson)
        {
            // Only advance if the new PBRT is strictly newer than the entry's own
            // PBRT at enqueue time. Without this guard, an idle cluster watch (PBRT
            // unchanged) would roll a newer MU's resume token backwards when its
            // entry got grouped with an older one.
            if (entry.PbrtClusterTimeUtc != DateTime.MinValue &&
                ts != DateTime.MinValue &&
                ts <= entry.PbrtClusterTimeUtc)
            {
                return;
            }

            SetResumeParameters(entry.Mu, ts, tokenJson, _syncBack);
            _trySaveMigrationUnit(entry.Mu, true);
        }

        private static string FormatTs(DateTime ts) => ts == DateTime.MinValue ? "?" : ts.ToString("o");
    }
}
#endif
