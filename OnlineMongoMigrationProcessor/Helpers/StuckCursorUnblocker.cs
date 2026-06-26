using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
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
            // Identity only -- never hold a MigrationUnit reference. Background tasks
            // that captured the old instance would otherwise have their writes silently
            // rejected by MigrationJobContext.SaveMigrationUnit (stale-instance guard)
            // when the cache rebuilds the MU (e.g. on add/remove). At advance time we
            // re-fetch the canonical instance via MigrationJobContext.GetMigrationUnit.
            public string MuId { get; init; } = string.Empty;
            public string JobId { get; init; } = string.Empty;
            public string DatabaseName { get; init; } = string.Empty;
            public string CollectionName { get; init; } = string.Empty;
            // ResumeToken / PbrtClusterTimeUtc are mutated across successive matched
            // events in a single cluster-watch round so each AdvanceMu call (and the
            // diagnostic logs) reflect the latest applied position.
            public string ResumeToken { get; set; } = string.Empty;
            public DateTime AddedUtc { get; init; }
            public DateTime PbrtClusterTimeUtc { get; set; }
        }

        private readonly Log _log;
        private readonly MongoClient _changeStreamMongoClient;
        private readonly bool _syncBack;
        private readonly string _syncBackPrefix;
        private readonly Func<float, int> _getBatchDurationInSeconds;
        private readonly Func<bool> _shouldSplitLargeEvents;
        // Optional: replay a single change directly to the target by documentKey,
        // returning true on success. Used to avoid losing the matched event when no
        // safe rewind position exists. Requires source/target client plumbing only
        // available on the owning processor, hence the callback.
        private readonly Func<MigrationUnit, BsonDocument, ChangeStreamOperationType, bool>? _tryReplayChange;
        private readonly ConcurrentDictionary<string, UnblockEntry> _entries =
            new ConcurrentDictionary<string, UnblockEntry>(StringComparer.Ordinal);

        public StuckCursorUnblocker(
            Log log,
            MongoClient changeStreamMongoClient,
            bool syncBack,
            string syncBackPrefix,
            Func<float, int> getBatchDurationInSeconds,
            Func<bool> shouldSplitLargeEvents,
            Func<MigrationUnit, BsonDocument, ChangeStreamOperationType, bool>? tryReplayChange = null)
        {
            _log = log;
            _changeStreamMongoClient = changeStreamMongoClient;
            _syncBack = syncBack;
            _syncBackPrefix = syncBackPrefix ?? string.Empty;
            _getBatchDurationInSeconds = getBatchDurationInSeconds;
            _shouldSplitLargeEvents = shouldSplitLargeEvents ?? (() => false);
            _tryReplayChange = tryReplayChange;
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
                MuId = mu.Id ?? string.Empty,
                JobId = mu.JobId ?? string.Empty,
                DatabaseName = mu.DatabaseName ?? string.Empty,
                CollectionName = mu.CollectionName ?? string.Empty,
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

            // Refresh each entry from the canonical MU before opening cluster-watch.
            // Between Enqueue and ResolveAsync the per-collection flush path may have
            // advanced the MU's resume token; using the enqueue-time snapshot as the
            // cluster-watch ResumeAfter would re-emit events the MU already processed
            // and risk a same-second backward advance.
            RefreshFromCanonical(working);
            if (working.Count == 0) return;

            UnblockEntry oldest = PickOldest(working.Values);

            _log.WriteLine(
                $"{_syncBackPrefix}[PBRT Unblock] resolving {working.Count} stuck MU(s) via cluster-watch; oldestKey={oldest.DatabaseName}.{oldest.CollectionName} pbrtTs={FormatTs(oldest.PbrtClusterTimeUtc)} tokenHash={ResumeTokenInspector.ShortHash(oldest.ResumeToken)}",
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
                var matchedNamespaces = new HashSet<string>(StringComparer.Ordinal);
                var stats = await WalkClusterWatchAsync(cursor, working, oldest, deadline, matchedNamespaces, cancellationToken);

                // Namespaces we replayed events for are handed back to their per-collection
                // cursors at the advanced resume position. Do NOT fast-forward them to the
                // cluster PBRT -- that would skip any events between the last replayed one
                // and the cluster tail.
                foreach (var ns in matchedNamespaces)
                {
                    working.Remove(ns);
                    _entries.TryRemove(ns, out _);
                }

                FastForwardLeftovers(cursor, working);

                _log.WriteLine(
                    $"{_syncBackPrefix}[PBRT Unblock] cluster-watch round done moveNext={stats.MoveNextCalls} withBatch={stats.MoveNextWithBatch} rawEvents={stats.RawEventsRead} matched={stats.MatchedEvents} matchedNamespaces={matchedNamespaces.Count}",
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
            // $project keeps only ns / _id (resume token) + documentKey (small,
            // used for skip-event audit logging) + the metadata the driver needs
            // to deserialize a ChangeStreamDocument. Namespace filtering is
            // performed client-side after MoveNextAsync.
            var stages = new List<BsonDocument>
            {
                new BsonDocument("$project", new BsonDocument
                {
                    { "operationType", 1 },
                    { "_id", 1 },
                    { "ns", 1 },
                    { "documentKey", 1 },
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
            HashSet<string> matchedNamespaces,
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

                stats.MatchedEvents += ProcessBatchEvents(cursor, working, effectiveResumeToken, effectiveTs, matchedNamespaces);
            }

            return stats;
        }

        // Captures the cursor's current PBRT into <paramref name="token"/>/<paramref name="ts"/>
        // BEFORE the next MoveNextAsync. Any event surfaced by that MoveNext has
        // clusterTime >= this PBRT, so resuming a collection-scoped watch from
        // <paramref name="token"/> guarantees the matched event will be re-emitted.
        private void CaptureEffectiveToken(
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
            DateTime preBatchTs,
            HashSet<string> matchedNamespaces)
        {
            int matched = 0;

            foreach (var change in cursor.Current ?? Enumerable.Empty<ChangeStreamDocument<BsonDocument>>())
            {
                if (change?.CollectionNamespace == null) continue;
                string ns = $"{change.CollectionNamespace.DatabaseNamespace.DatabaseName}.{change.CollectionNamespace.CollectionName}";

                string rawEventTokenJson = SafeResumeTokenToJson(change.ResumeToken);
                ResumeTokenInspector.TryDecodeUtc(rawEventTokenJson, out DateTime rawEventTs);

                if (!working.TryGetValue(ns, out var entry)) continue;
                if (change.ResumeToken == null || string.IsNullOrEmpty(rawEventTokenJson)) continue;

                // Defense in depth: only process events strictly newer than the entry's
                // current position. RefreshFromCanonical at Resolve start, plus in-batch
                // mutations from earlier matches, can leave the entry ahead of where
                // cluster-watch resumed. Re-replaying an older event would be wasted work
                // at best and AdvanceMu's backward-rollback guard would reject it anyway.
                if (ResumeTokenInspector.TryGetClusterPositionKey(rawEventTokenJson, out string eventKey) &&
                    ResumeTokenInspector.TryGetClusterPositionKey(entry.ResumeToken, out string entryKey) &&
                    string.CompareOrdinal(eventKey, entryKey) <= 0)
                {
                    continue;
                }

                // Replay the change to target. With per-event replay the MU advances past
                // every matched event, so the per-collection cursor resumes strictly after
                // the last replayed event and no change is lost.
                bool replayed = false;
                if (_tryReplayChange != null && change.DocumentKey != null)
                {
                    var muForReplay = MigrationJobContext.GetMigrationUnit(entry.MuId, entry.JobId);
                    if (muForReplay != null)
                    {
                        try
                        {
                            replayed = _tryReplayChange(muForReplay, change.DocumentKey, change.OperationType);
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine(
                                $"{_syncBackPrefix}[PBRT Unblock] replay attempt failed for {ns} op={change.OperationType}; advancing past event without re-applying. Details: {ex.Message}",
                                LogType.Warning);
                        }
                    }
                }

                DateTime stampTs = rawEventTs != DateTime.MinValue ? rawEventTs : DateTime.UtcNow;

                AdvanceMu(entry, stampTs, rawEventTokenJson);

                // Mutate the in-memory entry so the next event for this ns in this batch
                // compares against the new position and AdvanceMu's backward-rollback
                // guard sees forward-only motion.
                entry.ResumeToken = rawEventTokenJson;
                entry.PbrtClusterTimeUtc = stampTs;

                matchedNamespaces.Add(ns);
                matched++;

                string docKeyJson = SafeToJson(change.DocumentKey);
                if (replayed)
                {
                    _log.WriteLine(
                        $"{_syncBackPrefix}[PBRT Unblock] replayed 1 event for {ns} op={change.OperationType} documentKey={docKeyJson} tokenHash={ResumeTokenInspector.ShortHash(rawEventTokenJson)} ts={stampTs:o}",
                        LogType.Info);
                }
                else
                {
                    _log.WriteLine(
                        $"{_syncBackPrefix}[PBRT Unblock] WARNING advanced past 1 event WITHOUT replay for {ns} (replay unavailable/failed) op={change.OperationType} documentKey={docKeyJson} tokenHash={ResumeTokenInspector.ShortHash(rawEventTokenJson)} ts={stampTs:o}",
                        LogType.Warning);
                }
            }
            return matched;
        }

        private static string SafeToJson(BsonDocument? doc)
        {
            if (doc == null) return "<none>";
            try { return doc.ToJson(); }
            catch { return "<unserializable>"; }
        }

        private static string SafeResumeTokenToJson(BsonDocument? rt)
        {
            if (rt == null) return "<none>";
            try { return rt.ToJson(); }
            catch { return "<unserializable>"; }
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

        // Pulls each working entry's ResumeToken / PbrtClusterTimeUtc forward to the
        // canonical MU's current state. Drops the entry if the canonical MU is gone.
        // Called once at the top of ResolveAsync so cluster-watch resumes from the
        // newest known position rather than a stale enqueue-time snapshot.
        private void RefreshFromCanonical(IDictionary<string, UnblockEntry> working)
        {
            foreach (var key in working.Keys.ToList())
            {
                var stale = working[key];
                var mu = MigrationJobContext.GetMigrationUnit(stale.MuId, stale.JobId);
                if (mu == null)
                {
                    working.Remove(key);
                    _entries.TryRemove(key, out _);
                    _log.WriteLine(
                        $"{_syncBackPrefix}[PBRT Unblock] dropping {key}; canonical MU not found (muId={stale.MuId} jobId={stale.JobId})",
                        LogType.Warning);
                    continue;
                }

                string canonicalToken = mu.GetResumeToken(_syncBack) ?? string.Empty;
                if (string.IsNullOrEmpty(canonicalToken)) continue;

                bool canonicalIsNewer;
                if (ResumeTokenInspector.TryGetClusterPositionKey(canonicalToken, out string canonKey) &&
                    ResumeTokenInspector.TryGetClusterPositionKey(stale.ResumeToken, out string staleKey))
                {
                    canonicalIsNewer = string.CompareOrdinal(canonKey, staleKey) > 0;
                }
                else
                {
                    ResumeTokenInspector.TryDecodeUtc(canonicalToken, out DateTime canonTsOnly);
                    canonicalIsNewer = canonTsOnly != DateTime.MinValue && canonTsOnly > stale.PbrtClusterTimeUtc;
                }

                if (!canonicalIsNewer) continue;

                ResumeTokenInspector.TryDecodeUtc(canonicalToken, out DateTime canonTs);
                working[key] = new UnblockEntry
                {
                    MuId = stale.MuId,
                    JobId = stale.JobId,
                    DatabaseName = stale.DatabaseName,
                    CollectionName = stale.CollectionName,
                    ResumeToken = canonicalToken,
                    AddedUtc = stale.AddedUtc,
                    PbrtClusterTimeUtc = canonTs,
                };
                _log.WriteLine(
                    $"{_syncBackPrefix}[PBRT Unblock] refreshed {key} from canonical MU; staleHash={ResumeTokenInspector.ShortHash(stale.ResumeToken)} canonicalHash={ResumeTokenInspector.ShortHash(canonicalToken)}",
                    LogType.Info);
            }
        }

        private void AdvanceMu(UnblockEntry entry, DateTime ts, string tokenJson)
        {
            // Always resolve the canonical MU from the cache. Holding a stored MU
            // reference is unsafe: if the cache rebuilds (collection removed/re-added),
            // SaveMigrationUnit silently rejects writes from non-canonical instances.
            MigrationUnit mu = MigrationJobContext.GetMigrationUnit(entry.MuId, entry.JobId);
            if (mu == null)
            {
                _log.WriteLine(
                    $"{_syncBackPrefix}[PBRT Unblock] could not resolve canonical MU {entry.DatabaseName}.{entry.CollectionName} (muId={entry.MuId} jobId={entry.JobId}); skipping advance.",
                    LogType.Warning);
                return;
            }

            string preTokenJson = mu.GetResumeToken(_syncBack) ?? string.Empty;

            // Backward-rollback protection: compare ts+ordinal (the 16 hex chars after
            // the 0x82 type byte) of the candidate token against the canonical MU's
            // current token. v1 tokens are designed so this prefix sorts lex-equal to
            // oplog order, so this guard catches same-second backward-ordinal moves
            // that the ts-only guard would silently allow. Fall back to ts comparison
            // if either side fails to parse (rare; mainly defensive).
            if (ResumeTokenInspector.TryGetClusterPositionKey(tokenJson, out string candidateKey) &&
                ResumeTokenInspector.TryGetClusterPositionKey(preTokenJson, out string currentKey))
            {
                if (string.CompareOrdinal(candidateKey, currentKey) < 0)
                {
                    _log.WriteLine(
                        $"{_syncBackPrefix}[PBRT Unblock] AdvanceMu REJECTED backward move for {entry.DatabaseName}.{entry.CollectionName} muId={entry.MuId} candidateKey={candidateKey} currentKey={currentKey}",
                        LogType.Warning);
                    return;
                }
            }
            else if (entry.PbrtClusterTimeUtc != DateTime.MinValue &&
                     ts != DateTime.MinValue &&
                     ts < entry.PbrtClusterTimeUtc)
            {
                return;
            }

            string preTokenHash = ResumeTokenInspector.ShortHash(preTokenJson);
            SetResumeParameters(mu, ts, tokenJson, _syncBack);
            string postSetTokenJson = mu.GetResumeToken(_syncBack) ?? string.Empty;
            string postSetTokenHash = ResumeTokenInspector.ShortHash(postSetTokenJson);
            bool saved = MigrationJobContext.SaveMigrationUnit(mu, true);
            string postSaveTokenJson = mu.GetResumeToken(_syncBack) ?? string.Empty;
            string postSaveTokenHash = ResumeTokenInspector.ShortHash(postSaveTokenJson);

            _log.WriteLine(
                $"{_syncBackPrefix}[PBRT Unblock] AdvanceMu {entry.DatabaseName}.{entry.CollectionName} muId={entry.MuId} jobId={entry.JobId} preHash={preTokenHash} postSetHash={postSetTokenHash} postSaveHash={postSaveTokenHash} saved={saved}",
                LogType.Info);
        }

        private static string FormatTs(DateTime ts) => ts == DateTime.MinValue ? "?" : ts.ToString("o");
    }
}
#endif
