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
            public string ResumeToken { get; init; } = string.Empty;
            public DateTime AddedUtc { get; init; }
            public DateTime PbrtClusterTimeUtc { get; init; }
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

            // [Temp] Full token + embedded collection UUID at enqueue.
            _log.WriteLine(
                $"{_syncBackPrefix}[Temp][PBRT Unblock] ENQUEUE ns={collectionKey} muId={mu.Id} uuid={ExtractCollectionUuidHex(postBatchTokenJson!)} token={postBatchTokenJson}",
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
                $"{_syncBackPrefix}[PBRT Unblock] resolving {working.Count} stuck MU(s) via cluster-watch; oldestKey={oldest.DatabaseName}.{oldest.CollectionName} pbrtTs={FormatTs(oldest.PbrtClusterTimeUtc)} tokenHash={ResumeTokenInspector.ShortHash(oldest.ResumeToken)}",
                LogType.Info);

            // [Temp] Dump every entry in this round + the oldest token used as ResumeAfter.
            foreach (var kv in working)
            {
                _log.WriteLine(
                    $"{_syncBackPrefix}[Temp][PBRT Unblock] WORKING-ENTRY ns={kv.Key} muId={kv.Value.MuId} pbrtTs={FormatTs(kv.Value.PbrtClusterTimeUtc)} uuid={ExtractCollectionUuidHex(kv.Value.ResumeToken)} token={kv.Value.ResumeToken}",
                    LogType.Info);
            }
            _log.WriteLine(
                $"{_syncBackPrefix}[Temp][PBRT Unblock] OLDEST-RESUME-AFTER ns={oldest.DatabaseName}.{oldest.CollectionName} uuid={ExtractCollectionUuidHex(oldest.ResumeToken)} token={oldest.ResumeToken}",
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

                // [Temp] Post-MoveNextAsync snapshot of the cluster PBRT.
                string postMoveTokenJson = ReadCursorPbrtJson(cursor);
                ResumeTokenInspector.TryDecodeUtc(postMoveTokenJson, out DateTime postMoveTs);
                _log.WriteLine(
                    $"{_syncBackPrefix}[Temp][PBRT Unblock] POST-MOVENEXT moveNextIdx={stats.MoveNextCalls} batchCount={batchCount} postClusterPbrtTs={FormatTs(postMoveTs)} uuid={ExtractCollectionUuidHex(postMoveTokenJson)} token={postMoveTokenJson}",
                    LogType.Info);

                stats.MatchedEvents += ProcessBatchEvents(cursor, working, effectiveResumeToken, effectiveTs);
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

                // [Temp] Cluster-cursor PBRT captured just before MoveNextAsync.
                _log.WriteLine(
                    $"{_syncBackPrefix}[Temp][PBRT Unblock] PRE-MOVENEXT clusterPbrtTs={FormatTs(ts)} uuid={ExtractCollectionUuidHex(preJson)} token={preJson}",
                    LogType.Info);
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

                // [Temp] Every raw event surfaced by cluster-watch (matched or not).
                string rawEventTokenJson = SafeResumeTokenToJson(change.ResumeToken);
                ResumeTokenInspector.TryDecodeUtc(rawEventTokenJson, out DateTime rawEventTs);
                bool nsMatched = working.ContainsKey(ns);
                _log.WriteLine(
                    $"{_syncBackPrefix}[Temp][PBRT Unblock] RAW-EVENT ns={ns} op={change.OperationType} matched={nsMatched} eventTs={FormatTs(rawEventTs)} uuid={ExtractCollectionUuidHex(rawEventTokenJson)} eventToken={rawEventTokenJson}",
                    LogType.Info);

                if (working.TryGetValue(ns, out var entry))
                {
                    // [Temp] Anchor state at the moment we decide how to advance.
                    _log.WriteLine(
                        $"{_syncBackPrefix}[Temp][PBRT Unblock] MATCH-DECIDE-INPUT ns={ns} entryPbrtTs={FormatTs(entry.PbrtClusterTimeUtc)} entryUuid={ExtractCollectionUuidHex(entry.ResumeToken)} prevAnchorTs={FormatTs(prevAnchorTs)} prevAnchorUuid={ExtractCollectionUuidHex(prevAnchorToken)} prevAnchorToken={prevAnchorToken}",
                        LogType.Info);
                    // Default: rewind to "just before this event" (replay-safe).
                    string advanceToken = prevAnchorToken;
                    DateTime advanceTs = prevAnchorTs;
                    bool skippedEvent = false;
                    bool replayedEvent = false;

                    // If the rewind position equals (or is older than) the MU's
                    // own enqueued PBRT, the AdvanceMu guard would no-op and the
                    // MU would never move. In that case advance past the matched
                    // event using its own resume token. To avoid losing the event,
                    // attempt to replay it directly to the target first; only mark
                    // as "skipped" if replay was not possible or failed.
                    bool wouldBeNoOp =
                        entry.PbrtClusterTimeUtc != DateTime.MinValue &&
                        advanceTs != DateTime.MinValue &&
                        advanceTs <= entry.PbrtClusterTimeUtc;

                    // [STUCK-EMULATION-DISABLED] To re-enable forcing the replay/skip
                    // branch for collection_0001 (so the unblocker always treats its
                    // match as a no-op rewind candidate), add:
                    //
                    //   if (string.Equals(entry.CollectionName, "collection_0001", StringComparison.Ordinal))
                    //   {
                    //       wouldBeNoOp = true;
                    //   }

                    if (wouldBeNoOp)
                    {
                        try
                        {
                            var changeRt = change.ResumeToken;
                            if (changeRt != null)
                            {
                                string changeRtJson = changeRt.ToJson();
                                if (!string.IsNullOrEmpty(changeRtJson))
                                {
                                    advanceToken = changeRtJson;
                                    if (ResumeTokenInspector.TryDecodeUtc(changeRtJson, out DateTime ts2))
                                        advanceTs = ts2;
                                    skippedEvent = true;
                                }
                            }
                        }
                        catch { /* keep prev anchor */ }

                        if (skippedEvent && _tryReplayChange != null && change.DocumentKey != null)
                        {
                            var muForReplay = MigrationJobContext.GetMigrationUnit(entry.MuId, entry.JobId);
                            if (muForReplay != null)
                            {
                                try
                                {
                                    if (_tryReplayChange(muForReplay, change.DocumentKey, change.OperationType))
                                    {
                                        replayedEvent = true;
                                        skippedEvent = false;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _log.WriteLine(
                                        $"{_syncBackPrefix}[PBRT Unblock] replay attempt failed for {ns} op={change.OperationType}; falling back to skip. Details: {ex.Message}",
                                        LogType.Warning);
                                }
                            }
                        }
                    }

                    DateTime stampTs = advanceTs != DateTime.MinValue ? advanceTs : DateTime.UtcNow;

                    // [Temp] Final decision before persisting to MU.
                    _log.WriteLine(
                        $"{_syncBackPrefix}[Temp][PBRT Unblock] MATCH-DECIDE-OUTPUT ns={ns} wouldBeNoOp={wouldBeNoOp} skipped={skippedEvent} replayed={replayedEvent} advanceTs={FormatTs(stampTs)} advanceUuid={ExtractCollectionUuidHex(advanceToken)} advanceToken={advanceToken}",
                        LogType.Info);

                    AdvanceMu(entry, stampTs, advanceToken);
                    working.Remove(ns);
                    _entries.TryRemove(ns, out _);
                    matched++;

                    string docKeyJson = SafeToJson(change.DocumentKey);
                    if (replayedEvent)
                    {
                        _log.WriteLine(
                            $"{_syncBackPrefix}[PBRT Unblock] replayed 1 event for {ns} op={change.OperationType} documentKey={docKeyJson} tokenHash={ResumeTokenInspector.ShortHash(advanceToken)} ts={stampTs:o}; remaining={working.Count}",
                            LogType.Info);
                    }
                    else if (skippedEvent)
                    {
                        _log.WriteLine(
                            $"{_syncBackPrefix}[PBRT Unblock] WARNING skipped 1 event for {ns} (no safe rewind and replay unavailable/failed) op={change.OperationType} documentKey={docKeyJson} tokenHash={ResumeTokenInspector.ShortHash(advanceToken)} ts={stampTs:o}; remaining={working.Count}",
                            LogType.Warning);
                    }
                    else
                    {
                        _log.WriteLine(
                            $"{_syncBackPrefix}[PBRT Unblock] unstuck {ns} via cluster-watch event op={change.OperationType} documentKey={docKeyJson} tokenHash={ResumeTokenInspector.ShortHash(advanceToken)} ts={stampTs:o}; remaining={working.Count}",
                            LogType.Info);
                    }

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

        private static string SafeToJson(BsonDocument? doc)
        {
            if (doc == null) return "<none>";
            try { return doc.ToJson(); }
            catch { return "<unserializable>"; }
        }

        // [Temp] Best-effort serialization of an event's _id (resume token) bson.
        private static string SafeResumeTokenToJson(BsonDocument? rt)
        {
            if (rt == null) return "<none>";
            try { return rt.ToJson(); }
            catch { return "<unserializable>"; }
        }

        // [Temp] Extracts the embedded collection UUID hex (16 bytes / 32 hex chars)
        // from a v1 resume token's _data field. The UUID is encoded as BSON
        // binary (subtype 0x04), so we locate the 0x1004 marker and read the
        // next 32 hex chars. Returns "?" if not parseable.
        private static string ExtractCollectionUuidHex(string? tokenJson)
        {
            if (string.IsNullOrEmpty(tokenJson)) return "?";
            try
            {
                var doc = BsonDocument.Parse(tokenJson);
                if (!doc.Contains("_data")) return "?";
                string hex = doc["_data"].AsString;
                int idx = hex.IndexOf("1004", StringComparison.OrdinalIgnoreCase);
                if (idx < 0 || idx + 4 + 32 > hex.Length) return "?";
                return hex.Substring(idx + 4, 32).ToUpperInvariant();
            }
            catch
            {
                return "?";
            }
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

            // [Temp] Cluster PBRT used for fast-forwarding leftover MUs.
            _log.WriteLine(
                $"{_syncBackPrefix}[Temp][PBRT Unblock] FAST-FORWARD-PBRT count={working.Count} ts={FormatTs(stampTs)} uuid={ExtractCollectionUuidHex(pbrtJson)} token={pbrtJson}",
                LogType.Info);

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
            // Only reject true backward moves. Resume-token timestamps have 1s
            // resolution (the 4B BE seconds field after 0x82), so multiple ordered
            // events within the same second share the same ts; forward progress
            // inside that second is encoded in the ordinal that follows. Using <=
            // here would silently drop genuine same-second forward advances and
            // freeze the MU at the first event of any active second. Strict < still
            // blocks the original concern (an idle cluster watch with an older PBRT
            // rolling a newer MU's token backwards).
            if (entry.PbrtClusterTimeUtc != DateTime.MinValue &&
                ts != DateTime.MinValue &&
                ts < entry.PbrtClusterTimeUtc)
            {
                return;
            }

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

            // [Temp] Full pre/post tokens + UUIDs around the AdvanceMu save.
            _log.WriteLine(
                $"{_syncBackPrefix}[Temp][PBRT Unblock] AdvanceMu-DETAIL ns={entry.DatabaseName}.{entry.CollectionName} muId={entry.MuId} ts={FormatTs(ts)} saved={saved} preUuid={ExtractCollectionUuidHex(preTokenJson)} postSetUuid={ExtractCollectionUuidHex(postSetTokenJson)} postSaveUuid={ExtractCollectionUuidHex(postSaveTokenJson)} newUuid={ExtractCollectionUuidHex(tokenJson)} preToken={preTokenJson} newToken={tokenJson} postSaveToken={postSaveTokenJson}",
                LogType.Info);
        }

        private static string FormatTs(DateTime ts) => ts == DateTime.MinValue ? "?" : ts.ToString("o");
    }
}
#endif
